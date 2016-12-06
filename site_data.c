#include <stdio.h>
#include <stdlib.h>

#include "site_data.h"

int site_available[MAX_SITES];
int readVarValues(int site_num,int var_num,int ts);
int readOnlyVarValues(int site_num,int var_num,int trx_ts, int opn_ts, int trx_id);
void failSite(int siteNum);
void release_lock(int site_num, int trx_id);
int isReadAvailable(int site_num,int var_num,int trx_id);
void updateVersionTable(int site_num, int var_num, struct trx_opn *opr);
void addOpnToActvList(int siteNum, int var_num, struct trx_opn *opr, int status);
void addOpnToBlckdList(int siteNum,int var_num,struct trx_opn *opr);
int isLockRequired(int siteNum,int var_num,int trx_id,int lock);
int checkConflictAndDeadlock(int site_No,int var_num,int trx_id,int ts,int opn_type);

/**************** initSiteData Function *************************
 * Initialize all site related variables at their respective sites 
 ***************************************************************/
void initSiteData()
{
  int i,j,site_num; 

  for(site_num = 1; site_num < MAX_SITES; site_num++)
  {
    site_available[site_num] = 1;
  }

  for(i = 1; i <= (MAX_SITES-1); i++)
  {
    for(j = 1; j < (MAX_VARS-1); j++)
    {
      if(j%2==0)
      {
        struct version *temp = (struct version *) malloc (sizeof(struct version));
        sites_info[i].var[j].ver_flag = 1;
        temp->trx_id = -1;
        temp->value = 10*j;
 	temp->read_ts = 0;
	temp->write_ts = 0;
	temp->next = NULL;
	sites_info[i].var[j].head = temp;

        sites_info[i].lock_data[j].var_exists = 1;
        sites_info[i].lock_data[j].var_read_available = 1;
      }
      else
      {
        if(i==((j%10)+1))
        {
          struct version *temp = (struct version *) malloc (sizeof(struct version));
          sites_info[i].var[j].ver_flag = 1;
          temp->trx_id = -1;
          temp->value = 10*j;
          temp->read_ts = 0;
          temp->write_ts = 0;
          temp->next = NULL;
          sites_info[i].var[j].head = temp;

          sites_info[i].lock_data[j].var_exists = 1;
          sites_info[i].lock_data[j].var_read_available = 1;
        }
      }
    }
  }
}

/*************** perfOpn Function ************************************
 * Performs the operation in the transaction queue for each transaction
 * Checks if transactions require locks
 * Handles deadlock
 * ******************************************************************/
void perfOpn(struct trx_opn *opr, int siteNum)
{
  char log_desc[1000];
  int i, dump_val;
  if(site_available[siteNum] == 0 && opr->opn_type != RECOVER)
  {
    opr->site_opn_sts[siteNum] = OPERATION_REJECTED;
    return;
  }
  
  if(opr->trx_type == OTHER_TRX)
  {
    if(opr->opn_type == DUMP)
    {
      if(opr->var_num==ALL_VARS)
      {
        sprintf(log_desc,"\nVariables at Site:%d\n",siteNum); 
        appendLog(log_desc);
        for(i = 1; i < MAX_VARS; i++)
        {
          if(sites_info[siteNum].var[i].ver_flag == 1) 
          {
            dump_val = readVarValues(siteNum,i,opr->trx_ts);
            sprintf(log_desc," x%d: %d\n",i,dump_val);
            appendLog(log_desc);
          }
        }
        printf("\n");
      }   
      else
      {
        dump_val = readVarValues(siteNum,opr->var_num,opr->trx_ts);
        printf("Value read for dump operation is:\n",dump_val);
        sprintf(log_desc,"Site:%d x%d: %d\n",siteNum,opr->var_num,dump_val);
        appendLog(log_desc);
      } 
    
      opr->site_opn_sts[siteNum]= OPERATION_COMPLETE;
      return;
    }
   
    if(opr->opn_type == FAIL)
    {
      site_available[siteNum] = 0;
      failSite(siteNum);
      opr->site_opn_sts[siteNum]=OPERATION_COMPLETE;
      return;
    }
 
    if(opr->opn_type == RECOVER)
    {
      site_available[siteNum] = 1;
      for(int i = 0; i < MAX_VARS; i++)
      {
        if(sites_info[siteNum].var[i].ver_flag == 1)
        {
          if(i%2 == 0)
          {
            sites_info[siteNum].lock_data[i].var_read_available = 0;
          }
          else
          {
            sites_info[siteNum].lock_data[i].var_read_available = 1;
          }
        } 
      }    
      opr->site_opn_sts[siteNum] = OPERATION_COMPLETE;
      sprintf(log_desc,"\nSite %d Recovered:\n",siteNum);
      appendLog(log_desc);
      return;
    }
  }

  if(opr->trx_type == RO_TRX)
  {
    if(opr->opn_type == READ)
    {
      if(isReadAvailable(siteNum,opr->var_num,opr->trx_id) == 0)
      {
        opr->site_opn_sts[siteNum] = OPERATION_REJECTED;
        return; 
      }
      else
      {
        opr->read_val = readOnlyVarValues(siteNum,opr->var_num,opr->trx_ts,opr->opn_ts,opr->trx_id);
        opr->site_opn_sts[siteNum] = OPERATION_COMPLETE;
        return;
      }
    }
  }
  
  if(opr->trx_type == RW_TRX)
  {
    if(opr->opn_type == READ)
    {
      if(isReadAvailable(siteNum,opr->var_num,opr->trx_id) == 0)
      {
        opr->site_opn_sts[siteNum] = OPERATION_REJECTED;
        return;
      }
    } 

    if(opr->opn_type == READ || opr->opn_type == WRITE)
    {
      if(isLockRequired(siteNum,opr->var_num,opr->trx_id,opr->opn_type) == 1)
      {
        updateVersionTable(siteNum,opr->var_num,opr);
        opr->site_opn_sts[siteNum] = OPERATION_COMPLETE; /* Lock has been granted */
        return;
      }
     
      int status = checkConflictAndDeadlock(siteNum,opr->var_num,opr->trx_id,opr->trx_ts,opr->opn_type);
      if((status == 0) || (status == 2))
      {
        addOpnToActvList(siteNum,opr->var_num,opr,status);
        updateVersionTable(siteNum,opr->var_num,opr);
        opr->site_opn_sts[siteNum] = OPERATION_COMPLETE;
        return;
      }
      else if(status == 1)
      {
        addOpnToBlckdList(siteNum,opr->var_num,opr);
        opr->site_opn_sts[siteNum] = OPERATION_BLOCKED;
        return;
      }
      else
      {
        opr->site_opn_sts[siteNum] = OPERATION_REJECTED;
        return;
      }
    }
   
    if(opr->opn_type == END)
    {
      updateVersionTable(siteNum,-1,opr);
      release_lock(siteNum,opr->trx_id);
      opr->site_opn_sts[siteNum] = OPERATION_COMPLETE;
      return;
    }
    if(opr->opn_type = ABORT)
    {
      release_lock(siteNum, opr->trx_id);
      opr->site_opn_sts[siteNum] = OPERATION_COMPLETE;
      return;
    }
  }
}
/**************** release_lock Function ****************************************
 * Function is called whenever a transaction wants to commit or abort or it fails
 * Scan the active list of operations for a transaction and perform the operation
 * Scan the blocked list and set the operations to null
 * ****************************************************************************/
void release_lock(int site_num, int trx_id)
{
  int i;
  for(i = 1; i <= MAX_VARS; i++)
  {
    if(sites_info[site_num].lock_data[i].var_exists == 1) /* Check if variable is present at the site */
    {
      struct trx_opn *actv_list = sites_info[site_num].lock_data[i].first_actv_opn;
      /* Scanning the active list of operations for the transaction and performing the corres operation */
      if(actv_list != NULL)
      {
        if(actv_list->trx_id == trx_id)
        {
          sites_info[site_num].lock_data[i].first_actv_opn = NULL;
          if(sites_info[site_num].lock_data[i].first_blckd_opn != NULL)
          {
            sites_info[site_num].lock_data[i].first_actv_opn = sites_info[site_num].lock_data[i].first_blckd_opn;
            sites_info[site_num].lock_data[i].first_blckd_opn = sites_info[site_num].lock_data[i].first_blckd_opn->next_opn_site;
            perfOpn(sites_info[site_num].lock_data[i].first_actv_opn, site_num);
          }
        }
      }
      
      struct trx_opn *curr_blck_list = sites_info[site_num].lock_data[i].first_blckd_opn;
      if(curr_blck_list != NULL)
      {
        if(curr_blck_list->next_opn_site == NULL)
        {
          if(curr_blck_list->trx_id == trx_id) 
            sites_info[site_num].lock_data[i].first_blckd_opn = NULL; /* Set blocked operation as null for the transaction */
        }
        else
        {
          struct trx_opn *prev_opn = curr_blck_list;
          while(curr_blck_list != NULL)
          {
            if(curr_blck_list->trx_id == trx_id)
            {
              curr_blck_list = curr_blck_list->next_opn_site;
              prev_opn->next_opn_site = curr_blck_list;
              prev_opn = curr_blck_list;
            }
            else
            {
              prev_opn = curr_blck_list;
              curr_blck_list = curr_blck_list->next_opn_site;
            }
          }
        }
      }
    }
  }
}

/*********************** updateVersionTable Function *******************
 * Maintains the version table at each site for each variable
 * updates the read and write timestamps for a variable at a particular site 
 *************************************************************************/
void updateVersionTable(int site_num, int var_num, struct trx_opn *opr)
{
  int i = 0;
  if(var_num > 0)
  {
    if(sites_info[site_num].var[var_num].ver_flag == 1)
    {
      if(opr->opn_type == WRITE)
      {
        struct version *curr_var = sites_info[site_num].var[var_num].head;
        int trx_present = 0;
        while(curr_var != NULL)
        {
          if((opr->trx_id == curr_var->trx_id) && (curr_var->write_ts == MAX_TRX_TS))
          {
            curr_var->value = opr->write_val;
            trx_present = 1;
          }
          curr_var = curr_var->next;
        }
        if(trx_present == 0)
        {
          struct version *new_node = (struct version *) malloc (sizeof(struct version));
          new_node->trx_id = opr->trx_id;
          new_node->value = opr->write_val;
          new_node->read_ts = MAX_TRX_TS;
          new_node->write_ts = MAX_TRX_TS;
          new_node->next = sites_info[site_num].var[var_num].head;
          sites_info[site_num].var[var_num].head = new_node;
        }
      }
      if(opr->opn_type == READ)
      {
        struct version *curr_var = sites_info[site_num].var[var_num].head;
        int trx_present = 0;
        while(curr_var != NULL)
        {
          if((opr->trx_id == curr_var->trx_id) && (curr_var->write_ts == MAX_TRX_TS))
          {
            opr->read_val = curr_var->value;
            trx_present = 1;
          }
          curr_var = curr_var->next;
        }
        if(trx_present==0)
        {
          opr->read_val = readVarValues(site_num,var_num,opr->trx_ts);
        }
      }
    }
  }
  if(opr->opn_type == END)
  {
    int trx_id = 0;
    for(int i = 1;i <= MAX_VARS; i++)
    {
      if(sites_info[site_num].var[i].ver_flag == 1)
      {
        struct version *curr = sites_info[site_num].var[i].head;
        if(curr!=NULL)
        {
          while(curr!=NULL)
          {
            if((opr->trx_id == curr->trx_id)&&(curr->write_ts == MAX_TRX_TS) && opr->trx_type != RO_TRX)
            {
              curr->write_ts = opr->trx_ts;
              curr->read_ts = opr->trx_ts;
              if(isReadAvailable(site_num,i,opr->trx_id)==0)  /* If the variable is unavailable make it available */
              {
                sites_info[site_num].lock_data[i].var_read_available = 1;
              }
            }
            curr=curr->next;
          }
        }
      }
    }
  }   
}
/********************** readVarValues Function ****************
 * Reads the value of the variable from the requested site
 * Return the most recent committed value of a variable
 * **********************************************************/
int readVarValues(int site_num,int var_num,int ts)
{
  int read_value = 0;
  int latest = -1;
  if(sites_info[site_num].var[var_num].ver_flag == 1)
  {
    struct version *current_trx = sites_info[site_num].var[var_num].head;

    while(current_trx != NULL)
    {
      if(current_trx->write_ts <= ts && current_trx->write_ts > latest) /* Only allow value to be read if there has been a write on that variable */
      {
        latest = current_trx->write_ts;
        read_value = current_trx->value;
      }
      current_trx = current_trx->next;
    }
  }
  return read_value;
}

/********************** readVarValues Function ****************
 * Function is only called for a Read Only Transaction
 * Reads the value of the variable from the requested site
 * Return the most recent committed value of a variable
 * **********************************************************/
int readOnlyVarValues(int site_num,int var_num,int trx_ts, int opn_ts, int trx_id)
{
  int read_value = 0;
  int latest = -1;
 
  if(sites_info[site_num].var[var_num].ver_flag == 1)
  {
    struct version *current_trx = sites_info[site_num].var[var_num].head;

    while(current_trx != NULL)
    {
      if((current_trx->write_ts == MAX_TRX_TS && current_trx->trx_id > trx_id) || current_trx->trx_id != -1)
      {
        current_trx = current_trx->next;
      }
      else
      {
        latest = current_trx->write_ts;
        read_value = current_trx->value;
        break;
      }
    }
  }
  return read_value;
}

/************** failSite function ********************************************
 * Fails the site by releasing the locks on each variable from the failed site
 * ***************************************************************************/
void failSite(int siteNum)
{
  for(int i = 1; i < MAX_VARS; i++)
  {
    if(sites_info[siteNum].lock_data[i].var_exists == 1)
    {
      sites_info[siteNum].lock_data[i].first_actv_opn = NULL;
      sites_info[siteNum].lock_data[i].first_blckd_opn = NULL;
    }
  }
}

/**************** isReadAvailable Function *******************
 * Checks if a variable at the input site can be read
 * returns 1 if variable can be read, else returns 0
 * *********************************************************/
int isReadAvailable(int site_num,int var_num,int trx_id)
{
  struct trx_opn *temp_opn = sites_info[site_num].lock_data[var_num].first_actv_opn;
  if(temp_opn != NULL)
  {
    if(temp_opn->trx_id == trx_id && temp_opn->opn_type == WRITE)
      return 1;
  }
  return sites_info[site_num].lock_data[var_num].var_read_available;
}

/************************* addOpnToActvList Function ***************************************
 * Adds an operation for the transaction to the active list of operations to be executed at the site
 * Adds the operation at the end of the queue and sets the next operation site to NULL
 * *********************************************************************************************/
void addOpnToActvList(int siteNum, int var_num, struct trx_opn *opr, int status)
{
  if(status == 0)
  {
    sites_info[siteNum].lock_data[var_num].first_actv_opn = opr;
    opr->next_opn_site = NULL;
  }
  else
  {
    struct trx_opn *actv_list = sites_info[siteNum].lock_data[var_num].first_actv_opn;

    while(actv_list->next_opn_site != NULL)
    {
      actv_list = actv_list->next_opn_site;
    }

    opr->next_opn_site = opr;
    opr->next_opn_site = NULL;
  }
}

/************************* addOpnToActvList Function ***************************************
 * Adds an operation for the transaction to the list of operations blocked at the site
 * Adds the operation at the end of the queue and sets the next operation site to NULL
 * *********************************************************************************************/
void addOpnToBlckdList(int siteNum,int var_num,struct trx_opn *opr) 
{

  if(sites_info[siteNum].lock_data[var_num].first_blckd_opn == NULL) 
  {
    sites_info[siteNum].lock_data[var_num].first_blckd_opn = opr;
    opr->next_opn_site = NULL;
  }
  else
  {
    struct trx_opn *blck_list = sites_info[siteNum].lock_data[var_num].first_blckd_opn;
    while(blck_list->next_opn_site != NULL)
      blck_list = blck_list->next_opn_site;

    blck_list->next_opn_site = opr;
    opr->next_opn_site = NULL;
  }
}

/************************* isLockRequired Function ****************************************
 * Checks if a transaction requires a lock on a particular varibale requested at the input site
 * Returns 1 if lock is granted, else 0 is returned
 * ***************************************************************************************/
int isLockRequired(int siteNum,int var_num,int trx_id,int lock)
{
  struct trx_opn *first_actv_opn = sites_info[siteNum].lock_data[var_num].first_actv_opn;
  if(first_actv_opn != NULL )
  {
    if(first_actv_opn->trx_id != trx_id)
      return 0;
  
    else
    {
      if(first_actv_opn->opn_type >= lock) /* transaction has the same lock or a higher lock */
        return 1;
    
      else
        return 0;
    }
  }
}

int checkConflictAndDeadlock(int site_No,int var_num,int trx_id,int ts,int opn_type)
{
  int conflict_ind = 0;
  struct trx_opn *first_actv_opn = sites_info[site_No].lock_data[var_num].first_actv_opn;
 
  if(first_actv_opn == NULL)
    return 0;

  else
  {
    if(first_actv_opn->next_opn_site == NULL)
    {
      if(first_actv_opn->trx_id == trx_id)
        return 0;
    }
    if((first_actv_opn->opn_type == opn_type) && (opn_type == READ))
    {
      return 2;
    }
    else
    {
      while(first_actv_opn != NULL)
      {
       /* if(ts > first_actv_opn->trx_ts) 
        {
          printf("\nYounger Transaction is requesting a lock on a resource held by an older transaction");
          conflict_ind = 1; 
          return -1;
        } */
        if(first_actv_opn->next_opn_site == NULL)
          break;
        first_actv_opn = first_actv_opn->next_opn_site;
      }

      if(conflict_ind == 0)
      {
        printf("\n Transaction %d is blocked as Transaction %d has a lock on the variable x%d requested at site %d", trx_id, first_actv_opn->trx_id, var_num,site_No);
        return 1;
      }
    }
  }
}
