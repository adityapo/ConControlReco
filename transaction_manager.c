#include "transaction_manager.h"
#include "operations.h"
#include "site_data.h"
#include <ctype.h>
#include <sys/select.h>

#define SITE_FAILED_WAIT

#define WRITE_PENDING 1
#define IS_WRITE_FINISHED 0
#define WRITE_FAILED -1

#define SLEEP_DURATION 200

void abortTrx(struct trx_opn *opn);
void Sleep_ms(int time_ms);

/***************parseInputFile function:***************
 * parses the file received in input
 * stores the individual operations like begin, end, read, write etc into the transaction queue 
 * assigns timestamp to each operation
 * *************************************************************/
int parseInputFile(char *inputfile) 
{
  int status = 0;
  int time_stamp = 0 ;
  int i;
  int trx_id;
  FILE *fp;

  T[OTHER_TRX_ID].trxType = OTHER_TRX ;
  /* Open the file in read mode to read the input operations and store them in order of their requests */
  fp = fopen(inputfile, "r") ;
  if(fp == NULL) {
    printf("parseInputFile: failed to open file") ;
    return -1 ;
  }
  printf("file opened successfully\n");
  while(!feof(fp)) 
  {
    char opn_data[100]; 
    char *temp_opn;
    char *temp_tok;
    char *temp_tok1;
    char temp_token[100];
    const char s[2] = ";";
    memset(opn_data,0,100) ;
    memset(temp_token,0,100) ;
    fscanf(fp, "%s", opn_data) ;  
    if(opn_data[0] == '/' || opn_data[0] == '#') 
    {
      fgets(opn_data,100,fp);
      continue;
    }
    printf("operation data extracted from file is |%s|\n",opn_data);
    temp_tok1 = strstr(opn_data,s); 
    if(temp_tok1 != NULL)
    {
    temp_tok = strtok(opn_data,";");
    while(temp_tok != NULL)
    {
      printf("\n token string = %s",temp_tok);
      memset(temp_token,0,100);
      strncpy(temp_token,temp_tok,100); 

      if(strncmp(temp_token,"begin", strlen("begin")) == 0
      || strncmp(temp_token,"R", strlen("R")) == 0
      || strncmp(temp_token,"W", strlen("W")) == 0
      || strncmp(temp_token,"fail", strlen("fail")) == 0
      || strncmp(temp_token,"recover", strlen("recover")) == 0
      || strncmp(temp_token,"dump", strlen("dump")) == 0
      || strncmp(temp_token,"end", strlen("end")) == 0
      || strncmp(temp_token,"querystate", strlen("querystate")) == 0)
      {
        status = addOpn(temp_token, time_stamp);
        if(status == -1)
        {
          printf("\n parseInputFile: AddOpn failed for operation %s",opn_data);
          return -1;
        } 
      } 
      temp_tok = strtok(NULL,";");
    }
    time_stamp++; 
    }
    else
    {
    if(strncmp(opn_data,"begin", strlen("begin")) == 0 
    || strncmp(opn_data,"R", strlen("R")) == 0
    || strncmp(opn_data,"W", strlen("W")) == 0
    || strncmp(opn_data,"fail", strlen("fail")) == 0
    || strncmp(opn_data,"recover", strlen("recover")) == 0
    || strncmp(opn_data,"dump", strlen("dump")) == 0
    || strncmp(opn_data,"end", strlen("end")) == 0
    || strncmp(opn_data, "querystate", strlen("querystate")) == 0)
    {
      status = addOpn(opn_data, time_stamp);
      if(status == -1)
      {
        printf("\n parseInputFile: AddOpn failed for operation %s",opn_data);
        return -1;
      }
      /* Check if the line has more than one operation occuring concurrently */
      temp_opn = strstr(opn_data, ";");
      if(temp_opn == NULL)
      {
        time_stamp++;
      }
    } 
    }
  }
  return 0;
}

/***************addOpn Function******************
 * creates a new transaction for begin operation
 * prepares operation info for all other operation
 * Adds the operation to the transaction Queue
 * *********************************************/
int addOpn(char *opn_str, int ts)
{
  int trx_id = -1;
  int status;
  int opn_type;
  char *temp_str;
  int trx_type;
  struct trx_opn *temp_opn = (struct trx_opn *)malloc(sizeof(struct trx_opn ));
  if(temp_opn == NULL) 
  {
    printf("addOpn: critical failure") ;
    return -1 ;
  } 
  if(strncmp(opn_str,"beginRO", strlen("beginRO")) == 0) 
  {
    temp_str = opn_str;
    while(!isdigit(*temp_str))
      temp_str++;
    trx_id = atoi(temp_str);
    trx_type = RO_TRX;
    status = generateTrx(trx_id, trx_type, ts);
    if(status == -1) 
    {
      printf("addOpn: generateTrx returns error for new transaction tid %d\n", trx_id) ;
      return -1 ;
    }
  }
  else if(strncmp(opn_str,"begin", strlen("begin")) == 0)
  {
    temp_str = opn_str;
    while(!isdigit(*temp_str))
     temp_str++;
    trx_id = atoi(temp_str);
    trx_type = RW_TRX;
    status = generateTrx(trx_id, trx_type, ts);
    if(status == -1)
    {
      printf("addOpn: generateTrx returns error for new transaction tid %d\n", trx_id) ;
      return -1 ;
    }
  } 
  else if(strncmp(opn_str,"dump", strlen("dump")) == 0)
  {
    int var_num = -1;
    int write_val = -1;
    int site_no;

    trx_id = OTHER_TRX_ID;
    temp_str = strstr(opn_str, "(") ;
    if(temp_str == NULL)
    {
      return -1;
    }
    while(*temp_str != ')' && !isalpha(*temp_str) && !isdigit(*temp_str))
      temp_str++;
    if(*temp_str == ')') /* Check if dump operation needs to be performed on all variables of all sites */
    {
      var_num = ALL_VARS;
      site_no = ALL_SITES;
    }
    else if(isdigit(*temp_str))
    {
      var_num = ALL_VARS;
      site_no = atoi(temp_str);
    }  
    else if(isalpha(*temp_str))
    {
      while(!isdigit(*temp_str))
        temp_str++;

      var_num = atoi(temp_str);
      site_no = ALL_SITES;
    }
    opn_type = DUMP;
    status = prepOpnInfo(trx_id, opn_type, var_num, write_val, site_no, ts, temp_opn); 
    if(status == -1)
    {
      printf("addOpn:: prepOpnInfo returns error for dump operation\n") ;
      return -1 ;
    }
    addToQueue(trx_id, temp_opn) ;

  }

  else if(strncmp(opn_str,"end", strlen("end")) == 0)
  {
    int var_num = -1;
    int write_val = -1;
    int site_no = -1;
    temp_str = opn_str;
    while(!isdigit(*temp_str))
     temp_str++;

    trx_id = atoi(temp_str);
    opn_type = END;

    status = prepOpnInfo(trx_id, opn_type, var_num, write_val, site_no, ts, temp_opn);
    if(status == -1)
    {
      printf("prepOpnInfo failed to add end operation to queue %d\n", trx_id);
      return -1;
    }

    addToQueue(trx_id, temp_opn);
  }

  else if(strncmp(opn_str,"R", strlen("R")) == 0)
  {
    int var_num = -1;
    int write_val = -1;
    int site_no = -1;
    temp_str = opn_str;
    while(!isdigit(*temp_str))
     temp_str++ ;
    trx_id = atoi(temp_str) ;
    while(isdigit(*temp_str))
     temp_str++ ;
    while(!isdigit(*temp_str))
     temp_str++ ;
    
    var_num = atoi(temp_str);
    opn_type = READ;
    status = prepOpnInfo(trx_id, opn_type, var_num, write_val, site_no, ts, temp_opn); 
    if(status == -1) {
      printf("addOpn: prepOpnInfo returns error for new operation\n") ;
      return -1 ;
    }
    addToQueue(trx_id, temp_opn);
  }
  
  else if(strncmp(opn_str,"W", strlen("W")) == 0)
  {
    int var_num = -1;
    int write_val = -1;
    int site_no = -1;
    
    temp_str = opn_str;
    while(!isdigit(*temp_str))
      temp_str++ ;

    trx_id = atoi(temp_str);
    while(isdigit(*temp_str))
      temp_str++ ;
    while(!isdigit(*temp_str))
      temp_str++ ;

    var_num = atoi(temp_str) ;

    while(isdigit(*temp_str))
      temp_str++ ;

    while(!isdigit(*temp_str) && *temp_str != '-')
      temp_str++ ;

    write_val = atoi(temp_str);
    opn_type = WRITE;
    status = prepOpnInfo(trx_id, opn_type, var_num, write_val, site_no, ts, temp_opn); 
    if(status == -1) 
    {
      printf("storeOperation: prepareOperationNode returns error for new operation\n") ;
      return -1 ;
    }
    addToQueue(trx_id, temp_opn);
  }
  else if(strncmp(opn_str,"fail", strlen("fail")) == 0)
  {
    int site_no = 0;
    int var_num = -1;
    int write_val = -1 ;

    trx_id = OTHER_TRX_ID;
    temp_str = strstr(opn_str, "(") ;
    if(temp_str == NULL) {
     printf("addOpn: prepOpnInfo returns error for fail operation ") ;
     return -1 ;
    }
    while(!isdigit(*temp_str))
      temp_str++;
    site_no = atoi(temp_str) ;
    opn_type = FAIL;

    status = prepOpnInfo(trx_id, opn_type, var_num, write_val, site_no, ts, temp_opn);
    if(status == -1)
    {
      printf("prepOpnInfo failed to add fail operation to queue %d\n", trx_id);
      return -1;
    }

    addToQueue(trx_id, temp_opn);
  }
  else if(strncmp(opn_str,"recover", strlen("recover")) == 0) 
  {
    int site_no = 0;
    int var_num = -1; 
    int write_val = -1;

    trx_id = OTHER_TRX_ID ;
    temp_str = strstr(opn_str, "(") ;
    if(temp_str == NULL) 
    {
     printf("addOpn: Could not parse recover operation %s\n", opn_str) ;
     return -1 ;
    }
    while(!isdigit(*temp_str))
     temp_str++ ;

    site_no = atoi(temp_str);
    opn_type = RECOVER; 
    status = prepOpnInfo(trx_id, opn_type, var_num, write_val, site_no, ts, temp_opn);
    if(status == -1) 
    {
      printf("addOpn: prepOpnInfo returns error for recover operation\n") ;
      return -1 ;
    }
    addToQueue(trx_id, temp_opn);
  } 
  else if(strncmp(opn_str,"querystate", strlen("querystate")) == 0)
  {
    int site_no = 0;
    int var_num = -1;
    int write_val = -1;

    trx_id = OTHER_TRX_ID;
    site_no = ALL_SITES;
    var_num = ALL_VARS;
  
    opn_type = QUERY_STATE;
    status = prepOpnInfo(trx_id, opn_type, var_num, write_val, site_no, ts, temp_opn);
    if(status == -1) 
    {
      printf("addOpn: prepOpnInfo returns error for querystate operation\n") ;
      return -1 ;
    }
    addToQueue(trx_id, temp_opn);
  }
}

/*************initTransMngr Function:***************
 * initializes all values of the transaction stucture
 * Initializes site information for all sites
 * ***********************************************/
void initTransMngr()
{
  int trx_id, site_no, var_num;
  for(trx_id = 0; trx_id < MAX_TRANSACTIONS; trx_id++) 
  {
    T[trx_id].timestamp = -1 ;
    T[trx_id].trxType = -1 ;
    T[trx_id].trxStatus = -1 ;
    T[trx_id].inactiveTickNo = -1 ;
    T[trx_id].valid_trx_ind = 0 ;
    T[trx_id].first_opn = NULL;
    T[trx_id].last_opn = NULL; 
    T[trx_id].current_opn = NULL ;
    for(site_no = 0; site_no < MAX_SITES; site_no++)  
    {
      T[trx_id].sites_accessed[site_no].tick_first_access = -1 ;
      T[trx_id].sites_accessed[site_no].W_site_accessed = 0 ; 
    }
  }

  for(var_num = 1; var_num < MAX_VARS ; var_num++) 
  {
    if(var_num % 2 == 1)
    {
      site_no = (var_num % 10) + 1 ;
      siteInfo[site_no].var_exists_flag[var_num] = 1 ;
    }
    else
    {
      for(site_no = 1; site_no < MAX_SITES ; site_no++ )
      {
        siteInfo[site_no].var_exists_flag[var_num] = 1 ;
      }
    }
  }
  
  for(site_no = 1; site_no < MAX_SITES; site_no++)
  {
    siteInfo[site_no].site_up = 1 ; /* We assume that all sites will be up initially */
    siteInfo[site_no].up_ts = 0 ;
  }
}

/* Creates a new transaction for each begin or beginRO operation */
int generateTrx(int trx_id, int trx_type, int timestamp)
{
  if(trx_id >= MAX_TRANSACTIONS)
  {
    printf("generateTrx: Transaction limit exceeded.....abort\n");
    return -1;
  }
  if(T[trx_id].timestamp != -1)
  {
    printf("generateTrx: duplicate transaction ..... aborting\n");
    return -1;
  }
  T[trx_id].timestamp = timestamp ;
  T[trx_id].trxType = trx_type ;
  T[trx_id].valid_trx_ind = 1 ;

  return 0;
}

/***********prepOpnInfo Function:****************
 * Set operation paramters for each operation
 * set status for each operation as pending
 * Assigns transaction type to corresponding operation's timestamp
 * **************************************************************/
int prepOpnInfo(int trx_id, int opn_type, int var_num, int write_val, int site_no, int timestamp, struct trx_opn *opn)
{
  int siteNum = 0;

  opn->trx_id = trx_id;
  opn->opn_type = opn_type;
  opn->opn_ts = timestamp;
  opn->next_opn = NULL;
  opn->var_num = var_num;
  opn->write_val = write_val;
  opn->site_num = site_no;
  opn->opn_tick_num = -1;

  for(siteNum = 1; siteNum < MAX_SITES; siteNum++)
  {
    opn->site_opn_sts[siteNum] = OPERATION_PENDING;
  }

  if(T[trx_id].timestamp == -1 && trx_id == OTHER_TRX_ID)
  {
    T[trx_id].timestamp = timestamp;
  }
  opn->trx_type = T[trx_id].trxType;
  opn->trx_ts = T[trx_id].timestamp;

  return 0;
}

/************ addToQueue Function: *************************************
 * Adds current operation to the respective transaction's queue
 * Assign current operation to Transaction's first, current and last operation
 * ***************************************************************************/
void addToQueue(int trx_id, struct trx_opn *opr)
{
  if(T[trx_id].first_opn == NULL)
  {
    T[trx_id].first_opn = opr;
    T[trx_id].last_opn = opr;
    T[trx_id].current_opn = opr;
  }
  else
  {
    T[trx_id].last_opn->next_opn = opr;
    T[trx_id].last_opn = opr;
  }
  return;
}

/*************** startTrxMngr Function***************************
 * Fetches the operations from transaction queue
 * Calls perform operation function to perform each of the operations in the queue
 * In case of conflict, blocks the operation and adds it to the waitlist
 * At every tick, tries to perform the blocked operation
 * Commits each transaction on receipt of end operation
 * Sleeps every 200 milliseconds before the next tick
 * ************************************************************/
void startTrxMngr()  
{
  int tick_no = 0;
  int trx_id;
  int isPending = 1;
  int opn_status;
  int siteNum;

  while(isPending != 0)
  {
    printf("\n\n\n*******startTransactionManager: tick number %d************\n", tick_no) ;
    isPending = 0;
    while(T[OTHER_TRX_ID].current_opn != NULL)
    {
      if(isPending == 0)
        isPending = 1;

      if(T[OTHER_TRX_ID].current_opn->opn_ts > tick_no) /* transaction operation doesn't belong to current tick so we break */
      {
        break;
      }

      if(T[OTHER_TRX_ID].timestamp <= tick_no && T[OTHER_TRX_ID].current_opn->opn_ts <= tick_no)
      {
        struct trx_opn *opr = T[OTHER_TRX_ID].current_opn ;
        if(opr->opn_type == DUMP)
        {
          if(opr->var_num == ALL_VARS)
          {
            if(opr->site_num == ALL_SITES)
            {
              int i;
              for(i = 1; i < MAX_SITES; i++)
              {
                if(siteInfo[i].site_up == 0) /* If site is not up then do not send dump operation to it */        
                  continue;
                /* Calling function to perform the dump operation */
                perfOpn(opr, i);
                opn_status = opr->site_opn_sts[i];

                if(opn_status != OPERATION_COMPLETE)
                {
                  printf("dump operation did not complete at site %d\n", i);
                  opr->site_opn_sts[i] = OPERATION_COMPLETE; /* Set operation to complete to free the site for access by other transactions */
                }
              }
            }
            else if(opr->site_num != ALL_SITES)
            {
              if(siteInfo[opr->site_num].site_up == 0)
              {
                printf("Dump operation cannot be performed as site %s is down\n",opr->site_num);
              }
              else
              {
                perfOpn(opr, opr->site_num);
                opn_status = opr->site_opn_sts[opr->site_num];          
               
                if(opn_status != OPERATION_COMPLETE)
                {
                  printf("dump operation did not complete at site %d\n", opr->site_num);
                  opr->site_opn_sts[opr->site_num] = OPERATION_COMPLETE;
                }
              }
            }
          }
          else if(opr->var_num != ALL_VARS)
          {
            if(opr->var_num % 2 == 1)
            {
              siteNum = (opr->var_num % 10) + 1;
              if(siteInfo[siteNum].site_up == 0)
              {
                printf("Dump operation cannot be performed on variable %d as site %d is down\n",opr->var_num,siteNum);
              }
              else
              {
                perfOpn(opr, siteNum);
                opn_status = opr->site_opn_sts[siteNum];
          
                if(opn_status != OPERATION_COMPLETE)
                {
                  printf("dump operation did not complete at site %d\n", siteNum);
                  opr->site_opn_sts[siteNum] = OPERATION_COMPLETE;
                }
              }
            }
            else
            {
              for(siteNum = 1; siteNum < MAX_SITES; siteNum++)  
              {
                if(siteInfo[siteNum].site_up == 0)
                {
                  printf("Dump operation cannot be performed on variable %d as site %d is down\n",opr->var_num,siteNum);
                  continue;
                }
                perfOpn(opr, siteNum);
                opn_status = opr->site_opn_sts[siteNum];

                if(opn_status != OPERATION_COMPLETE)
                {
                  printf("dump operation did not complete at site %d\n", siteNum);
                  opr->site_opn_sts[siteNum] = OPERATION_COMPLETE;
                }
              }
            }
          }
        }
        else if(opr->opn_type == FAIL )
        {
          perfOpn(opr, opr->site_num);   

          siteInfo[opr->site_num].site_up = 0; /* Make site unavailable for read or write */
 
        }
        else if(opr->opn_type == RECOVER)
        {
          perfOpn(opr, opr->site_num);
       
          if(siteInfo[opr->site_num].site_up == 0)
          {
            siteInfo[opr->site_num].site_up = 1;
            siteInfo[opr->site_num].up_ts = tick_no;
          }
        }
        
        else if(opr->opn_type == QUERY_STATE)
        {
          int site_num; 
          int opn_status = 0;
          int tid;
          for(site_num = 1 ; site_num < MAX_SITES; site_num++)
          {
            if(siteInfo[site_num].site_up == 0)
              continue;
             perfOpn(opr, site_num);
          
            opn_status = opr->site_opn_sts[site_num] ;
            if(opn_status == OPERATION_REJECTED)
            {
               printf("Query state operation rejected at site %d\n", site_num);
            }
            if(opn_status == OPERATION_BLOCKED)
            {  
               printf("Query state operation blocked at site %d\n", site_num);
            }
            if(opn_status != OPERATION_COMPLETE) 
            {
              printf("Query state operation failed to complete at site %d\n", site_num);
              opr->site_opn_sts[site_num] = OPERATION_COMPLETE;
            }
          } 
          for(tid = 0; tid < MAX_TRANSACTIONS ; tid++) 
          {
            if(tid == OTHER_TRX_ID || T[tid].timestamp > tick_no || T[tid].timestamp == -1) 
              continue ;

            if(T[tid].current_opn == NULL) 
            {
              if(T[tid].trxStatus == TRX_COMMITED) 
              {
                printf("startTrxMngr: querystate - Transaction ID: %d COMMITED\n", tid) ;
              }
              else 
              {
                printf("startTrxMngr: querystate - Transaction ID: %d ABORTED\n", tid) ;
              }
            }
            else if(T[tid].current_opn->opn_ts < tick_no)
            {
              if(T[tid].current_opn->opn_type == READ) 
              {
                printf("startTrxMngr: querystate - Transaction ID: %d is waiting for operation read on variable %d arrived at tick No. %d to be completed\n", tid, T[tid].current_opn->var_num, T[tid].current_opn->opn_ts) ;
              }
              else if(T[tid].current_opn->opn_type == WRITE) 
              {
                printf("startTrxMngr: querystate - Transaction ID: %d is waiting for operation write on variable %d with value %d arrived at tick No. %d to be completed\n", tid, T[tid].current_opn->var_num, T[tid].current_opn->write_val, T[tid].current_opn->opn_ts) ;
              }
            }
            else if(T[tid].current_opn->opn_ts >= tick_no) 
            {
               printf("startTrxMngr: querystate - Transaction ID: %d is waiting for new operation to arrive from input Sequence\n", tid, T[tid].current_opn->var_num, T[tid].current_opn->write_val, T[tid].current_opn->opn_ts);

            }
          } 
        }   
        T[OTHER_TRX_ID].current_opn = T[OTHER_TRX_ID].current_opn->next_opn;
      }
    }

    for(trx_id = 0 ; trx_id < MAX_TRANSACTIONS ; trx_id++) 
    {
      if(trx_id == OTHER_TRX_ID) 
        continue;

      if(T[trx_id].current_opn != NULL)
      {
        if(isPending == 0)
          isPending = 1;
      
      if(T[trx_id].timestamp > tick_no)
        continue;
 
      if(T[trx_id].current_opn->opn_ts <= tick_no)
      {
        struct trx_opn *temp_opr = T[trx_id].current_opn;

        if(temp_opr->opn_type == READ)
        {
          if(temp_opr->var_num % 2 == 1)
          {
            int siteNo = (temp_opr->var_num % 10) + 1 ;
            temp_opr->site_num = siteNo;
            if(temp_opr->site_opn_sts[siteNo] == OPERATION_PENDING )
            {
              if(siteInfo[siteNo].site_up == 0 )
              {
#ifdef ABORT_SITE_FAIL
                printf("Transaction ID: %d Aborted since read on variable %d failed due to site %d failure\n", trx_id, temp_opr->var_num, temp_opr->site_num);
                T[trx_id].current_opn = NULL; 
#endif
#ifdef SITE_FAILED_WAIT
                if(temp_opr->opn_tick_num == -1)
                  temp_opr->opn_tick_num = tick_no;
                
                temp_opr->site_opn_sts[siteNo] = OPERATION_PENDING;
                printf("Transaction ID: %d blocked at read on variable %d since site %d has temporarily failed. Retrying on next tick\n", trx_id, temp_opr->var_num, siteNo);
                continue;
#endif
              }
              else
              {
                temp_opr->site_num = siteNo ;
                temp_opr->opn_tick_num = -1 ;
                perfOpn(temp_opr, siteNo) ; 
               
                if(temp_opr->site_opn_sts[siteNo] == OPERATION_REJECTED)
                {
                  printf("Transaction ID: %d will abort as read on var_num %d is rejected by site %d\n", trx_id, temp_opr->var_num, temp_opr->site_num);
                  abortTrx(temp_opr);
                  T[trx_id].trxStatus = TRX_ABORTED;
                  T[trx_id].current_opn = NULL;
                }
                else if(temp_opr->site_opn_sts[siteNo] == OPERATION_BLOCKED)
                {
                  printf("Transaction ID: %d is blocked for read on var_num %d  at site %d\n", trx_id, temp_opr->var_num, temp_opr->site_num);
                }
                else if(temp_opr->site_opn_sts[siteNo] == OPERATION_COMPLETE)
                {
                  if(T[trx_id].sites_accessed[siteNo].tick_first_access == -1)
                    T[trx_id].sites_accessed[siteNo].tick_first_access = tick_no;
              
                  T[trx_id].current_opn = T[trx_id].current_opn->next_opn;
                  printf("Transaction ID: %d Read operation on var_num %d returns %d value from site %d\n", trx_id, temp_opr->var_num, temp_opr->read_val, temp_opr->site_num);
                }
              }   /* End of else condition */
            } /* End of if condition for OPERATION_PENDING */
            else if(temp_opr->site_opn_sts[siteNo] == OPERATION_BLOCKED)
            {
              if(siteInfo[siteNo].site_up == 0) 
              {
#ifdef ABORT_SITE_FAIL
                printf("Transaction ID: %d will abort as read on var_num %d at site %d timed out\n", trx_id, temp_opr->var_num, temp_opr->site_num);
                abortTrx(temp_opr);
                T[trx_id].trxStatus = TRX_ABORTED;
                T[trx_id].current_opn = NULL;
#endif
#ifdef SITE_FAILED_WAIT 
                if(temp_opr->opn_tick_num == -1)
                  temp_opr->opn_tick_num = tick_no;

                printf("Transaction ID: %d has been blocked at read on var_num %d as site %d has failed\n", trx_id, temp_opr->var_num, temp_opr->site_num);
                temp_opr->site_opn_sts[siteNo] = OPERATION_PENDING;
              }
#endif
            } /* End of else if condition for OPERATION_BLOCKED */
            else if(temp_opr->site_opn_sts[siteNo] == OPERATION_COMPLETE)
            {
              if(T[trx_id].sites_accessed[siteNo].tick_first_access == -1)
                 T[trx_id].sites_accessed[siteNo].tick_first_access = tick_no;

              T[trx_id].current_opn = T[trx_id].current_opn->next_opn;
              printf("Transaction ID: %d Read operation on var_num %d returns %d value from site %d\n", trx_id, temp_opr->var_num, temp_opr->read_val, temp_opr->site_num);
            }
          } /* End of if condition for check of var_num % 2 */
          else
          {
            ATTEMPT_READ_AGAIN:
            if(temp_opr->site_num == -1)
              temp_opr->site_num = 1;
 
            if(siteInfo[temp_opr->site_num].site_up == 0)
            { 
              while(siteInfo[temp_opr->site_num].site_up == 0 && temp_opr->site_num < MAX_SITES)
              {
                temp_opr->site_num++;
              }
              if(temp_opr->site_num == MAX_SITES)
              {
#ifdef ABORT_SITE_FAIL
                abortTrx(temp_opr);
                T[trx_id].trxStatus = TRX_ABORTED;
                T[trx_id].current_opn = NULL;
                continue;
#endif
#ifdef SITE_FAILED_WAIT
                if(temp_opr->opn_tick_num == -1)
                   temp_opr->opn_tick_num = tick_no;

                temp_opr->site_opn_sts[temp_opr->site_num] = OPERATION_PENDING;
                for(int i = 1; i < MAX_SITES; i++)
                  temp_opr->site_opn_sts[temp_opr->site_num] = OPERATION_PENDING;
                
                temp_opr->site_num = 1;
                printf("Transaction ID: %d is blocked at read on var_num %d as all sites have failed. Retrying on the sites at the next tick\n", trx_id, temp_opr->var_num);
                continue;
#endif
              }
            }
            if(temp_opr->site_opn_sts[temp_opr->site_num] == OPERATION_PENDING)
            {
              temp_opr->opn_tick_num = -1;
              perfOpn(temp_opr, temp_opr->site_num);

              if(temp_opr->site_opn_sts[temp_opr->site_num] == OPERATION_REJECTED)
              {
                temp_opr->site_num++;
                if(temp_opr->site_num == MAX_SITES)
                {
                  abortTrx(temp_opr);
                  T[trx_id].trxStatus = TRX_ABORTED;
                  T[trx_id].current_opn = NULL;
                  continue;
                }
                else
                {
                  goto ATTEMPT_READ_AGAIN;
                }
              }
              else if(temp_opr->site_opn_sts[temp_opr->site_num] == OPERATION_BLOCKED)
              {
                printf("Transaction ID: %d blocked for read on var %d at site %d since site could not provide the lock\n", trx_id, temp_opr->var_num, temp_opr->site_num);
              }
              else if(temp_opr->site_opn_sts[temp_opr->site_num] == OPERATION_COMPLETE)
              {
                if(T[trx_id].sites_accessed[temp_opr->site_num].tick_first_access == -1)
                   T[trx_id].sites_accessed[temp_opr->site_num].tick_first_access = tick_no;

                printf("Transaction ID: %d Read operation on var_num %d returns %d value from site %d\n", trx_id, temp_opr->var_num, temp_opr->read_val, temp_opr->site_num); 
                T[trx_id].current_opn = T[trx_id].current_opn->next_opn;
              }
            }
            else if(temp_opr->site_opn_sts[temp_opr->site_num] == OPERATION_BLOCKED)
            {
              if(siteInfo[temp_opr->site_num].site_up == 0)
              {
                temp_opr->site_num++;
                if(temp_opr->site_num == MAX_SITES)
                {
#ifdef ABORT_SITE_FAIL
                  printf("Transaction ID: %d will abort as read on var_num %d at site %d timed out\n", trx_id, temp_opr->var_num, temp_opr->site_num) ;
                  abortTrx(temp_opr);
                  T[trx_id].trxStatus = TRX_ABORTED;
                  T[trx_id].current_opn = NULL;
                  continue;
#endif
#ifdef SITE_FAILED_WAIT
                  if(temp_opr->opn_tick_num == -1)
                    temp_opr->opn_tick_num = tick_no;
                  for(int i = 1; i < MAX_SITES; i++)
                  {
                    temp_opr->site_opn_sts[i] == OPERATION_PENDING;
                  }
                  temp_opr->site_num = 1;
                  printf("Transaction ID: %d blocked at read on var_num %d as all sites have failed. Retrying on the sites at the next tick\n", trx_id, temp_opr->var_num);
                  continue; 
#endif
                }
              }
              else
                printf("\n Transaction %d waiting to read from site %d for variable x%d",trx_id, temp_opr->site_num, temp_opr->var_num);
            }
            else if(temp_opr->site_opn_sts[temp_opr->site_num] == OPERATION_COMPLETE)
            {
              if(T[trx_id].sites_accessed[temp_opr->site_num].tick_first_access == -1)
                 T[trx_id].sites_accessed[temp_opr->site_num].tick_first_access = tick_no;

              printf("Transaction ID: %d Read operation on var_num %d returns %d value from site %d\n", trx_id, temp_opr->var_num, temp_opr->read_val, temp_opr->site_num); 
              T[trx_id].current_opn = T[trx_id].current_opn->next_opn;
            }
          }
        } /* End of if condition for READ operation */
   
        else if(temp_opr->opn_type == WRITE)
        {
          if(temp_opr->var_num %2 == 1)
          {
            int siteNo = (temp_opr->var_num % 10) + 1 ;
            temp_opr->site_num = siteNo;
            if(temp_opr->site_opn_sts[siteNo] == OPERATION_PENDING) 
            {
              if(siteInfo[siteNo].site_up == 0)
              {
#ifdef ABORT_SITE_FAIL
                printf("Transaction: %d will abort as write on variable x%d with value %d failed at site %d \n", trx_id, temp_opr->var_num, temp_opr->write_val, temp_opr->site_num) ;
                T[trx_id].current_opn = NULL ;
#endif 
#ifdef SITE_FAILED_WAIT
                if(temp_opr->opn_tick_num == -1)
                    temp_opr->opn_tick_num = tick_no;

                temp_opr->site_opn_sts[siteNo] = OPERATION_PENDING ;
                printf("Transaction %d blocked for write on variable x%d with value %d as site %d is down\n", trx_id, temp_opr->var_num, temp_opr->write_val, temp_opr->site_num) ;
#endif
              }
              else
              {
                temp_opr->site_num = siteNo ;
                temp_opr->opn_tick_num = -1 ;
                perfOpn(temp_opr, siteNo);
 
                if(temp_opr->site_opn_sts[siteNo] == OPERATION_REJECTED ) 
                {
                  printf("Transaction %d will abort as write on variable %d with value %d is rejected by site %d\n", trx_id, temp_opr->var_num, temp_opr->write_val, temp_opr->site_num) ;
                  abortTrx(temp_opr) ;
                  T[trx_id].trxStatus = TRX_ABORTED;
                  T[trx_id].current_opn = NULL;
                }
                else if(temp_opr->site_opn_sts[siteNo] == OPERATION_BLOCKED) 
                {
                  printf("Transaction ID: %d blocked for write on variable x%d with value %d at site %d as lock not granted\n", trx_id, temp_opr->var_num, temp_opr->write_val, temp_opr->site_num) ;
                } 
                else if(temp_opr->site_opn_sts[siteNo] == OPERATION_COMPLETE)
                {
                  printf("Transaction %d write on variable x%d with value %d done at site %d\n", trx_id, temp_opr->var_num, temp_opr->write_val, temp_opr->site_num) ;
                  T[trx_id].current_opn = T[trx_id].current_opn->next_opn ;

                  if(T[trx_id].sites_accessed[siteNo].tick_first_access == -1)
                    T[trx_id].sites_accessed[siteNo].tick_first_access = tick_no;

                  if(T[trx_id].sites_accessed[siteNo].W_site_accessed == 0) 
                      T[trx_id].sites_accessed[siteNo].W_site_accessed = 1 ;       //Set a flag indicating transaction has accessed the site for write operation
                }
              }
            }
            else if(temp_opr->site_opn_sts[temp_opr->site_num] == OPERATION_BLOCKED) 
            {
              if(siteInfo[temp_opr->site_num].site_up == 0)
              {
#ifdef ABORT_SITE_FAIL
                printf("Transaction: %d will abort as write on variable x%d with value %d failed at site %d due to timeout\n", trx_id, temp_opr->var_num, temp_opr->write_val, temp_opr->site_num) ;
                abortTrx(temp_opr);
                T[trx_id].trxStatus = TRX_ABORTED;
                T[trx_id].current_opn = NULL;
#endif
#ifdef SITE_FAILED_WAIT
                if(temp_opr->opn_tick_num == -1)
                    temp_opr->opn_tick_num = tick_no;

                temp_opr->site_opn_sts[siteNo] = OPERATION_PENDING ;
                printf("Transaction %d blocked for write on variable x%d with value %d as site %d is down\n", trx_id, temp_opr->var_num, temp_opr->write_val, temp_opr->site_num) ;
#endif
              } 
              else
              {
                printf("Transaction %d waiting for write on variable x%d with value %d at site %d\n", trx_id, temp_opr->var_num, temp_opr->write_val, temp_opr->site_num);
              }
            }
            else if(temp_opr->site_opn_sts[temp_opr->site_num] == OPERATION_COMPLETE)
            {
              printf("Transaction %d write on variable x%d with value %d done\n", trx_id, temp_opr->var_num, temp_opr->write_val, temp_opr->site_num) ;
              T[trx_id].current_opn = T[trx_id].current_opn->next_opn; 
            }
          }
          else
          {
            int site_num; 
            int write_status = IS_WRITE_FINISHED;
            int write_done = 0 ;
            for(site_num = 1; site_num < MAX_SITES; site_num++)
            {
              if(temp_opr->site_opn_sts[site_num] == OPERATION_IGNORE || temp_opr->site_opn_sts[site_num] == OPERATION_COMPLETE)
              {
                printf("\n Inside else condition for write on even variable inside OPERATION_IGNORE");
                continue;
              }
        
              else if(temp_opr->site_opn_sts[site_num] == OPERATION_PENDING)
              {
                if(siteInfo[site_num].site_up == 0) 
                {
                  printf("\n site %d is down, write cannot be done",site_num);
                  temp_opr->site_opn_sts[site_num] = OPERATION_IGNORE;
                  printf("\n temp_opr->write_val = %d",temp_opr->write_val);
                  continue;
                }
                else
                {
                  temp_opr->opn_tick_num = -1 ;
                  perfOpn(temp_opr, site_num) ;
              
                  if(temp_opr->site_opn_sts[site_num] == OPERATION_REJECTED)
                  {
                    printf("Transaction %d write on variable x%d with value %d is rejected by site %d\n", trx_id, temp_opr->var_num, temp_opr->write_val, site_num);
                    write_status = WRITE_FAILED;
                    break;
                  }
                  else if(temp_opr->site_opn_sts[site_num] == OPERATION_BLOCKED)
                  {
                    printf("Transaction %d blocked for write on variable x%d with value %d at site %d as lock\n", trx_id, temp_opr->var_num, temp_opr->write_val, site_num);
                    if(write_status != WRITE_PENDING) 
                    {
                      write_status = WRITE_PENDING;
                    }
                  }
                  else if(temp_opr->site_opn_sts[site_num] == OPERATION_COMPLETE)
                  {
                    printf("\n Inside else if condition for OPERATION_COMPLETE, write_val = %d",temp_opr->write_val);
                    if(write_done == 0)
                    {
                         printf("\n Inside write done == 0");
                         write_done = 1;
                    }

                    if(T[trx_id].sites_accessed[site_num].tick_first_access == -1)
                    {
                      T[trx_id].sites_accessed[site_num].tick_first_access = tick_no;
                    }

                    if(T[trx_id].sites_accessed[site_num].W_site_accessed == 0)
                    {
                      T[trx_id].sites_accessed[site_num].W_site_accessed = 1 ;
                    }

                    printf("Transaction %d write on variable %d with value %d at site %d finished\n", trx_id, temp_opr->var_num, temp_opr->write_val, site_num);
                  }
                }
              }
              else if(temp_opr->site_opn_sts[site_num] == OPERATION_BLOCKED)
              {
                if(siteInfo[site_num].site_up == 0)
                {
                  temp_opr->site_opn_sts[site_num] = OPERATION_IGNORE;
                } 
                else
                {
                  printf("Transaction %d waiting for write on variable x%d with value %d at site %d\n", trx_id, temp_opr->var_num, temp_opr->write_val, site_num);

                  if(write_status != WRITE_PENDING)
                    write_status = WRITE_PENDING;
                } 
              }
              else if(temp_opr->site_opn_sts[site_num] == OPERATION_COMPLETE)
              {
                if(T[trx_id].sites_accessed[site_num].tick_first_access == -1)
                   T[trx_id].sites_accessed[site_num].tick_first_access = tick_no;

                if(T[trx_id].sites_accessed[site_num].W_site_accessed == 0)
                   T[trx_id].sites_accessed[site_num].W_site_accessed = 1;

                if(write_done == 0)
                   write_done = 1;

                printf("Transaction %d write on variable x%d with value %d at site %d finished\n", trx_id, temp_opr->var_num, temp_opr->write_val, site_num);
              }
            }
            if(write_status == IS_WRITE_FINISHED)
            {
              if(write_done == 1) 
              {
                T[trx_id].current_opn = T[trx_id].current_opn->next_opn ;
                printf("Transaction %d write on variable %d with value %d completed at all available sites\n", trx_id, temp_opr->var_num, temp_opr->write_val);
              }
              else 
              {
#ifdef ABORT_SITE_FAIL
                printf("Transaction %d will be aborted as write on var_num %d with value %d could not be completed at any of the sites\n", trx_id, temp_opr->var_num, temp_opr->write_val) ;
                abortTrx(temp_opr);
                T[trx_id].trxStatus = TRX_ABORTED ;
                T[trx_id].current_opn = NULL ;
#endif
#ifdef SITE_FAILED_WAIT
                if(temp_opr->opn_tick_num == -1) 
                {
                  temp_opr->opn_tick_num = tick_no;
                }
                for(int i = 1; i < MAX_SITES; i++) 
                { 
                   temp_opr->site_opn_sts[i] = OPERATION_PENDING ;
                }
                printf("Transaction ID: %d blocked at write on variable x%d with value %d as all sites have failed. Retrying on the sites at the next tick\n", trx_id, temp_opr->var_num, temp_opr->write_val) ;
#endif
              }
            }
            else if(write_status == WRITE_FAILED) 
            {
              printf("Transaction %d ABORTED since write on varNo %d with value to be written %d rejected by site %d\n", trx_id, temp_opr->var_num, temp_opr->write_val, site_num) ;
              abortTrx(temp_opr) ;
              T[trx_id].trxStatus = TRX_ABORTED;
              T[trx_id].current_opn = NULL;
            }
          }
        }
        else if(temp_opr->opn_type == END)
        {
          int siteNo;
          int commit_ind = 1 ;
          for(siteNo = 1; siteNo < MAX_SITES && commit_ind == 1 && T[trx_id].trxType != RO_TRX; siteNo++) 
          {
              if(T[trx_id].sites_accessed[siteNo].tick_first_access != -1) 
              {
                if(siteInfo[siteNo].up_ts > T[trx_id].sites_accessed[siteNo].tick_first_access)
                {
                  commit_ind = 0;
                  printf("\n Transaction %d aborted as site failed after first access\n",trx_id);
                  abortTrx(temp_opr);
                  T[trx_id].trxStatus = TRX_ABORTED;
                  T[trx_id].current_opn = NULL ;
                }
              } 
          }
          if(commit_ind == 1)
          {
            printf("\n Transaction %d commited at tick number %d\n",trx_id, tick_no);
            for(siteNo = 1; siteNo < MAX_SITES; siteNo++)
            {
              if(T[trx_id].sites_accessed[siteNo].tick_first_access != -1 && siteInfo[siteNo].site_up == 1)
              {
                printf("\n perform end operation on site %d", siteNo);
                perfOpn(T[trx_id].current_opn,siteNo);
              }
            }
            T[trx_id].current_opn = NULL ;
            T[trx_id].trxStatus = TRX_COMMITED;
          }
        } 
      } /* End of if condition for operation time stamp <= tick_no */
     }
     else if(T[trx_id].current_opn == NULL && (T[trx_id].trxStatus == -1 && T[trx_id].valid_trx_ind == 1)) 
     {
        if(isPending == 0) 
        {
          isPending = 1;
        }
        if(T[trx_id].inactiveTickNo == -1) 
        {
          T[trx_id].inactiveTickNo = tick_no;
          printf("\n Transaction %d is waiting for input operation\n", trx_id);
        }
        else 
        {
          printf("\nTransaction %d still WAITING for a new operation from the input for over %d ticks\n",trx_id, (tick_no - T[trx_id].inactiveTickNo));
        }
      }
    }
    Sleep_ms(SLEEP_DURATION);
    tick_no++;
  }
}

/********** abortTrx Function: **************
 * Aborts the current transaction
 * sets all its operations to NULL
 * Clear all the site information for the transaction
 * ******************************************/
void abortTrx(struct trx_opn *opn) 
{
  struct trx_opn abort_opr;
  int site_no ;
  int trx_id = opn->trx_id;
  memcpy(&abort_opr, opn, sizeof(struct trx_opn)); 
  abort_opr.opn_type = ABORT;

  for(site_no = 1; site_no < MAX_SITES; site_no++) 
  {
    abort_opr.site_num = site_no;
    abort_opr.next_opn_site = NULL ;
    if(T[trx_id].sites_accessed[site_no].tick_first_access != -1 && siteInfo[site_no].site_up == 1) 
    {
      perfOpn(&abort_opr, site_no) ;
    }
  }
  return ;
}

/* Function to make the transaction manager sleep for 200 milliseconds */
void Sleep_ms(int time_ms) 
{
  struct timeval tv ;
  tv.tv_sec = 0 ;
  tv.tv_usec = time_ms * 1000 ;
  select(0, NULL , NULL, NULL, &tv) ;
  return ;
}
