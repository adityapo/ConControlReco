#include "operations.h" 
#include <time.h>

#define MAX_TRANSACTIONS 1000
#define OTHER_TRX_ID 999             /* operations like dump, recover or fail will be assigned to this tid */

#define TRX_COMMITED 1
#define TRX_ABORTED 0

struct siteAccessInfo /* This structure gives access information for a site for a specific transaction */
{
 int tick_first_access;
 int W_site_accessed;
};

struct siteInformation 
{
  int var_exists_flag[MAX_SITES]; /* A flag indicating whether the variable is present at the given site or not */
  int site_up;	/* A flag indicating if the given site is available or down */
  int up_ts; /* The tick time since site is up */
};

struct Transaction {
 struct siteAccessInfo sites_accessed[MAX_SITES];
 int trxType;
 int trxStatus;
 int inactiveTickNo;
 int timestamp;
 int valid_trx_ind;
 struct trx_opn *first_opn;
 struct trx_opn *last_opn;
 struct trx_opn *current_opn;
};

struct Transaction T[MAX_TRANSACTIONS];

typedef struct siteInformation site_info;
site_info siteInfo[MAX_SITES];

int parseInputFile(char *inputfile);
void initTransMngr();
void startTrxMngr();
int addOpn(char *opn_str, int ts);
int generateTrx(int trx_id, int trx_type, int timestamp);
int prepOpnInfo(int trx_id, int opn_type, int var_num, int write_val, int site_no, int timestamp, struct trx_opn *opn);
void addToQueue(int trx_id, struct trx_opn *opr);
