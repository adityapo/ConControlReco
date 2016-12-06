#include "operations.h"
#define MAX_TRX_TS 10000
struct lock_table
{
int var_exists;  /* To check if the variable exist at that site. 1 = exists, 0 = does not exist */
int var_read_available;
struct trx_opn *first_actv_opn;
struct trx_opn *first_blckd_opn;
};

struct version
{
 int trx_id;    /* This will be used when transaction updates the version table during commit */
 int value;
 int read_ts;
 int write_ts;
 struct version *next;
};

struct version_table
{
  int ver_flag; 
  struct version *head;
};
 
struct site
{
  struct version_table var[MAX_VARS];
  struct lock_table lock_data[MAX_VARS];
} sites_info[MAX_SITES];

void initSiteData();
void perfOpn(struct trx_opn *opr, int siteNum);
