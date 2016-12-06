#ifndef _OPERATION_
#define _OPERATION_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>
#include <time.h>

#define RO_TRX 0 /* READ ONLY Transaction */
#define RW_TRX 1 /* Read Write Transaction */
#define OTHER_TRX 2 /* For any other transaction which have operations such as recover, fail, dump etc. */

#define READ 0
#define WRITE 1
#define END 2
#define ABORT -2
#define DUMP 3
#define FAIL -4
#define RECOVER 4
#define QUERY_STATE 5

#define OPERATION_PENDING 0
#define OPERATION_BLOCKED -1
#define OPERATION_REJECTED -2
#define OPERATION_IGNORE -3
#define OPERATION_COMPLETE 1

/* For operations performed on all variables */
#define ALL_VARS 99
#define ALL_SITES 99

/* There are total 10 sites and a total of 20 variables distributed across these sites */
#define MAX_SITES 11
#define MAX_VARS 21

struct trx_opn 
{
  int trx_id ;
  int trx_type;
  int trx_ts; /* Timestamp of the transaction */
  int opn_type;
  int opn_ts; /* Timestamp of operation perfromed by a transaction */
  int site_num;
  int var_num;
  int read_val;
  int write_val;
  int site_opn_sts[MAX_SITES]; /* The status of operation set at each site */
  int opn_tick_num; /* value of tick at which operation was sent to a given site */
  struct trx_opn *next_opn_site;
  struct trx_opn *next_opn;
};

void appendLog(char *); /* Function to append the output to the log file */
#endif
