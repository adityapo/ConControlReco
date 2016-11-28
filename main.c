/* main file of project */
#include "globals.h"
#include "transaction_manager_structures.h"
#include "site.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

int main(int argc, char *argv[]) 
{
  char inputfile[100], log_desc[1000] ;
  int ret_sts ;

  if(argc != 2) 
  {
    printf("Usage: %s <input-transaction-file-path>\n", argv[0]) ;
    return  0 ;
  }

  strcpy(inputfile, argv[1]) ;
  ret_sts = checkFileExists(inputfile) ;

  if(ret_sts == -1) 
  {
   printf("main: File %s does not exist or is an empty file. Returning...\n", inputfile) ;
   return  0 ;
  }

  FILE *fp = fopen("output.log", "w") ;
  if(fp == NULL) 
  {
    printf("main: failed to open log file. Error: %s\n", (char *)strerror(errno)) ;
  }
  else 
    fclose(fp) ;
  
  /* Calling function to initialize Transaction Manager  */
  initTransMngr() ;

  /* Calling function to initialize data on the sites */
  initSiteData() ;

  ret_sts = parseInput(inputfile) ; /* Calling function to parse Input File */
  if(ret_sts == -1) 
  {
   printf("main: could not parse input file - %s. Returning...\n", inputfile) ;
   return  0 ;
  }
  
  /* Calling function to start transaction manager to perform each operation present in the input file */
  startTransMngr() ;

  sprintf(log_desc, "main: exiting Transaction Manager \n") ;
  appendLog(log_desc) ;
  return  0 ;
}

/* This function checks if the given exists or not and returns err if the file is empty */
int checkFileExists(char *file_name) 
{
  int status;
  struct  stat sb;
  status = lstat(file_name, &sb) ;

  if (status == -1) {
   printf("lstat function returned with error for file %s: %s\n", file_name, (char *)strerror(errno)) ;
   return -1 ;
  }
  if(sb.st_size == 0){
   printf("File is empty, returning error");
   return -1 ;
  }
  return 0 ;
}

void appendLog(char * log_desc) 
{
   FILE *fp = fopen("output.log", "a") ;
   printf("%s", log_desc) ;
   if(fp == NULL) {
     printf("appendLog: failed to open log file in append mode. Error: %s\n", (char *)strerror(errno)) ;
     return ;
   }
   fprintf(fp, "%s", log_desc) ;
   fclose(fp) ;
}
