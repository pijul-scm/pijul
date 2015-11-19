#include <pijul.h>
#include <stdlib.h>
#include <stdio.h>
int main(){
  pijul_repository repository;
  pijul_open_repository("/tmp/a",&repository);
  pijul_add_file(repository,"/tmp/a/a",0);
  char* br=pijul_get_current_branch(repository);
  printf("branch : %s\n",br);
  free(br);
  pijul_close_repository(repository);
}
