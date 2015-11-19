#include<stdlib.h>
#define IS_DIRECTORY 1

struct pijul_repository;
typedef struct pijul_repository* pijul_repository;
int pijul_open_repository(char*,pijul_repository*);
void pijul_close_repository(pijul_repository);
void pijul_add_file(pijul_repository,char*,int);
void pijul_move_file(pijul_repository,char*,char*,int);
void pijul_remove_file(pijul_repository,char*);
char* pijul_get_current_branch(pijul_repository);
void pijul_new_internal(pijul_repository,char*);
void pijul_register_hash(pijul_repository,char*,char*,size_t);
struct pijul_changes_t;
typedef struct pijul_changes_t* pijul_changes_t;
struct pijul_updates_t;
typedef struct pijul_updates_t* pijul_updates_t;
int pijul_record(pijul_repository,char*,pijul_changes_t*,pijul_updates_t*);
void pijul_has_patch(pijul_repository,char*,char*,size_t);

struct pijul_patch_t;
typedef struct pijul_patch_t* pijul_patch_t;
pijul_patch_t pijul_create_patch(pijul_changes_t);

int pijul_apply(pijul_repository,pijul_patch_t,char*);
int pijul_write_changes_file(pijul_repository,char*);
void pijul_sync_file_additions(pijul_repository,pijul_changes_t,pijul_updates_t,char*);
int pijul_output_repository(pijul_repository,char*,pijul_patch_t);
