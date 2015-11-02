#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<lmdb.h>

enum opened_dbi { DBI_NODES,DBI_TREE,DBI_REVTREE };

unsigned int dbi_flags[]={ 0,0,0 };
char*dbi_names[]={ "nodes","tree","revtree" };
typedef struct pijul_repository {
  MDB_dbi dbi_nodes;
  MDB_dbi dbi_tree;
  MDB_dbi dbi_revtree;
  unsigned int opened;
  MDB_val current_branch;
  MDB_env* env;
  MDB_txn* txn;
} pijul_repository;

MDB_dbi db_open(pijul_repository*repo,unsigned int op){
  MDB_dbi*dbis=(MDB_dbi*)repo;
  if( ! ((repo->opened) & (1<<op))) {
    mdb_dbi_open(repo->txn,dbi_names[op],dbi_flags[op],dbis+op);
    repo->opened|=(1<<op);
  }
  return dbis[op];
}

int pijul_open_repository(const char* path,pijul_repository*repo){
  if(*repo == NULL)
    return -1;
  int ret;
  if((ret=mdb_env_create(&(*repo)->env))) { free(*repo);return ret; }
  if((ret=mdb_env_set_mapsize((*repo)->env,1<<30))) goto cleanup;
  if((ret=mdb_env_set_maxdbs((*repo)->env,9))) goto cleanup;
  int dead;
  if((ret=mdb_reader_check((*repo)->env,&dead))) goto cleanup;
  if((ret=mdb_env_open((*repo)->env,path,0,0750))) goto cleanup;
  if((ret=mdb_txn_begin((*repo)->env,NULL,0,&(*repo)->txn))) goto cleanup;

  return 0;

 cleanup:
  mdb_env_close((*repo)->env);
  return ret;
}

void pijul_close_repository(pijul_repository*repo){
  if(repo->txn) mdb_txn_abort(repo->txn);
  mdb_env_close(repo->env);
  free(repo->current_branch.mv_data);
}

#define INODE_SIZE 16

int add_inode(pijul_repository*repo,char*inode,char**path,int pathc){
  MDB_val k,v;
  int i;
  char*buf=calloc(INODE_SIZE,1);
  char inode0[INODE_SIZE];
  int ret=0;
  for(i=0;i<pathc;i++){
    int s=strlen(path[i]);
    // we want buf to become the concatenation of the current inode and path[i].
    buf=realloc(buf,INODE_SIZE+s);
    memcpy(buf+INODE_SIZE,path[i],s);
    k.mv_size=s;
    k.mv_data=buf;
    // Is the concatenation in dbi_tree?
    ret=mdb_get(repo->txn,repo->dbi_tree,&k,&v);
    if(ret==0){
      // if so, just move on to the next iteration.
      memcpy(buf,v.mv_data,INODE_SIZE);
    } else if(ret==MDB_NOTFOUND){
      // Else, create a new inode.
      char*inode_;
      if(inode && i==pathc-1) {
        // either using the existing one if applicable.
        inode_=inode;
      } else {
        // or generating a random one.
        inode_=inode0;
        int j;
        for(j=0;j<INODE_SIZE;j++){
          inode0[j]=rand() & 0xff;
        }
      }
      v.mv_size=INODE_SIZE;
      v.mv_data=inode_;
      if((ret=mdb_put(repo->txn,repo->dbi_tree,&k,&v,0))) goto cleanup;
      if((ret=mdb_put(repo->txn,repo->dbi_revtree,&v,&k,0))) goto cleanup;
      memcpy(buf,inode_,INODE_SIZE);
    } else goto cleanup;
  }
  ret=0;
 cleanup:
  free(buf);
  return ret;
}

struct line {
  MDB_val key;
  unsigned char flags;
  struct line** children;
  size_t children_capacity;
  size_t children_off;
  int index;
  int lowlink;
};
#define LINE_FREED 1
#define LINE_SPIT 2
#define LINE_ONSTACK 4

void free_line(struct line* line){
  if((line->flags & LINE_FREED) == 0) {
    int i;
    line->flags=line->flags | LINE_FREED;
    for(i=0;i<line->children_off;i++)
      free_line(line->children[i]);
    free(line->children);
    free(line);
  }
}


void push_children(struct line* line, struct line* child){
  if(line->children_off >= line->children_capacity){
    line->children_capacity=(line->children_capacity>0) ? (line->children_capacity << 1) : 1;
    line->children=realloc(line->children,line->children_capacity);
  }
  line->children[line->children_off]=child;
  line->children_off++;
}

#define HASH_SIZE 20
#define LINE_SIZE 4
#define KEY_SIZE (HASH_SIZE+LINE_SIZE)

unsigned int hash_key(char* str){
  unsigned int h=0;
  unsigned char*ah=(unsigned char*) &h;
  int i;
  for(i=0;i<KEY_SIZE;i++){
    ah[ i % sizeof(int) ] = ah [ i % sizeof(int)] ^ str[i];
  }
  return h;
}

struct hashtable {
  void** table; // allocated at 2*size.
  size_t size;
  size_t elements;
};

struct hashtable* new_hashtable(int size){
  struct hashtable* t=malloc(sizeof(struct hashtable));
  t->table=malloc(2*size*sizeof(void*));
  t->size=size;
  t->elements=0;
  return t;
}
void free_hashtable(struct hashtable*t){
  free(t->table);
  free(t);
}
void insert(struct hashtable*,char*,void*);
void rehash(struct hashtable*t){
  void** table=malloc(t->size*2*sizeof(void*));
  void** old=t->table;
  int oldsize=t->size;
  t->table=table;
  t->size*=2;
  t->elements=0;
  int i;
  for(i=0;i<oldsize*2;i+=2){
    if(old[i])
      insert(t,old[i],old[i+1]);
  }
  free(old);
}

void insert(struct hashtable*t,char*key,void*value){
  int h=(hash_key(key) % t->size);
  while((t->table[2*h]) && (strncmp(t->table [2*h], key, KEY_SIZE) != 0)) {
    h += (h+1) % t->size;
  }
  if(!(t->table[2*h]))
    t->elements++; // This is an actual insertion (else it is a replacement).
  t->table[2*h]=key;
  t->table[2*h+1]=value;
  if(t->elements > t->size/2) rehash(t);
}
#define PIJUL_NOTFOUND -1
int get(struct hashtable*t,char*key,void**value){
  int h=(hash_key(key) % t->size);
  while((t->table[2*h] != NULL) && (strncmp(t->table [2*h], key, KEY_SIZE) != 0)) {
    h+=(h+1) % t->size;
  }
  if((t->table[2*h]) == NULL)
    return PIJUL_NOTFOUND;
  else {
    *value=t->table[2*h+1];
    return 0;
  }
}

#define PSEUDO_EDGE 1
#define FOLDER_EDGE 2
#define PARENT_EDGE 4
#define DELETED_EDGE 8

struct line* retrieve(pijul_repository*repo,char*key){
  struct hashtable*cache=new_hashtable(1024);

  struct line* retrieve_dfs(char*key) {
    struct line* l;
    int ret=get(cache,key,(void*) &l);
    if(ret){
      return l;
    } else {
      l=malloc(sizeof(struct line));
      memset(l,0,sizeof(struct line));
      l->key.mv_data=key;
      l->key.mv_size=KEY_SIZE;
      l->index= -1;
      insert(cache,key,l);
      MDB_cursor* curs;
      mdb_cursor_open(repo->txn,repo->dbi_nodes,&curs);
      MDB_val v;
      v.mv_data=children_edge;
      v.mv_size=1;
      ret=mdb_cursor_get(curs,&(l->key),&v,MDB_GET_BOTH_RANGE);
      while(!ret && (((char*)v.mv_data)[0]==0 || ((char*)v.mv_data)[0]==PSEUDO_EDGE)){
        push_children(l,retrieve_dfs(v.mv_data));
        ret=mdb_cursor_get(curs,&(l->key),&v,MDB_NEXT_DUP);
      }
      mdb_cursor_close(curs);
      return l;
    }
  }
  struct line* l=retrieve_dfs(key);
  free_hashtable(cache);
  return l;
}

int tarjan(struct line* l){
  int stack_size=1024;
  int stack_off=0;
  struct line** stack=malloc(stack_size*sizeof(struct line*));
  void push(struct line*l){
    if(stack_off>=stack_size){
      stack_size=stack_size<<1;
      stack=realloc(stack,stack_size);
    }
    stack[stack_off]=l;
    stack_off++;
  }
  int index=0;
  void dfs(struct line*l){
    l->index=index;
    l->lowlink=index;
    l->flags |= LINE_ONSTACK;
    push(l);
    index++;
    int i;
    for(i=0;i<l->children_off;i++){
      struct line* chi=l->children[i];
      if(chi->index < 0){
        dfs(chi);
        l->lowlink = (l->lowlink) < (chi->lowlink) ? l->lowlink:chi->lowlink;
      } else
        if(chi->flags & LINE_ONSTACK)
          l->lowlink = (l->lowlink) < (chi->index) ? l->lowlink:chi->index;
    }
    if(l->index == l->lowlink) {
      stack_off-=2;
      while(stack_off > 0 && (stack[stack_off] != l))
        stack_off--;
    }
  }
  dfs(l);
  free(stack);
  return (index-1);
}





// apply just one edge, not deleting anything else.
int apply_edge(MDB_txn*txn,MDB_dbi dbi_internal,MDB_cursor *curs_nodes,
                const char* internal_patch_id,
                MDB_val*eu,char flag,const MDB_val*ev,MDB_val*ep){
  char pu[1+KEY_SIZE+HASH_SIZE];
  char pv[1+KEY_SIZE+HASH_SIZE];
  MDB_val internal_u;
  char* deu=(char*)eu->mv_data;
  char* dev=(char*)ev->mv_data;
  // Find out internal keys
  MDB_val eeu,eev;
  int ret;
  eeu.mv_data=eu->mv_data;
  eeu.mv_size=eu->mv_size-LINE_SIZE;
  if((ret=mdb_get(txn,dbi_internal,&eeu,&internal_u))) return ret;
  memcpy(pu+1,internal_u.mv_data,HASH_SIZE);
  pu[0]=flag ^ PARENT_EDGE ^ DELETED_EDGE;

  memcpy(pu+1+HASH_SIZE,deu+eu->mv_size-LINE_SIZE,LINE_SIZE);

  eeu.mv_data=eu->mv_data;
  eeu.mv_size=eu->mv_size-LINE_SIZE;
  if((ret=mdb_get(txn,dbi_internal,&eeu,&internal_u))) return ret;
  memcpy(pv+1,internal_u.mv_data,HASH_SIZE);
  pv[0]=flag^DELETED_EDGE;

  memcpy(pv+1+HASH_SIZE,dev+ev->mv_size-LINE_SIZE,LINE_SIZE);

  if((ret=mdb_get(txn,dbi_internal,ep,&internal_u))) return ret;
  memcpy(pu+1+KEY_SIZE,internal_u.mv_data,HASH_SIZE);
  memcpy(pv+1+KEY_SIZE,internal_u.mv_data,HASH_SIZE);

  // Remove deleted version of the edge
  eeu.mv_data=pu+1;
  eeu.mv_size=KEY_SIZE;
  eev.mv_data=pv+1;
  eev.mv_size=1+KEY_SIZE+HASH_SIZE;
  ret=mdb_cursor_get(curs_nodes,&eeu,&eev,MDB_GET_BOTH);
  if(!ret){
    mdb_cursor_del(curs_nodes,0);
  } else if(ret!=MDB_NOTFOUND) return ret;
  eeu.mv_data=pv+1;
  eev.mv_data=pu;
  ret=mdb_cursor_get(curs_nodes,&eeu,&eev,MDB_GET_BOTH);
  if(!ret){
    mdb_cursor_del(curs_nodes,0);
  } else if(ret!=MDB_NOTFOUND) return ret;

  // Now insert actual version
  memcpy(pu+1+KEY_SIZE,internal_patch_id,HASH_SIZE);
  memcpy(pv+1+KEY_SIZE,internal_patch_id,HASH_SIZE);
  pv[0]=flag;
  pu[0]=flag^PARENT_EDGE;
  if((ret=mdb_cursor_put(curs_nodes,&eeu,&eev,0))) return ret;
  eeu.mv_data=pu+1;
  eev.mv_data=pv;
  if((ret=mdb_cursor_put(curs_nodes,&eeu,&eev,0))) return ret;
  return 0;
}

// Apply a sequence of new nodes
int apply_newnodes(MDB_txn*txn,MDB_dbi dbi_internal,MDB_dbi dbi_nodes, MDB_dbi dbi_contents,
                   char* internal_patch_id,
                   char flag,
                   int first_line_num,
                   MDB_val* upContext,size_t nupContext,
                   MDB_val* nodes,size_t nnodes,
                   MDB_val* downContext, size_t ndownContext){
  int ret;
  MDB_val vv,ww;
  char pv[1+KEY_SIZE+HASH_SIZE];
  char pw[1+KEY_SIZE+HASH_SIZE];
  memcpy(pw+1+KEY_SIZE,internal_patch_id,HASH_SIZE);
  memcpy(pv+1+KEY_SIZE,internal_patch_id,HASH_SIZE);

  // Write the first line in buffer pw.
  ww.mv_size=1+KEY_SIZE+HASH_SIZE;
  ww.mv_data=pw;
  pw[0]=flag;
  memcpy(pw+1,internal_patch_id,HASH_SIZE);
  // write linenum into w, little endian.
  int j;
  int linenum=first_line_num;
  for(j=0;j<LINE_SIZE;j++){
    pw[1+HASH_SIZE+j]=linenum & 0xff;
    linenum >>= 8; //monads!
  }
  pv[0]=flag ^ PARENT_EDGE;
  // Add bindings between all upcontexts and the first line.
  int i;
  MDB_val uu;
  vv.mv_data=pv+1;
  vv.mv_size=KEY_SIZE;
  MDB_val context;
  for(i=0;i<nupContext;i++){
    // Find the internal context of this up context.
    if(upContext[i].mv_size==LINE_SIZE){
      uu.mv_size=HASH_SIZE;
      uu.mv_data=internal_patch_id;
    } else {
      context.mv_data=upContext[i].mv_data;
      context.mv_size=upContext[i].mv_size - LINE_SIZE;
      if((ret=mdb_get(txn,dbi_internal,&context,&uu)))
        return ret;
    }
    memcpy(pv+1,uu.mv_data,HASH_SIZE);
    // Copy upcontext line number
    char*upc=(char*)upContext[i].mv_data;
    memcpy(pv+1+HASH_SIZE,upc+(upContext[i].mv_size)-LINE_SIZE,LINE_SIZE);

    // Add the edges.
    // First direction
    if((ret=mdb_put(txn,dbi_nodes,&vv,&ww,0))) return ret;
    // Other direction.
    vv.mv_data=pw+1;
    ww.mv_data=pv;
    if((ret=mdb_put(txn,dbi_nodes,&vv,&ww,0))) return ret;
  }

  char*ppv=pv;
  char*ppw=pw;
  memcpy(pw+1,internal_patch_id,HASH_SIZE);
  pv[0]=flag^PARENT_EDGE;
  pw[0]=flag;
  vv.mv_size=KEY_SIZE;
  ww.mv_size=1+KEY_SIZE+HASH_SIZE;
  for(i=0;i<nnodes-1;i++){
    // invariant: ppv contains the current node nodes[i]
    vv.mv_data=ppv+1;
    ww.mv_data=ppw;
    ppv[0]=flag ^ PARENT_EDGE;
    ppw[0]=flag;

    int linenum=first_line_num+i+1;
    int j;
    for(j=0;j<LINE_SIZE;j++){
      ppw[1+HASH_SIZE+j]=linenum & 0xff;
      linenum >>= 8;
    }

    if((ret=mdb_get(txn,dbi_contents,&vv,nodes+i))) return ret;

    if((ret=mdb_put(txn,dbi_nodes,&vv,&ww,0))) return ret;

    vv.mv_data=ppw+1;
    ww.mv_data=ppv;

    if((ret=mdb_put(txn,dbi_nodes,&vv,&ww,0))) return ret;


    // invert ppv / ppw
    char*tmp=ppv;
    ppv=ppw;
    ppw=tmp;
  }

  vv.mv_data=ppv+1;
  vv.mv_size=KEY_SIZE;
  ppv[0]=flag ^ PARENT_EDGE;
  ppw[0]=flag;
  // Now ppv contains the last new node. We need to link it to the down context.
  for(i=0;i<ndownContext;i++){

    if(downContext[i].mv_size==LINE_SIZE){
      uu.mv_size=HASH_SIZE;
      uu.mv_data=internal_patch_id;
    } else {
      context.mv_data=upContext[i].mv_data;
      context.mv_size=upContext[i].mv_size - LINE_SIZE;
      if((ret=mdb_get(txn,dbi_internal,&context,&uu))) return ret;
    }

    char*upc=(char*)downContext[i].mv_data;
    memcpy(ppw+1,uu.mv_data,HASH_SIZE);
    memcpy(ppw+1+HASH_SIZE,upc+(upContext[i].mv_size)-LINE_SIZE,LINE_SIZE);
    // Add the edges.
    // First direction
    ww.mv_data=ppw;
    ww.mv_size=1+KEY_SIZE+HASH_SIZE;
    mdb_put(txn,dbi_nodes,&vv,&ww,0);
    // Other direction.
    vv.mv_data=pw+1;
    ww.mv_data=pv;
    mdb_put(txn,dbi_nodes,&vv,&ww,0);
  }
  return 0;
}



void check_pseudo_edges(MDB_txn*txn,MDB_dbi dbi_internal,MDB_cursor *curs_nodes,
                        const char* internal_patch_id,
                        const MDB_val*eu,char flag,const MDB_val*ev,const MDB_val*ep){

  if(flag & DELETED_EDGE){
    int ret=0;

    const MDB_val *deleted=(flag&PARENT_EDGE) ? ev:eu;
    char pu[1+KEY_SIZE+HASH_SIZE];
    char pv[1+KEY_SIZE+HASH_SIZE];
    pu[0]=flag^PARENT_EDGE;

    MDB_val external,internal;
    external.mv_data=deleted->mv_data;
    external.mv_size=deleted->mv_size-LINE_SIZE;
    mdb_get(txn,dbi_internal,&external,&internal);
    memcpy(pu+1,internal.mv_data,HASH_SIZE);
    char*ce=deleted->mv_data;
    memcpy(pu+1+HASH_SIZE,ce + deleted->mv_size-LINE_SIZE,LINE_SIZE);
    // pu+1 now contains the full id of the patch, the first 1+KEY_SIZE are correct.

    // Does pu have any alive parent or folder parent?
    MDB_val u,v;
    u.mv_data=pu+1;
    u.mv_size=KEY_SIZE;
    v.mv_data=pv;
    v.mv_size=1+HASH_SIZE+KEY_SIZE;
    memset(pv,0,1+HASH_SIZE+KEY_SIZE);
    pv[0]=PARENT_EDGE;
    ret=mdb_cursor_get(curs_nodes,&u,&v,MDB_GET_BOTH_RANGE);
    if(ret==0 && v.mv_size>0){
      if(((char*)v.mv_data)[0] != PARENT_EDGE) // if the neighbor is not a parent
        ret=MDB_NOTFOUND;
    } else {ret=MDB_NOTFOUND;}
    // here ret==0 if and only if u has a parent.
    // does it have a pseudo-parent?
    if(ret!=0){
      v.mv_data=pv;
      v.mv_size=1+HASH_SIZE+KEY_SIZE;
      pv[0]=PARENT_EDGE | FOLDER_EDGE;
      ret=mdb_cursor_get(curs_nodes,&u,&v,MDB_GET_BOTH_RANGE);
      if(ret==0 && v.mv_size>0) { // has some neighbor
        if(((char*)v.mv_data)[0] != (PARENT_EDGE|FOLDER_EDGE)) // if the neighbor is not a folder parent
          ret=MDB_NOTFOUND;
      } else { ret=MDB_NOTFOUND; }
    }
    // here ret==0 if and only if u has a parent or a folder parent
    // while u has a pseudo parent
    while(!ret){
      MDB_val u,v;
      pv[0]=PSEUDO_EDGE | PARENT_EDGE; // v is a pseudo-parent of u.
      ret=mdb_cursor_get(curs_nodes,&u,&v,MDB_GET_BOTH_RANGE);
      if(!ret && v.mv_size>0 && v.mv_size==pv[0]) {
        memcpy(pv+1,v.mv_data+1,KEY_SIZE);
        memcpy(pu+1+KEY_SIZE,v.mv_data+1+KEY_SIZE,HASH_SIZE);
        char*ppv=v.mv_data;
        pu[0]=ppv[0] ^ PARENT_EDGE;

        mdb_cursor_del(curs_nodes,0);

        u.mv_data=pv+1;
        u.mv_size=KEY_SIZE;
        v.mv_data=pu;
        v.mv_size=1+KEY_SIZE+HASH_SIZE;
        ret=mdb_cursor_get(curs_nodes,&u,&v,MDB_GET_BOTH);
        if(!ret) {
          mdb_cursor_del(curs_nodes,0);
        }
      } else ret=MDB_NOTFOUND;
    }
  }
}

char deleted_parent_edge[1+HASH_SIZE+KEY_SIZE]={PARENT_EDGE|DELETED_EDGE};
char parent_edge[1+HASH_SIZE+KEY_SIZE]={PARENT_EDGE};
char children_edge[1+HASH_SIZE+KEY_SIZE]={0};

void connect_up(MDB_txn*txn,MDB_dbi nodes,char*internal_patch_id,MDB_val*a0,MDB_val*b){
  char va[1+KEY_SIZE+HASH_SIZE];
  char vb[1+KEY_SIZE+HASH_SIZE];
  MDB_val psa,psb;
  memcpy(va+1+KEY_SIZE, internal_patch_id, HASH_SIZE);
  memcpy(vb+1+KEY_SIZE, internal_patch_id, HASH_SIZE);
  va[0]=PSEUDO_EDGE|PARENT_EDGE;
  vb[0]=PSEUDO_EDGE;
  void connect(MDB_val*a){
    MDB_val c;
    c.mv_data=deleted_parent_edge;
    c.mv_size=1+HASH_SIZE+KEY_SIZE;
    MDB_cursor*curs0;
    mdb_cursor_open(txn,nodes,&curs0);
    int ret=mdb_cursor_get(curs0,a,&c,MDB_GET_BOTH_RANGE);
    int flag;
    char*d=c.mv_data;
    if(c.mv_size > 0) { flag=d[0]; } else {ret=MDB_NOTFOUND;}
    while(!ret && flag==deleted_parent_edge[0]){
      c.mv_data=d+1;
      c.mv_size=KEY_SIZE;
      connect(&c);
      ret=mdb_cursor_get(curs0,a,&c,MDB_NEXT_DUP);
      if(c.mv_size > 0) { d=c.mv_data; flag=d[0]; } else {ret=MDB_NOTFOUND;}
    }

    c.mv_data=parent_edge;
    ret=mdb_cursor_get(curs0,a,&c,MDB_GET_BOTH_RANGE);
    if(!ret && c.mv_size>0){
      d=c.mv_data;
      if(d[0] < DELETED_EDGE && d[0]>=PARENT_EDGE && a!=a0) {
        // a has the right kind of neighbors and a!=a0, add a pseudo-edge.
        psa.mv_data=va+1;psa.mv_size=KEY_SIZE;
        psb.mv_data=vb;psb.mv_size=1+KEY_SIZE+HASH_SIZE;
        mdb_put(txn,nodes,&psa,&psb,0);
        psa.mv_data=vb+1;psa.mv_size=KEY_SIZE;
        psb.mv_data=va;psb.mv_size=1+KEY_SIZE+HASH_SIZE;
        mdb_put(txn,nodes,&psa,&psb,0);
      }
    }
    mdb_cursor_close(curs0);
  }
  connect(a0);
}


void connect_down(MDB_txn*txn,MDB_dbi nodes,char*internal_patch_id,MDB_val*a,MDB_val*b0){
  char va[1+KEY_SIZE+HASH_SIZE];
  char vb[1+KEY_SIZE+HASH_SIZE];
  MDB_val psa,psb;
  memcpy(va+1+KEY_SIZE, internal_patch_id, HASH_SIZE);
  memcpy(vb+1+KEY_SIZE, internal_patch_id, HASH_SIZE);
  va[0]=PSEUDO_EDGE|PARENT_EDGE;
  vb[0]=PSEUDO_EDGE;
  void connect(MDB_val*b){
    MDB_val c;
    c.mv_data=children_edge;
    c.mv_size=1+HASH_SIZE+KEY_SIZE;
    MDB_cursor*curs0;
    mdb_cursor_open(txn,nodes,&curs0);
    int ret=mdb_cursor_get(curs0,a,&c,MDB_GET_BOTH_RANGE);
    int flag;
    char*d=c.mv_data;
    if(c.mv_size > 0) { flag=d[0]; } else {ret=MDB_NOTFOUND;}
    while(!ret && flag==deleted_parent_edge[0]){
      c.mv_data=d+1;
      c.mv_size=KEY_SIZE;
      connect(&c);
      ret=mdb_cursor_get(curs0,a,&c,MDB_NEXT_DUP);
      if(c.mv_size > 0) { d=c.mv_data; flag=d[0]; } else {ret=MDB_NOTFOUND;}
    }

    // is b alive?
    c.mv_data=parent_edge;
    ret=mdb_cursor_get(curs0,b,&c,MDB_GET_BOTH_RANGE);
    if(!ret && c.mv_size>0){
      d=c.mv_data;
      if(d[0] < DELETED_EDGE && d[0]>=PARENT_EDGE && a!=a0) {
        // a has the right kind of neighbors and a!=a0, add a pseudo-edge.
        psa.mv_data=va+1;psa.mv_size=KEY_SIZE;
        psb.mv_data=vb;psb.mv_size=1+KEY_SIZE+HASH_SIZE;
        mdb_put(txn,nodes,&psa,&psb,0);
        psa.mv_data=vb+1;psa.mv_size=KEY_SIZE;
        psb.mv_data=va;psb.mv_size=1+KEY_SIZE+HASH_SIZE;
        mdb_put(txn,nodes,&psa,&psb,0);
      }
    }
    mdb_cursor_close(curs0);
  }
  connect(b0);
}





/* TODO: (R=must be done in rust, V=done)

  UnsafeApply

  V unsafe_apply for edges and newnodes
  V reconnect upwards
    reconnect downwards
  V remove obsolete pseudoedges

  Note: reconnect upwards and downwards must be called for both edges and newnodes, but after these have been inserted.

V/R Fetch repository

  V/R Tarjan
  R output_repository

V add_file
  move_file
  del_file

*/
