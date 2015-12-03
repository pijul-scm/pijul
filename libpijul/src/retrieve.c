/*
  Copyright Florent Becker and Pierre-Etienne Meunier 2015.

  This file is part of Pijul.

  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Affero General Public License for more details.

  You should have received a copy of the GNU Affero General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include<stdio.h>
#include<string.h>
#include<stdlib.h>
#include<lmdb.h>

struct c_line {
  char *key;
  unsigned char flags;
  int children;
  int n_children;
  unsigned int index;
  unsigned int lowlink;
  unsigned int scc;
};

#define LINE_FREED 1
#define LINE_SPIT 2
#define LINE_ONSTACK 4
#define LINE_VISITED 8
#define LINE_HALF_DELETED 16

void c_free_line(struct c_line* line,void** chi){
  free(line);
  free(chi);
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

void insert(struct hashtable*,char*,struct c_line*);
void rehash(struct hashtable*t){
  void** old=t->table;
  int oldsize=t->size;
  t->size*=2;
  t->elements=0;
  t->table=calloc(t->size*2,sizeof(void*));
  int i;
  for(i=0;i<oldsize*2;i+=2){
    if(old[i])
     insert(t,old[i],old[i+1]);
  }
  free(old);
}

void insert(struct hashtable*t,char*key,struct c_line*value){
  int h=(hash_key(key) % t->size);
  while((t->table[2*h]) && (memcmp(t->table [2*h], key, KEY_SIZE) != 0)) {
    h = (h+1) % t->size;
  }
  if(!(t->table[2*h]))
    t->elements++; // This is an actual insertion (else it is a replacement).
  t->table[2*h]=key;
  t->table[2*h+1]=value;
  if(t->elements > t->size/2) rehash(t);
}

#define PIJUL_NOTFOUND -1
int get(struct hashtable*t,char*key,struct c_line**value){
  int h=(hash_key(key) % t->size);
  while((t->table[2*h] != NULL) && (memcmp(t->table [2*h], key, KEY_SIZE) != 0)) {
    h=(h+1) % t->size;
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

void c_retrieve(MDB_txn* txn,MDB_dbi dbi_nodes,unsigned char*key,struct c_line**p_lines,int**p_children){
  printf("c_retrieve called\n");
  struct hashtable cache;
  unsigned int size=1024;
  cache.table=calloc(2*size,sizeof(void*));
  cache.size=size;
  cache.elements=0;


  struct c_line* lines=malloc(sizeof(struct c_line));
  int n_lines=1;
  int off_lines=0;

  int* children=malloc(sizeof(int));
  int n_children=1;
  int off_children=0;


  int retrieve_dfs(unsigned char*key) {
    printf("retrieving ");
    int i;
    for(i=0;i<KEY_SIZE;i++) printf("%02x",key[i]);
    printf("\n");
    int n_l;
    int ret=get(&cache,key,&n_l);
    if(ret==0){
      /*
      printf("existing ");
      int i;
      for(i=0;i<KEY_SIZE;i++) printf("%02x",l->key[i]);
      printf("\n");
      */
      return n_l;
    } else {
      if(off_lines>=n_lines){
        n_lines*=2;
        lines=realloc(lines,n_lines*sizeof(struct c_line));
      }
      struct c_line* l=lines+off_lines;
      insert(&cache,key,off_lines);
      memset(l,0,sizeof(struct c_line));
      l->key=key;

      int off_lines0 = off_lines;
      off_lines++;

      MDB_val k,v;
      char children_edge=PARENT_EDGE | DELETED_EDGE;
      v.mv_data=&children_edge;
      v.mv_size=1;
      k.mv_data=l->key;
      k.mv_size=KEY_SIZE;
      ret=mdb_cursor_get(curs,&k,&v,MDB_GET_BOTH_RANGE);
      if(ret==0 && v.mv_size>0 && ((char*)v.mv_data)[0] == children_edge)
        l->flags=LINE_HALF_DELETED;

      children_edge=0;
      v.mv_data=&children_edge;
      v.mv_size=1;
      k.mv_data=l->key;
      k.mv_size=KEY_SIZE;

      l->children=off_children;

      MDB_cursor* curs;
      int e=mdb_cursor_open(txn,dbi_nodes,&curs);

      ret=mdb_cursor_get(curs,&k,&v,MDB_GET_BOTH_RANGE);
      while(!ret && (((char*)v.mv_data)[0]==0 || ((char*)v.mv_data)[0]==PSEUDO_EDGE)){
        if(off_children >= n_children) {
          n_children*=2;
          children=realloc(children,n_children*sizeof(int));
        }
        off_children++;
        ret=mdb_cursor_get(curs,&k,&v,MDB_NEXT_DUP);
      }
      l->n_children=off_children- l->children;

      i=l->children;

      children_edge=0;
      v.mv_data=&children_edge;
      v.mv_size=1;
      k.mv_data=l->key;
      k.mv_size=KEY_SIZE;

      ret=mdb_cursor_get(curs,&k,&v,MDB_GET_BOTH_RANGE);
      while(!ret && (((char*)v.mv_data)[0]==0 || ((char*)v.mv_data)[0]==PSEUDO_EDGE)){
        children[i] = retrieve_dfs(((char*)v.mv_data)+1);
        printf("->: %d %d\n",i,children[i]);
        i++;
        ret=mdb_cursor_get(curs,&k,&v,MDB_NEXT_DUP);
      }

      mdb_cursor_close(curs);

      return off_lines0;
    }
  }
  retrieve_dfs(key);
  free(cache.table);
  printf("retrieve :%p %p\n",children,lines);
  *p_children=children;
  *p_lines=lines;
}
