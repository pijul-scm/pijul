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
  struct c_line** children;
  size_t children_capacity;
  size_t children_off;
  unsigned int index;
  unsigned int lowlink;
};

#define LINE_FREED 1
#define LINE_SPIT 2
#define LINE_ONSTACK 4
#define LINE_VISITED 8

void c_free_line(struct c_line* line){
  if((line->flags & LINE_FREED) == 0) {
    int i;
    line->flags=line->flags | LINE_FREED;
    for(i=0;i<line->children_off;i++)
      c_free_line(line->children[i]);
    free(line->children);
    free(line);
  }
}


void push_children(struct c_line* line, struct c_line* child){
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

int c_retrieve(MDB_txn* txn,MDB_dbi dbi,char*key, struct c_line**result){
  struct hashtable*cache=new_hashtable(1024);

  struct c_line* retrieve_dfs(char*key) {
    struct c_line* l;
    int ret=get(cache,key,(void*) &l);
    if(ret){
      return l;
    } else {
      l=malloc(sizeof(struct c_line));
      memset(l,0,sizeof(struct c_line));
      l->key=key;
      l->index= -1;
      insert(cache,key,l);
      MDB_cursor* curs;
      mdb_cursor_open(txn,dbi,&curs);
      MDB_val k,v;
      char children_edge=0;
      v.mv_data=&children_edge;
      v.mv_size=1;
      k.mv_data=l->key;
      k.mv_size=KEY_SIZE;
      ret=mdb_cursor_get(curs,&k,&v,MDB_GET_BOTH_RANGE);
      while(!ret && (((char*)v.mv_data)[0]==0 || ((char*)v.mv_data)[0]==PSEUDO_EDGE)){
        push_children(l,retrieve_dfs(v.mv_data));
        ret=mdb_cursor_get(curs,&k,&v,MDB_NEXT_DUP);
      }
      mdb_cursor_close(curs);
      return l;
    }
  }
  *result=retrieve_dfs(key);
  free_hashtable(cache);
  if(*result) return 0; else return 1;
}
