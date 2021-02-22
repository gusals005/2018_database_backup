#ifndef __BPT_H__
#define __BPT_H__

// Uncomment the line below if you are compiling on Windows.
// #define WINDOWS
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include<iostream>
#include<sstream>
#include<string>
#include<assert.h>
#include<algorithm>
#include<vector>
#include<time.h>
#include<pthread.h>
#include<set>
#include<unordered_map>
#include<list>
using namespace std;

#ifdef WINDOWS
#define bool char
#define false 0
#define true 1
#endif

// Default order is 4.
#define DEFAULT_ORDER 4

// Minimum order is necessarily 3.  We set the maximum
// order arbitrarily.  You may change the maximum order.
#define MIN_ORDER 3
#define MAX_ORDER 20

// Constants for printing part or all of the GPL license.
#define LICENSE_FILE "LICENSE.txt"
#define LICENSE_WARRANTEE 0
#define LICENSE_WARRANTEE_START 592
#define LICENSE_WARRANTEE_END 624
#define LICENSE_CONDITIONS 1
#define LICENSE_CONDITIONS_START 70
#define LICENSE_CONDITIONS_END 625

// constants for on-disk
#define PAGE_SIZE 4096
#define OFFSET_SZ 8
#define KEY_SZ 8
#define VALUE_SZ 120


//for thread
#define NUM_THREADS 10
// structures.

/*
 * record
 */
typedef struct record {
        int64_t key;
        int64_t value[15];
} record;

typedef uint64_t pagenum_t;
typedef struct page_t{
        int64_t next_offset;            // parent page or next_free_page
        int is_leaf;
        int num_of_keys;
        int64_t root_page_offset;
        int64_t num_of_page;
        int64_t num_of_columns;
        int64_t special_page_offset; // ex, right sibling offset , or leftmost child offset
        int64_t key[256];
        int64_t page_offset[256];
        int64_t value[32][15];
        int page_type; // 0-header, 1-internal, 2-leaf, 3-free
        pagenum_t pagenum;
}page_t;

typedef struct header_page{
    int64_t free_page_offset;
    int64_t root_page_offset;
    int64_t num_of_pages;
    int64_t num_of_columns;
    int64_t reserved[508];
}header_page;

typedef struct internal_page{
    int64_t parent_page_offset;
    int is_leaf;
    int num_of_keys;
    int64_t reserved[13];
    int64_t special_page_offset;
    int64_t key_n_pageoffset[248][2]; 
}internal_page;

typedef struct leaf_page{
    int64_t parent_page_offset;
    int is_leaf;
    int num_of_keys;
    int64_t reserved[13];
    int64_t special_page_offset;
    record key_n_value[31];
}leaf_page;

typedef struct free_page{
    int64_t next_free_page_offset;
    int64_t reserved[511];
}free_page;


typedef struct fp {
    FILE * fi;
} fp;

typedef struct buffer{
    page_t* page;
    int table_id;
    pagenum_t pagenum;
    int is_dirty;
    int pin_count;
    struct buffer * next_buffer;
    struct buffer * prev_buffer;
}buffer;

/*record Join query*/
typedef struct queryops{
    int64_t left_tid;          //조교님은 table을 직접 저장했음.
    int64_t left_col;
    int64_t right_tid;
    int64_t right_col;
    int64_t num_of_product;
}query_op;

// GLOBALS.

//for buffer and store filenames;
extern char tables[11][101];
extern int buffer_pool_size;
extern buffer* header_buffer;

//for filemanager
extern FILE * fi;
extern fp* fp_arr;
//orders
extern int order;
extern int internal_order;
extern int leaf_order;

// join.cc
//extern int NUM_THREADS;
extern vector< vector<int64_t> > table[11];    // these save table_info.
extern int64_t num_of_record_in_table[11];
extern int l_key_index, r_key_index; // for key of multimap
extern unordered_multimap<int64_t , vector<int64_t> > u_map; //make hash_map
extern vector<vector<int64_t> > result;        // result_ of hash join
extern int left_table_index_in_hash;
extern int right_table_index_in_hash;
extern int64_t total_record;



// multi-thread
extern pthread_t threads[NUM_THREADS];         // for threads.
extern pthread_mutex_t mutex;
extern pthread_cond_t cond;
extern int range_start, range_end; // for divide range.
extern bool left_table_hash_or_right_table_hash;
extern int is_thread_finished[NUM_THREADS];

// check time
extern float query_optimizer_time;
extern float query_parser_time;
extern float query_join_time;               // query_join_time
extern float query_sum_time;
extern float clear_time;                     // check clear time
extern float query_control_time;
extern float hash_join_time;
extern float set_table_location_time;
extern float left_table_hash_time;
extern float right_table_hash_time;
extern float sum_thread_result_time;

// FUNCTION PROTOTYPES.
//filemanager

pagenum_t file_alloc_page(int table_id);
int open_db(int table_id,char* pathname, int num_column);
void init_file(FILE*fi , int num_column);
void file_free_page(int table_id, pagenum_t pagenum);
void file_read_page(int table_id, pagenum_t pagenum, page_t* dest);
void file_write_page(int table_id, pagenum_t pagenum, const page_t* src);

// Output and utility.

void notice_my_bplustree();
void enqueue( int64_t * q , page_t * page, int queue_sz );        //출력 시 queue 이용. enqueue
int64_t dequeue( int64_t * q, int queue_sz );                 // dequeue
int path_to_root(int table_id, page_t * tm);          // path로 부터 root 길이 구해줌
void print_leaves(int table_id);               // leaves 만 쭉 print
void print_tree(int table_id);                 // bfs 로 tree를 출력하시오
void find_and_print(int table_id, int64_t key);        // 키가 있는지 없는지 일단 체크함
page_t * find_leaf( int table_id, page_t * root, int64_t key);         // key 가 있을 leaf를 반환해줌. 만약 root 가 null이면, null를반환해줌.   
int64_t * find(int table_id, int64_t key); // key가 tree에 있는지 확인함 있으면, 그 위치가 return 없으면 null
int cut( int length );          //merge 또는 split를 위해서, 중간치 (한 노드당 최소한 가져야하는 값의 개수)를 계산해줌.
page_t * find_root(int table_id); // root page 를 찾아 그 page의 주소를 return 하는 함수

// Insertion.

record * make_record(int64_t key, int64_t* value);    //입력 받은 value를 가지고 record 만듬
page_t * make_page( int table_id );           //node를 하나 만들고, 할당받고, default값들을 입력함.
page_t * make_leaf( int table_id );           //make node를 해서 leaf 로 받고, is_leaf=true로 바꾼 다음에 node를 반환
int get_left_index(page_t * parent, page_t * left);         // 해당되는 node의 index를 찾아서 return 하자.
void insert_into_leaf( int table_id, page_t * leaf, int64_t key, record * pointer );      // leaf 가 꽉차지 않으면 그냥 위치에 넣으면 끝
void insert_into_leaf_after_splitting( int table_id, page_t * leaf, int64_t key,      // 꽉 찼다면 new_leaf 만들고 반띵한 다음에 insert parent로넘김
                                        record * pointer);
void insert_into_node( int table_id, page_t * parent,  //key와 pointer를 받았으니, pointer에 해당되는 index에 key를 넣기
        int left_index, int64_t key, page_t * right);
void insert_into_node_after_splitting(int table_id, page_t * parent,             // splitting 한다음에 생기는 key를 또 위로 올리기 //root 될때까지
                                int left_index, int64_t key, page_t * right);
void insert_into_parent( int table_id, page_t* left, int64_t key, page_t * right);  //splitting하고 나면, left와 right가 나오고 그 key(right 첫번째)를 가지고 insert진행 
void insert_into_new_root( int table_id,page_t * left, int64_t key, page_t * right);         //insert를 하면서, root가 위로쌓아야한다면 root 만들기
void start_new_tree(int table_id, int64_t key, record * pointer, page_t * root);       // 새로운 루트 만듦. is_leaf = true 이고, key, pointer 한개씩 넣음.
int insert( int table_id, int64_t key, int64_t * value );       //insert 하기

// Deletion.

int64_t get_neighbor_offset(int table_id, page_t * n );           //redistribute 를 위한 sibling 찾기
int64_t find_left_leaf_offset(int table_id,page_t * n);
int adjust_root(int table_id,page_t * root);              // root의 첫 자식을 root로 조정
int coalesce_nodes(int table_id,page_t * neighbor, page_t * n, page_t * parent);
int redistribute_nodes(int table_id,page_t * neighbor, page_t * n , page_t * parent);
int delete_entry(int table_id, page_t * n, int64_t key, int pointer_index);          // key에 해당하는 자료를 삭제하는 것.
int erase( int table_id, int64_t key );          // 일단, delete 신호를 처음 받을 때,
void remove_entry_from_node(page_t * n, int key_index, int pointer_index);

// buffer

void buffer_init( buffer* buffer);
void buffer_init_without_list(buffer* iter);
void free_buffer_pool(buffer* iter);
void set_buffer_list(buffer* curbuffer);
int init_db(int num_buf);
int open_table(char * pathname, int num_column);
int close_table(int table_id);
int shutdown_db();
buffer* select_buffer();
buffer* find_in_pool(int table_id, pagenum_t pagenum);
void buffer_put_page( int table_id , pagenum_t pagenum, page_t * src );
void buffer_get_page( int table_id ,pagenum_t pagenum, page_t * dest);
void buffer_free_page(int table_id, pagenum_t pagenum);
pagenum_t buffer_alloc_page(int table_id);

// join

vector<query_op> query_parser(string query);
vector<query_op> query_planning(vector<query_op> old_querys);
vector<vector<int64_t> > get_table(int64_t table_id);
int get_index(query_op query, int left_or_right);
void hash_join(query_op query, int left_or_right);
int64_t join(const char* query);
void set_left_n_right_table( query_op& query, int check_num);
void* input_to_hash(void* thread_num);

// 안쓰는 함수들 

//void license_notice( void );            // license 버전을 적어놓은것.
//void print_license( int licence_part );         // license 출력
//void usage_1( void );           // b+ 트리 설명에 대해
//void usage_2( void );           // b+ 트리 사용가능한 기능에 대해
//void usage_3( void );           // order 정보 출력
//int height( node * root );              // tree의 height 를 구해줌
//void find_and_print_range(node * root, int range1, int range2, bool verbose); // range에 해당하는 내용 다 출력하기
//int find_range( node * root, int key_start, int key_end, bool verbose,          //range에 해당하는 것들 다 returned_key, pointers 에넣기
//       int returned_keys[], void * returned_pointers[]); 
//void destroy_tree_nodes(node * root);           // 재귀적으로 tree의 할당된 것들을 free 해줌.
//node * destroy_tree(node * root);               // tree를 destroy 해주는 것.

#endif /* __BPT_H__*/


