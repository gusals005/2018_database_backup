/*
 *  bpt.c
 */
#define Version "1.14"

#ifndef __BPT_H__

#include "bpt.h"
#endif
// GLOBALS.

/*
    default values set here.
*/
int internal_order = 249;   // order of internal_node 249
int leaf_order = 32;        // order of leaf node 32
int order = DEFAULT_ORDER;  // order of default which is fixed '4'.

/*
 *   FUNCTION DEFINITIONS.
 */

// OUTPUT AND UTILITIES
// PRINT / FIND

/*
    notice my program
*/
void notice_my_bplustree(){
    printf("------------------------\n");
    printf("It is test main, you can use only these functions.\n");
    printf("if you press\n\n---Opendb---\no : opendb and link with b+tree\n");
    printf("---Find&Print---\n");
    printf("f <key> : find key in b+tree\n");
    printf("t : print all element\n");
    printf("l : print only leaves in b+tree\n");
    printf("---Insert---\n");
    printf("i <key> <values> : input key-value pair\n");
    printf("a <number> : insert array(key : 1 to number / value : '0' + key %c 10 \n", '%');
    printf("---Delete---\n");
    printf("d <key> : find the key-value and delete it\n");
    printf("---Join---\n");
    printf("j <query> : you can receive return value\n"); 
    printf("---Exit---\n");
    printf("q : exit this program\n");
    printf("\n-----------------------\n");
}
/*
 * These are used in queue for implementing print_tree(function).
 */

void enqueue( int64_t * q , page_t * page, int queue_sz ) {       //queue 에 page_offset 넣기
    int i, j=0;
    int original = queue_sz-page->num_of_keys-1;
    q[original] = page->special_page_offset;
    for( i=original+1 ; i<queue_sz; i++,j++ )
        q[i] = page->page_offset[j];
}

int64_t dequeue( int64_t * q , int queue_sz) {                    //queue 에서 첫 page_offset 빼내기 
    int64_t first = q[0];
    int i=1;
    for( ; i< queue_sz; i++){
        q[i-1] = q[i];
    }
    return first;
}


/* 
    calculate length of leaf & root
*/
int path_to_root(int table_id, page_t * tm){
    int n=0;
    int64_t head_offset = tm->next_offset;
    
    page_t * parent = (page_t *)malloc(sizeof(page_t));
    while( head_offset != 0){
        n++;
        parent->page_type = 1;
        buffer_get_page(table_id, head_offset/PAGE_SIZE,parent);
        head_offset = parent->next_offset;
        buffer_put_page(table_id,parent->pagenum,parent);
    }
    free(parent);
    return n;
}

/*
 * only print tree's leaf pages' key and value
 * this function can check whether tree's leaves are linked list
*/

void print_leaves(int table_id) {      // leaves 를 출력하는 문
    int i;
    page_t * c = find_root(table_id);       // 우선 root 를 가져오고,
    buffer_put_page(table_id, c->pagenum, c);
    if (c->num_of_keys == 0) {
        printf("Empty tree.\n");
        return;
    }
    //가장 왼쪽 자식으로 이동
    while (!c->is_leaf){
        c->page_type = 1;
        buffer_get_page(table_id, c->special_page_offset/PAGE_SIZE, c);
        buffer_put_page(table_id, c->pagenum, c);
    }     
            
    while (true) {      // 내꺼 출력하고 내 sibling들 출력.
        for (i = 0; i < c->num_of_keys; i++) {
            printf("%ld ", c->key[i]);
        }
        if ( c->special_page_offset > 0) {       // order - 1 index에는 다음 leaf의 주소가 들어가 있음.
            printf(" | ");
            c->page_type = 2;
            buffer_get_page(table_id,c->special_page_offset/PAGE_SIZE,c);
            buffer_put_page(table_id, c->pagenum, c);
        }
        else
            break;
    }
    free(c);
    printf("\n");
}


// index 가 어떻게 생겼는지 출력하는 함수.
void print_tree(int table_id) {       //root 를 입력을 하면
    
    page_t * header = (page_t*)malloc(sizeof(page_t));
    header->page_type = 0;
    buffer_get_page(table_id,0,header);
    buffer_put_page(table_id,0,header); 
    page_t * temp = find_root(table_id);            // root page 업
    buffer_put_page(table_id,temp->pagenum,temp);
    
    page_t * temp_parent = (page_t *)malloc(sizeof(page_t));    //부모가 존재할 것을 대비
    int temp_parent_check = 0;              // 그 부모 page가 pinup 되어 있는지 체크
    int i = 0;
    int rank = 0;
    int new_rank= 0;
    int queue_sz= 0;

    int64_t * q = (int64_t *)malloc(sizeof(int64_t)); //queue 배열

   // printf("temp의 0번째 키 : %ld ", temp->key[0]);
    if (temp->num_of_keys == 0) { // root 가 null 이면 empty
        printf("Empty tree.\n");
        return;
    }
    q[0] = temp->pagenum * PAGE_SIZE;
    queue_sz++;
    while( queue_sz != 0 ) {
        
        int64_t pg_temp = dequeue(q,queue_sz);         // 우선 pop 한번 하고, 그 값을 n 에 넣기
        queue_sz--;
        // free(temp);
        // temp = (page_t *)malloc(sizeof(page_t));
        temp->page_type = 1;
        buffer_get_page(table_id, pg_temp/PAGE_SIZE, temp); //다음 page pinup++
        buffer_put_page(table_id, temp->pagenum,temp);
        if( temp-> next_offset != 0){
            // free(temp_parent);
            // temp_parent = (page_t *)malloc(sizeof(page_t));
            temp_parent->page_type = 1;
            temp_parent_check = 1;
            buffer_get_page(table_id , temp->next_offset/PAGE_SIZE, temp_parent);
            buffer_put_page(table_id,temp_parent->pagenum,temp_parent);
        }
        if (temp->next_offset != 0 && pg_temp == temp_parent->special_page_offset) {     // pop을 한 node의 parent 가 null이 아니고, 그 parent의 첫번째 pointer가 나라면,
                rank = path_to_root( table_id, temp );
                if( new_rank != rank){
                    new_rank = rank;
                    printf("\n");
                }
        }       
        if (!temp->is_leaf){ // is_leaf 가 1 이면, leaf 인듯, leaf 가 아니니 pointers[i]를 queue에 넣음.
            //printf("page : %ld ",temp->pagenum);
            for (i = 0; i < temp->num_of_keys; i++) {     /// n 의 num_keys 에 대해서, 출력
                printf("%ld ", temp->key[i]);
            }

            queue_sz += temp->num_of_keys+1;
            q = (int64_t *)realloc(q,sizeof(int64_t)*(queue_sz+10));
            enqueue(q, temp, queue_sz);
        }
        else{
            //printf("page : %ld ",temp->pagenum);
            for (i = 0; i< temp->num_of_keys ;i++){
                printf("%ld :",temp->key[i]);
                for( int k=0; k<header->num_of_columns ; k++)
                    printf("%ld ",temp->value[i][k]);
                printf(",");
            }
        }
        printf("| ");   // dequeue 한번 하고 | 출력
    }
    printf("\n");   // 모든 출력이 끝나면 엔터

    free(q);
    free(temp);
    free(temp_parent);
    free(header);
}


/*
 * Finds the record under a given key and prints an
 * appropriate message to stdout.
 */
void find_and_print( int table_id , int64_t key) {   // key를 넘기고, verbose를 넘기는데, p 이면 true, f이면 false
    page_t* header = (page_t *)malloc(sizeof(page_t));
    header->page_type = 0;
    buffer_get_page(table_id,0,header);
    buffer_put_page(table_id, 0, header);
    int i;

    int* receive_find_result = (int*)malloc(sizeof(int));
    int64_t * r = find( table_id, key, 0, receive_find_result);      // key 가 존재하는지 find를 하고,
    if (r == NULL)      //  찾아봤는데 없으면, record 존재하지 않는다.
        printf("Record not found under key %ld.\n", key);
    else{   //존재하면, //key 와 value 를 출력
        printf("key %ld : ", key);
        for( i = 0 ; i < header->num_of_columns;  i++)
            printf("%ld ", r[i]);
        printf("\n");
    }          
    free(header);
}


/* find leaf and return leaf_page.
*/
page_t * find_leaf( int table_id, page_t * root, int64_t find_key ) {    //leaf
    int i = 0;
    page_t * c = root;
    int64_t tp;
    if (root->num_of_keys == 0) {        //root 가 null 이면 ,못찾겠다. 말하고, root 반환
        buffer_put_page(table_id,c->pagenum,c);
        return NULL;
    }
    while (!c->is_leaf) {       // root 가 잎사귀가 아니라면    마지막 노드가 되면 ,while 문 빠져나감
        i = 0;
        while (i < c->num_of_keys) {   // i 가 root의 keys 개수보다 작을 때까지
            if (find_key >= c->key[i]) i++; // key 보다 크면 i++
            else break;
        }
        if( i== 0)
            tp = c->special_page_offset/PAGE_SIZE;
        else
            tp = c->page_offset[i-1]/PAGE_SIZE;
        buffer_put_page(table_id,c->pagenum,c);
        // free(c);
        // c=(page_t *)malloc(sizeof(page_t));
        c->page_type = 1;
        buffer_get_page(table_id,tp, c); // c를 그 i번째 index로 바꿈
    }
    return c;       // 빠져나오고 leaf 반환
}


// node spliting 에서 쓰임.
int cut( int length ) {
    if (length % 2 == 0)
        return length/2;
    else
        return length/2 + 1;
}


/*
    find root's position and return address
*/
page_t * find_root(int table_id){
    page_t * header = (page_t *)malloc( sizeof(page_t));
    page_t * root =(page_t *)malloc( sizeof(page_t));

    header->page_type = 0;
    buffer_get_page(table_id, 0,header);
    root->page_type = 1;
    buffer_get_page(table_id, header->root_page_offset/PAGE_SIZE, root);
    buffer_put_page(table_id,0,header);
    free(header);
    return root;
}


// INSERTION

/* Creates a new record to hold the value
 * to which a key refers.
 */
record * make_record(int64_t key,int64_t * value) {       // record 만들기
    int i;
    record * new_record = (record *)malloc(sizeof(record));     // struct record 에 대해서 malloc
    if (new_record == NULL) {       // new_record가 null이라면,(malloc 실패))
        perror("Record creation.");
        exit(EXIT_FAILURE);
    }
    else {  // record value 에 value 넣자
        new_record->key = key;
        for( i = 0 ; i< 15 ; i++)
            new_record->value[i] = value[i];
    }
    return new_record;
}


/* Creates a new general node, which can be adapted
 * to serve as either a leaf or an internal node.
 */
page_t * make_page( int table_id ) {      //node 새로만들기
    pagenum_t alloc_pg = buffer_alloc_page(table_id);            //node를 할당받고 pin nothing.
    
    int i;
    page_t * new_page = (page_t *)malloc(sizeof(page_t));
    new_page->page_type =3;
    buffer_get_page(table_id, alloc_pg, new_page);      // alloc 받은 page pinup.
    if (new_page == NULL) {     //할당 오류잡고
        perror("Node creation.");
        exit(EXIT_FAILURE);
    }
    for( i=0; i<256 ; i++ )   // keys에 대해서 order-1로 갯수 제한함
        new_page->key[i]= 0;

    for( i =0;  i<32 ; i++){
        for( int k=0; k<15 ;k++)
            new_page->value[i][k] = 0;
    } //pointer 들은 key보다 한개 많음
 
    for( i = 0 ; i<256; i++)
        new_page->page_offset[256] = 0;

    new_page->is_leaf = 0;  //default 설정
    new_page->num_of_keys = 0;
    new_page->next_offset = 0;
    new_page->special_page_offset = 0;
    new_page->page_type = -1;
    new_page->pagenum = alloc_pg;
    new_page->num_of_page = 0;
    new_page->root_page_offset = 0;
    return new_page;
}


/* Creates a new leaf by creating a node
 * and then adapting it appropriately.
 */
page_t * make_leaf( int table_id ) {      // make node를 해서 leaf 로 받고, is_leaf=true로 바꾼 다음에 node를 반환
    page_t * leaf = make_page(table_id);
    leaf->is_leaf = 1;
    leaf->page_type = 3;
    return leaf;
}


/* Helper function used in insert_into_parent
 * to find the index of the parent's pointer to
 * the node to the left of the key to be inserted.
 */
int get_left_index(page_t * parent, page_t * left) {        

    int left_index = 0;
    if(parent->special_page_offset == left->pagenum *PAGE_SIZE){
        return 0;
    }
    left_index++;
    while (left_index <= parent->num_of_keys &&
            parent->page_offset[left_index-1] != left->pagenum * PAGE_SIZE)
        left_index++;
    return left_index;
}

/* Inserts a new pointer to a record and its corresponding
 * key into a leaf.
 * Returns the altered leaf.
 */
void insert_into_leaf( int table_id, page_t * leaf, int64_t key, record * pointer ) {     //key와 pointer를 받았으니, pointer에 해당되는 key를 넣기

    int i, insertion_point;
    int j;

    insertion_point = 0;
    while (insertion_point < leaf->num_of_keys && leaf->key[insertion_point] < key)   // 어디에 넣어야하는지, linear하게 search.
        insertion_point++;

    for (i = leaf->num_of_keys; i > insertion_point; i--) {        //key와 pointer 를 한칸 씩 뒤로 미루고
        leaf->key[i] = leaf->key[i - 1];
        for( j =0 ; j<15 ; j++)
            leaf->value[i][j] = leaf->value[i-1][j];
    }
    leaf->key[insertion_point] = key;      // 그자리에 key, pointer 넣기
    for( j =0;  j<15 ; j++)
        leaf->value[insertion_point][j] = pointer->value[j];

    leaf->num_of_keys++;
    buffer_put_page(table_id, leaf->pagenum, leaf); // leaf 넣으면 끝
}


/* Inserts a new key and pointer
 * to a new record into a leaf so as to exceed
 * the tree's order, causing the leaf to be split
 * in half.
 */
void insert_into_leaf_after_splitting(int table_id, page_t * leaf, int64_t key, record * pointer) {      //splitting 해보자
    //leaf put 해주어야함.

    page_t * new_leaf;        // 새로운 leaf 가 필요하고
    int64_t * temp_keys;        // 중간 저장 keys인듯
    int64_t temp_pointers[32][15];  // 중간 저장 pointers 인듯
    int insertion_index, split, new_key, i, j;

    new_leaf = make_leaf(table_id); // 일단 새로운 leaf를 만들고    new leaf pinup
    new_leaf->page_type = 2;
    new_leaf->is_leaf = 1;

    temp_keys = (int64_t *)malloc( leaf_order * sizeof(int64_t) );  // key를 malloc하고
    if (temp_keys == NULL) {        // 할당 오류면 program 종료
        perror("Temporary keys array.");
        exit(EXIT_FAILURE);
    }

    insertion_index = 0;        // 일단 index 는 0으로
    while (insertion_index < leaf_order - 1 && leaf->key[insertion_index] < key)    // 내 key에 해당되는 위치를 찾아
        insertion_index++;

    for (i = 0, j = 0; i < leaf->num_of_keys; i++, j++) {  //우선 내 밑으로 해당되는 key들을 저장을 함.
        if (j == insertion_index) j++;  // index까지 오면, j를 하나 더 ++함.
        temp_keys[j] = leaf->key[i];
        for( int k =0; k< 15 ; k++)
            temp_pointers[j][k] = leaf->value[i][k];
    }

    temp_keys[insertion_index] = key;   //그리고, 임시저장을 하고 난다음에, 내가 있어야 할 위치에 새로운 key, pointer 를 넣음
    for( int k = 0 ; k<15 ; k++)
        temp_pointers[insertion_index][k] = pointer->value[k];

    leaf->num_of_keys = 0;     // leaf의 num을 0으로 바꾸어버림
    new_leaf->num_of_keys = 0;
    split = cut(leaf_order - 1); //order -1 / 2 를 함. order 가 홀수면, +1해주고 (ex, 5 -> 3) (4 -> 2)

    for (i = 0; i < split; i++) {   // 0~ split 전까지  원래 leaf에다가 데이터 넣어줌
        for( int k=0;k<15; k++)
            leaf->value[i][k] = temp_pointers[i][k];
        leaf->key[i] = temp_keys[i];
        leaf->num_of_keys++;
    }

    for (i = split, j = 0; i < leaf_order; i++, j++) {   // 그리고 new_leaf에다가 그 다음것들 넣어줌
        for( int k =0 ; k<15 ; k++)
            new_leaf->value[j][k] = temp_pointers[i][k];
        new_leaf->key[j] = temp_keys[i];
        new_leaf->num_of_keys++;
    }
    free(temp_keys);    // temp 는 이제 다썼으니 삭제

    new_leaf->special_page_offset = leaf->special_page_offset;  // 원래 leaf에 연결되었던 pointer를 new_leaf로 change
    leaf->special_page_offset = new_leaf->pagenum * PAGE_SIZE ;   // 원래 leaf에 있던 것들은 new_leaf를 가르키게 함.

    //원래 들어가있던 주소들은 다 nll로 바꾸어줌
    for (i = leaf->num_of_keys; i < leaf_order; i++){
        for(int k=0; k<15 ; k++)
            leaf->value[i][k] = 0;
    }    
    for (i = new_leaf->num_of_keys; i < leaf_order; i++){    // new_leaf도 마찬가지.
        for(int k=0; k<15 ; k++)
            new_leaf->value[i][k] = 0;
    }
        
    new_leaf->next_offset = leaf->next_offset;    // new_leaf의 parent는 원래 leaf의 parent를 가르키게 바꿈
    new_key = new_leaf->key[0];        // new_leaf의 첫번째 key를 위로 올림.

    buffer_put_page(table_id, leaf->pagenum, leaf);
    buffer_put_page(table_id, new_leaf->pagenum, new_leaf); // leaf와 new_leaf를 put을 해주었지.

    insert_into_parent(table_id, leaf, new_key, new_leaf);   // 이제 parent가 하나 늘었구나.
    free(new_leaf);
}


/* Inserts a new key and pointer to a node
 * into a node into which these can fit
 * without violating the B+ tree properties.
 */
void insert_into_node(int table_id,  page_t * n,  // left_index 와 right 가 있고, n(parent고 지금은 현재 노드)을 받았지.
        int left_index, int64_t key, page_t * right) {
    int i;

    for (i = n->num_of_keys; i > left_index; i--) {        // left_index까지 정보를 한칸씩 뒤로 밈
        n->page_offset[i] = n->page_offset[i-1];
        n->key[i] = n->key[i - 1];
    }
    n->page_offset[left_index] = right->pagenum *PAGE_SIZE;    // 그다음에 left_index에 해당하는 곳에 key와 right를 넣음
    n->key[left_index] = key;
    n->num_of_keys++;
    buffer_put_page(table_id, n->pagenum, n);
}


/* Inserts a new key and pointer to a node
 * into a node, causing the node's size to exceed
 * the order, and causing the node to split into two.
 */
void insert_into_node_after_splitting(int table_id,  page_t * old_node, int left_index,       //left index 가져오고, right와 key가 잇음
        int64_t key, page_t * right) {
    // old node pin up 만 신경쓰면됨.

    int i, j, split;
    int64_t k_prime;           // k_prime 이 멀까?
    page_t * new_page;   // child와 new_node 만들기
    int64_t * temp_keys;        // temp_keys, temp_pointer 만들기
    int64_t * temp_page_offset;

    /* First create a temporary set of keys and pointers
     * to hold everything in order, including
     * the new key and pointer, inserted in their
     * correct places.
     * Then create a new node and copy half of the
     * keys and pointers to the old node and
     * the other half to the new.
     */

    temp_page_offset = (int64_t *)malloc( (internal_order + 1) * sizeof(int64_t *) );    // 임시저장 공간 만들기 split 과는 달리 숫자하나씩 더 할당받음
    if (temp_page_offset == NULL) {
         perror("Temporary pointers array for splitting nodes.");
         exit(EXIT_FAILURE);
    }
    temp_keys = (int64_t *)malloc( internal_order * sizeof(int64_t) );
    if (temp_keys == NULL) {
        perror("Temporary keys array for splitting nodes.");
        exit(EXIT_FAILURE);
    }

    temp_page_offset [0] = old_node->special_page_offset;
    for (i = 0, j = 1; i < old_node->num_of_keys; i++, j++) {  // 내가 들어갈 위치 빼고 다 복사하기
        if (j == left_index+1) j++;
        temp_page_offset[j] = old_node->page_offset[i];
    }

    for (i = 0, j = 0; i < old_node->num_of_keys; i++, j++) {
        if (j == left_index) j++;
        temp_keys[j] = old_node->key[i];
    }

    int k=0;
    temp_page_offset[left_index+1] = right->pagenum * PAGE_SIZE;      // 내가 들어갈 위치에 값 넣기
    temp_keys[left_index] = key;
     /* Create the new node and copy
     * half the keys and pointers to the
     * old and half to the new.
     */
    split = cut(internal_order);         // order를 반으로 짤라서 split 중간  만들기
    new_page = make_page(table_id);     // node를 하나 만들기  newpage pindown도 해야함.
    new_page->page_type = 1;
    old_node->num_of_keys = 0;

    old_node->special_page_offset = temp_page_offset[0];

    for (i = 0; i < split - 1; i++) {       // split -1 까지는,임시저장된 pointer들을 넣고
        old_node->page_offset[i] = temp_page_offset[i+1];
        old_node->key[i] = temp_keys[i];
        old_node->num_of_keys++;
    }
    
    //old_node->page_offset[i] = temp_page_offset[i];       // 왼쪽 자리 맨 오른쪽 pointer에는 중간에 해당하는 pointer 넣고
    k_prime = temp_keys[split - 1];     // k_prime : key  를 하나 정의 하고
    ++i;

    new_page->special_page_offset = temp_page_offset[i];
    for ( ++i, j = 0; i < internal_order+1; i++, j++) {     // 위로는 또 key와 pointer들을 넣음
        new_page->page_offset[j] = temp_page_offset[i];
        new_page->key[j] = temp_keys[i-1];
        new_page->num_of_keys++;
    }
    free(temp_page_offset);      //임시 저장된 것들을 다 free 해줌
    free(temp_keys);

    page_t * child = (page_t *)malloc(sizeof(page_t));

    new_page->next_offset = old_node->next_offset;        // parent는 같게 해줌

    for (i = -1; i < new_page->num_of_keys; i++) {     // new_node들의 자식들이 new_node를 parent로 가르키게 만듬)
        int64_t child_page_offset;
        if( i == -1)
            child_page_offset= new_page->special_page_offset;
        else
            child_page_offset = new_page->page_offset[i];
        child->page_type = 1;
        buffer_get_page(table_id, child_page_offset/PAGE_SIZE,child);
        child->next_offset = new_page->pagenum * PAGE_SIZE;
        buffer_put_page(table_id, child->pagenum,child);
    }

    /* Insert a new key into the parent of the two
     * nodes resulting from the split, with
     * the old node to the left and the new to the right.
     */
    buffer_put_page(table_id,new_page->pagenum, new_page);
    buffer_put_page(table_id,old_node->pagenum, old_node);
    free(child);
    insert_into_parent(table_id,old_node, k_prime, new_page);
    free(new_page);      // 다시 parent를 넣어줌
}



/* Inserts a new node (leaf or internal node) into the B+ tree.
 * Returns the root of the tree after insertion.
 */
void insert_into_parent(int table_id, page_t * left, int64_t key, page_t * right) {    //부모로 넘어왔다면,

    //parent로 넘어와서는 아무런 pin 이 up 되어 있지는 않음.

    int left_index; //left_index와
    
    page_t * parent = (page_t *)malloc(sizeof(page_t));  //parent를 일단 선언하고

    /* Case: new root. */

    if (left->next_offset == 0){     // 만약 parent 가 없다면, root 라면
        insert_into_new_root(table_id,left, key, right);  // new root를 생성하자
        free(parent);
        return;
    }
    /* Case: leaf or node. (Remainder of
     * function body.)
     */
    parent->page_type = 1;
    buffer_get_page(table_id, left->next_offset/PAGE_SIZE, parent);// 부모 노드 를 지금 pin up한것.

    /* Find the parent's pointer to the left
     * node.
     */

    left_index = get_left_index(parent, left);      //일단, left_index를 가져오자
   
    /* Simple case: the new key fits into the node.
     */
   
    
    if (parent->num_of_keys < internal_order - 1){   // 만일 parent가 자리가 남으면 
        insert_into_node(table_id, parent, left_index, key, right);     // parnet pin down 해줬음.
        free(parent);
        return;
    }
    /* Harder case:  split a node in order
     * to preserve the B+ tree properties.
     */

    //여기도 parent만 신경쓰면 됨. 만약에 right가 바꾸면 right 다시 overwrite하면됨.
    insert_into_node_after_splitting(table_id, parent, left_index, key, right); // 그게 아니라면, splitting 해서 또 넣자.
    free(parent);
}


/* Creates a new root for two subtrees
 * and inserts the appropriate key into
 * the new root.
 */
void insert_into_new_root(int table_id, page_t * left, int64_t key, page_t * right) {       // insert를 하면서, root가 위로쌓아야한다면

    page_t * root = make_page(table_id);              //root를 만들고 -> root page를 새로만든것.
    page_t * header = (page_t *)malloc(sizeof(page_t));
    header->page_type = 0;
    buffer_get_page(table_id,0,header);

    root->key[0] = key;                    //key를 넣고
    root->special_page_offset = left->pagenum * PAGE_SIZE;
    root->page_offset[0] = right->pagenum*PAGE_SIZE;              //내 parent는 없고, 내 자식들의 parent는 나로 지정.
    root->num_of_keys++;
    root->next_offset = 0;
    root->page_type = 1;
    left->next_offset = root->pagenum * PAGE_SIZE;
    right->next_offset = root->pagenum * PAGE_SIZE;
    header->root_page_offset = root->pagenum * PAGE_SIZE;
    buffer_put_page(table_id, 0, header);
    buffer_put_page(table_id , root->pagenum , root);
    buffer_put_page(table_id ,left->pagenum,left);
    buffer_put_page(table_id ,right->pagenum,right);
    free(root);
    free(header);
}



/* First insertion:
 * start a new tree.
 */
void start_new_tree(int table_id, int64_t key, record * pointer, page_t* root2) {      // 새로운 root page 만들기

    int i=0;
    page_t * root = root2;
    root->key[0] = key;        //  key 넣고, pointer넣고, 마지막 order-1 에는 null을 넣고, parent도 null.
    for( i=0; i<15 ; i++ )
        root->value[0][i] = pointer->value[i];
    root->special_page_offset = -1;
    root->next_offset = 0;
    root->num_of_keys++;
    root->page_type = 2;
    buffer_put_page(table_id, root->pagenum, root);
    free(root);
}



/* Master insertion function.
 * Inserts a key and an associated value into
 * the B+ tree, causing the tree to be adjusted
 * however necessary to maintain the B+ tree
 * properties.
 */
int  insert( int table_id, int64_t key, int64_t * value ) {      // insert를 하자

    fi = fp_arr[table_id].fi;

    record * pointer;       // 일단 record* pointer를 갖고
    page_t * leaf;            // leaf 노드 하나 생성
    page_t * root;
    
    
    int* receive_find_result = (int*)malloc(sizeof(int));
    if (find(table_id , key, 0, receive_find_result ) != NULL)     //우선, 그 값이 있는지 찾아.NULL이 아니라면 -> return 을 시켜라 ( NULL이 아니라는 건 노드가 있다는것) ),
         return -1;            // null 이 아니라면, 이미 그값을 가진 record가 있다는 것.

    root = find_root(table_id);     //root pinup
    pointer = make_record(key,value);       // 그다음, record를 만들자

    //root에 값이 하나도 없을 땐,    
    if (root->num_of_keys == 0){                   //root가 null인지 확인하고
         start_new_tree(table_id, key, pointer, root);    //null이면, 새로운 트리 만들어야함.
         free(pointer);
         return 0;
    }

    //leaf 노드가 하나라도 존재한다면
    leaf = find_leaf(table_id, root, key);  //leaf node pinup. root pindown.

    if( leaf == NULL){
        buffer_put_page(table_id, leaf->pagenum, leaf);
        return -1;    
    }
    
    //그냥 그 leaf node 에 넣으면 끝날때,
    if (leaf->num_of_keys < leaf_order - 1) {          // leaf 안에 있는 key들이 order-1보다 작을때
        insert_into_leaf(table_id, leaf, key, pointer);    //leaf 에 key 넣기, pointer는 record*
        free(pointer);
        free(root);
        return 0;                            // 그리고 root 리턴
    }

    //split 이 필요하다면,
    insert_into_leaf_after_splitting(table_id, leaf, key, pointer);      // split이 필요하면 함수 실행해야함.
    free(root);
    free(pointer);
    return 0;
}



// DELETION.

/* Utility function for deletion.  Retrieves
 * the index of a node's nearest neighbor (sibling)
 * to the left if one exists.  If not (the node
 * is the leftmost child), returns -1 to signify
 * this special case.
 */

int64_t get_neighbor_offset( int table_id, page_t * n ) {
    buffer_put_page(table_id, n->pagenum, n);
    int i;
    page_t * parent  = (page_t*)malloc(sizeof(page_t));
    parent->page_type = 1;
    buffer_get_page(table_id, n->next_offset/PAGE_SIZE, parent);
    int64_t return_offset;

    if(n-> next_offset ==0) // n이 root라면,
        return -1;

    /* Return the index of the key to the left
     * of the pointer in the parent pointing
     * to n.
     * If n is the leftmost child, this means
     * return -1.
     */
    if( n->pagenum * PAGE_SIZE == parent -> special_page_offset){
        return_offset = parent->page_offset[0];
    }
    else if ( n->pagenum * PAGE_SIZE == parent -> page_offset[0] ){
        return_offset = parent->special_page_offset;
    }
    else{
        
        for (i = 1; i <= parent->num_of_keys; i++)
            if (parent->page_offset[i] == n->pagenum * PAGE_SIZE)
                return_offset = parent->page_offset[i-1];
    }
    buffer_put_page(table_id,parent->pagenum,parent);
    free(parent);
    return return_offset;
}


void remove_entry_from_node(page_t * n, int key_index, int pointer_index) {  //pointer 와 n(leaf node) 을 받고나면,


    int i,j, num_pointers;
 

    if(n->is_leaf){             // leaf node 라면,
        for( i= key_index+1 ; i<n->num_of_keys ; i++ ){
            n->key[i-1] = n->key[i];
            for(int k= 0; k<15; k++)
                n->value[i-1][k] = n->value[i][k];
        }      
        n->key[n->num_of_keys - 1] = 0;
        for(j=0; j<120 ; j++)
            n->value[n->num_of_keys-1][j] = '0';
        n->num_of_keys--;
    }
    else{                       // leaf_node 가 아니라면,
        for( i= key_index+1 ; i< n->num_of_keys ; i++ ){
            n->key[i-1] = n->key[i];
        }
        n->key[n->num_of_keys - 1] = 0;
        
        if( pointer_index == 0 ){
            n->special_page_offset = n->page_offset[0];
            for( i =1 ; i < n->num_of_keys ; i++)
                n->page_offset[i-1] = n->page_offset[i];
            n->page_offset[n->num_of_keys-1] = 0;
        }
        else{
            pointer_index--;
            for( i = pointer_index + 1 ; i < n->num_of_keys ; i++)
                n->page_offset[i-1] = n->page_offset[i];
            n->page_offset[n->num_of_keys-1] =0;
        }
        n->num_of_keys--;
    }
}


int adjust_root(int table_id, page_t * root) {   // 새로운 루트를 만들 땐, 꼭, new_root의 자식들이 있는지 없는지 확인.

    page_t * new_root= (page_t*)malloc(sizeof(page_t));        // 새로운 root 를 만들고
    page_t * header = (page_t *)malloc(sizeof(page_t));
    new_root->page_type = 1;
    buffer_get_page( table_id, root->special_page_offset/PAGE_SIZE, new_root );
    header->page_type = 0;
    buffer_get_page(table_id,0,header);
    new_root->next_offset = 0;
    header->root_page_offset = new_root->pagenum * PAGE_SIZE;
    buffer_put_page(table_id,new_root->pagenum,new_root);
    buffer_put_page(table_id,0, header);
    buffer_put_page(table_id,root->pagenum,root);
    buffer_free_page(table_id,root->pagenum);
    free(new_root);
    free(header);
    
    return 0;       
}


/* Coalesces a node that has become
 * too small after deletion
 * with a neighboring node that
 * can accept the additional entries
 * without exceeding the maximum.
 */

int coalesce_nodes(int table_id,page_t * neighbor, page_t * n, page_t * parent){
    // 결국 page를 하나 삭제하게 된단말이지.
    
    int64_t k_prime; // parent에서 빠질 key & merge에서 끼어들 key
    int i=0;
    int result;
    page_t * child = (page_t *)malloc(sizeof(page_t));
    int64_t child_offset;
    if( n->pagenum * PAGE_SIZE == parent->special_page_offset){
        k_prime = parent->key[0];
        n->key[0] = k_prime;
        n->page_offset[0] = neighbor->special_page_offset;
        n->num_of_keys++;

        for ( i = 0 ; i < neighbor->num_of_keys ; i++){
            n->key[i+1] = neighbor->key[i];
            n->page_offset[i+1] = neighbor->page_offset[i];
            n->num_of_keys++;
        }
        for( i=0 ;i< n->num_of_keys+1 ; i++){
            child_offset = n->special_page_offset;
            if(i != 0)
                child_offset = n->page_offset[i-1];
            // free(child);
            // child = (page_t *)malloc(sizeof(page_t));
            child->page_type = 1;
            buffer_get_page(table_id,child_offset/PAGE_SIZE,child);
            child-> next_offset = n->pagenum * PAGE_SIZE;
            buffer_put_page(table_id,child->pagenum,child);
        }
        buffer_put_page(table_id,n->pagenum,n);
        buffer_put_page(table_id,neighbor->pagenum, neighbor);
        buffer_free_page(table_id,neighbor->pagenum);
        
        buffer_put_page(table_id,parent->pagenum,parent);
        result = delete_entry(table_id, parent, k_prime, 1 );
        free(child);
        return result;
    }
    else{
        i=0;
        while( i < parent->num_of_keys && parent->page_offset[i] != n->pagenum * PAGE_SIZE)
            i++;
        k_prime = parent->key[i];

        neighbor->key[ neighbor->num_of_keys ] = k_prime;
        neighbor->page_offset[neighbor->num_of_keys] = n->special_page_offset;
        neighbor->num_of_keys++;

        child_offset = neighbor->page_offset[neighbor->num_of_keys -1];
        child->page_type = 1;
        buffer_get_page(table_id, child_offset/PAGE_SIZE, child);
        child-> next_offset = neighbor->pagenum * PAGE_SIZE;
        buffer_put_page(table_id, child->pagenum,child);
        buffer_put_page(table_id,neighbor->pagenum,neighbor);
        buffer_put_page(table_id, n->pagenum, n);
        buffer_free_page(table_id,n->pagenum);
    
        
        // int j=0;
        // printf("k_prime : %ld\n",k_prime);
        // for( j =0 ; j<neighbor->num_of_keys ; j++)
        //     printf("%ld ",neighbor->key[j]);
        // printf("\n");
        buffer_put_page(table_id,parent->pagenum,parent);
        result = delete_entry(table_id , parent, k_prime, i+1);
        free(child);
        return result;
    }

}

/* Redistributes entries between two nodes when
 * one has become too small after deletion
 * but its neighbor is too big to append the
 * small node's entries without exceeding the
 * maximum
//  */

int redistribute_nodes(int table_id, page_t * neighbor, page_t * n, page_t * parent){
    //neighbor 에서 키를 하나 가져오는 거임.

    int64_t k_prime; // parent에서 빠질 key & merge에서 끼어들 key
    int i=0;
    int result;
    page_t * child = (page_t *)malloc(sizeof(page_t));
    int64_t child_offset;
    if( n->pagenum * PAGE_SIZE == parent->special_page_offset){
        k_prime = parent->key[0];
        n->key[0]=k_prime;
        n->page_offset[0] = neighbor->special_page_offset;
        n->num_of_keys++;
        parent->key[0] = neighbor->key[0];
        
        child -> page_type  = 1;

        buffer_get_page(table_id,n->page_offset[0]/PAGE_SIZE , child);
        child->next_offset = n->pagenum * PAGE_SIZE;
        buffer_put_page(table_id,child->pagenum , child);
        buffer_put_page(table_id,parent->pagenum, parent);
        buffer_put_page(table_id,n->pagenum , n);
        buffer_put_page(table_id, neighbor->pagenum, neighbor);
        result = delete_entry(table_id,  neighbor, neighbor->key[0] , 0 );
        free(child);
        return result;
    }
    else{
        i=0;
        while( i < parent->num_of_keys && parent->page_offset[i] != n->pagenum * PAGE_SIZE)
            i++;
        k_prime = parent->key[i];
        n->key[0] = k_prime;
        n->page_offset[0] = n->special_page_offset;
        n->special_page_offset = neighbor->page_offset[ neighbor->num_of_keys -1 ];
        n->num_of_keys++;
        parent->key[i] = neighbor->key[neighbor -> num_of_keys - 1];
        
        child->page_type =1;
        buffer_get_page(table_id,n->special_page_offset/PAGE_SIZE , child);
        child->next_offset = n->pagenum * PAGE_SIZE;
        buffer_put_page(table_id,child->pagenum , child);
        
        buffer_put_page(table_id, parent->pagenum , parent);
        buffer_put_page(table_id,n->pagenum, n);
        
        buffer_put_page(table_id,neighbor->pagenum, neighbor);
        result = delete_entry( table_id, neighbor, neighbor->key[ neighbor->num_of_keys -1 ] , neighbor->num_of_keys );
        free(child);
        return result;
    }


    return 1;
}


int64_t find_left_leaf_offset(int table_id, page_t * n){

    buffer_put_page(table_id,n->pagenum,n);
    page_t * root = find_root(table_id);
    buffer_put_page(table_id,root->pagenum,root);
    while( root->is_leaf != 1){
        root->is_leaf=1;
        buffer_get_page(table_id, root->special_page_offset/PAGE_SIZE, root);
        buffer_put_page(table_id,root->pagenum,root);
    }
    
    if( n->pagenum == root->pagenum  )
        return -1;
    else{
        while(root->special_page_offset != n->pagenum * PAGE_SIZE ){
            root->page_type = 1;
            buffer_get_page(table_id, root->special_page_offset/PAGE_SIZE, root);
            buffer_put_page(table_id, root->pagenum,root);
        }
        return root->pagenum * PAGE_SIZE;

    }
    

}

/* Deletes an entry from the B+ tree.
 * Removes the record and its key and pointer
 * from the leaf, and then makes all appropriate(적절한,  도용하다)
 * changes to preserve the B+ tree properties.
 */
// 성공여부에 따라 -1 or 0 리턴, root free해주어야함.
int delete_entry( int table_id,page_t * n, int64_t key, int pointer_index) { // delete key in n

    int64_t cur_page_offset = n->pagenum * PAGE_SIZE;
    int64_t neighbor_offset;
    int k_prime_index;
    int64_t k_prime;  // 부모 노드에서 지워져야할 key
    int64_t left_offset, right_offset;
    int i, result;
    page_t * parent = (page_t *)malloc(sizeof(page_t));
    // Remove key and pointer from node.

    if( n->is_leaf){        // leaf_node 인 상태에서 이 함수를 실행한다면,
        i = 0;
        while(i <n->num_of_keys && n->key[i] != key)
            i++;    
        remove_entry_from_node(n, i,i);
        if(n->num_of_keys >= 1){            // leaf node 위로 굳이 지울필요없을 때,
            free(parent);
            buffer_put_page(table_id, n->pagenum, n);
            return 0;
        }
        else{
            if( n->next_offset == 0){       // tree 안에 leaf가 하나밖에 없는 상황.
                parent->page_type =0;
                buffer_get_page(table_id, 0, parent);
                parent->root_page_offset = n->pagenum * PAGE_SIZE;
                buffer_put_page(table_id,0, parent);
                buffer_put_page(table_id, n->pagenum, n);
                free(parent);
                return 0;
            } 
            else{                           // 부모 노드가 존재하는 상황
                parent->page_type =1;
                buffer_get_page(table_id, n->next_offset/PAGE_SIZE , parent);
                

                pointer_index = 0;
                if( cur_page_offset != parent->special_page_offset){
                    i=0;
                    while( i < parent->num_of_keys && parent->page_offset[i] != cur_page_offset)
                        i++;
                    pointer_index = i+1;
                }

                
                if( pointer_index != 0)
                    k_prime_index = pointer_index-1;
                else
                    k_prime_index = 0;
                k_prime = parent->key[k_prime_index];

                buffer_put_page(table_id, parent->pagenum, parent);

                // 내 special off set 내 neighbor 이랑 이어주기
                page_t * neighbor = (page_t *)malloc(sizeof(page_t));
                neighbor_offset = find_left_leaf_offset(table_id, n);   // npindown함.

                if( neighbor_offset != -1){
                    neighbor->page_type = 1;
                    buffer_get_page(table_id, neighbor_offset/PAGE_SIZE, neighbor);
                    neighbor->special_page_offset = n->special_page_offset;
                    buffer_put_page(table_id, neighbor->pagenum, neighbor);
                }
                result = delete_entry(table_id, parent, k_prime, pointer_index);
                buffer_free_page(table_id,n->pagenum);
                free(neighbor);
                free(parent);
                return result;
            }
            
        }

    }
    else{                   //internal_node 인 상태에서 이 함수를 실행하면,
        //parent로 넘어올때는 pincount 없이 넘어오긴함.

        buffer_get_page(table_id,n->pagenum , n);
        i = 0;
        while(i <n->num_of_keys && n->key[i] != key)
            i++;

        remove_entry_from_node( n, i, pointer_index);       // 우선, key와 pointer 하나씩 지우고

        if( n-> num_of_keys >= 1){          //key의 개수 파악
            free(parent);
            // printf("%ld \n",n->key[0]);
            // printf("%ld \n", n->next_offset);
            buffer_put_page(table_id, n->pagenum, n);
            return 0;
        }
        else{                               // key가 다 없어져서 조치가 필요할 때
            if (n->next_offset == 0){       // 그 internal node가 root 면
                result = adjust_root(table_id, n);  //여기서 n pin down해야됨.
                free(parent);
                return result;
            }                               
            else{                           // root가 아니면,
                                            // 무조건, special_page_offset 에는 내자식이 있음.

                neighbor_offset = get_neighbor_offset(table_id, n);
                page_t * neighbor = (page_t *)malloc(sizeof(page_t));

                neighbor->page_type = 1;
                parent->page_type = 1;

                buffer_get_page(table_id,neighbor_offset/PAGE_SIZE , neighbor);
                buffer_get_page(table_id,n->next_offset/PAGE_SIZE , parent);


                if(neighbor->num_of_keys < internal_order -1 ){         // 내 sibling이 꽉 차지 않음.
                    
//                    printf("%ld \n", neighbor->key[0]);
                    result =  coalesce_nodes(table_id, neighbor, n , parent);   //neighbor 랑 parent랑 pindown해줘야함.
                    free(neighbor);
                    free(parent);
                    return result;
                }                       // 내 sibling 이 꽉참.
                else{
                    result = redistribute_nodes( table_id, neighbor, n, parent );   // neighbor ㄹkd parent pindwon 필요.
                    free(neighbor);
                    free(parent);
                    return result;
                }
            } 
        }
    }
}



// /* Master deletion function.
//  */

int erase(int table_id, int64_t key) {       // root 를 입력받고 key를 입력받음

    fi = fp_arr[table_id].fi;

    page_t * key_leaf;
    int64_t * key_record;            // record 가 key_record 이고
    page_t * root = find_root(table_id);        //rootpagepinup.
    buffer_put_page(table_id,root->pagenum,root);   //root page pindown
    int result;

    int* receive_find_result = (int*)malloc(sizeof(int));
    key_record = find(table_id, key, 0, receive_find_result);        // find를 할 건데, root,key,false를 넣을 거.   
    key_leaf = find_leaf(table_id, root, key);     // leaf에 있는지도 확인 해봄 //key_leaf pindown필요.
    
    if ( key_record != NULL ) {       // 둘중 하나라도 찾으면, 둘중 하나라도 null 이 아니면,
        result = delete_entry(table_id, key_leaf,key,-1);       //delete entry에서 key_leaf pindown해야함.
        if(root->pagenum == key_leaf->pagenum)
            free(root);
        else{
            free(root);
            free(key_leaf);
        }
        return result;   // delete entry 는 -1 or 0 을 반환해야함. 또, root를 free시켜줘야함.
    }
    if(key_leaf != NULL && key_leaf->pagenum == root->pagenum){
        buffer_put_page(table_id,key_leaf->pagenum,key_leaf);
        free(key_leaf);
    }
    else{
            free(root);
    }
    return -1;    //fail
}
