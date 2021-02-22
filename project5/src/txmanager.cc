//transaction Manager


// typedef struct trx_t {
//     int tid;
//     int op_index;
//     vector<pagenum_t> pages // to use in hash_map to delete locks
//     int wait_transaction_id; // lock object that trx is waiting
//     vector<undo_log> old_data;  //if trx_t aborted, roll back this data.
// }trx_t;

#ifndef __BPT_H__ 
	#include "bpt.h"
#endif


pthread_mutex_t tx_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t trx_list_lock = PTHREAD_MUTEX_INITIALIZER;  //lock for preventing trx_list
pthread_cond_t tx_cond = PTHREAD_COND_INITIALIZER;


int now_transaction_number = 0; //tid
vector<trx_t*> trx_list;     // list of running TRX


int begin_tx(){ 
    //allocate new transaction and connect transaction list.
    trx_t* new_trx = (trx_t*)malloc(sizeof(trx_t));

    //trx_list 락을 잡고 초기화.
    pthread_mutex_lock(&trx_list_lock);
    now_transaction_number++;
    new_trx->tid = now_transaction_number;
    new_trx->op_index = 0;

    //trx_list에 새로운 transaction 넣고, 락을 푼 다음 return
    trx_list.push_back(new_trx);
    pthread_mutex_unlock(&trx_list_lock);

    return new_trx->tid;
}

int end_tx(int tid){        //tid is transaction id

    bool is_not_founded = true;
    // trx_list 락을 먼저 잡고//lock_table_lock 잡는다.
    pthread_mutex_lock(&trx_list_lock);
    pthread_mutex_lock(&lock_table_mutex);

    
    //내 trx가 어딨는지 우선 찾고, 그 안에 lock 들 다 erase. 
    for(int i= 0; i < trx_list.size() ; i++)
    {
        if(trx_list[i]->tid == tid){     // find.

            // 그 안에 lock 들 다 지우기.
            for( int j =0; j < trx_list[i]->pages.size(); j++ ){

                // 만일, lock_table 안에 page id와 transaction id 가 같은게 있다면 지워야함.
                auto it = lock_table.find( trx_list[i]->pages[j] );
                for( int k = it->second.size() -1 ; k >=0 ; k--){
                    //trnascation id 가 같은지 찾아보기.
                    if( it->second[k].trx->tid == tid ){

                        //wake up 을 위한 buffer 찾기
                        page_t* temp = (page_t*)malloc(sizeof(page_t));
                        buffer* buffer_for_wake = buffer_get_page(it->second[k].table_id,it->second[k].page_id,temp);
                        buffer_put_page(it->second[k].table_id,temp->pagenum,temp);
                        free(temp);

                        //지우기
                        it->second.erase( it->second.begin() + k );
                        // 지우고 나면, buffer는 깨울 수 있다.
                        pthread_cond_broadcast(&buffer_for_wake->page_cond);
                    }
                }
            }

            free(trx_list[i]);
            trx_list.erase( trx_list.begin()+i);
            is_not_founded = false;
        }
        else{
            auto its = trx_list[i]->wait_transaction_id.find( tid );
            if(its != trx_list[i]->wait_transaction_id.end()){
                trx_list[i]->wait_transaction_id.erase(its);
            }

        }

    }

    pthread_mutex_unlock(&lock_table_mutex);
    pthread_mutex_unlock(&trx_list_lock);
    if( is_not_founded){
        return 0;
    }
    return tid;
}


trx_t* find_trx(int tid){
    
    for( int i =0; i< trx_list.size() ; i++){
        if( trx_list[i]->tid == tid){
            return trx_list[i];
        }
    }
    return NULL;
}


// index 안에 key 가 존재하는지 find해줌.
int64_t * find(int table_id, int64_t find_key , int tid, int* result) {   // find, key와 verbose를 받음(verbose가 true 면 path를 출력 ,false 면 그수만 찾기)
    
    
    fi = fp_arr[table_id].fi;

    int i = 0;
    page_t * root;
    root = find_root(table_id);     // root page pinup
    page_t * c = find_leaf( table_id, root, find_key); // 노드를 하나 만들고, find_leaf를 함.       //leaf page pinup
    
    buffer* get_buffer;
    if( tid ){
        //lock_t를 할당받기 위해 우선, trx_t* 와 page_id 를 구하기.
        buffer_put_page(table_id,c->pagenum, c);
        get_buffer = buffer_get_page(table_id, c->pagenum , c);
        buffer_put_page(table_id, c->pagenum ,c);

        pthread_mutex_lock(&trx_list_lock);
        trx_t* now_trx = find_trx(tid);
        //trx 의 op_index ++
        now_trx->op_index++;
        pthread_mutex_unlock(&trx_list_lock);
        if( now_trx == NULL){
            *result = 0;
            return NULL;
        }


        //lock_t를 할당해주자.
        lock_t s_lock = lock_alloc(table_id, c->pagenum, now_trx,SHARED);

        // lock_table 에 latch를 걸고 lock_table에 연결.
        pthread_mutex_lock(&lock_table_mutex);
        int is_lock_success = get_lock(table_id,c->pagenum,now_trx,SHARED, s_lock);

        //buffer_page latch 잡기
        pthread_mutex_lock(&buffer_pool_mutex);
        pthread_mutex_lock(&get_buffer->page_latch);
        get_buffer->is_locking = 1;
        pthread_mutex_unlock(&buffer_pool_mutex);

        // is_lock_success 가 lock_success 면 성공
        if( is_lock_success == LOCK_SUCCESS){
            pthread_mutex_unlock(&lock_table_mutex);
        }
        else{
            //complict 하면 우선 dead_lock checking 부터 한다.
            int is_deadlock = deadlock_check(tid);

            //만약에 deadlock 이라면,
            if(is_deadlock){
                
                abort_transaction(now_trx);
                pthread_mutex_unlock(&lock_table_mutex);
                *result =0;
                return NULL;
            }
            else{
                pthread_mutex_unlock(&lock_table_mutex);
                while(1){
                    pthread_cond_wait(&get_buffer->page_cond, &get_buffer->page_latch);
                    pthread_mutex_lock(&lock_table_mutex);
                    // 다시 확인했는데 complict라면,
                    if(!check_can_tx_run(table_id,c->pagenum,now_trx, SHARED, s_lock)){
                        pthread_mutex_unlock(&lock_table_mutex);
                        continue;
                    }
                    else{
                        pthread_mutex_unlock(&lock_table_mutex);
                        break;
                    }
                }
            }

        }

        get_buffer->owner_tx_id = tid;    
    }

    if (c == NULL) {     // leaf에 값이     없다면, null 반환
        free(root);
        if(tid){
            *result = 1;
            get_buffer->is_locking = 0;
            pthread_mutex_unlock(&get_buffer->page_latch);
        }
        return NULL;
    }
    for (i = 0; i < c->num_of_keys; i++)   // 값이 있다면, 그 num_keys 에 대해서 내가 찾는 값이 있으면 break 하고,
        if (c->key[i] == find_key) break;
    
    if (i == c->num_of_keys) // 만약 key와 같은 값이 없으면 바로 null return
    {
        buffer_put_page(table_id, c->pagenum,c);
        if( c->pagenum == root->pagenum)
            free(c);
        else{
            free(c);
            free(root);
        }
        if( tid){
            *result = 1;
            get_buffer->is_locking =0;
            pthread_mutex_unlock(&get_buffer->page_latch);
        }
        return NULL;
    }
    else    // 그게 아니라면, 그 key 에 해당하는 record의 value값을 return 한다.
    {
        buffer_put_page(table_id, c->pagenum , c);
        free(root);
        if(tid){
            *result =1;
            get_buffer->is_locking =0;
            pthread_mutex_unlock(&get_buffer->page_latch);
        }
        return c->value[i];
    }
}

int update(int table_id, int64_t key, int64_t *values, int tid, int* result){
    
    //transaction 구하고, op_index를 ++ 한다.
    pthread_mutex_lock(&trx_list_lock);
    trx_t* now_trx = find_trx(tid);
    //trx 의 op_index ++
    now_trx->op_index++;
    pthread_mutex_unlock(&trx_list_lock);

    //page_id 를 찾는다.
    page_t * root = find_root(table_id);     // root page pinup
    page_t * c = find_leaf( table_id, root, key); // 노드를 하나 만들고, find_leaf를 함.       //leaf page pinup
    
    //lock_t를 할당받기 위해 page_id 를 구하기.
    buffer_put_page(table_id,c->pagenum, c);
    buffer* get_buffer = buffer_get_page(table_id, c->pagenum , c);
    buffer_put_page(table_id, c->pagenum ,c);

    if( c->pagenum != root->pagenum)
        free(root);
    //lock_t 를 할당 받는다.
    lock_t lock_x = lock_alloc(table_id, c->pagenum,now_trx, EXCLUSIVE);

    //lock_table 을 잡고, lock을 연결한다.
    pthread_mutex_lock(&lock_table_mutex);
    int is_lock_success = get_lock(table_id,c->pagenum,now_trx,EXCLUSIVE, lock_x);

    //buffer_page latch 잡기
    pthread_mutex_lock(&buffer_pool_mutex);
    pthread_mutex_lock(&get_buffer->page_latch);
    get_buffer->is_locking = 1;
    pthread_mutex_unlock(&buffer_pool_mutex);

    //만약 lock 잡는게 성공했다면,
    if( is_lock_success == LOCK_SUCCESS){
        pthread_mutex_unlock(&lock_table_mutex);
    }
    // lock 잡는게 실패했다면,
    else{
        //complict 하면 우선 dead_lock checking 부터 한다.
        int is_deadlock = deadlock_check(tid);

        //만약에 deadlock 이라면,
        if(is_deadlock){
            abort_transaction(now_trx);
            pthread_mutex_unlock(&lock_table_mutex);
            *result = 0;
            free(c);
            return -1;
        }
        else{
            pthread_mutex_unlock(&lock_table_mutex);
            while(1){
                pthread_cond_wait(&get_buffer->page_cond, &get_buffer->page_latch);
                pthread_mutex_lock(&lock_table_mutex);

                // 다시 확인했는데 complict라면,
                if(!check_can_tx_run(table_id,c->pagenum,now_trx, EXCLUSIVE, lock_x)){
                    pthread_mutex_unlock(&lock_table_mutex);
                    continue;
                }
                else{
                    pthread_mutex_unlock(&lock_table_mutex);
                    break;
                }
            }
        }
    }

    //owner 설정
    get_buffer->owner_tx_id = tid;
    

    int key_index =-1;
    for( int i = 0 ; i< c->num_of_keys ; i++){
        if(c->key[i] == key ){
            key_index = i;
            break;
        }
    }

    if( key_index == -1)    // not_found key
    {
        free(c);
        pthread_mutex_unlock(&get_buffer->page_latch);
        *result = 1;
        return -1;
    }
    else{       //find key.
        //save undo_log
        undo_log save_old;
        save_old.table_id = table_id;
        save_old.pagenum = c->pagenum;
        save_old.key = key;
        for( int i =0; i < c->num_of_columns; i++){
            save_old.values[i] = c->value[key_index][i];
        }
        now_trx->old_data.push_back(save_old);
        

        //change value
        for( int i = 0; i< c->num_of_columns; i++){
            c->value[key_index][i] = values[i];
        }
        buffer_put_page(table_id,c->pagenum,c);

        //unlock buffer lock
        pthread_mutex_unlock(&get_buffer->page_latch);
        *result = 1;
        return 0;
    }
    //bool lock_success = get_lock(table_id,);
}


//abort
void abort_transaction(trx_t* now_trx){

    //trx_list 락을 잡고,
    pthread_mutex_lock(&trx_list_lock);

    // trx가 잡고 있던 page들에 대해서 다 roll_back 시켜줌.
    for(int i = now_trx->old_data.size()-1 ; i>= 0 ; i--){
        page_t* roll_back_page = (page_t*)malloc(sizeof(page_t));
        roll_back_page->page_type = 1;
        buffer_get_page(now_trx->old_data[i].table_id, now_trx->old_data[i].pagenum,roll_back_page);

        for( int j = 0; j <roll_back_page->num_of_keys ; j++){
            if(now_trx->old_data[i].key == roll_back_page->key[j]){

                for( int k=0; k < roll_back_page->num_of_columns ; k++){
                    roll_back_page->value[j][k] = now_trx->old_data[i].values[k];
                }   
                break;
            }
        }
        buffer_put_page(now_trx->old_data[i].table_id, roll_back_page->pagenum, roll_back_page);
        free(roll_back_page);
    }

    // 그 다음 trx 이 잡고 있던 모든 lock들은 erase 해줘야함.
    for(int i= 0; i < trx_list.size() ; i++)
    {
        if(trx_list[i]->tid == now_trx->tid){     // find.

            // 그 안에 lock 들 다 지우기.
            for( int j =0; j < trx_list[i]->pages.size(); j++ ){

                // 만일, lock_table 안에 page id와 transaction id 가 같은게 있다면 지워야함.
                auto it = lock_table.find( trx_list[i]->pages[j] );
                for( int k = it->second.size() -1 ; k >=0 ; k--){
                    //trnascation id 가 같은지 찾아보기.
                    if( it->second[k].trx->tid == now_trx->tid ){

                        //wake up 을 위한 buffer 찾기
                        page_t* temp = (page_t*)malloc(sizeof(page_t));
                        buffer* buffer_for_wake = buffer_get_page(it->second[k].table_id,it->second[k].page_id,temp);
                        buffer_put_page(it->second[k].table_id,temp->pagenum,temp);
                        free(temp);

                        //지우기
                        it->second.erase( it->second.begin() + k );
                        // 지우고 나면, buffer는 깨울 수 있다.
                        pthread_cond_broadcast(&buffer_for_wake->page_cond);
                    }
                }
            }

            free(trx_list[i]);
            trx_list.erase( trx_list.begin()+i);
            break;
        }
    }
    pthread_mutex_unlock(&trx_list_lock);
    return;
}

