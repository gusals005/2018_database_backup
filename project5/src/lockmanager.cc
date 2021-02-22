//lock manager

#ifndef __BPT_H__ 
	#include "bpt.h"
#endif

// typedef struct lock_t {
//     int table_id;
//     int page_id; // or key
//     lock_mode mode; // SHARED, EXCLUSIVE
//     trx_t* trx; // backpointer to lock holder
//     int op_index;
// }lock_t;

unordered_map<int64_t , vector<lock_t> > lock_table; //make hash_map
pthread_mutex_t lock_table_mutex = PTHREAD_MUTEX_INITIALIZER;   //prvent lock_table

// lock allocate 하는 함수.
// lock 할당하고 초기화 한다.
lock_t lock_alloc( int table_id, pagenum_t page_id, trx_t* trx  ,lock_mode mode){
    lock_t alloc_lock;

    alloc_lock.table_id = table_id;
    alloc_lock.page_id = page_id;
    alloc_lock.mode = mode;
    alloc_lock.trx = trx;
    alloc_lock.op_index = trx->op_index;
    return alloc_lock;
}


// get_lock 은 락을 요청했을 때 락을 잡아주는 것.
// 만약 락을 잡을 수 없다면, 잠을 자야함.
// 리턴 값은 성공(1), 실패(0) 임.
// get_lock 함수에 들어오기 전에 미리 lock 을 할당하고, lock_table mutex를 잡고 올 것.
int get_lock(int table_id ,pagenum_t page_id, trx_t* trx,lock_mode mode, lock_t input_lock){


    auto it = lock_table.find(page_id);

    if(it == lock_table.end()){     // don't find same page_id_lock
        vector<lock_t> first_lock;
        first_lock.push_back(input_lock);
        lock_table.insert( make_pair(page_id, first_lock) );
        return LOCK_SUCCESS;
    }
    else{   // find same page_id_lock
        bool complict = false;
        for( int i = 0 ; i < it->second.size() ; i++){
            if(it->second[i].table_id == table_id && it->second[i].page_id == page_id){
                if( (it->second[i].mode == EXCLUSIVE || mode == EXCLUSIVE) && it->second[i].trx->tid != trx->tid){
                    complict = true;
                    trx->wait_transaction_id.insert(it->second[i].trx->tid);
                }
            }
        }
        it->second.push_back(input_lock);
        if(complict){
            return LOCK_FAIL;
        }
        else{
            return LOCK_SUCCESS;
        }
    }

}


vector<vector<int> > adj_list;
vector<int> visited;
vector<int> finished;
bool is_deadlock =false;

void dfs(int start_index){
    visited[start_index] = 1;

    for( int i=0; i < adj_list[start_index].size(); i++){
        if(visited[ adj_list[start_index][i] ] == 0)
            dfs( adj_list[start_index][i] );
        // visited 되어 있는데, 끝나지 않았다면 cycle이 생긴 것.
        else if( finished[ adj_list[start_index][i] ] == 0 )
            is_deadlock = true;
    }
    finished[start_index] = 1;
}
// if deadlock, return 1
// else return 0
// use dfs to find cycle(deadlock) in DAG
int deadlock_check(int tid){
    is_deadlock = false;
    pthread_mutex_lock(&trx_list_lock);
    map<int,int> trx_to_index;

    int start_index;
    for( int i =0; i<trx_list.size() ;i++){
        trx_to_index[trx_list[i]->tid] = i;
        if( trx_list[i]->tid == tid){
            start_index = i;
        }
    }

    for( int i =0; i<trx_list.size() ; i++){
        vector<int> adj;
        for( auto j= trx_list[i]->wait_transaction_id.begin() ; j != trx_list[i]->wait_transaction_id.end() ; j++){
            adj.push_back( trx_to_index.find(*j)->second );
        }
        adj_list.push_back(adj);
        visited.push_back(0);
        finished.push_back(0);
    }

    dfs(start_index);

    pthread_mutex_unlock(&trx_list_lock);
    if(is_deadlock)
        return 1;
    else
        return 0;

}

//if tx can run, return true;
bool check_can_tx_run( int table_id, pagenum_t pagenum, trx_t*  trx , lock_mode mode, lock_t input_lock){

    // find lock
    auto it = lock_table.find(pagenum);
    for( int i =0 ; i < it->second.size() ; i++){
        // 현재 trx와 같은 트랜젝션이 아닌데, complict 하는 게 있는지.
        if( it->second[i].trx->tid != trx->tid){
            if( (it->second[i].mode == EXCLUSIVE || mode == EXCLUSIVE) ){

                return false;
            }
        }
        // 만약 내 트렌젝션까지 왔다면 break 해도됨. 내앞에 겹치는 게 없는것.
        else{
            if( it->second[i].op_index == input_lock.op_index )
                break;
        }
    }
    return true;
}