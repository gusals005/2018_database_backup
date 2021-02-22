#ifndef __BPT_H__
    #include "bpt.h"
#endif


   
/*declare global variable*/
vector< vector<int64_t> > table[11];    // these save table_info.
int64_t num_of_record_in_table[11];     // it save num_of_record_in_table
int64_t total_record=0;

float query_optimizer_time=0;           // it save optimizer time
float query_parser_time=0;              // query_parser_time
float query_join_time =0;               // query_join_time
float query_sum_time=0;
float clear_time=0;                     // check clear time
float query_control_time=0;
float hash_join_time=0;
float set_table_location_time=0;
float left_table_hash_time=0;
float right_table_hash_time=0;
float sum_thread_result_time=0;
clock_t op_start,op_end;                // to save optimizer time

pthread_t threads[NUM_THREADS];         // for threads.
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;  //mutex_lock
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;     //sleep lock

int range_start, range_end; // for divide range.
int l_key_index, r_key_index; // for key of multimap
bool left_table_hash_or_right_table_hash;
unordered_multimap<int64_t , vector<int64_t> > u_map; //make hash_map

int left_table_index_in_hash;
int right_table_index_in_hash;
vector<vector<int64_t> > result;        // result_ of hash join
vector<vector<int64_t> > thread_result[NUM_THREADS];
vector<int64_t> first_line;

int is_thread_finished[NUM_THREADS] = {0};


/*Query Parser*/
vector<query_op> query_parser(string query){
    vector<query_op> querys;
    istringstream iss(query);
    while(1){
        query_op q;
        char equal = ' ', point =' ', another= ' ';
        iss >> q.left_tid >> point >> q.left_col;
        assert(point == '.');
        iss >> equal;
        assert(equal == '=');
        iss >> q.right_tid >> point >> q.right_col;
        assert(point == '.');
        q.num_of_product = num_of_record_in_table[q.left_tid] * num_of_record_in_table[q.right_tid];
        querys.push_back( move(q));
        if(!(iss>>another) || (another != '&')) break;

    }
    return querys;
}

/*Make a good Query Plan*/
vector<query_op> query_planning(vector<query_op> old_querys){
    /*declare local variables*/
    vector<query_op> new_querys;
    int check[11] = {0};        // check table already joined.
    int64_t min_index = 0;      // find min
    int64_t min_value = -1;      //find min_value

    /*estimate best query plan*/

    // find first query
    for( int i =0; i<old_querys.size() ; i++){
        if(min_value == -1 || min_value > old_querys[i].num_of_product){
            min_index = i;
            min_value = old_querys[i].num_of_product;
        }
    }
    // input first query, and delete query 0
    new_querys.push_back(old_querys[min_index]);
    check[ old_querys[min_index].left_tid ] = 1;
    check[ old_querys[min_index].right_tid ] = 1;
    old_querys.erase(old_querys.begin() + min_index);
    //and check which table is in result;
    while(old_querys.size()){
        min_index = 0;
        min_value = -1;
        for( int i = 0 ; i< old_querys.size(); i++){
            if(check[old_querys[i].left_tid] || check[old_querys[i].right_tid]){

                if(min_value == -1 || min_value > old_querys[i].num_of_product){
                    min_index = i;
                    min_value = old_querys[i].num_of_product;
                }
            }
        }
        new_querys.push_back(old_querys[min_index]);
        check[ old_querys[min_index].left_tid ] = 1;
        check[ old_querys[min_index].right_tid] =1;
        old_querys.erase(old_querys.begin() + min_index);
    }
    /*return query_plan*/
    return new_querys;
}

/*change table into vector<>*/
vector<vector<int64_t> > get_table(int64_t table_id){


    int tid = (int)table_id;    //table_id to use buf_funcs
    int64_t num_col;
    vector<vector<int64_t> > table;
    vector<int64_t> first_line; // first_line include tid and col info.
    vector<int64_t> next_line; // next_line include tuples in table;
    int num_key; // leaf page's num_of_keys
    num_of_record_in_table[table_id] =0;

    /*save information*/

        // use header page to get num_of_colums
    page_t* header = (page_t *)malloc(sizeof(page_t));
    header->page_type = 0;
    buffer_get_page(tid,0,header);
    buffer_put_page(tid,0,header);
    num_col = header->num_of_columns;

        //add first_line to table.
    for(int i =0; i<(int)num_col+1 ; i++){
        first_line.push_back((int64_t)tid);
        first_line.push_back((int64_t)(i+1));
    }
    table.push_back(move(first_line));

        // find mostleft page
    page_t* leaf = (page_t*)malloc(sizeof(page_t));
    leaf->page_type = 1;
    buffer_get_page(tid, header->root_page_offset/PAGE_SIZE,leaf);
    buffer_put_page(tid,leaf->pagenum, leaf);
    while(!leaf->is_leaf){
        leaf->page_type = 1;
        buffer_get_page(tid,leaf->special_page_offset/PAGE_SIZE,leaf);
        buffer_put_page(tid,leaf->pagenum,leaf);
    }
        // search every leaf page and add tuple to table
    while(1){
        num_key = leaf->num_of_keys;

        num_of_record_in_table[table_id] += num_key;

        for(int i=0; i<leaf->num_of_keys; i++){
            next_line.clear();
            next_line.push_back(leaf->key[i]);
            for( int j=0; j<(int)num_col ;j++)
                next_line.push_back(leaf->value[i][j]);
            table.push_back(next_line);
        }
        if(leaf->special_page_offset <= 0)
            break;
        else{
            leaf->page_type = 1;
            buffer_get_page(tid,leaf->special_page_offset/PAGE_SIZE,leaf);
            buffer_put_page(tid,leaf->pagenum,leaf);
        }
    }

    total_record += num_of_record_in_table[table_id];
    return table;
}

/*find key's index of table to use key of hash*/
int get_index(query_op query, int left_or_right){
    int tid,colid;
    int index;
    int now_table_index=0;

    if(!left_or_right){     //compare left
        tid = query.left_tid;
        colid = query.left_col;
        now_table_index = left_table_index_in_hash;
    }
    else{                   //compare right
        tid = query.right_tid;
        colid = query.right_col;
        now_table_index = right_table_index_in_hash;
    }

    for( int i = 0 ; i < table[now_table_index][0].size() ; i++){
        int64_t t_id, col_id;
        t_id = table[now_table_index][0][i];
        i++;
        col_id = table[now_table_index][0][i];
        if(t_id == tid && col_id == colid){
            index = i/2;
            break;
        }
    }
    return index;
}

void set_left_n_right_table(query_op& query, int check_num){
    // if check_num == 2, the query is first
    if(check_num == 2){       // in case, first join.
        if( num_of_record_in_table[query.right_tid] < num_of_record_in_table[query.left_tid]){  //make small table to left.
            int64_t tid_temp, col_temp;
            tid_temp = query.left_tid;
            col_temp = query.left_col;
            query.left_tid = query.right_tid;
            query.left_col = query.right_col;
            query.right_tid = tid_temp;
            query.right_col = col_temp;
        }
        left_table_index_in_hash = query.left_tid;
        right_table_index_in_hash = query.right_tid;
    }
    // check_num == 0, result already has left_table
    else if( check_num == 0 ){   // if left_or_right is 0, right_table include left_table
        if( num_of_record_in_table[query.right_tid] < table[0].size() ){  //make small table to left.
            int64_t tid_temp, col_temp;
            tid_temp = query.left_tid;
            col_temp = query.left_col;
            query.left_tid = query.right_tid;
            query.left_col = query.right_col;
            query.right_tid = tid_temp;
            query.right_col = col_temp;
            left_table_index_in_hash = query.left_tid;
            right_table_index_in_hash = 0;  // 0 이 table[0] (전체 결과) 를 뜻함.
        }
        else{
            left_table_index_in_hash = 0; // 0 equal result
            right_table_index_in_hash = query.right_tid;
        }
    }
    // check_num == 1, result already has right_table
    else{      //table[0] include right_table
        if( num_of_record_in_table[query.left_tid] < table[0].size() ){  //make small table to left.
            left_table_index_in_hash = query.left_tid;
            right_table_index_in_hash = 0;  // 0 이 table[0] (전체 결과) 를 뜻함.
        }
        else{
            int64_t tid_temp, col_temp;
            tid_temp = query.left_tid;
            col_temp = query.left_col;
            query.left_tid = query.right_tid;
            query.left_col = query.right_col;
            query.right_tid = tid_temp;
            query.right_col = col_temp;
            left_table_index_in_hash = 0; // 0 equal result
            right_table_index_in_hash = query.right_tid;
        }
    }
}

void* input_to_hash(void* thread_num){

    while(1){
        long tnum = (long) thread_num;
        pthread_mutex_lock(&mutex);
        pthread_cond_wait(&cond,&mutex);
        pthread_mutex_unlock(&mutex);
        thread_result[tnum].clear();
        if(tnum ==0)
            thread_result[tnum].push_back(first_line);

        int start = range_start + ((range_end - range_start) / NUM_THREADS) * tnum;
        int fin = range_start + ((range_end - range_start) / NUM_THREADS * (tnum+1));
        if(tnum == NUM_THREADS -1)
            fin = range_end+1;

        if(left_table_hash_or_right_table_hash) // now is hashing left_table
        {
            for(int i=start; i< fin ; i++){
                pthread_mutex_lock(&mutex);
                //u_map.insert(make_pair(left_table[i][l_key_index],left_table[i]));
                pthread_mutex_unlock(&mutex);
            }
            is_thread_finished[tnum] = 1;
        }
        else{
            for( int i=start ; i<fin ;i++){
                auto its = u_map.equal_range(table[right_table_index_in_hash][i][r_key_index]);
                for( auto it = its.first; it != its.second ; it++){
                    vector<int64_t> add_line;
                    for( int j=0; j< (it->second).size() ;j++){
                        add_line.push_back( (it->second)[j] );
                    }
                    for(int j = 0 ; j<table[right_table_index_in_hash][i].size();j++){
                        add_line.push_back(table[right_table_index_in_hash][i][j]);
                    }
                    //pthread_mutex_lock(&mutex);
                    thread_result[tnum].push_back(move(add_line));
                    //result.push_back( move(add_line) );
                    //pthread_mutex_unlock(&mutex);
                }
            }
            is_thread_finished[tnum] = 1;
        }
    }

}
/*Hash Join*/
void hash_join(query_op query, int left_or_right){

    

    /*declare local variables*/
    op_start = clock();
    //vector<vector<int64_t> > empty_result;
    //result = empty_result;
    // result.clear();
    // for( int i =0; i < NUM_THREADS ;i++)
    //     thread_result[i].clear();

    first_line.clear();
    op_end = clock();


    clear_time += (float) (op_end-op_start) / CLOCKS_PER_SEC;


    /*set left and right table*/
    if(table[0].size() == 0)
        left_or_right = 2;
    op_start = clock();
    set_left_n_right_table(query,left_or_right);       // left 의 index와 right의 index를 구해주자.
                                                                // left_or_right = 0 이면, table[0] 안에 left_table이 포함
                                                                // left_or_right = 1 이면, table[0] 안에 right_table이 포함.
    op_end = clock();

    set_table_location_time += (float) (op_end-op_start) / CLOCKS_PER_SEC;


    /* join by using hash    left : original table / right : add table */

    op_start = clock();
    unordered_multimap<int64_t , vector<int64_t> > empty_u_map; //make hash_map
    u_map = empty_u_map;
//    u_map.clear();
    op_end = clock();
    clear_time += (float) (op_end-op_start) / CLOCKS_PER_SEC;
    /* to merge two table */

        // push_back result_info

    op_start = clock();
    for(int i = 0 ; i< table[left_table_index_in_hash][0].size() ; i++)
        first_line.push_back(table[left_table_index_in_hash][0][i]);
    for(int i = 0 ; i< table[right_table_index_in_hash][0].size() ; i++)
        first_line.push_back(table[right_table_index_in_hash][0][i]);
    /*
        이 실행문은 합치기 전에 실행
        thread_result[0].push_back(move(first_line));
    */
    
    //result.push_back(first_line);

        // first, input left_tuples

    //line 1 to n-1
    range_start = 1;
    //range_end = table[left_table_index_in_hash].size() - 1;
    l_key_index = get_index(query ,0);
    left_table_hash_or_right_table_hash = true;

    if(left_table_hash_or_right_table_hash) // now is hashing left_table
    {
        for(int i=range_start; i< table[left_table_index_in_hash].size() ; i++){
            u_map.insert(make_pair(table[left_table_index_in_hash][i][l_key_index],table[left_table_index_in_hash][i]));
        }
    }
    op_end = clock();

    left_table_hash_time += (float) (op_end-op_start) / CLOCKS_PER_SEC;


    // wake up thread.
    // pthread_mutex_lock(&mutex);
    // pthread_cond_broadcast(&cond);
    // pthread_mutex_unlock(&mutex);
    // for( int i =1; i<left_table.size() ; i++){
    //     u_map.insert(make_pair(left_table[i][l_key_index], left_table[i]));
    // }

        //compare right_tuples, and check tuple has same key.

    //wait all thread finished.
    // while(1){
    //     bool all_thread_is_finished =true;
    //     for ( int j =0 ; j<NUM_THREADS ; j++){
    //         if(is_thread_finished[j] == 0){
    //             all_thread_is_finished = false;
    //             break;
    //         }
    //     }
    //     if(all_thread_is_finished)
    //         break;
    // }

    
    op_start = clock();
    for( int i=0; i<NUM_THREADS ;i++)
        is_thread_finished[i] =0;
    range_start = 1;
    range_end = table[right_table_index_in_hash].size() - 1;
    r_key_index = get_index(query ,1);
    left_table_hash_or_right_table_hash = false;
    //thread wake up
    pthread_cond_broadcast(&cond);
    
    
    // for( int i = 1 ; i< right_table.size() ; i++){
    //     auto its = u_map.equal_range(right_table[i][r_key_index]);      //it contains same key. this is join.
    //     for( auto it = its.first ; it != its.second ; it++ ){   
    //         vector<int64_t> add_line;
    //         add_line.assign( (it->second).begin(), (it->second).end() );
    //         for(int j = 0 ; j<right_table[i].size();j++)
    //             add_line.push_back(right_table[i][j]);
    //         result.push_back(add_line);
    //     }
    // }
    //wait all thread finished.

    while(1){
        bool all_thread_is_finished =true;
        for ( int j =0 ; j<NUM_THREADS ; j++){
            if(is_thread_finished[j] == 0){
                all_thread_is_finished = false;
                break;
            }
        }
        if(all_thread_is_finished)
            break;
    }
    for( int j =0; j<NUM_THREADS; j++)
        is_thread_finished[j] =0;
    op_end = clock();
    

    right_table_hash_time += (float) (op_end-op_start) / CLOCKS_PER_SEC;

    /*return result*/

    op_start = clock();
    vector<vector<int64_t> > empty_result_set;
    table[0] = empty_result_set;
    //result_set.clear();
    op_end = clock();

    clear_time += (float) (op_end-op_start) / CLOCKS_PER_SEC;

    op_start = clock();
    for( int i=0; i< NUM_THREADS ;i++){
        for( int j=0; j< thread_result[i].size() ; j++){
            table[0].push_back(move(thread_result[i][j]));
        }
    }
    op_end = clock();

    sum_thread_result_time += (float) (op_end-op_start) / CLOCKS_PER_SEC; 
    // for( int j=0; j<result.size() ; j++ )
    // table[0].push_back(move(result[j]));
}

/*Join*/
int64_t join(const char* query){

    
    /*declare local variables*/
    string query_str = ""; // make query's type string.
    vector<query_op> querys; // save query after parsing.
    int table_info[11] = {0};

    clock_t opt_start,opt_end;

    op_start = clock();
    vector<vector<int64_t> > empty_result_set;
    table[0] = empty_result_set;
    op_end = clock();
    clear_time += (float) (op_end-op_start) / CLOCKS_PER_SEC;

    int check_which_tid_in_result_set =0;
    int cnt=0; // for debugging
    /*Do query_parsering*/
    op_start = clock();
    query_str = query;
    querys = query_parser(query_str);
    op_end = clock();
    query_parser_time += (float) (op_end - op_start) / CLOCKS_PER_SEC;

    //for loop querys
    int q_idx=0;
    int query_size = querys.size();


    /*decide query plan*/
    op_start = clock();
    querys = query_planning(querys);
    op_end = clock();

    query_optimizer_time += (float) (op_end - op_start) / CLOCKS_PER_SEC;



    /*make result_set*/

    opt_start = clock();
    clock_t c_start,c_end;
    while(q_idx != query_size)
    {
        //add info

        c_start = clock();
        if( table_info[querys[q_idx].left_tid])         // if result has left table
            check_which_tid_in_result_set = 0;
        else if ( table_info[querys[q_idx].right_tid])  // if result has right table
            check_which_tid_in_result_set = 1;
        table_info[querys[q_idx].left_tid] = 1;
        table_info[querys[q_idx].right_tid] = 1;
        c_end = clock();
        query_control_time += (float) (c_end - c_start) / CLOCKS_PER_SEC;

        //Do hash join
        c_start = clock();
        hash_join(querys[q_idx], check_which_tid_in_result_set);
        c_end = clock();
        hash_join_time += (float) (c_end - c_start) / CLOCKS_PER_SEC;
        cnt++;
        // next loop for next_query
        q_idx++;

        //debugging
        // printf("%dth join result\n", cnt);
        // for( int i = 1 ;i<result_set.size() ; i++){
        //     for( int j =0; j<result_set[i].size() ; j++)
        //         printf("%ld ", result_set[i][j]);
        //     printf("\n");
        // }

    }
    opt_end = clock();
    query_join_time += (float) (opt_end - opt_start) / CLOCKS_PER_SEC;

    /*calculate sum of keys*/
    op_start = clock();
    int64_t sum=0;
    for( int i = 1 ; i< table[0][0].size(); i+= 2){
        if( table[0][0][i] == 1){
            for( int j= 1; j <table[0].size(); j++){
                sum += table[0][j][i/2];
            }
        }
    }
    op_end = clock();
    query_sum_time += (float) (op_end-op_start) /CLOCKS_PER_SEC;
    //debug
    // printf("\n");
    return sum;
}
