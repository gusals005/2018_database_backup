#ifndef __BPT_H__
    #include "bpt.h"
#endif

#include<set>
#include<unordered_map>

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
        querys.push_back(q);
        if(!(iss>>another) || (another != '&')) break; 
    }
    return querys;
}

/*Make a good Query Plan*/
vector<query_op> query_planning(vector<query_op> old_querys){
    /*declare local variables*/
    vector<query_op> new_querys;
    int check[11] = {0};
    /*estimate best query plan*/
    // input first query, and delete query 0
    new_querys.push_back(old_querys[0]);
    check[ old_querys[0].left_tid ] = 1;
    check[ old_querys[0].right_tid ] = 1;
    old_querys.erase(old_querys.begin());
    //and check which table is in result;
    while(old_querys.size()){
        for( int i = 0 ; i< old_querys.size(); i++){
            if(check[old_querys[i].left_tid] || check[old_querys[i].right_tid]){
                new_querys.push_back(old_querys[i]);
                check[ old_querys[i].left_tid ] = 1;
                check[ old_querys[i].right_tid] =1;
                old_querys.erase(old_querys.begin() + i);
                break;
            }
        }
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
    table.push_back(first_line);

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

        for(int i=0; i<leaf->num_of_keys; i++){
            next_line.clear();
            next_line.push_back(leaf->key[i]);
            for( int j=0; j<(int)num_col ;j++)
                next_line.push_back(leaf->value[i][j]);
            table.push_back(next_line);
        }
        if(leaf->special_page_offset == -1)
            break;
        else{
            leaf->page_type = 1;
            buffer_get_page(tid,leaf->special_page_offset/PAGE_SIZE,leaf);
            buffer_put_page(tid,leaf->pagenum,leaf);
        }
    }
    return table;
}

/*find key's index of table to use key of hash*/
int get_index(query_op query, int left_or_right, vector<int64_t> table){
    int tid,colid;
    int index;

    if(!left_or_right){     //compare left
        tid = query.left_tid;
        colid = query.left_col;
    }
    else{                   //compare right
        tid = query.right_tid;
        colid = query.right_col;
    }

    for( int i = 0 ; i < table.size() ; i++){
        int64_t t_id, col_id;
        t_id = table[i];
        i++;
        col_id = table[i];
        if(t_id == tid && col_id == colid){
            index = i/2;
            break;
        }
    }
    return index;
}

/*Hash Join*/
vector<vector<int64_t> > hash_join(vector<vector<int64_t> > result_table, query_op query, int left_or_right){

    /*declare local variables*/
    vector<vector<int64_t> > left_table;
    vector<vector<int64_t> > right_table;
    vector<vector<int64_t> > result;
    int l_key_index, r_key_index; // for key of multimap

    /*set left and right table*/
    if(result_table.size() == 0){       // in case, first join.
        left_table = get_table(query.left_tid);
        right_table = get_table(query.right_tid);
    }
    else if( !left_or_right ){   // if left_or_right is 0, result_table include left_table
        left_table.assign(result_table.begin(), result_table.end());
        right_table = get_table(query.right_tid);
    }
    else{      //result_table include right_table
        left_table = get_table(query.left_tid);
        right_table.assign(result_table.begin(), result_table.end());
    }

    /* join by using hash    left : original table / right : add table */
    unordered_multimap<int64_t , vector<int64_t> > u_map; //make hash_map

    /* to merge two table */

        // push_back result_info
    vector<int64_t> first_line;
    for(int i = 0 ; i< left_table[0].size() ; i++)
        first_line.push_back(left_table[0][i]);
    for(int i = 0 ; i< right_table[0].size() ; i++)
        first_line.push_back(right_table[0][i]);
    result.push_back(first_line);

        // first, input left_tuples
    l_key_index = get_index(query ,0,left_table[0]);
    for( int i =1; i<left_table.size() ; i++){
        u_map.insert(make_pair(left_table[i][l_key_index], left_table[i]));
    }

        //compare right_tuples, and check tuple has same key.
    r_key_index = get_index(query ,1,right_table[0]); 
    for( int i = 1 ; i< right_table.size() ; i++){
        auto its = u_map.equal_range(right_table[i][r_key_index]);      //it contains same key. this is join.
        for( auto it = its.first ; it != its.second ; it++ ){
            vector<int64_t> add_line;
            add_line.assign( (it->second).begin(), (it->second).end() );
            for(int j = 0 ; j<right_table[i].size();j++)
                add_line.push_back(right_table[i][j]);
            result.push_back(add_line);
        }
    }

    /*return result*/
    return result;
}

/*Join*/
int64_t join(const char* query){
    
    /*declare local variables*/

    string query_str = ""; // make query's type string.
    vector<query_op> querys; // save query after parsing.
    vector<vector<int64_t> > result_set; // save result_set
    set<int64_t> table_info; //save which tables were used in result_set.
    int check_which_tid_in_result_set =0;
    int cnt=0; // for debugging

    /*Do query_parsering*/
    query_str = query;
    querys = query_parser(query_str);


    /*decide query plan*/
    querys = query_planning(querys);
    for( int i = 0; i <querys.size() ; i++)
        printf("query %d :%ld %ld %ld %ld\n", i,querys[i].left_tid,querys[i].left_col,querys[i].right_tid,querys[i].right_col);
    printf("query pasing is good\n");

    /*make result_set*/
    while(!querys.empty()){

        //add info
        if( table_info.find(querys[0].left_tid) == table_info.end() )    //left가 없다면,
            check_which_tid_in_result_set = 1;  //right는 있는거.
        if( table_info.find(querys[0].right_tid) == table_info.end() )    //left가 없다면,
            check_which_tid_in_result_set = 0;  //left는 있는거.
        table_info.insert(querys[0].left_tid);
        table_info.insert(querys[0].right_tid);
        //Do hash join
        result_set = hash_join(result_set,querys[0], check_which_tid_in_result_set);

        cnt++;
        printf("%dth join result\n", cnt);
        for( int i = 1 ;i<result_set.size() ; i++){
            for( int j =0; j<result_set[i].size() ; j++)
                printf("%ld ", result_set[i][j]);
            printf("\n");
        }

        //erase first query.
        querys.erase(querys.begin());
    }

    /*calculate sum of keys*/
    int sum=0;
    for( int i = 1 ; i< result_set[0].size(); i+= 2){
        if( result_set[0][i] == 1){
            for( int j= 1; j <result_set.size(); j++){
                sum += result_set[j][i/2];
            }
        }
    }
    printf("\n");
    return sum;
}
