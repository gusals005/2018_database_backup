#ifndef __BPT_H__
	#include "bpt.h"
#endif


#include<fstream>
// TEST main

void read_file(string s){
    
    
    ifstream input_file(s);
    
    char input_string[1000];
    
    int i=0;
    int64_t join_result=0;
    float total_time =0;
    clock_t start,end;

    while(!input_file.eof()){
        input_file.getline(input_string,100);
        if(input_string[0] >= '0' && input_string[0] <='9'){ //join
            if(i%2 == 0){   //join
                start = clock();
                join_result = join(input_string);
                end = clock();
                total_time += (float)(end-start)/CLOCKS_PER_SEC;
                i++;
            }
            else{   //result
                string join_r = to_string(join_result);
                string result(input_string);
                if( join_r == result){
                    //printf("일치\n");
                    true;
                }
                else{
                    printf("결과가 일치하지 않음. 다음과 같은 곳에서\n");
                    cout << "구한 답 : " << join_r << endl;
                    cout << input_string << "  Line : " << i << endl;
                    exit(EXIT_FAILURE);
                    //printf("no일치\n");
                } 
                i++;
            }
        }
        else{       //table open
            open_table(input_string,2);
        }
    }
    
    cout << "join execution time : " << total_time << endl;
    // cout << "query_optimizer time : " << query_optimizer_time  << endl;
    // cout << "query_parser time : " << query_parser_time << endl;
    // cout << "query_join_time : " << query_join_time << endl;
    // cout << "query_sum_time : " << query_sum_time << endl;
    // cout << "clear_time : " << clear_time << endl;
    // cout << "query_control_time : " << query_control_time << endl;
    // cout << "hash_join_time : " << hash_join_time << endl;
    // cout << "set_table_location_time : " << set_table_location_time << endl;
    // cout << "left_table_hash_time : " << left_table_hash_time << endl;
    // cout << "right_table_hash_time : " << right_table_hash_time << endl;
    // cout << "sum_thread_result_time : " << sum_thread_result_time << endl;
    input_file.close();
}

int main (int argc, char ** argv){

    // local variable
    char instruction;
    int64_t input;
    int64_t value[120];
    int result;
    int i=0, j; // for iteration
    char open_file[120];
    pagenum_t test[10];
    // print notice this program's information
    notice_my_bplustree();
    page_t * header;
    int cur_table_id;
    int num_column;

    fi = NULL;
    init_db(10);
    printf("buffer initialize\n");

    int64_t * test_value = (int64_t *)malloc(sizeof(int64_t)*15);
    int64_t * test_last = (int64_t *)malloc(sizeof(int64_t)*15);
    char * query = (char *)malloc(sizeof(char) * 100);
    

    //for input file
    string test_set;

    //input command
    printf(">> ");
    while (scanf("%c", &instruction) != EOF) {  // scanf 를 입력 받자. EOF 는 ctrl + z 로 입력할 수 다. -1 과 같다.
        switch (instruction) {
        case 'd':           // d 는 delete 이고, input 을 받아 delete하자.
            scanf("%ld", &input);
            result = erase(cur_table_id, input);
            if( result != 0)
                printf("delete failed\n");
            else
                printf("delete success\n");
            print_tree(cur_table_id);
            break;
        case 'i':           //  i는 insert 이고, input을 받아 insert한다.
            i=0;
            scanf("%ld", &input);
            while(1){
                scanf("%ld", &value[i]);
                if(value[i] == -1){
                    value[i] = 0;
                    break;
                }
                i++;
                if( i == num_column)
                    break;
            }
            result = insert(cur_table_id, input, value );
            if( result == 0){
                printf("insert succeeded input : %ld : ", input);
                for(j=0; j<i ;j++)
                    printf("%ld ", value[j]);
                printf("\n");
            }
            else{
                printf("insert failed input : %ld \n", input);
                exit(EXIT_FAILURE);
            }
            print_tree(cur_table_id);
            break;
        case 'a':
            scanf("%ld", &input);
            for( i = 1; i < (int)input; i++){
                
                for( j=0; j<num_column ; j++){
                    value[j] = (i*j)%10 + 12*j;
                }
                result = insert(cur_table_id, (int64_t)i,value);  // 처음 넣으면, key와 value는 같다.
                if( result == 0){
                    printf("insert succeeded input : %d\n", i);
                }
                else{
                    printf("insert failed input : %d\n", i);
                    exit(EXIT_FAILURE);
                }
            }
            print_tree(cur_table_id);
            break;
        case 'f':
            scanf("%ld", &input);            // f랑 p는 값이 있는지 찾는 것, 트리를 print하는 역할이다.
            find_and_print(cur_table_id,(int64_t)input);    //find하고 있으면 그 위치를 출력, 없으면, 레코드는 없다고 출력
            break;
        case 'l':       //l 은 leaves 만 print하는것.
            print_leaves(cur_table_id);
            break;
        case 'q':       // q는 뒤에 무슨 글자가 와도 결국 exit 하게됨.
            while (getchar() != (int)'\n');
			if( fi != NULL){
				fclose(fi);
			}
            return EXIT_SUCCESS;
            break;
        case 't':   // t 는 print tree
            print_tree(cur_table_id);
            break;
        case 'o':   // open file
            scanf("%s", open_file);
            scanf("%d", &num_column);
            cur_table_id = open_table(open_file, num_column);
            if(cur_table_id == -1){
                perror("open file error\n");
                exit(EXIT_FAILURE);
            }
            printf("%d번째 file open success \n", cur_table_id);
            break;
        case 'p' :
            for ( i = 0 ; i<10001 ; i++){
                value[0] = 10;
                value[1] = i%10 + 20;
                value[2] = (i*7)%10+ 30;
                result = insert(cur_table_id, (int64_t)i,value);  // 처음 넣으면, key와 value는 같다.
                if( result == 0){
                    printf("insert succeeded input : %d\n", i);
                }
                else{
                    printf("insert failed input : %d\n", i);
                    exit(EXIT_FAILURE);
                }
            }
            // for ( i = 10000 ; i>5000 ; i--){
            //     value[0] = ;
            //     value[1] = i%10 + '0';
            //     value[2] = (i*7)%10+ '0';
            //     result = insert(cur_table_id, (int64_t)i,value);  // 처음 넣으면, key와 value는 같다.
            //     if( result == 0){
            //         //printf("insert succeeded input : %d, value : %s\n", i,value);
            //     }
            //     else{
            //         printf("insert failed input : %d, value : %s\n", i, value);
            //         exit(EXIT_FAILURE);
            //     }
            // }
            print_tree(cur_table_id);

            for( i = 0 ; i<= 10000 ; i++){
                //test_value = find(cur_table_id, (int64_t)i);
                if( test_value == NULL){
                    printf("not find %d\n", i);
                    exit(EXIT_FAILURE);
                }
            }

            print_tree(cur_table_id);

            for( i=0 ; i<10001; i++){
                result = erase(cur_table_id ,(int64_t)i);
                if( result != 0){
                    printf("delete %d failed\n", i);
                    exit(EXIT_FAILURE);
                }
            }
            for( i = 0 ; i< 10001 ; i++){
                //test_value = find(cur_table_id, (int64_t)i);
                if( test_value != NULL){
                    printf("find %d\n", i);
                    exit(EXIT_FAILURE);
                }
            }
            print_tree(cur_table_id);

            // for( i=1 ; i<10000; i+=2){
            //     result = erase(cur_table_id, (int64_t)i);
            //     if( result != 0){
            //         printf("delete %d failed\n", i);
            //         exit(EXIT_FAILURE);
            //     }
            // }
            // for( i = 1 ; i< 10000 ; i+=2){
            //     test_char = find(cur_table_id, (int64_t)i);
            //     if( test_char != NULL){
            //         printf("find %d : %s\n", i, find(cur_table_id, (int64_t)i));
            //         exit(EXIT_FAILURE);
            //     }
            // }
            print_tree(cur_table_id);
            
            test_last[0] = 1;
            test_last[1] = 2;
            test_last[2] = 3;
            result =insert(cur_table_id, (int64_t)1, test_last);
            print_tree(cur_table_id);
            break;
        case 's' :
            result = shutdown_db();
            if( result == 0){
                printf("shutdown succss\n");
                exit(EXIT_FAILURE);
            }
        case 'c' :
            scanf("%ld", &input);
            result = close_table((int)input);
            if( result == 0)
                printf("close %d table success\n", (int)input);
            break;
        case 'j' :
            scanf("%s", query);
            result = (int)join(query);
            printf("join result : %d\n", result);
            break;
        case 'm' :
            cin >> test_set;
            read_file(test_set);
            break;
        default:    // 아무것도 아니라면, 머라도 입력하라는 표시가 뜸
            printf("you can input only above command\n");
            break;
        }
        while (getchar() != (int)'\n'); // 그 뒤에 무슨 글자가 와도 상관없음
        printf(">> ");
    }
    printf("\n");
    free(test_value);
    free(test_last);
    free(query);
    return 0;
}