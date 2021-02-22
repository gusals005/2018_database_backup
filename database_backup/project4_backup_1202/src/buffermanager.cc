#ifndef __BPT_H__
    #include "bpt.h"
#endif

char tables[11][101];
int buffer_pool_size = 0;
buffer* header_buffer = NULL;
bool is_thread_created = false;


void buffer_init( buffer* buffer){
    buffer->page = NULL;
    buffer->table_id = -1;
    buffer->pagenum = -1;
    buffer->is_dirty = 0;
    buffer->pin_count = 0;
    buffer->next_buffer = NULL;
    buffer->prev_buffer = NULL;
}

void buffer_init_without_list(buffer* iter){
    free(iter->page);
    iter->page = NULL;
    iter->is_dirty = 0;
    iter->pin_count=0;
    iter->pagenum = -1;
    iter->table_id = -1;
}

void free_buffer_pool(buffer* iter){
    if( iter -> next_buffer != NULL)
        free_buffer_pool(iter->next_buffer);
    free(iter);
}
void set_buffer_list(buffer* curbuffer){ 
    if( curbuffer->next_buffer == NULL && curbuffer->prev_buffer == NULL){       // 그 buffer 는 지금 아예 연결안되어 있는상태.
        curbuffer->next_buffer = header_buffer->next_buffer;
        curbuffer->prev_buffer = header_buffer;
        header_buffer->next_buffer = curbuffer;
        if(curbuffer->next_buffer != NULL)
            curbuffer->next_buffer->prev_buffer = curbuffer;
    } 
    else if(curbuffer->next_buffer == NULL){        // list의 맨 마지막 아이라면,
        curbuffer->prev_buffer->next_buffer = curbuffer->next_buffer;
        curbuffer->prev_buffer = header_buffer;
        curbuffer->next_buffer = header_buffer->next_buffer;
        header_buffer->next_buffer->prev_buffer = curbuffer;
        header_buffer->next_buffer = curbuffer;
    }
    else{           //prev 는 무조건 있음. list의 중간이다.
        curbuffer->prev_buffer->next_buffer = curbuffer->next_buffer;
        curbuffer->next_buffer->prev_buffer = curbuffer->prev_buffer;

        curbuffer->next_buffer = header_buffer->next_buffer;
        curbuffer->prev_buffer = header_buffer;
        
        curbuffer->next_buffer->prev_buffer = curbuffer;
        curbuffer->prev_buffer->next_buffer = curbuffer;
    }
}

int init_db(int num_buf){
    
    header_buffer = (buffer*)malloc(sizeof(buffer));
    buffer_init(header_buffer);
 
    fp_arr = (fp*)malloc( 11*sizeof(fp));
    
    buffer_pool_size = num_buf;
    int i=0; // for iterator
    for( i= 0 ; i< buffer_pool_size ; i++){
        buffer* add_free_buffer = (buffer*)malloc(sizeof(buffer));
        buffer_init(add_free_buffer);
        set_buffer_list(add_free_buffer);
    }
    for( i= 1; i <11 ; i++)
        fp_arr[i].fi = NULL;
    return 0;
}

int open_table(char * pathname, int num_column){

    int i = 0;
    int result;
    int blank =-1;
    for ( i=1 ; i<11 ; i++){
        if(!strcmp(tables[i], pathname)){      //strcmp는 0일때 같음.
            result = open_db( i, pathname, num_column);
            return i;
        }
        else if ( tables[i][0] == '\0'){
            blank = i;
            break;
        }
    }
    if( blank == -1)
        return -1;

    result = open_db(blank, pathname, num_column);
    memcpy(tables[blank],pathname,101);

    //for join, get_table already()
    table[blank] = get_table(blank);


    //make threads and do thread function.
    if(!is_thread_created){
        for( long i =0; i<NUM_THREADS ; i++){
            if(pthread_create(&threads[i],0,input_to_hash,(void*)i)<0){
                printf("pthread_create error!\n");
                exit(EXIT_FAILURE);
            }
            is_thread_created = true;
        }
    }
    return blank;
//    return -1;  // open fail.
}

int close_table(int table_id){
    fi = fp_arr[table_id].fi;

    buffer* iter = header_buffer;
    
    while( iter->next_buffer != NULL){      //모든 list를 search하기 위해
        iter = iter->next_buffer;
        if(iter->table_id == table_id ){         // 만약, table_id 가같으면
            if(iter->is_dirty == 1){            //write가 필요하다면, write하고, list 제외하고 reset.
                file_write_page(table_id,iter->pagenum , iter->page);
            }
            buffer_init_without_list(iter);
        }
    }
    tables[table_id][0] = '\0';
    fclose(fp_arr[table_id].fi);
    return 0;
}

int shutdown_db(){
    int i=0,open;
    buffer* iter = header_buffer;
    while( iter->next_buffer != NULL){      //모든 list를 search하기 위해
        iter = iter->next_buffer;
        if(iter->page != NULL)
            fi = fp_arr[iter->table_id].fi; 
        if(iter->is_dirty == 1){            //write가 필요하다면, write하고, list 제외하고 reset.
            file_write_page(iter->table_id ,iter->pagenum , iter->page);
        }
        buffer_init_without_list(iter);
    }
    buffer_pool_size = 0;
    for( i=0 ; i<11 ;i++ ){
        tables[i][0] = '\0';
        if( fp_arr[i].fi != NULL)
            fclose(fp_arr[i].fi);
    }
    free_buffer_pool(header_buffer);
    return 0;
}


buffer* select_buffer(){        // 빈 버퍼가 있으면, 그 버퍼의 주소가 return 되고, 없으면 NULL이 리턴.
   
    buffer* iter = header_buffer;
    while( iter->next_buffer != NULL){
        iter = iter->next_buffer;
        if(iter->page == NULL)
            return iter;
    }

    while(1){
        if( iter == header_buffer){
            while(iter->next_buffer != NULL)
                iter = iter->next_buffer;
        }

        if(iter->pin_count == 0)
            return iter;
        else
            iter = iter->prev_buffer;
    }
}


buffer* find_in_pool(int table_id, pagenum_t pagenum){
    buffer* iter = header_buffer;
    while( iter->next_buffer != NULL){
        iter = iter->next_buffer;
        if(iter->table_id == table_id && iter->pagenum == pagenum)
            return iter;
    }
    return NULL;
}

void buffer_get_page( int table_id ,pagenum_t pagenum, page_t * dest){

    fi = fp_arr[table_id].fi;
    //find page in buffer pool
    int i;

    // printf("%ld page pin++\n", pagenum);
    // printf("%ld page 의 종류 : ",pagenum);
    // if( dest->page_type == 0 )
    //     printf("header\n");
    // else if ( dest->page_type == 1)
    //     printf("internal or leaf\n");
    // else if ( dest->page_type == 2)
    //     printf("leaf\n");
    // else
    //     printf("free\n");
        
    buffer* index = find_in_pool(table_id , pagenum);

    if( index != NULL){     // buffer pool 안에 page를 찾은거임.
        while( index->pin_count > 0) ;
        index->pin_count++;
        index->pagenum = pagenum;
        index->page->pagenum = pagenum;
        *dest = *(index->page);

    }
    else{           // buffer pool 안에 해당 page를 못찾았음.
        
        index = select_buffer();

        if( index->page == NULL ){        // 빈 프레임이 있다면,

            index->pin_count++;

            page_t * read_page = (page_t *)malloc(sizeof(page_t));        
            
            file_read_page( table_id, pagenum, dest );

            *read_page = *dest;
            index->page = read_page;
            index->is_dirty = 0;
            index->pagenum = pagenum;
            index->table_id = table_id;

            //list 연결
            set_buffer_list(index);
        }
        else{           //빈 프레임이 없다면, index에는 지금 pin되지 않은 아이가 쓰여져있음.
            if(index->is_dirty > 0){    //만일, file에 써져야 된다면,
                fi = fp_arr[index->table_id].fi;
                file_write_page(index->table_id, index->pagenum , index->page);
            }

            index->pin_count++;
            fi = fp_arr[table_id].fi;
            file_read_page(table_id, pagenum,dest);
            *(index->page) = *dest;
            index->is_dirty = 0;
            index->pagenum = pagenum;
            index->table_id = table_id;

            //list 연결
            set_buffer_list(index);
        }
    }
}


void buffer_put_page( int table_id , pagenum_t pagenum, page_t * src  ){

    // printf("%ld page pin--\n", pagenum);
    // printf("%ld page 의 종류 : ",pagenum);
    // if( src->page_type == 0 )
    //     printf("header\n");
    // else if ( src->page_type == 1)
    //     printf("internal or leaf\n");
    // else if ( src->page_type == 2)
    //     printf("leaf\n");
    // else
    //     printf("free\n");

    int i;
    page_t * src_copy = (page_t*)malloc(sizeof(page_t));
    *src_copy = *src;
    //find page in buffer pool
    buffer* index = find_in_pool( table_id, pagenum );
    // if exist, pin_count up.
    // else , find a page and push down to disk, then push up new page.
    if( index != NULL){         // buffer pool 안에 있다면.
        //정보 수정
        free(index->page);  // 원래 page free 해주고.
        index->page = src_copy;
        index->is_dirty = 1;
        index->pagenum = pagenum;
        index->table_id = table_id;
        //list연결
        set_buffer_list(index);
        if( index->pin_count >0)
            index->pin_count--;
    }
    else{
        index = select_buffer();            // frame 찾기
        if( index->page == NULL){                             // 굳이 frame 교환이 필요없을 때 (어떤 buffer가 비어있을때))
            
            index->pin_count++;
            //그 페이지를 buffer에 넣기, initialize.
            index->page = src_copy;
            index->is_dirty = 1;
            index->table_id = table_id;
            index->pagenum = pagenum;
            //list 연결
            set_buffer_list(index);
            index->pin_count--;
        }
        else{                                                   // 어떤 frame 하나를 내리고 다시 올릴때,
            if(index->is_dirty > 0){  //dirty여서 없애야한다면,
                fi = fp_arr[index->table_id].fi;
                file_write_page(index->table_id, index->pagenum, index->page);
            }   
            //그자리에 내 값쓰기.
            index->pin_count++;
            free(index->page);
            index->page = src_copy;
            index->pagenum = pagenum;
            index->is_dirty = 1;
            index->table_id = table_id;
            //list 연결
            set_buffer_list(index);
            index->pin_count--;
        }
    }
}


void buffer_free_page(int table_id, pagenum_t pagenum){
    fi = fp_arr[table_id].fi;

    //printf("%ld page free\n", pagenum);
    buffer* index = find_in_pool(table_id, pagenum);

    page_t* header = (page_t*)malloc(sizeof(page_t));
    header->page_type = 0;
    buffer_get_page(table_id,0,header);
    header->next_offset = pagenum * PAGE_SIZE;
    buffer_put_page(table_id,0,header);
    if( index == NULL){
        file_free_page(table_id, pagenum);
    }
    else{
        buffer_init_without_list(index);
        file_free_page(table_id, pagenum);
    }
}

pagenum_t buffer_alloc_page(int table_id){
    fi = fp_arr[table_id].fi;
    page_t* header = (page_t*)malloc(sizeof(page_t));
    page_t* free_pg = (page_t*)malloc(sizeof(page_t));
    header->page_type = 0;
    free_pg->page_type = 3;
    buffer_get_page(table_id, 0, header);
    buffer_get_page(table_id,header->next_offset/PAGE_SIZE, free_pg);

    if(free_pg->next_offset == 0){
        header->next_offset = header->num_of_page * PAGE_SIZE;
        header->num_of_page += 1;
    }
    else{
        header->next_offset = free_pg->next_offset;
    }
    buffer_put_page(table_id,0,header);
    pagenum_t alloc = file_alloc_page(table_id);
    buffer_put_page(table_id,alloc, free_pg);

    return alloc;
}
