#ifndef __BPT_H__ 
	#include "bpt.h"
#endif

FILE *fi = NULL;
fp* fp_arr = NULL;

int open_db(int table_id, char* pathname , int num_column){		// file open function

    //printf("pathname : %s\n", pathname);
	fp_arr[table_id].fi = fopen(pathname, "r+b");		// first open file to binary format
	if( fp_arr[table_id].fi == NULL){	// if it isn't exist, make and open file
		FILE * mkfile = fopen(pathname, "w+b");
		fclose(mkfile);

		fp_arr[table_id].fi = fopen(pathname, "r+b");
		init_file(fp_arr[table_id].fi , num_column);
		return 0;
	}
	else{	// else, return 0 (success)
		return 0;
	}
}

void init_file(FILE * fi , int num_column){		// Header page 가 없으면,만들기
	
	//우선, file_초기화
	int64_t input[512*3];
	int i,j;
	for( i = 0 ; i<512*3 ; i++){
		input[i] = 0;
	}
	fseek(fi, 0 ,SEEK_SET);
    fwrite(input,sizeof(int64_t), 512*3,fi);
    //make header page
    header_page header_pg;
    header_pg.free_page_offset = PAGE_SIZE*2;
    header_pg.root_page_offset = PAGE_SIZE;
    header_pg.num_of_columns = num_column;
    header_pg.num_of_pages = 3;
    for( i = 0 ; i <508 ; i++)
        header_pg.reserved[i] = 0;

    fseek(fi,0,SEEK_SET);
    fwrite(&header_pg, sizeof(header_page),1,fi);
    
    //make root page, first root is leaf.
    leaf_page root_pg;
    root_pg.parent_page_offset = 0;
    root_pg.is_leaf =  1;
    root_pg.num_of_keys = 0;
    root_pg.special_page_offset = -1;
    for( i =0; i<13; i++) 
        root_pg.reserved[i]=0;

    for( i=0; i< 31; i++){
        root_pg.key_n_value[i].key = 0;
        for(j = 0; j<15 ; j++)
            root_pg.key_n_value[i].value[j] = 0;
    }
    fseek(fi,PAGE_SIZE,SEEK_SET);
    fwrite(&root_pg, sizeof(leaf_page),1,fi);
    //in this function, not need free_page
    fflush(fi);
}

pagenum_t file_alloc_page(int table_id){
	fi = fp_arr[table_id].fi;
	pagenum_t pagenum;
	int64_t free_page_offset = 0;
	int64_t next_free_page_offset=0;
    int i = 0 ; //for iterator.
    //header의 free_page_offset
	fseek(fi,0,SEEK_SET);
	fread(&free_page_offset, sizeof(int64_t), 1,fi);
    //현 free_page의 next_free_page_offset
	fseek(fi,free_page_offset,SEEK_SET);
	fread(&next_free_page_offset, sizeof(int64_t), 1,fi);

	if(next_free_page_offset == 0){     //next free_page 가 없을 때, allocation needed.
        // make a free_page
        free_page new_free_page;
        new_free_page.next_free_page_offset = 0;
        for( i=0; i <511 ; i++)
            new_free_page.reserved[i] = 0;
	    // read current number_of_pages
		fseek(fi,16,SEEK_SET);
		fread(&pagenum,sizeof(int64_t), 1, fi);
        // write new free page
        fseek(fi,0,SEEK_END);
        fwrite(&new_free_page,sizeof(free_page),1,fi);
   		// rewrite free_page_offset, numofpages of Header page
		fseek(fi, 0,SEEK_SET);
		next_free_page_offset = pagenum * PAGE_SIZE;
		fwrite(&next_free_page_offset, sizeof(int64_t), 1 , fi );

        pagenum++;
		fseek(fi,16,SEEK_SET);
		fwrite(&(pagenum), sizeof(pagenum_t), 1,fi);
		fflush(fi);
	}
	else{
		// rewrite free_page_offset of Header page
		fseek(fi, 0,SEEK_SET);
		fwrite(&next_free_page_offset, sizeof(int64_t), 1 , fi );
		fflush(fi);
	}

	return free_page_offset / PAGE_SIZE;
}

void file_free_page(int table_id, pagenum_t pagenum){
	
    fi = fp_arr[table_id].fi;

    int64_t cur_page_offset = pagenum * PAGE_SIZE;
    int i=0;
	//먼저, header 에 연결되어있는 freepage offset 가져오기
	fseek(fi,0,SEEK_SET);
	int64_t next_free_page_offset;
	fread(&next_free_page_offset,sizeof(int64_t),1,fi);

	//header 의 freepage offset에 내 offset을 넣어주기
	fseek(fi,0,SEEK_SET);
	fwrite(&cur_page_offset,sizeof(int64_t),1, fi);


	// 내 페이지로 넘어와서 원래 header에 연결되어있던 offset 연결해주기
    free_page new_free_pg;
    new_free_pg.next_free_page_offset = next_free_page_offset;
    for( i = 0 ; i< 511 ; i++)
        new_free_pg.reserved[i] = 0;
    
	fseek(fi,cur_page_offset, SEEK_SET);
	fwrite(&new_free_pg, sizeof(free_page), 1, fi);
	fflush(fi);
}


void file_read_page(int table_id,pagenum_t pagenum, page_t* dest){

    fi = fp_arr[table_id].fi;

	int64_t cur_page_offset = pagenum*PAGE_SIZE;
    int i,j; // for iterator
    //file 넘겨줄때, dest 의 type을 정해주자.

    // internal 인지 leaf인지는 모를수 있다.
    int isleaf;
    fseek(fi,cur_page_offset+8,SEEK_SET);
    fread(&isleaf, sizeof(int),1,fi);

    if(dest->page_type == 1 && isleaf == 1)
        dest->page_type = 2;

    // page 별 read 시작
    header_page header;
    internal_page internal;
    leaf_page leaf;
    free_page free_pg;

    fseek(fi,cur_page_offset,SEEK_SET);
    switch(dest->page_type){
        // header page
        case 0 :                
            fread(&header, sizeof(header_page),1,fi);
            dest->next_offset = header.free_page_offset;
            dest->root_page_offset = header.root_page_offset;
            dest->num_of_page = header.num_of_pages;
            dest->num_of_columns = header.num_of_columns;
            break;
        //internal page    
        case 1 :       
            fread(&internal,sizeof(internal_page),1,fi);
            dest->next_offset = internal.parent_page_offset;
            dest->is_leaf = internal.is_leaf;
            dest->num_of_keys = internal.num_of_keys;
            dest->special_page_offset = internal.special_page_offset;
            for( i= 0; i<internal.num_of_keys ;i++ ){
                dest->key[i] = internal.key_n_pageoffset[i][0];
                dest->page_offset[i] = internal.key_n_pageoffset[i][1];
            }
            break;
        //leaf page
        case 2 :   
            fread(&leaf,sizeof(leaf_page),1,fi);
            dest->next_offset = leaf.parent_page_offset;
            dest->is_leaf = leaf.is_leaf;
            dest->num_of_keys = leaf.num_of_keys;
            dest->special_page_offset = leaf.special_page_offset;
            for( i= 0; i<leaf.num_of_keys ;i++ ){
                dest->key[i] = leaf.key_n_value[i].key;
                for( j= 0 ; j<15; j++)
                    dest->value[i][j] = leaf.key_n_value[i].value[j];  
            }
            break;
        //free page
        case 3 :   
            fread(&free_pg, sizeof(free_page), 1,fi);
            dest->next_offset = free_pg.next_free_page_offset;
            break;
        default:
            printf("pagetype 미정\n");
            exit(EXIT_FAILURE);
    }
    dest->pagenum = pagenum;
}
 
void file_write_page(int table_id, pagenum_t pagenum, const page_t* src){

//    printf("%ld pagenum write\n", pagenum);
    fi = fp_arr[table_id].fi;
    int64_t cur_page_offset = pagenum * PAGE_SIZE;
	
	int i=0, j=0;

    //src->page_type으로 write 하기.
    header_page header;
    internal_page internal;
    leaf_page leaf;
    free_page free_pg;
	
    fseek(fi,cur_page_offset, SEEK_SET);
	switch(src->page_type){
		case 0 :	//header
			header.free_page_offset = src->next_offset;
            header.root_page_offset = src->root_page_offset;
            header.num_of_pages = src->num_of_page;
            header.num_of_columns = src->num_of_columns;
            for( i = 0 ; i < 508 ; i++)
                header.reserved[i] = 0;
            fwrite(&header, sizeof(header_page), 1,fi);
            break;
		case 1 :	//internal
			internal.parent_page_offset = src->next_offset;
            internal.is_leaf = src->is_leaf;
            internal.num_of_keys = src->num_of_keys;
            for( i = 0 ; i < 13 ; i++)
                internal.reserved[i] = 0;
            internal.special_page_offset = src->special_page_offset;
            for( i = 0 ; i < src->num_of_keys ; i++){
                internal.key_n_pageoffset[i][0] = src->key[i];
                internal.key_n_pageoffset[i][1] = src->page_offset[i];
            }
            fwrite(&internal, sizeof(internal_page),1,fi);
            break;
		case 2 :	//leaf
			leaf.parent_page_offset = src->next_offset;
            leaf.is_leaf = src->is_leaf;
            leaf.num_of_keys = src->num_of_keys;
            for( i = 0 ; i < 13 ; i++)
                leaf.reserved[i] = 0;
            leaf.special_page_offset = src->special_page_offset;
            for( i = 0 ; i < src->num_of_keys ; i++){
                leaf.key_n_value[i].key = src->key[i];
                for( j=0; j <15; j++ )
                    leaf.key_n_value[i].value[j] = src->value[i][j];
            }
            fwrite(&leaf, sizeof(leaf_page),1,fi);
			break;
		default : // free page
			free_pg.next_free_page_offset = src->next_offset;
            for(i = 0 ; i <511 ;i++)
                free_pg.reserved[i] = 0;
            fwrite(&(free_pg), sizeof(free_page), 1, fi);
			break;
	}
    fflush(fi);
}
