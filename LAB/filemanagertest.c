#include<stdio.h>
#include<string.h>

typedef struct record{
    int a;
    char b[10];
}record;

typedef struct test{
    long b;
    int a[20][2];
    long af;
    int c;
    record value[31];
} test;

int main(){

    FILE * fi;

    fi = fopen("test.txt", "r+b");

    char ss[10] = "gasgagye";

    test t;
    t.b=1;
    t.a[1][0]=33;
    t.a[1][1]=44;
    t.a[2][0] = 13;
    t.a[2][1] = 24;
    t.a[0][0] = 22;
    t.a[0][1] = 23;
    t.af = 14;
    t.c = 6;
    t.value[1].a = 12;
    memcpy( t.value[1].b , ss , 10 );
    t.value[0].a = 1222222;
    memcpy( t.value[0].b , ss , 10 );


    fseek(fi,0,SEEK_SET);
    fwrite(&t, sizeof(test), 1, fi);

    test tb;
    fseek(fi,0,SEEK_SET);
    fread(&tb,sizeof(test),1,fi);

    printf("b : %ld\n", tb.b);
    int i=0;
    for( i= 0 ; i< 3 ;i++ ){
        printf("a : %d\n", tb.a[i][0]);
        printf("a : %d\n", tb.a[i][1]); 
    }
    printf("af : %ld\n", tb.af);    
    printf("c : %d\n", tb.c);
    printf("value.a : %d\n", tb.value[0].a);
    printf("value.b : %s\n",tb.value[0].b);
    printf("value1.a : %d\n", tb.value[1].a);
    printf("value1.b : %s\n",tb.value[1].b);
    


    return 0;
}