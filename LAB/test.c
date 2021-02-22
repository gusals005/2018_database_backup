#include<stdio.h>
#include<stdlib.h>
typedef struct hi{
	int a;
	char b;
}hi;

int main(){

	hi* a = (hi*)malloc(sizeof(hi));

	hi* b = (hi*)malloc(sizeof(hi));


	a->a = 5;
	a->b = 'a';

	*b = *a;
	
	free(a);
	printf("%d %c\n", b->a,b->b);
	return 0;
}
