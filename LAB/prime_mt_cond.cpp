#include <stdio.h>
#include <pthread.h>
#include <math.h>

#define NUM_THREAD  10

int thread_ret[NUM_THREAD];

int range_start;
int range_end;
int check_thread=0;

pthread_cond_t cond = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

bool IsPrime(int n) {
    if (n < 2) {
        return false;
    }

    for (int i = 2; i <= sqrt(n); i++) {
        if (n % i == 0) {
            return false;
        }
    }
    return true;
}

void* ThreadFunc(void* arg) {
   
	while( 1 ) {
		long tid = (long)arg;
		
		printf("%ld thread 실행\n", tid);
		pthread_mutex_lock(&mutex);
		//printf("%ld 번째 thread 에서 lock 했음\n", tid);
		pthread_cond_wait(&cond, &mutex);
		//printf("%ld 번째 thread에서 wait함\n",tid);
		pthread_mutex_unlock(&mutex);
		//printf("%ld번째 thread에서 unlock함\n",tid);
    
   		// Split range for this thread
    	int start = range_start + ((range_end - range_start) / NUM_THREAD) * tid;
    	int end = range_start + ((range_end - range_start) / NUM_THREAD) * (tid+1);
    	if (tid == NUM_THREAD - 1) {
        	end = range_end + 1;
    	}
    
   		long cnt_prime = 0;
   		for (int i = start; i < end; i++) {
   		    if (IsPrime(i)) {
 	    	       cnt_prime++;
  	 		}
   		}
	
    	thread_ret[tid] = cnt_prime;

		if( check_thread != NUM_THREAD -1){
			check_thread++;
			pthread_mutex_lock(&mutex);
		//printf("%ld 번째 thread 에서 lock 했음\n", tid);
			pthread_cond_wait(&cond, &mutex);
		//printf("%ld 번째 thread에서 wait함\n",tid);
			pthread_mutex_unlock(&mutex);
		}
		else{
			check_thread = 0;
			pthread_cond_broadcast(&cond);
		}

	}
    return NULL;
}
int main(void) {
    pthread_t threads[NUM_THREAD];
    
	for( long i = 0 ; i < NUM_THREAD ; i++){
		if (pthread_create(&threads[i], 0, ThreadFunc, (void*)i) < 0) {
            printf("pthread_create error!\n");
	        return 0;
		}
	}
    while (1) {
        // Input range
        scanf("%d", &range_start);
        if (range_start == -1) {
            break;
        }
        scanf("%d", &range_end);

		printf("%d 와 %d 를 입력받음\n",range_start,range_end);
		pthread_cond_broadcast(&cond);
	
		/*
        // Create threads to work
        for (long i = 0; i < NUM_THREAD; i++) {
            if (pthread_create(&threads[i], 0, ThreadFunc, (void*)i) < 0) {
                printf("pthread_create error!\n");
                return 0;
            }
        }
		*/
        // Wait threads end
		printf("조인시작\n");
        // for (int i = 0; i < NUM_THREAD; i++) {
		// //	pthread_mutex_lock(&mutex);
		// 	printf("%d 번째 join 시작\n", i);
		// //	pthread_cond_broadcast(&cond);
		// 	pthread_join(threads[i],NULL);
		// //	pthread_mutex_unlock(&mutex);
		// 	printf("%d 번째 join 끝\n",i);
        // }

        // Collect results
		pthread_mutex_lock(&mutex);
		pthread_cond_wait(&cond,&mutex);
		pthread_mutex_unlock(&mutex);
        int cnt_prime = 0;
        for (int i = 0; i < NUM_THREAD; i++) {
            cnt_prime += thread_ret[i];
        }
        printf("number of prime: %d\n", cnt_prime);
    }
 
    return 0;
}

