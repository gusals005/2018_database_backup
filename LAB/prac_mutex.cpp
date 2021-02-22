#include <stdio.h>
#include <pthread.h>

#define NUM_THREADS 10

#define NUM_INCREMENT 1000000

long cnt_global = 0;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;


void* thread_func(void* arg) {
	long cnt_local = 0;
	for (int i = 0; i < NUM_INCREMENT; i++) {
		pthread_mutex_lock(&mutex);
		cnt_global++; // Increase global value
		pthread_mutex_unlock(&mutex);
		cnt_local++; // Increase local value
	}
	pthread_exit((void*)cnt_local);
}

int main(void) {
	pthread_t threads[NUM_THREADS];
	// Create threads
	for (int i = 0; i < NUM_THREADS; i++) {
		if (pthread_create(&threads[i], NULL, thread_func, NULL) < 0) {
			printf("pthread_create error!\n");
			return -1;
		}
	}
	// Wait for threads end
	long ret;
	for (int i = 0; i < NUM_THREADS; i++) {
		pthread_join(threads[i], (void**)&ret);
		printf("thread %ld, local count: %ld\n", threads[i], ret);
	}
	printf("global count: %ld\n", cnt_global);
	return 0;
}
