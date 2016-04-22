
/*
 * Implementation file for simple MapReduce framework.  Fill in the functions
 * and definitions in this file to complete the assignment.
 *
 * Place all of your implementation code in this file.  You are encouraged to
 * create helper functions wherever it is necessary or will make your code
 * clearer.  For these functions, you should follow the practice of declaring
 * them "static" and not including them in the header file (which should only be
 * used for the *public-facing* API.
 */


/* Header includes */
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include "mapreduce.h"


/* Size of shared memory buffers */
#define MR_BUFFER_SIZE 1024

void *mapHelper(void *);
void *reduceHelper(void *);

static struct map_reduce *new;
pthread_mutex_t lock;
//Struct used to hold arguments of map/reduce functions
//	for use by their respective helper functions.
struct helperArgs{
	struct map_reduce *mr;
	int id, helpThreads;
	const char *inpath, *outpath;
};

/* Allocates and initializes an instance of the MapReduce framework */
struct map_reduce *
mr_create(map_fn map, reduce_fn reduce, int threads)
{
	//Allocate space for new map reduce struct.
	new = (struct map_reduce *)malloc(sizeof(struct map_reduce));
	if(new != 0){				//If malloc succeeded (we have memory left)...
	
		new->map = map;			//Set map function
		new->reduce = reduce;	//Set reduce function
		new->threads = threads;	//Set number of threads

		//Define space for mapper threads, size of number of threads.
		new->mappers = malloc(threads*sizeof(pthread_t));	
		new->args = malloc(threads*sizeof(struct helperArgs));
		new->args2 = malloc(sizeof(struct helperArgs));
		new->mapStatus = malloc(threads*sizeof(int));
		new->prod = malloc(threads*sizeof(int));
		new->cons = malloc(threads*sizeof(int));	
		new->notempty = malloc(threads*sizeof(pthread_cond_t));
		new->notfull = malloc(threads*sizeof(pthread_cond_t));

		//Create locks and then initialize them
		new->locks = malloc(threads*sizeof(pthread_mutex_t));		
		for(int i = 0; i < threads; i++)
			pthread_mutex_init(&new->locks[i], NULL);

		//Initialize conditional variables.
		for(int i = 0; i < threads; i++){
			pthread_cond_init(&new->notempty[i], NULL);
			pthread_cond_init(&new->notfull[i], NULL);
		}

		//Initialize a string array with #threads elements.
		new->buffer = malloc(threads*sizeof(char*));
		
		//Initialize each of the string buffers from above to size of a char.
		for(int i = 0; i < threads; i++){
			new->buffer[i] = malloc(MR_BUFFER_SIZE*sizeof(char));

			//Initialize the counts of each produce and consume pointer to 0.
			new->prod[i] = 0;
			new->cons[i] = 0;
		}

		//Array of uint32s to keep track of each buffer's size
		new->bufferSize = malloc(threads*sizeof(uint32_t));
		return new;
	}
	else{						//Else, we're out of memory.
		//printf("OOM in mr_create.\n");
		free(new);
		return NULL;
	}
}

/* Destroys and cleans up an existing instance of the MapReduce framework */
void
mr_destroy(struct map_reduce *mr){
	//printf("Entered destroy. mr->threads = %d\n", mr->threads);	
	free(mr->mapStatus);
	free(mr->mappers);
	free(mr->args);
	free(mr->args2);
	free(mr->prod);
	free(mr->cons);
	free(mr->locks);
	free(mr->notempty);
	free(mr->notfull);
	//printf("About to free buffers.\n");
	for(int i = 0; i < mr->threads; i++){
		free(mr->buffer[i]);
		//printf("Freed buffer[%d]\n",i);
	}
	free(mr->buffer);
	free(mr->bufferSize);
	free(mr);	
}

/* Begins a multithreaded MapReduce operation */
int
mr_start(struct map_reduce *mr, const char *inpath, const char *outpath)
{
	//Open INFILE once to confirm it exists, then close it.
	mr->INFILE = open(inpath, O_RDONLY);
	if(mr->INFILE == -1)
		return -1;
	close(mr->INFILE);
	
	//printf("Number of threads = %d\n", mr->threads);
	for(int i = 0; i < mr->threads; i++){
		//printf("Entered mapper for loop for %d time.\n", i);

		//BEGIN MAPPER[i] ARGUMENT STRUCT INITIALIZATION
		mr->args[i].id = i;
		mr->mapStatus[i] = -2;
		mr->args[i].helpThreads = mr->threads;
		mr->args[i].mr = mr;
		mr->args[i].inpath = inpath;
		//END MAPPER[i] ARGUMENT STRUCT INITIALIZATION
		//printf("\t\tBefore create, num threads = %d, inpath addr. = %p\n", mr->threads, inpath);
		if(pthread_create(&mr->mappers[i], NULL, &mapHelper,(void *)&mr->args[i])){
			//printf("Error creating mapper thread %d.\n", i);
			return -1;
		}
		//printf("mapStatus[%d] after map return = %d\n", i, mr->mapStatus[i]);
	}
	//printf("Exited mapper for-loop\n");	

	mr->reduceStatus = -2;
	mr->args2->mr = mr;
	mr->args2->outpath = outpath;
	//printf("\t\tBefore Reduce: outpath addr. = %p\n", outpath);
	mr->args2->helpThreads = mr->threads;
	if(pthread_create(&mr->reducer, NULL, &reduceHelper, (void *)mr->args2)){
		//printf("Error creating reducer thread.\n");
		return -1;
	}

	//printf("mr_start returning 0.\n");
	return 0;
}

/* Blocks until the entire MapReduce operation is complete */
int
mr_finish(struct map_reduce *mr)
{
	int check1, check2;
	for(int i = 0; i < mr->threads; i++){		//Wait for all threads to finish.
		//printf("Joining mapper thread %d.\n", i);
		if(pthread_join(mr->mappers[i], NULL))
			return -1;
	}
	//printf("Joined all mapper threads\n");	
	//printf("About to join reducer thread\n");
	if(pthread_join(mr->reducer, NULL))
		return -1;
	//printf("Joined reducer thread\n");

	for(int i = 0; i < new->threads; i++){		//Check for errors in map and reduce functions.
		if(new->mapStatus[i] != 0 || new->reduceStatus != 0){
			//printf("We have an error.\n");
			
			return -1;
		}
	}
 	
	//printf("Returning 0\n");
	return 0;
}

/* Called by the Map function each time it produces a key-value pair */
//Uses a circular array with "produce" and "consume" pointers indicating
//	where in the buffer the program may produce new kvpairs and consume
//	new kvpairs respectively.
//Buffer will store in this order:
//	[key size][k][e][y][value size][value]
//	The key may (and likely will take up multiple bytes of space.
int
mr_produce(struct map_reduce *mr, int id, const struct kvpair *kv)
{
	//Lock produce such that reduce thread will not try and consume while producing.
	pthread_mutex_lock(&mr->locks[id]);
	//printf("Entered mr_produce(%d)\n", id);
	//int *prod = mr->prod;	
	uint32_t keysz, valsz;
	keysz = kv->keysz;
	valsz = kv->valuesz;
	//Step 1: Check for ample space.	
	//If the size of the bytes to be written is larger than the total size of the
	//	 buffer, then there is not enough space. Wait until there is space.
	//Space to be written = size of key + size of value + (8 bytes for keysz+valuesz)
	while((mr->prod[id]+keysz+valsz+8) >= MR_BUFFER_SIZE){
		//printf("Producer(%d) error: Not enough buffer space.\n", id);
		pthread_cond_wait(&mr->notfull[id], &mr->locks[id]);
	}

	//Run when we have space.
	//Step 2: Insert key size, key, value size, and value into buffer
	//Key size will always be 4 bytes.	
	//printf("\tProduce(%d) has enough space, about to write kvpair to buffer.\n", id);
	memmove(&mr->buffer[id][mr->prod[id]], &kv->keysz, 4);				//Move size of key into buffer, first.
	mr->prod[id] += 4;												//Increment produce location by 4 bytes.

	memmove(&mr->buffer[id][mr->prod[id]], kv->key, kv->keysz);		//Move key into buffer, second.
	mr->prod[id] += kv->keysz;								//Increment produce location by key size.

	memmove(&mr->buffer[id][mr->prod[id]], &kv->valuesz, 4);				//Move size of value into buffer, third.
	mr->prod[id] += 4;												//Increment produce location by 4 bytes.

	memmove(&mr->buffer[id][mr->prod[id]], kv->value, kv->valuesz);	//Move value into buffer, fourth.
	mr->prod[id] += kv->valuesz;								//Increment produce location by value size.

	//printf("Produce(%d) success.\n", id);
	
	//Signal to waiting threads blocked by notfull cond var (unblocks them). 
	pthread_cond_signal(&mr->notempty[id]);
	pthread_mutex_unlock(&mr->locks[id]);
	return 1;
}

/* Called by the Reduce function to consume a key-value pair */
//Remove from the buffer a kvpair of size defined by the given kvpair ptr,
//	kv. Will wait for kvpair from mapper. Returns 1 on success, 0 when 
//	mapper returns without producing a pair, and -1 on error.
int
mr_consume(struct map_reduce *mr, int id, struct kvpair *kv)
{
	pthread_mutex_lock(&mr->locks[id]);
	
	//printf("Entered mr_consume(%d)\n", id);
	//int *prod = mr->prod;	
	uint32_t keysz, valsz;
	keysz = kv->keysz;
	valsz = kv->valuesz;
	int cons = 0;
	
	//While there is nothing in our buffer AND the mapper[id]
	//	is not finished, wait for a kvpair.
	while(mr->prod[id] == 0){
		//printf("Consume mapStatus[%d] = %d\n", id, mr->mapStatus[id]);
		if(mr->mapStatus[id] == 0){
			//printf("Mapper(%d) finished, Consume(%d) returning 0.\n", id, id);
			//pthread_mutex_unlock(&mr->locks[id]);
			return 0;
		}
		pthread_cond_wait(&mr->notempty[id], &mr->locks[id]);
	}
	
	//printf("Consume(%d) has something to pass, about to read kvpair from buffer.\n", id);
	//printf("i. prod[%d]=%d, cons=%d\n", id, mr->prod[id], cons);
	memmove(&kv->keysz, &mr->buffer[id][cons], 4);			//Move size of key into kv, first.
	mr->prod[id] -= 4;										//Decrement produce location by 4 bytes.
	cons += 4;												//Increment consume location by 4 bytes.
	//printf("ii. prod[%d]=%d, cons=%d\n", id, mr->prod[id], cons);

	memmove(kv->key, &mr->buffer[id][cons], kv->keysz);			//Move key into kv, second.
	mr->prod[id] -= kv->keysz;									//Decrement produce location by key size.
	cons += kv->keysz;											//Increment consume location by key size.
	//printf("iii. prod[%d]=%d, cons=%d\n", id, mr->prod[id], cons);

	memmove(&kv->valuesz, &mr->buffer[id][cons], 4);			//Move size of value into kv, third.
	mr->prod[id] -= 4;										//Decrement produce location by 4 bytes.
	cons += 4;												//Increment consume location by 4 bytes.
	//printf("iv. prod[%d]=%d, cons=%d\n", id, mr->prod[id], cons);

	memmove(kv->value, &mr->buffer[id][cons], kv->valuesz);		//Move value into kv, fourth.
	mr->prod[id] -= kv->valuesz;									//Decrement produce location by value size.
	cons += kv->valuesz;											//Increment consume location by value size.
	//printf("v. prod[%d]=%d, cons=%d\n", id, mr->prod[id], cons);

	//Shift memory in buffer to the beginning
	memmove(&mr->buffer[id][0], &mr->buffer[id][cons], (MR_BUFFER_SIZE - cons));

	//Signal to waiting threads blocked by notempty cond var (unblocks them).
	pthread_cond_signal(&mr->notfull[id]);
	pthread_mutex_unlock(&mr->locks[id]);

	//printf("Consume(%d) success.\n", id);
		
	return 1;
}

void *mapHelper(void *arg)
{
	//printf("Entered mapHelper\n");	
	struct helperArgs *argument;
	argument = (struct helperArgs*)arg;	
	int INFILE = open(argument->inpath, O_RDONLY);

	//printf("\tinpath addr. = %p, id = %d, arg threads = %d\n", argument->inpath, argument->id, argument->helpThreads);
	
	//Set the pass/fail status of each mapper ID with result of map function.
	argument->mr->mapStatus[argument->id] = argument->mr->map(argument->mr, INFILE, argument->id, argument->helpThreads);	
	close(INFILE);

	return NULL;
}	

void *reduceHelper(void *arg)
{
	//printf("Entered reduceHelper\n");
	struct helperArgs *argument;

	argument = (struct helperArgs*)arg;
	//mr = argument->mr;
	//new->threads = argument->helpThreads;
	int OUTFILE = open(argument->outpath, O_WRONLY | O_CREAT | O_TRUNC, S_IWGRP);
	//printf("\toutpath addr. = %p, OUTFILE = %d, threads = %d\n", argument->outpath, OUTFILE, argument->helpThreads);

	argument->mr->reduceStatus = argument->mr->reduce(argument->mr, OUTFILE, argument->helpThreads);
	//printf("Exiting reduceHelper\n");
	close(OUTFILE);
	//If we returned not 0, then we're out of kvpairs to map.
	return NULL;
}
