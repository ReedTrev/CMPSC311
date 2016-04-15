
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
int numThreads;
//Struct used to hold arguments of map/reduce functions
//	for use by their respective helper functions.
struct helperArgs{
	struct map_reduce *mr;
	int id, helpThreads;
	const char *inpath, *outpath;
};

//struct helperArgs *reduceArgs{
	
//};

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

		numThreads = threads;	//Set placeholder for number of threads

		//Define space for mapper threads, size of number of threads.
		new->mappers = malloc(threads*sizeof(pthread_t));	
		new->args = malloc(threads*sizeof(struct helperArgs));
		new->args2 = malloc(sizeof(struct helperArgs));
		new->mapStatus = malloc(threads*sizeof(int));	
		//new->buffer = malloc(sizeof(struct kvpair)* threads * MR_BUFFER_SIZE);

		//Initialize an arry of kvpair ptrs with #threads elements.
		new->buffer = malloc(threads*sizeof(struct kvpair*));
		
		//Initialize each of the kvpair ptrs from above to size of a kvpair.
		for(int i = 0; i < threads; i++){
			new->buffer[i] = malloc(MR_BUFFER_SIZE*sizeof(struct kvpair));
		}

		//Array of uint32s to keep track of each buffer's size
		new->bufferSize = malloc(threads*sizeof(uint32_t));
		return new;
	}
	else{						//Else, we're out of memory.
		printf("OOM in mr_create.\n");
		free(new);
		return NULL;
	}
}

/* Destroys and cleans up an existing instance of the MapReduce framework */
void
mr_destroy(struct map_reduce *mr){
	printf("Entered destroy. mr->threads = %d\n", mr->threads);	
	free(mr->mapStatus);
	free(mr->mappers);
	//free(mr->reducer);
	free(mr->args);
	free(mr->args2);
	printf("About to free buffers.\n");
	for(int i = 0; i < mr->threads; i++){
		free(mr->buffer[i]);
		printf("Freed buffer[%d]\n",i);
	}
	free(mr->buffer);
	free(mr->bufferSize);
	free(mr);	
}

/* Begins a multithreaded MapReduce operation */
int
mr_start(struct map_reduce *mr, const char *inpath, const char *outpath)
{
	mr->INFILE = open(inpath, O_RDONLY);	//Consider having each mapper individually open the INFILE.
	if(mr->INFILE == -1)
		return -1;
	close(mr->INFILE);
	//int OUTFILE;
	//Open output with write only, create if needed, truncate, and give group write privi.
	//mr->OUTFILE = open(outpath, O_WRONLY | O_CREAT | O_TRUNC, S_IWGRP);	
	
	printf("Number of threads = %d\n", mr->threads);
	for(int i = 0; i < mr->threads; i++){
		printf("Entered mapper for loop for %d time.\n", i);

		//BEGIN MAPPER[i] ARGUMENT STRUCT INITIALIZATION
		//mr->args[i].INFILE = mr->INFILE;
		mr->args[i].id = i;
		//printf("INFILE = %d, id = %d\n", mr->args[i].INFILE, mr->args[i].id);
		mr->mapStatus[i] = 0;
		printf("\t\tBefore create, num threads = %d, inpath addr. = %p\n", mr->threads, inpath);
		mr->args[i].helpThreads = mr->threads;
		mr->args[i].mr = mr;
		mr->args[i].inpath = inpath;
		//END MAPPER[i] ARGUMENT STRUCT INITIALIZATION

		if(pthread_create(&mr->mappers[i], NULL, &mapHelper,(void *)&mr->args[i])){
			printf("Error creating mapper thread %d.\n", i);
			return -1;
		}
	}
	printf("Exited mapper for-loop\n");	

	mr->reduceStatus = 0;
	//mr->args2[0].OUTFILE = mr->OUTFILE;
	mr->args2->mr = mr;
	mr->args2->outpath = outpath;
	printf("\t\tBefore Reduce: outpath addr. = %p\n", outpath);
	mr->args2->helpThreads = mr->threads;
	//printf("mr->args2->helpThreads = %d\n", mr->args2->helpThreads);
	if(pthread_create(&mr->reducer, NULL, &reduceHelper, (void *)mr->args2)){
		printf("Error creating reducer thread.\n");
		return -1;
	}

	printf("mr_start returning 0.\n");
	return 0;
}

/* Blocks until the entire MapReduce operation is complete */
int
mr_finish(struct map_reduce *mr)
{
	int i = 0;
	int check1, check2;
	for(i = 0; i < mr->threads; i++){		//Wait for all threads to finish.
		printf("Joining mapper thread %d.\n", i);
		check1 = pthread_join(mr->mappers[i], NULL);
		if(check1 != 0)
			return -1;
	}
	printf("Joined all mapper threads\n");

	printf("About to join reducer thread\n");
	check2 = pthread_join(mr->reducer, NULL);
	if(check2 != 0)
		return -1;
	printf("Joined reducer thread\n");

	for(i = 0; i < new->threads; i++){		//Check for errors in map and reduce functions.
		if(new->mapStatus[i] != 0 || new->reduceStatus != 0){
			printf("We have an error.\n");
			
			return -1;
		}
	}
 	
	printf("Returning 0\n");
	return 0;
}

/* Called by the Map function each time it produces a key-value pair */
int
mr_produce(struct map_reduce *mr, int id, const struct kvpair *kv)
{
	printf("Entered mr_produce\n");

	for(int i = 0; i < mr->threads; i++){ //DO WORK
		if(mr->bufferSize[id] < MR_BUFFER_SIZE){
			printf("Entered if in mr_produce\n");	
			mr->buffer[id][i].key = kv->key;
			mr->buffer[id][i].value = kv->value;
			mr->bufferSize[id] = mr->bufferSize[id] + kv->keysz + kv->valuesz;
			
		}
		else{
			printf("Buffer is full\n");
			return 0;
		}
	}
	
	//return ret;
	return 1;
}

/* Called by the Reduce function to consume a key-value pair */
int
mr_consume(struct map_reduce *mr, int id, struct kvpair *kv)
{
	return 0;
}

void *mapHelper(void *arg)
{
	printf("Entered mapHelper\n");	
	struct helperArgs *argument;
	argument = (struct helperArgs*)arg;	
	int INFILE = open(argument->inpath, O_RDONLY);

	printf("\tinpath addr. = %p, id = %d, arg threads = %d\n", argument->inpath, argument->id, argument->helpThreads);
	
	//Set the pass/fail status of each mapper ID with result of map function.
	argument->mr->mapStatus[argument->id] = argument->mr->map(argument->mr, INFILE, argument->id, argument->helpThreads);
	printf("Exiting mapHelper\n");
	close(INFILE);
	return NULL;
}	

void *reduceHelper(void *arg)
{
	printf("Entered reduceHelper\n");
	struct helperArgs *argument;

	argument = (struct helperArgs*)arg;
	//mr = argument->mr;
	//new->threads = argument->helpThreads;
	int OUTFILE = open(argument->outpath, O_WRONLY | O_CREAT | O_TRUNC, S_IWGRP);
	printf("\toutpath addr. = %p, OUTFILE = %d, threads = %d\n", argument->outpath, OUTFILE, argument->helpThreads);

	argument->mr->reduceStatus = argument->mr->reduce(argument->mr, OUTFILE, argument->helpThreads);
	printf("Exiting reduceHelper\n");
	close(OUTFILE);
	return NULL;
}
