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

//Struct used to hold arguments of map/reduce functions
//	for use by their respective helper functions.
struct helperArgs{
	//struct map_reduce *mr;
	int INFILE, OUTFILE, id;
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
		//new->reducer = malloc(sizeof(pthread_t));
		new->args = malloc(threads*sizeof(struct helperArgs));
		new->args2 = malloc(sizeof(struct helperArgs));
		new->mapStatus = malloc(threads*sizeof(int));
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
mr_destroy(struct map_reduce *mr)
{
	close(mr->INFILE);
	close(mr->OUTFILE);
	free(mr->mapStatus);
	free(mr->mappers);
	//free(mr->reducer);
	free(mr->args);
	free(mr->args2);
	free(mr);	
}

/* Begins a multithreaded MapReduce operation */
int
mr_start(struct map_reduce *mr, const char *inpath, const char *outpath)
{
	//int INFILE;
	mr->INFILE = open(inpath, O_RDONLY);
	//int OUTFILE;
	//Open ouutpat with write only, create if needed, truncate, and give group write privi.
	mr->OUTFILE = open(outpath, O_WRONLY | O_CREAT | O_TRUNC, S_IWGRP);

	//pthread_t mappers[mr->threads];			//pthread pointers for each mapper thread
	//pthread_t reducer;						//One reducer thread
	//struct mapHelperArgs *args[mr->threads];		//Makes array of arguments for each mapper
	//args = (struct mapHelperArgs *) calloc (1, sizeof(struct mapHelperArgs));
	//struct helperArgs *args = malloc(mr->threads*sizeof(struct helperArgs));
	//struct helperArgs *args2 = malloc (sizeof(struct helperArgs));
	//args2 = (struct reduceHelperArgs *) calloc (mr->threads, sizeof(struct reduceHelperArgs));
	
	int i;
	//int id = 0;
	printf("Number of threads = %d\n", mr->threads);
	for(i = 0; i < mr->threads; i++){
		printf("Entered mapper for loop for %d time.\n", i);
		//args[i] = (struct mapHelperArgs *) calloc (mr->threads, sizeof(struct mapHelperArgs));
		mr->args[i].INFILE = mr->INFILE;
		mr->args[i].id = i;
		printf("INFILE = %d, id = %d\n", mr->args[i].INFILE, mr->args[i].id);
		mr->mapStatus[i] = 0;
		pthread_create(&mr->mappers[i], NULL, &mapHelper, (void *)&mr->args[i]);	
	}
	printf("Exited mapper for-loop\n");	

	mr->reduceStatus = 0;
	pthread_create(&mr->reducer, NULL, &reduceHelper, (void *)&mr->args2);
	
	printf("No errors, begin closing\n");

	//close(INFILE);
	printf("Closed INFILE.\n");
	//close(OUTFILE);
	printf("Closed OUTFILE.\n");
	return 0;
}

/* Blocks until the entire MapReduce operation is complete */
int
mr_finish(struct map_reduce *mr)
{
	int i = 0;
	for(i = 0; i < mr->threads; i++){		//Wait for all threads to finish.
		printf("Joined mapper thread %d\n", i);
		pthread_join(mr->mappers[i], NULL);
	}
	printf("Joined all mapper threads\n");

	printf("About to join reducer thread\n");
	pthread_join(mr->reducer, NULL);
	printf("Joined reducer thread\n");

	for(i = 0; i < mr->threads; i++){		//Check for errors in map and reduce functions.
		if(mr->mapStatus[i] != 0 || mr->reduceStatus != 0){
			printf("We have an error.\n");
			//free(args);
			//free(args2);
			//close(INFILE);
			//close(OUTFILE);
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
	return 0;
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

	//struct helperArgs *argument;
	int INFILE, id;
	struct map_reduce *mr;

	//argument = (struct helperArgs*)arg;

	//mr = argument->mr;
	INFILE = new->INFILE;
	id = argument->id;
	printf("\tINFILE = %d, id = %d, threads = %d\n", INFILE, id, new->threads);

	mr->mapStatus[id] = mr->map(mr, INFILE, id, mr->threads);
	printf("Exiting mapHelper\n");

	return NULL;
}	

void *reduceHelper(void *arg)
{
	printf("Entered reduceHelper\n");
	struct helperArgs *argument;
	int OUTFILE;
	struct map_reduce *mr;

	argument = (struct helperArgs*)arg;

	mr = argument->mr;
	OUTFILE = argument->OUTFILE;
	printf("\tOUTFILE = %d, threads = %d\n", OUTFILE, mr->threads);


	mr->reduceStatus = mr->reduce(mr, OUTFILE, mr->threads);
	return NULL;
}




