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

struct mapHelperArgs{
	struct map_reduce *mr;
	int INFILE;
	int id;
};

struct reduceHelperArgs{
	struct map_reduce *mr;
	int OUTFILE;
};

/* Allocates and initializes an instance of the MapReduce framework */
struct map_reduce *
mr_create(map_fn map, reduce_fn reduce, int threads)
{
	//If we are able to allocate the memory, initialize new	map_reduce struct ptr.	
	struct map_reduce *new = (struct map_reduce *)malloc(sizeof(struct map_reduce));
	if(new != 0){
	
		new->map = map;
		new->reduce = reduce;
		new->threads = threads;

		return new;
	}
	else{
		printf("OOM in mr_create.\n");
		free(new);
		return NULL;
	}
}

/* Destroys and cleans up an existing instance of the MapReduce framework */
void
mr_destroy(struct map_reduce *mr)
{
	free(mr);	
}

/* Begins a multithreaded MapReduce operation */
int
mr_start(struct map_reduce *mr, const char *inpath, const char *outpath)
{
	int INFILE;
	INFILE = open(inpath, O_RDONLY);
	int OUTFILE;
	OUTFILE = open(outpath, O_WRONLY |  O_CREAT | O_TRUNC); //might need O_TRUNC

	//pthread_t mappers[mr->threads];			//pthread pointers for each mapper thread
	//pthread_t reducer;						//One reducer thread
	struct mapHelperArgs *args[mr->threads];		//Makes array of arguments for each mapper
	//args = (struct mapHelperArgs *) calloc (1, sizeof(struct mapHelperArgs));
	struct reduceHelperArgs *args2;
	args2 = (struct reduceHelperArgs *) calloc (mr->threads, sizeof(struct reduceHelperArgs));
	pthread_attr_t attr;
	pthread_attr_init(&attr);
	int i, j;
	int id = 0;
	int check1 = 0;
	int check2 = 0;
	printf("Number of threads = %d\n", mr->threads);
	for(i = 0; i < mr->threads; i++){
		printf("Entered mapper for loop for %d time.\n", i);
		args[i] = (struct mapHelperArgs *) calloc (mr->threads, sizeof(struct mapHelperArgs));
		args[i]->mr = mr;
		args[i]->INFILE = INFILE;
		args[i]->id = id;
		printf("INFILE = %d, id = %d\n", args[i]->INFILE, args[i]->id);
		mr->mapStatus[i] = 0;
		pthread_create(&mr->mappers[i], &attr, &mapHelper, (void *)args[i]);
		//check1 = mr->map(mr, INFILE, id, mr->threads);
		//pthread_join(mappers[i], NULL);
		id++;	
	}
	printf("Exited mapper for-loop\n");	

	mr->reduceStatus = 0;
	pthread_create(&mr->reducer, &attr, &reduceHelper, (void *)args2);
	
	printf("No errors, begin freeing\n");
	//for(i = 0; i < mr->threads; i++)
		//free(args[i]);
	//args = NULL;
	//free(args);
	//free(args2);
	//printf("Freed args2.\n");
	close(INFILE);
	printf("Closed INFILE.\n");
	close(OUTFILE);
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

	struct mapHelperArgs *argument;
	int INFILE, id;
	struct map_reduce *mr;

	argument = (struct mapHelperArgs*)arg;

	mr = argument->mr;
	INFILE = argument->INFILE;
	id = argument->id;
	printf("INFILE = %d, id = %d \n", INFILE, id);

	mr->mapStatus[id] = mr->map(mr, INFILE, id, mr->threads);
	printf("Exiting mapHelper\n");

	return NULL;
}	

void *reduceHelper(void *arg)
{
	struct reduceHelperArgs *argument;
	int OUTFILE;
	struct map_reduce *mr;

	argument = (struct reduceHelperArgs*)arg;

	mr = argument->mr;
	OUTFILE = argument->OUTFILE;

	mr->reduceStatus = mr->reduce(mr, OUTFILE, mr->threads);
	return NULL;
}




