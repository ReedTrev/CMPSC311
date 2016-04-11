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

#include "mapreduce.h"


/* Size of shared memory buffers */
#define MR_BUFFER_SIZE 1024


/* Allocates and initializes an instance of the MapReduce framework */
struct map_reduce *
mr_create(map_fn map, reduce_fn reduce, int threads)
{
	//If we are able to allocate the memory, initialize new		
	struct map_reduce *new = (struct map_reduce *)malloc(sizeof(struct map_reduce*));
	if(new != NULL){
	
		new->map = map;
		new->reduce = reduce;
		new->threads = threads;

		return new;
	}
	else
		return NULL;
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
	FILE* INFILE;
	INFILE = fdopen(*inpath, "r");
	FILE* OUTFILE;
	OUTFILE = fdopen(*outpath, "w");

	int i;
	int id = 0;
	int check1;
	int check2;
	for(i = 0; i < mr->threads; i++){
		check1 = map(mr, INFILE, id, mr->threads);
		check2 = reduce(mr, OUTFILE, mr->threads);
		id++;
		if(check1 != 0 || check2 != 0)
			return -1;	
	}

	return 0;
}

/* Blocks until the entire MapReduce operation is complete */
int
mr_finish(struct map_reduce *mr)
{
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
