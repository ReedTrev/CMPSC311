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
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "mapreduce.h"


/* Size of shared memory buffers */
#define MR_BUFFER_SIZE 1024


/* Allocates and initializes an instance of the MapReduce framework */
struct map_reduce *
mr_create(map_fn map, reduce_fn reduce, int nmaps)
{
	struct map_reduce new = (struct map_reduce *)malloc(sizeof(struct map_reduce));
	
	new->nmaps = nmaps;

	//Client called
	if(reduce == NULL){
		new->map = map;
		new->reduce = NULL;
		new->mapSockets = malloc(nmaps*sizeof(int));

	}

	//Server called
	else if(map == NULL){
		new->reduce = reduce;
		new->map = NULL;
		new->serverSockets = malloc(nmaps*sizeof(int));

	}

	else
		return NULL;
}

/* Destroys and cleans up an existing instance of the MapReduce framework */
void
mr_destroy(struct map_reduce *mr)
{

}

/* Begins a multithreaded MapReduce operation */
int
mr_start(struct map_reduce *mr, const char *path, const char *ip, uint16_t port)
{
	//Client
	if(mr->reduce == NULL){
		int INFILE = open(path, RD_ONLY);
		struct addr_in conn = new struct addr_in;
		for(int i = 0; i < mr->nmaps; i++){
		
			mr->mapSockets[i] = socket(AF_INET, SOCK_STREAM, 0);	//Create client socket.
			connect(mr->mapSockets[i], );
		}
		return 0;
	}

	//Server
	else if(mr->map == NULL){
		int OUTFILE = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
		for(int i = 0; i < mr->nmaps; i++){

		}
		return 0;
	}
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
