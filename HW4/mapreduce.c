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

struct map_reduce *new;
//struct sockaddr_in *conn;

struct helperArgs{
	struct map_reduce *mr;
	int id, helpThreads;
	const char *path;
};

/* Allocates and initializes an instance of the MapReduce framework */
struct map_reduce *
mr_create(map_fn map, reduce_fn reduce, int nmaps)
{
	new = (struct map_reduce *)malloc(sizeof(struct map_reduce));
	if(new != 0){
		new->conn = (struct sockaddr_in *)malloc(sizeof(struct sockaddr_in));
		new->args = malloc(nmaps*sizeof(struct helperArgs));
		new->args2 = malloc(sizeof(struct helperArgs));
		new->nmaps = nmaps;

		new->buffer = malloc(nmaps*sizeof(char*));
		//Initialize each of the string buffers from above to size of a char.
		for(int j = 0; j < nmaps; j++)
			new->buffer[j] = malloc(MR_BUFFER_SIZE*sizeof(char));

		//Client called
		if(reduce == NULL){
			new->mappers = malloc(nmaps*sizeof(pthread_t));
			new->mapStatus = malloc(nmaps*sizeof(int));
			new->map = map;
			new->reduce = NULL;
			new->clientSockets = malloc(nmaps*sizeof(int));
			return new;
		}

		//Server called
		else if(map == NULL){
			new->reduce = reduce;
			new->map = NULL;
			new->serverSockets = malloc(nmaps*sizeof(int));
			return new;
		}

		else
			return NULL;
	}

	else{						//Else, we're out of memory.
		//printf("OOM in mr_create.\n");
		free(new);
		return NULL;
	}
}

/* Destroys and cleans up an existing instance of the MapReduce framework */
void
mr_destroy(struct map_reduce *mr)
{
	//General destroy
	free(mr->conn);
	free(mr->args);
	free(mr->args2);

	//Client destroy
	if(mr->reduce == NULL){
		for(int i = 0; i < mr->nmaps; i++)
			close(mr->clientSockets[i]);
		free(mr->mapStatus);
		free(mr->mappers);
		free(mr->clientSockets);
	}
	//Server destroy
	else{
		for(int i = 0; i < mr->nmaps; i++)
			close(mr->serverSockets[i]);
		close(mr->listSocket);
		free(mr->serverSockets);
	}

	for(int i = 0; i < mr->nmaps; i++){
		free(mr->buffer[i]);
		//printf("Freed buffer[%d]\n",i);
	}
	free(mr->buffer);
	free(mr);
}

/* Begins a multithreaded MapReduce operation */
int
mr_start(struct map_reduce *mr, const char *path, const char *ip, uint16_t port)
{
	bcopy(ip, (char *)&mr->conn->sin_addr.s_addr, sizeof(ip));

	mr->conn->sin_family = AF_INET;
	mr->conn->sin_port = htons(port);
	//mr->conn->sin_addr = ip;

	//Client
	if(mr->reduce == NULL){

		for(int i = 0; i < mr->nmaps; i++){
			
			mr->args[i].id = i;
			mr->mapStatus[i] = -2;
			mr->args[i].helpThreads = mr->nmaps;
			mr->args[i].mr = mr;
			mr->args[i].path = path;

			mr->clientSockets[i] = socket(AF_INET, SOCK_STREAM, 0);	//Create client socket.
			if(connect(mr->clientSockets[i], (struct sockaddr *)&mr->conn, sizeof(mr->conn) < 0)) //Wait for Connection of socket to IP:port
				perror("ERROR: Socket did not connect.");

			//Connection established, create thread.
			if(pthread_create(&mr->mappers[i], NULL, &mapHelper,(void *)&mr->args[i])){
				//printf("Error creating mapper thread %d.\n", i);
				return -1;
			}

			
		}
		return 0;
	}

	//Server
	else if(mr->map == NULL){

		mr->reduceStatus = -2;
		mr->args2->helpThreads = mr->nmaps;
		mr->args2->mr = mr;
		mr->args2->path = path;

		mr->listSocket = socket(AF_INET, SOCK_STREAM, 0);	//Create server listen socket.

		if(bind(mr->listSocket, (struct sockaddr *)&mr->conn, sizeof(mr->conn) < 0)) //Bind listen socket to IP:port
			perror("ERROR: Binding failed.");

		if(pthread_create(&mr->reducer, NULL, &reduceHelper,(void *)mr->args2)){	//Create server thread.
			//printf("Error creating mapper thread %d.\n", i);
			return -1;
		}
		return 0;
	}
	else
		return -1;
}

/* Blocks until the entire MapReduce operation is complete */
int
mr_finish(struct map_reduce *mr)
{
	//Client
	if(mr->reduce == NULL){
		for(int i = 0; i < mr->nmaps; i++){		//Wait for all threads to finish.
			//printf("Joining mapper thread %d.\n", i);
			if(pthread_join(mr->mappers[i], NULL))
				return -1;
			close(mr->clientSockets[i]);
		}

		for(int i = 0; i < mr->nmaps; i++)		//Check for errors in mapper threads.
			if(new->mapStatus[i] != 0)				
				return -1;
			

		return 0;
	}
		//printf("Joined all mapper threads\n");	
		//printf("About to join reducer thread\n");
	//Server
	else{
		if(pthread_join(mr->reducer, NULL))
			return -1;

		if(mr->reduceStatus != 0)		//Check for error in reduce thread.
			return -1;

		for(int i = 0; i < mr->nmaps; i++)
			close(mr->serverSockets[i]);
	 	
	 	close(mr->listSocket);
		return 0;
	}
}

/* Called by the Map function each time it produces a key-value pair */
int
mr_produce(struct map_reduce *mr, int id, const struct kvpair *kv)
{
	char *buff = malloc(MR_BUFFER_SIZE*sizeof(char));	//Create temporary buffer
	int prod = 0, size = 0;
	char *ping = malloc(sizeof(char));

	printf("Key size in produce = %d", kv->keysz);

	memmove(&buff[prod], &kv->keysz, sizeof(uint32_t));				//Move key into buffer, second.
	prod += sizeof(uint32_t);

	memmove(&buff[prod], kv->key, kv->keysz);			//Move value into buffer, fourth.
	prod += kv->keysz;

	memmove(&buff[prod], &kv->valuesz, sizeof(uint32_t));			//Move value into buffer, fourth.
	prod += sizeof(uint32_t);

	memmove(&buff[prod], kv->value, kv->valuesz);			//Move value into buffer, fourth.
	prod += kv->valuesz;

	write(mr->clientSockets[id], &buff, prod);				//Write to server through socket connection.
	read(mr->clientSockets[id], &ping, 1);					//Wait for server to finish reading.

	free(buff);
	free(ping);

	return 1;
}

/* Called by the Reduce function to consume a key-value pair */
int
mr_consume(struct map_reduce *mr, int id, struct kvpair *kv)
{
	char *buff = malloc(MR_BUFFER_SIZE*sizeof(char));	//Create temporary buffer
	int cons = 0, size = 0;
	char *ping = malloc(sizeof(char));

	if(read(mr->serverSockets[id], &buff, MR_BUFFER_SIZE) == 0){
		free(buff);
		free(ping);		
		return 0;			//Read from socket.
	}
	printf("We have something to read. \n");
	memmove(&kv->keysz, &buff[cons], sizeof(uint32_t));
	cons += sizeof(uint32_t);

	memmove(kv->key, &buff[cons], kv->keysz);						//Move key into buffer, second.
	cons += kv->keysz;

	memmove(&kv->valuesz, &buff[cons], sizeof(uint32_t));			//Move value into buffer, fourth.
	cons += sizeof(uint32_t);

	memmove(kv->value, &buff[cons], kv->valuesz);			//Move value into buffer, fourth.
	cons += kv->valuesz;

	write(mr->serverSockets[id], &ping, 1);					//Write to server through socket connection.

	free(buff);
	free(ping);

	return 1;
}

void *mapHelper(void *arg)
{
	//printf("Entered mapHelper\n");	
	struct helperArgs *ar;
	ar = (struct helperArgs*)arg;	
	int INFILE = open(ar->path, O_RDONLY);

	//printf("\tinpath addr. = %p, id = %d, arg threads = %d\n", ar->inpath, ar->id, ar->helpThreads);
	
	//Set the pass/fail status of each mapper ID with result of map function.
	ar->mr->mapStatus[ar->id] = ar->mr->map(ar->mr, INFILE, ar->id, ar->helpThreads);	
	close(INFILE);
	//Signal to the consumer that mapper[id] has finished.
	//pthread_cond_signal(&ar->mr->notempty[ar->id]);
	return NULL;
}	

void *reduceHelper(void *arg)
{
	//printf("Entered reduceHelper\n");
	struct helperArgs *ar;
	ar = (struct helperArgs*)arg;
	int OUTFILE = open(ar->path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
	socklen_t clilen = sizeof(ar->mr->conn);
	//printf("\toutpath addr. = %p, OUTFILE = %d, threads = %d\n", ar->outpath, OUTFILE, ar->helpThreads);

	//Wait for all connections.
	for(int i = 0; i < ar->mr->nmaps; i++){
		//Accept connections
		ar->mr->serverSockets[i] = accept(ar->mr->listSocket, (struct sockaddr *)&ar->mr->conn, &clilen);
	}

	ar->mr->reduceStatus = ar->mr->reduce(ar->mr, OUTFILE, ar->helpThreads);
	//printf("Exiting reduceHelper\n");
	close(OUTFILE);
	//If we returned not 0, then we're out of kvpairs to map.
	return NULL;
}