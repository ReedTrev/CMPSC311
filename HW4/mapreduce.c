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
#include <netdb.h>
#include <pthread.h>
#include "mapreduce.h"


/* Size of shared memory buffers */
#define MR_BUFFER_SIZE 1024

void *mapHelper(void *);
void *reduceHelper(void *);

struct map_reduce *new;

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
		new->args = malloc(nmaps*sizeof(struct helperArgs));
		new->args2 = malloc(sizeof(struct helperArgs));
		new->nmaps = nmaps;

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
		free(new);
		return NULL;
	}
}

/* Destroys and cleans up an existing instance of the MapReduce framework */
void
mr_destroy(struct map_reduce *mr)
{
	//General destroy
	free(mr->args);
	free(mr->args2);

	//Client destroy
	if(mr->reduce == NULL){
		for(int i = 0; i < mr->nmaps; i++)		//Close all client sockets before freeing.
			close(mr->clientSockets[i]);
		free(mr->mapStatus);
		free(mr->mappers);
		free(mr->clientSockets);
	}
	//Server destroy
	else{
		for(int i = 0; i < mr->nmaps; i++)		//Close all server sockets before freeing.
			close(mr->serverSockets[i]);
		close(mr->listSocket);
		free(mr->serverSockets);
	}

	free(mr);
}

/* Begins a multithreaded MapReduce operation */
int
mr_start(struct map_reduce *mr, const char *path, const char *ip, uint16_t port)
{
	//Client
	if(mr->reduce == NULL){

		struct hostent *server;
		server = gethostbyname(ip); 

		struct sockaddr_in client_addr;
		bzero((char*) &client_addr, sizeof(client_addr));
		client_addr.sin_family = AF_INET;

		bcopy((char*) server->h_addr, (char*)&client_addr.sin_addr.s_addr, server->h_length);
		
		client_addr.sin_port = htons(port);
		
		for(int i = 0; i < mr->nmaps; i++){
			
			mr->args[i].id = i;
			mr->mapStatus[i] = -2;
			mr->args[i].helpThreads = mr->nmaps;
			mr->args[i].mr = mr;
			mr->args[i].path = path;

			mr->clientSockets[i] = socket(AF_INET, SOCK_STREAM, 0);	//Create client socket
			if(connect(mr->clientSockets[i], (struct sockaddr*) &client_addr, sizeof(client_addr)) < 0){ //Wait for Connection of socket to IP:port
				perror("ERROR: Socket did not connect.");
				return -1;
			}

			//Connection established, create thread.
			if(pthread_create(&mr->mappers[i], NULL, &mapHelper,(void *)&mr->args[i])){
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

		struct hostent *server;
		server = gethostbyname(ip); 

		mr->listSocket = socket(AF_INET, SOCK_STREAM, 0);	//Create server listen socket.
		
		struct sockaddr_in serv_addr;
		bzero((char*) &serv_addr, sizeof(serv_addr));
		serv_addr.sin_family = AF_INET;

		bcopy((char*) server->h_addr, (char*)&serv_addr.sin_addr.s_addr, server->h_length);
		
		serv_addr.sin_port = htons(port);
		
		if(bind(mr->listSocket, (struct sockaddr*) &serv_addr, sizeof(serv_addr)) < 0){ //Bind listen socket to IP:port
			perror("ERROR: Binding failed.");
			return -1;
		}

		if(listen(mr->listSocket, mr->nmaps) < 0){			//Listen for connections with listen socket.
			perror("ERROR: Listening failed."); 
			return -1;
		}

		if(pthread_create(&mr->reducer, NULL, &reduceHelper,(void *)mr->args2) < 0){	//Create server thread.
			perror("ERROR: Reducer pthread not created.");
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
		for(int i = 0; i < mr->nmaps; i++){		//Wait for all mapper threads to finish.
			if(pthread_join(mr->mappers[i], NULL))
				return -1;
			close(mr->clientSockets[i]);
		}

		for(int i = 0; i < mr->nmaps; i++)		//Check for errors in mapper threads.
			if(new->mapStatus[i] != 0)				
				return -1;
			

		return 0;
	}

	//Server
	else{
		if(pthread_join(mr->reducer, NULL))		//Join reducer thread and wait for it to terminate.
			return -1;

		if(mr->reduceStatus != 0)		//Check for error in reduce thread.
			return -1;

		for(int i = 0; i < mr->nmaps; i++)	//Close all server sockets.
			close(mr->serverSockets[i]);
	 	
	 	close(mr->listSocket);				//Close listen socket.
		return 0;
	}
}

/* Called by the Map function each time it produces a key-value pair */
int
mr_produce(struct map_reduce *mr, int id, const struct kvpair *kv)
{	
	//Write key size to socket[id].
	if(write(mr->clientSockets[id], &kv->keysz, sizeof(uint32_t)) < 0){
		perror("ERROR: Key size not written.");
		return -1;
	}

	//Write key to socket[id].
	if(write(mr->clientSockets[id], kv->key, kv->keysz) < 0){
		perror("ERROR: Key not written.");
		return -1;
	}

	//Write value size to socket[id].
	if(write(mr->clientSockets[id], &kv->valuesz, sizeof(uint32_t)) < 0){
		perror("ERROR: Value size not written.");
		return -1;
	}

	//Write value to socket[id].
	if(write(mr->clientSockets[id], kv->value, kv->valuesz) < 0){
		perror("ERROR: Value not written.");
		return -1;
	}

	return 1;
}

/* Called by the Reduce function to consume a key-value pair */
int
mr_consume(struct map_reduce *mr, int id, struct kvpair *kv)
{
	int bytes = 0;

	//Read key size from socket[id].
	bytes = read(mr->serverSockets[id], &kv->keysz, sizeof(uint32_t));

	//If no bytes were read, then there are no more kvpairs.
	if(bytes == 0)
		return 0;

	//If read returns -1, there is an error.
	if(bytes < 0){
		perror("ERROR: Key size not read.");
		return -1;
	}

	//Read key from socket[id].
	if(read(mr->serverSockets[id], kv->key, kv->keysz) < 0){
		perror("ERROR: Key not read.");
		return -1;
	}

	//Read value size from socket[id].
	if(read(mr->serverSockets[id], &kv->valuesz, sizeof(uint32_t)) < 0){
		perror("ERROR: Value size not read.");
		return -1;
	}

	//Read value from socket[id].
	if(read(mr->serverSockets[id], kv->value, kv->valuesz) < 0){
		perror("ERROR: Value not read.");
		return -1;
	}

	return 1;
}

void *mapHelper(void *arg)
{
	struct helperArgs *ar;
	ar = (struct helperArgs*)arg;
	int INFILE = open(ar->path, O_RDONLY);	//Open infile with read privileges
	
	//Set the pass/fail status of each mapper ID with result of map function.
	ar->mr->mapStatus[ar->id] = ar->mr->map(ar->mr, INFILE, ar->id, ar->helpThreads);	
	close(INFILE);

	return NULL;
}	

void *reduceHelper(void *arg)
{
	struct helperArgs *ar;
	ar = (struct helperArgs*)arg;
	int OUTFILE = open(ar->path, O_WRONLY | O_CREAT | O_TRUNC, 0666);	//Open outfile with write privileges.

	//Wait for and accept all connections.
	for(int i = 0; i < ar->mr->nmaps; i++){
		//Accept connections
		ar->mr->serverSockets[i] = accept(ar->mr->listSocket, (struct sockaddr *)NULL, NULL);
		if(ar->mr->serverSockets[i] < 0)
			perror("ERROR: Server socket not accepted.");
	}

	ar->mr->reduceStatus = ar->mr->reduce(ar->mr, OUTFILE, ar->helpThreads);
	close(OUTFILE);
	
	return NULL;
}
