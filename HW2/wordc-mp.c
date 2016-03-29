#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <errno.h>
#include <unistd.h>
#include <stdlib.h>

//Programmers:
//Trevor Reed
//Alison Pastore

struct word_t{
	char *word;
	int count;
	struct word_t *next;
};

//Declare global head pointer
struct word_t* head = NULL;

//Takes char array, keeps alphanumerics, forces lowercase.
void cleanWord(char str[]){
	int len = strlen(str);
	int i = 0;
		
	while(len!=-1){
		//If the character is alphanumeric...
		if(isalnum(str[len-1])){
			//If the found character is uppercase...
			if(str[len-1]>=65 && str[len-1]<=90)
				str[len-1]=str[len-1]+32; //... make it lowercase.
			len--;// and advance.
		}
		//If the character is special...
		else{
			i = len-1;
			//Makeshift pop-function for char array.
			while(str[i]!=NULL){
				str[i] = str[i+1];//Shift char to the right over to the left
				i++;
			}
			len--;
		}
	}	
};

//Insert word into linked list
//Use strcmp(char *str1, char*str2)
//returns <0 if str1 < str2
//   ''   >0 if str1 > str2
//   ''   =0 if str1 = str2
void listInsert(char str[]){
	struct word_t *iter = head;
	struct word_t *new = (struct word_t *)malloc(sizeof(struct word_t*));

	new->word = str;
	new->count = 1;
	new->next = NULL;

	//If head is empty, we need a new head:
	if(iter==NULL){
		//new->next = *head;
		head = new;
	}
	//If we already have a head...
	else{
		iter = head;//Reset iter back to head

		//While we're in the list and there isn't a repeat word...
		while(iter != NULL){
			//If it's the same word:
			if((strcmp(str, iter->word) == 0)){
				iter->count = iter->count + 1; //Increment count
				break;
			}			
			//If we're at head, and new word should come before:
			else if((iter==head) && (strcmp(str, iter->word)<0)){
				new->next = head;
				head = new;	
				break;
			}
			//Word at the end:
			else if(iter->next == NULL){
				iter->next = new;
				break;
			}
			//If new word should go between current and next word:
			else if((strcmp(str, iter->word)>0) && (strcmp(str, iter->next->word)<0)){
				new->next = iter->next;
				iter->next = new;
				break;
			}
			else
				iter = iter->next;
		}
	}
};

//Derivative version of listInsert. Takes a predefined count in addition
//	to the word to insert into the list.
void listMerge(char str[], int cnt){
	struct word_t *iter = head;
	struct word_t *new = (struct word_t *)malloc(sizeof(struct word_t*));

	new->word = str;
	new->count = cnt;
	new->next = NULL;

	//If head is empty, we need a new head:
	if(iter==NULL){
		//new->next = *head;
		head = new;
		head->next = NULL;
	}
	//If we already have a head...
	else{
		iter = head;//Reset iter back to head

		//While we're in the list and there isn't a repeat word...
		while(iter != NULL){
			//If it's the same word:
			if((strcmp(str, iter->word) == 0)){
				iter->count = iter->count + cnt; //Increment count
				break;
			}			
			//If we're at head, and new word should come before:
			else if((iter==head) && (strcmp(str, iter->word)<0)){
				new->next = head;
				head = new;	
				break;
			}
			//Word at the end:
			else if(iter->next == NULL){
				iter->next = new;
				break;
			}
			//If new word should go between current and next word:
			else if((strcmp(str, iter->word)>0) && (strcmp(str, iter->next->word)<0)){
				new->next = iter->next;
				iter->next = new;
				break;
			}
			else
				iter = iter->next;
		}
	}
};

//Writes contents of list to specified file (arg[2])
void writeOut(FILE *OUT){
	struct	word_t *iter = head;

	while(iter != NULL){
		if(iter->word != NULL){
			fprintf(OUT,"%s, %d\n", iter->word, iter->count);
		}
		iter = iter->next;
	}
};

//Write to the specified time file, argv[3].
void writeTime(FILE *OUTTIME, struct timeval t0, struct timeval t1){
	time_t begtime = t0.tv_sec;
	long elapsed = (t1.tv_sec - t0.tv_sec)*1000000+t1.tv_usec-t0.tv_usec;	
	fprintf(OUTTIME,"Start time: %d sec\n\tRun time: %d usec\n",begtime,elapsed);
};

//Child function. Takes file descriptor as input to write to.
//Opens a stream of file descriptor for writing,
//	and iterates through child's linked list until the end,
//	printing each word and its count to the stream.
void write_to_pipe (int file){
	FILE *stream;		
	stream = fdopen(file, "w");
	struct word_t *iter = head;
	while(iter != NULL){
		fprintf(stream, "%s %d\n", iter->word, iter->count);
		iter = iter->next;
	}
	fclose(stream);
};

//Parent function. Takes a file descriptor as input to read from.
//Opens a stream of the file descriptor for reading,
//	and while we are not at the end of the file,
//	parse the stream for a word and count and
//	merge said data into parent's linked list. 
int read_from_pipe(int file){
	FILE *stream;
	char *str;
	char test;
	int count;

	stream = fdopen(file, "r");

	while(!feof(stream)){
		str = malloc(50*sizeof(char));
		test = fscanf(stream, "%s", str);
		test = fscanf(stream, "%d", &count);
		listMerge(str,count);
	}
	fclose(stream);						//Done reading, close stream.
};

int main(int argc, char** argv){
		
	struct timeval t0, t1; //Create structs for begin and end times.
	time_t begtime, endtime; //Variables to hold the two times.
	gettimeofday(&t0, NULL); //Get the time at the start of the program.

	//Get input file as first run argument.
	FILE* INFILE;// = argv[1];
	INFILE = fopen(argv[1],"r");//"r" open file for reading only.

	//Get count output file as second run argument.
	FILE* OUTFILE;// = argv[2];
	OUTFILE = fopen(argv[2],"w");//"w" create file if it doesn't exist, write over existing.

	//Get runtime output file as third run argument.
	FILE* TIMEFILE;// = argv[3];
	TIMEFILE = fopen(argv[3],"a");//"a" open if it exists and append to end.

	//Find out size of input file in bytes.
	fseek(INFILE, 0L, SEEK_END);	//Send INFILE pointer to end of file...
	int fsize = ftell(INFILE);		//... and set fsize to size of INFILE.
	fseek(INFILE, 0L, SEEK_SET);	//Reset pointer to start of INFILE.
	//fclose(INFILE);
	
	long int n = atoi(argv[4]);		//Get # of processes
	int byteID = 0;				//ID number indicating what byte section of file process will traverse.
	int fds[n-1][2];			//Pipe file descriptor

	if(argv[1] == NULL || argv[2] == NULL || argv[3] == NULL || argv[4] == NULL){
		printf("Oh dear, we seem to be missing a file or input! Make sure to input three files and process parameter in the format, ./wordC input.txt wordCount.txt runtime.txt #ofProcesses. %s\n", strerror(errno));
	}

	char *str;

//START OF PARENT/CHILD FORKING AND PIPING

	pid_t child_pid = 1;
	
	int byteCnt = 0;
	int offset; //Offset for fseek used by children.
	
	int i;
	for(i = 1; (i<n && child_pid != 0); i++){	//While there are processes left to make and we are the parent...

		if(n != 1){
			child_pid = fork();		//Create a child via fork
			byteID++;			//Indicates what "position" process has in the input file. Parent is always 0.

			//Create the pipe between parent and child:
		if (pipe(fds[byteID-1])){
      		printf ("Pipe failed.\n");	//If the pipe returns -1, it has failed.
	    	return 1;				//Return error.
    		}
		}
	}
	if(child_pid == 0){
		//This is a child
		//printf("I'm a child process: pid = %d, byte ID = %d\n", getpid(), byteID);
		
		INFILE = fopen(argv[1],"r");
		offset = byteID*(fsize/n); //Set fseek offset.
		fseek(INFILE, offset, SEEK_SET); //Point file pointer to offset location to begin searching

		//While the # of bytes traversed < # of bytes to be traversed OR child process and EOF:	

		printf("Child, 308, fsize = %d\n", fsize);
		while(byteCnt < (fsize/n) && !feof(INFILE)){
			str = (char*)malloc(100); //Initialize space for str	
			//printf("Child, 311, ByteCnt = %d\n", byteCnt);
			fscanf(INFILE, "%s", str);//Set str as next word
			byteCnt = byteCnt + strlen(str) + 1;	//Compute bytes in word + space
			cleanWord(str);//clean the word up
			if(str[0] != '\0')
				listInsert(str);//Insert the word into the linked list			
		}
	
		//Begin Child piping
	
		close(fds[byteID-1][0]);				//Close read end of child's pipe
		write_to_pipe(fds[byteID-1][1]);		//Write to pipe.
		close(fds[byteID-1][1]);				//Close write end of child's pipe.
		fclose(INFILE);							//Close input file.
	}
	else{
		//This is the parent
		//printf("I'm the parent process: pid = %d, %d children created\n", getpid(), byteID);

		INFILE = fopen(argv[1],"r");			
		//While the # of bytes traversed < # of bytes to be traversed OR only 1 process and EOF:
		while(byteCnt < (fsize/n) && !feof(INFILE)){
			str = (char*)malloc(100); 		//Initialize space for str			
			fscanf(INFILE, "%s", str);		//Set str as next word
			byteCnt = byteCnt + strlen(str) + 1; 	//Compute bytes in word + space
			cleanWord(str);				//clean the word up
			if(str[0] != '\0')
				listInsert(str);//Insert the word into the linked list	
		}
		

		
		int k, l=0;
		//while(l < n-1){
		for(l = 0; l < n-1; l++){					//For all the children to read from...
			close(fds[l][1]);						//Close write-end of the pipe of child "l"
			k = read_from_pipe(fds[l][0]);			//Read from child "l"
			close(fds[l][0]);						//Close read-end of the pipe of child "l"
		}
		writeOut(OUTFILE); //Write word count to OUTFILE.
		fclose(INFILE);fclose(OUTFILE);fclose(TIMEFILE); //Parent is done, close all files.								
	}
	
	gettimeofday(&t1, NULL);
	if(child_pid != 0)				//If we are the parent...
		writeTime(TIMEFILE,t0,t1);	//Write run-time to TIMEFILE. 
	
	return 0;	
};
