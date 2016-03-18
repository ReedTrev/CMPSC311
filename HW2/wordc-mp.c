#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <errno.h>
#include <unistd.h>

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

//CITE LATER FROM GNU C LIBRARY
void write_to_pipe (int file, struct word_t *iter){
	printf("Entered Write to pipe, 168\n");
	FILE *stream;
	stream = fdopen(file, "w");
	while(iter != NULL){
		fprintf(stream, "%s %d\0", iter->word, iter->count);
		iter = iter->next;
	}
	fclose(stream);
};

//CITE LATER FROM GNU C LIBRARY
int read_from_pipe (int file){
	printf("Entered read from pipe, 180\n");  	
	FILE *stream;
 	char str[50];
	char *entry;
	char *test;
	int count;
	printf("186, %d\n", getpid());

 	stream = fdopen(file, "r");

	printf("188, %d\n", getpid());
	
 	while(fgets(str, 50, stream) != NULL){
		printf("191, %d\n", getpid());		
		entry = strtok(str, " ");			//Tokenize string into word and count, and get word (first token).
		count = atoi(strtok(NULL, " "));	//Turn char* count to int (second token).
		listMerge(entry, count);			//Add the word and its count to the parent's linked list.
		printf("195, %d\n", getpid());
 	}	
  	return fclose(stream);
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
	fseek(INFILE, 0L, SEEK_END);//Send INFILE pointer to end of file...
	int fsize = ftell(INFILE);//... and set fsize to size of INFILE.
	fseek(INFILE, 0L, SEEK_SET);//Reset pointer to start of INFILE.
	
	int n = atoi(argv[4]);		//Get # of processes
	int byteID = 0;			//ID number indicating what byte section of file process will traverse.
	int fds[n-1][2];		//Pipe file descriptor

	if(argv[1] == NULL || argv[2] == NULL || argv[3] == NULL || argv[4] == NULL){
		printf("Oh dear, we seem to be missing a file or input! Make sure to input three files and process parameter in the format, ./wordC input.txt wordCount.txt runtime.txt #ofProcesses. %s\n", strerror(errno));
	}

	char *str;

//START OF PARENT/CHILD FORKING AND PIPING CHANGES

	pid_t child_pid = 1;
	
	printf("Number of processes (incl. parent) = %d\n",n);
	int byteCnt = 0;
	int offset; //Offset for fseek used by children.
	

	
	int i;
	for(i = 1; i<=n && child_pid != 0; i++){	//While there are processes left to make and we are the parent...
		printf("247\n");

		if(n != 1){
			child_pid = fork();		//Create a child via fork
			byteID++;			//Indicates what "position" process has in the input file. Parent is always 0.

			//Create the pipe:
			if (pipe(fds[byteID-1])){
      			fprintf (stderr, "Pipe failed.\n");	//If the pipe returns -1, it has failed.
	      		exit(-1);				//Return error.
    		}
		}

		if(child_pid > 0){
			//This is the parent
			printf("I'm the parent process: pid = %d, %d children created\n", getpid(), byteID);
			int p;						
			for(p = 0; p < n; p++)
				close(fds[p][1]);

			//While the # of bytes traversed < # of bytes to be traversed OR only 1 process and EOF:
			while(byteCnt < (fsize/n) && !feof(INFILE)){
				//printf("Entered main loop\n");
				str = (char*)malloc(100); 		//Initialize space for str				
				fscanf(INFILE, "%s", str);		//Set str as next word
				byteCnt = byteCnt + strlen(str) + 1; 	//Compute bytes in word + space
				cleanWord(str);				//clean the word up
				listInsert(str);			//Insert the word into the linked list
				printf("Parent, 271\n");
			}

			printf("Parent, 271\n");
		
			/*wait_pid = waitpid(child_pid, NULL, WNOHANG);
			
			if(wait_pid == -1){
				perror("waitpid");
				exit(-1);
			}*/
			
			int j = -1;
			int k, l=0;
			while(l < n-1){
				j = (1 + j)%(n-1);
				k = read_from_pipe(fds[j][0]);
				close(fds[j][0]);
				printf("Parent, 287\n");
				if(k == 0)
					l++;				//Will increment as many times as we have children.
			}

		}
		else if(child_pid == 0){
			//This is a child
			printf("I'm a child process: pid = %d, byte ID = %d\n", getpid(), byteID);
			int p;
			for(p = 0; p < n; p++)		//Close the read end of the pipe
				close(fds[p][0]);
				

			offset = byteID*(fsize/n); //Set fseek offset.
			fseek(INFILE, offset, SEEK_SET); //Point file pointer to offset location to begin searching

			//While the # of bytes traversed < # of bytes to be traversed OR child process and EOF:	

			printf("Child, 308, fsize = %d\n", fsize);
			while(byteCnt < (fsize/n) && !feof(INFILE)){
				//printf("Entered main loop\n");
				str = (char*)malloc(100); //Initialize space for str	
				printf("Child, 311, ByteCnt = %d\n", byteCnt);
				fscanf(INFILE, "%s", str);//Set str as next word
				byteCnt = byteCnt + strlen(str) + 1;	//Compute bytes in word + space
				cleanWord(str);//clean the word up
				listInsert(str);//Insert the word into the linked list				
			}
			printf("Child, 309\n");
			//Begin Child piping
			int j = 0;
			//char *childStr;
			struct word_t *iter = head;
							
			j = byteID-1;
			write_to_pipe(fds[j][1], iter);
			close(fds[j][1]);
				
			
		}
	}
		

//END OF PARENT/CHILD FORKING AND PIPING CHANGES

	/*while(!feof(INFILE)){
		//printf("Entered main loop\n");
		str = (char*)malloc(100); //Initialize space for str
		fscanf(INFILE, "%s", str);//Set str as next word
		cleanWord(str);//clean the word up
		listInsert(str);//Insert the word into the linked list
	}*/
	printf("337, %d\n", getpid());
	writeOut(OUTFILE); //Write word count to OUTFILE.

	gettimeofday(&t1, NULL);
	writeTime(TIMEFILE,t0,t1); //Write 

	fclose(INFILE);fclose(OUTFILE);fclose(TIMEFILE); //Close all files.
	printf("344, %d\n", getpid());
	return 0;	
};
