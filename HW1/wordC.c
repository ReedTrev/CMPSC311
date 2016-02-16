#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>
#include <sys/time.h>

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
	//struct word_t *iter = (struct word_t *)malloc(sizeof(struct word_t*));
	struct word_t *iter = head;
	struct word_t *new = (struct word_t *)malloc(sizeof(struct word_t*));

	new->word = str;
	new->count = 1;
	new->next = NULL;

	int same = 0;

	printf("The word to be inserted is: %s\n", str);
	if(iter==NULL){
		//new->next = *head;
		head = new;
		printf("Head is NULL\tHead word: %s\n", new->word);		
	}
	else{
		printf("Head is NOT NULL\tNew word: %s\n", new->word);
		//iter = *head;
		printf("\tCurrent word: %s\n", iter->word);

		//Check to see if word already exists
		while(iter != NULL){
			//If it's the same word:
			if((strcmp(str, iter->word) == 0)){
				printf("\tSame word: str1(%s)==str2(%s)\n", str, iter->word);
				iter->count = iter->count + 1; //Increment count
				printf("%s count = %d\n", str, iter->count);
				same = 1; //Set flag
				break;
			}
			iter = iter->next; //Move to next word
		}

		iter = head;
		//While we're in the list and there isn't a repeat word...
		while(iter != NULL && same == 0){
			printf("\t\tEntered Insert while loop\n");
						
			//If we're at head, and new word should come before:
			if((iter==head) && (strcmp(str, iter->word)<0)){
				printf("\tNew word is new head\n");
				new->next = head;
				head = new;	
				break;
			}
			//Word at the end:
			else if(iter->next == NULL){
				printf("\tNew word at end of list\n");
				iter->next = new;
				break;
			}
			//If new word should go between current and next word:
			else if((strcmp(str, iter->word)>0) && (strcmp(str, iter->next->word)<0)){
				printf("\tNew word is after current\n");
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
		if(iter->word != ""){
			printf("\n%s\n",iter->word);
			fprintf(OUT,"%s, %d\n", iter->word, iter->count);
		}
		iter = iter->next;
	}
	printf("Finished writeOut\n");
};

int main(int argc, char** argv){
	
	struct timeval t0, t1;
	time_t begtime, endtime;
	gettimeofday(&t0, NULL);
	begtime = t0.tv_sec;

	FILE* INFILE = argv[1];
	INFILE = fopen(INFILE,"r");

	FILE* OUTFILE = argv[2];
	OUTFILE = fopen(OUTFILE,"w");

	FILE* TIMEFILE = argv[3];
	TIMEFILE = fopen(TIMEFILE,"w");

	int count = 1;
	int cnt = 0;	
	char *str;

	while(!feof(INFILE)){
		printf("Entered main loop\n");
		str = (char*)malloc(100); //Initialize space for str
		count = fscanf(INFILE, "%s", str);//Set str as next word
		cleanWord(str);//clean the word up
		listInsert(str);//Insert the word into the linked list
	}
	
	writeOut(OUTFILE);
	printf("Start time: %d\n", begtime);
	gettimeofday(&t1, NULL);
	long elapsed = (t1.tv_sec - t0.tv_sec)*1000000+t1.tv_usec-t0.tv_usec;
	printf("Total time: %d usec.\n", elapsed);
	return 0;	
};
