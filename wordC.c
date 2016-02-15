#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>

struct word_t{
	char *word;
	int count;
	struct word_t *next;
};

struct word_t* head = NULL;

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
				str[i] = str[i+1];
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

	printf("The word to be inserted is: %s\n", str);
	if(iter==NULL){
		//new->next = *head;
		head = new;
		printf("Head is NULL\tHead word: %s\n", new->word);		
	}
	else{
		printf("Head is NOT NULL\tNew word: %s\n", new->word);
		//iter = *head;
		printf("\tHead word: %s\n", iter->word);
		while(iter != NULL){
			printf("\t\tEntered Insert while loop\n");
			//If we're at head, and new word should come before:
			if((iter==head) && (strcmp(str, iter->word)<0)){
				printf("\tNew word is new head\n");
				new->next = head;
				head = new;	
			}
			//If new word should come after:
			else if((strcmp(str, iter->word)>0)){
				printf("\tNew word is after current\n");
				new->next = iter->next;
				iter->next = new;
			}
			//If it's the same word:
			else if((strcmp(str, iter->word)==0)){
				printf("\tSame word: str1(%s)==str2(%s)\n", str, iter->word);
				iter->count = iter->count + 1;
				printf("%s count = %d", str, iter->count);
				
			}
			//Word at the end:
			else{
				printf("\tNew word at end of list\n");
				iter->next = new;
			}
			iter = iter->next;
		}
	}
	
};

//NOT FIXED YET
void writeOut(struct word_t **head, FILE *OUT){
	struct	word_t *iter = (struct word_t *)malloc(sizeof(struct word_t*));
	fprintf(OUT, "Test123\n");
	iter = *head;
	while(iter->next != NULL){
		printf("\n%s\n",iter->word);
		fprintf(OUT,"%s, %d\n", iter->word, iter->count);
		iter++;
	}
	printf("Finished writeOut\n");
};

int main(int argc, char** argv){
	
	FILE* INFILE = argv[1];
	INFILE = fopen(INFILE,"r");

	FILE* OUTFILE = argv[2];
	OUTFILE = fopen(OUTFILE,"w");

	FILE* TIMEFILE = argv[3];
	TIMEFILE = fopen(TIMEFILE,"w");

	int count = 1;
	int cnt = 0;	
	char *str;
	char word;
	struct word_t *curr;
	struct word_t *head;
	struct word_t  list= {
		.word = NULL,
		.count = 0,
		.next = NULL
	};
	
	curr = &list;
	head = NULL;
	while(!feof(INFILE)){
		curr = (struct word_t *)malloc(sizeof(struct word_t*));
		printf("Entered while loop\n");
		//count = strlen(str);//get length of word
		str = (char*)malloc(100); //Initialize space for str
		count = fscanf(INFILE, "%s", str);//Set str as next word
		cleanWord(str);//clean the word up
		printf("%s  %d\n",str);//Print the cleaned word.

		listInsert(str);//Insert the word into the linked list

		/*//Save head pointer
		if(cnt == 0){
			cnt = 1;
			head = curr;
			printf("\nHead set to curr\n");
		};
		//printf("Word at curr: %s\n", curr->word)
		curr = head;*/
	}
	writeOut(&head, OUTFILE); 
	return 0;	
};
