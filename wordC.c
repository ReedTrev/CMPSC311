#include <stdio.h>

int main(){

        FILE *pb;
        pb = fopen("PangurBan.txt", "r+");
        fprintf(pb, "This is testing for fprintf...\n");
        fclose(pb);

        return 0;
}

/*struct word_t {
        char *word;
        int count;
        struct word_t *next;
};*/
