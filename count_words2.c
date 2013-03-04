#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include "mpi.h"
#define NHASH 291639 //Use a prime number!
#define MULT 31
#define BIGSTRINGSIZE 800000
struct node
{
    char *word;
    int count;
    struct node * next;
} node;

typedef struct node Node;

Node *bin[NHASH];

struct word_struct { 
    char word[35];
    int word_count;
};


unsigned int hash(char *p){
    unsigned int h = 0;
    for(; *p; p++)
        h = MULT * h + *p;
    return h % NHASH;
}

void incword(char *s)
{
    Node * p;
    int h = hash(s);
    for(p = bin[h]; p!= NULL; p = p->next){
        if(strcmp(s, p->word) == 0){
            (p->count)++;
            return;
        }
    }
    p = (Node *)malloc(sizeof(node));
    if(!p)
        return;
    p->count = 1;
    p->word = (char *)malloc(strlen(s)+1);
    strcpy(p->word, s);
    p->next = bin[h];
    bin[h] = p;
}

void addword(int num, char *s){
    int h = hash(s);
    Node * p;
    for(p = bin[h]; p!= NULL; p = p->next){
        if(strcmp(s, p->word) == 0){
            (p->count)+=num;
            return;
        }
    }
    p = (Node *)malloc(sizeof(node));
    if(!p)
        return;
    p->count = num;
    p->word = (char *)malloc(strlen(s)+1);
    strcpy(p->word, s);
    p->next = bin[h];
    bin[h] = p;
}

int CountWords(FILE *f){
    char c;
    int num=0;
    int flag= 0;
    while((c=fgetc(f))!=EOF){
        if((c==' ')||(c=='\n')||(c==',')||(c==';')||(c==':')){
            flag=0 ;        
        }                         
        else if(flag==0){
            num++;
            flag=1;     
        }
    }
    printf("\t\n\n\nNumber of words is %d\n",num);
    return num;
}
void cut_string(char *final_string, char *initial_string, int my_rank, int processors, int words_per_processor[]){
    char *p1;
    char *p2;
    char * pch;
    int p_index = my_rank;
    int words_on_p = words_per_processor[p_index];
    int words_until_p = 0;
    int chars_until_p = 0;
    int chars_on_p = 0;
    int i;
    char c;
    p1 = initial_string;
    
    for (i = 0; i < p_index; i++){
        words_until_p += words_per_processor[i];
    }

    for (i=0; i <= words_until_p; ){//count the number of characters until the
                                        //string we want to cut
        c = *p1++;
        chars_until_p++;
        if ((c == ' ')||(c == EOF)){
            i++;
        }
    }
    chars_until_p--;//subtract the last space character

    for (i=0; i<= words_on_p; ){
        c = *p1++;
        chars_on_p++;
        if ((c == ' ')||(c == EOF)||(c == '\0')){
            i++;
        }
    }
    chars_on_p--;//subtract last space
    p1 = initial_string + chars_until_p;
    p2 = final_string;
    strncpy(p2, p1, chars_on_p);
    p2 += chars_on_p;
    *p2 = '\0';

    //printf("chars on p %d words on p %d chars until p %d words until p %d\n", chars_on_p, words_on_p, chars_until_p, words_until_p);
/*    printf("length of final string %u\n", (unsigned)strlen(final_string));*/
/*    printf("%s\n", final_string);*/
    return;

}
void count_letters(char *string, int counts []){
    int i;
    for (i = 0; i < strlen(string); i++){
        if ((string[i] >= 97) && (string[i] <= 122)){//lower letter
                counts[(int)string[i] - 97]+=1;
        }
    }
}

void convert_to_lower(char *string){
    int i;
    for (i = 0; i < strlen(string); i++){
        if ((string[i] >= 65) && (string[i] <= 90)){//upper letter
            string[i] = string[i] + 32;
        }
    }
    
}
void print_int_array(int array[][2], size_t len){ 
    size_t i;
 
    for(i=0; i<len; i++) 
        printf("%7d | %c\n", array[i][0], (char)array[i][1]);
} 
int int_cmp(const void *a, const void *b) 
{ 
    const int *ia = (const int *)a; // casting pointer types 
    const int *ib = (const int *)b;
    return ib[0] - ia[0];
} 
int struct_cmp_by_count(const void *a, const void *b) 
{ 
    struct word_struct *ia = (struct word_struct *)a;
    struct word_struct *ib = (struct word_struct *)b;
    return ib->word_count - ia->word_count;
	/* float comparison: returns negative if b > a 
	and positive if a > b. We multiplied result by 100.0
	to preserve decimal fraction */ 
} 
void print_struct_array(struct word_struct *array, size_t len) 
{ 
    size_t i;
    for(i=0; i<len; i++) 
        printf(" count: %6d | word: %s \n", array[i].word_count, array[i].word);
} 
int main(int argc, char* argv[])
{
    char buf[100];//word size
    char big_string[BIGSTRINGSIZE];
    int i,j=0, k;
    int processors;
    int total_uniq_on_p = 0;
    char * pch;//pointer to word tokens
    int my_rank;
    int count_now = 0, word_size = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &processors);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Status status;
    
    double time = MPI_Wtime();
    
    char processor_string[(int)(BIGSTRINGSIZE/processors)];
    int words_per_processor[processors];
    int unique_words_per_processor[processors];
    int counts[26];
    int counts3[26][2];
    for (i = 0; i < 26; i += 1){
        counts[i] = 0;
    }

    Node * p;
    for (i=0; i<NHASH; i++)
        bin[i] = NULL;
    if (my_rank == 0){
        char file_name[] = "words_only.txt";
        FILE *in_file;
        in_file = fopen(file_name, "r");
        fgets(big_string, BIGSTRINGSIZE-1, in_file);
        printf("got string\n");
        rewind(in_file);
        int word_num = CountWords(in_file);
        fclose(in_file);

        for (i=0; i<processors; i++){
            words_per_processor[i] = (int)(word_num / processors);
        }
        printf("there are %d words\n", word_num);
        
        words_per_processor[processors-1] = words_per_processor[processors-1] + (int)(word_num % processors);
/*        printf("word number is %d\n", word_num);*/
/*        printf("length of the string is %u\n", (unsigned)strlen(big_string));*/
    }

    
    MPI_Bcast(words_per_processor, processors, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(big_string, BIGSTRINGSIZE, MPI_CHAR, 0, MPI_COMM_WORLD);
    cut_string(processor_string, big_string, my_rank, processors, words_per_processor);
    printf("processor %d begins\n", my_rank);
    convert_to_lower(processor_string);
    count_letters(processor_string, counts);
    printf("processor %d done\n", my_rank);
    pch = strtok(processor_string, " ");
    while(pch != NULL){
        strcpy(buf, pch);
        incword(buf);
        pch = strtok (NULL, " ");
    }

    
    if (my_rank != 0){
        for (i = 0; i < NHASH; i++){
            for (p = bin[i]; p != NULL; p = p->next){
                total_uniq_on_p +=1;
            }
        }
        MPI_Send(&total_uniq_on_p, 1, MPI_INT, 0, my_rank, MPI_COMM_WORLD);
    }
    if (my_rank == 0){
        for(i = 1; i < processors; i++){
            MPI_Recv(&unique_words_per_processor[i], 1, MPI_INT, i, i, MPI_COMM_WORLD, &status);
        }
    }
    if (my_rank != 0){
        for (i = 0; i < NHASH; i++){
            for (p = bin[i]; p!=NULL; p= p->next){
                MPI_Send(&(*p).count, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
                j = (int)(strlen(p->word));
                MPI_Send(&j, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
                MPI_Send(p->word, (int)(strlen(p->word)), MPI_CHAR, 0, 2, MPI_COMM_WORLD);
            }
        }
        MPI_Send(counts, 26, MPI_INT, 0, 3, MPI_COMM_WORLD);
    }
    if (my_rank == 0){
        int counts2[26];
        for (i = 1; i < processors; i++){
            printf("receiving from %d\n", i);
            for (k = 0; k < unique_words_per_processor[i]; k++){
                MPI_Recv(&count_now, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &status);
                MPI_Recv(&word_size, 1, MPI_INT, i, 1, MPI_COMM_WORLD, &status);
                MPI_Recv(buf, word_size, MPI_CHAR, i, 2, MPI_COMM_WORLD, &status);
                buf[word_size] = '\0';
                addword(count_now, buf);
/*                printf("%s\n", buf);*/
            }
            MPI_Recv(counts2, 26, MPI_INT, i, 3, MPI_COMM_WORLD, &status);
            for (j = 0; j < 26; j += 1){
                counts[j] += counts2[j];
            }
        }
        for (i = 0; i < 26; i++){
            counts3[i][0] = counts[i];
            counts3[i][1] = i+97;
         }
         qsort(counts3,26, 2*sizeof(int), int_cmp);
         print_int_array(counts3, 26);
    }
    
    
    if (my_rank == 0){
        total_uniq_on_p = 0;
        for (i = 0; i < NHASH; i++)
            for (p = bin[i]; p != NULL; p = p->next)
                total_uniq_on_p++;
        struct word_struct structs[total_uniq_on_p];
        j = 0;
        for (i = 0; i < NHASH; i++){
            for (p = bin[i]; p != NULL; p = p->next){
                strcpy(structs[j].word, p->word);
                structs[j].word_count = p->count;
                j++;
            }
        }
        size_t structs_len = sizeof(structs) / sizeof(struct word_struct);
        qsort(structs, structs_len, sizeof(struct word_struct), struct_cmp_by_count);
        print_struct_array(structs, 10);
    }
    
    double time2 = MPI_Wtime() - time;

    if (my_rank != 0)
        MPI_Send(&time2, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
    else{
        double time3;
        printf("Processor 0 time %f\n", time2);
        for (i = 1; i < processors; i ++){
            MPI_Recv(&time3, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, &status);
            printf("Processor %d time %f\n", i, time3);
        }
    }
    MPI_Finalize();
    return 0;
}
