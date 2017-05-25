/* Enver BASHIROV 21203023
CS342 Project3 Part A

References to:
https://github.com/korpeoglu/cs342-spring2016-p3
for project skeleton

http://stackoverflow.com/questions/16764276/measuring-time-in-millisecond-precision
for the purpose of time measurement*/

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <pthread.h>
#include <ctype.h>

#define MAXTHREADS 8
#define MAXBUFSIZE 1000
#define MAXLINESIZE 4096
#define MAXWORDSIZE 64
#define TERMINATE -1

//BB node implementation
typedef struct bbNode {
	int id;
	char str[MAXWORDSIZE];
	struct bbNode *next;
} item;

//BB list implementation
typedef struct bbList {
	struct bbNode *head;
	struct bbNode *tail;
	int count;//number of items in the BBLL
	pthread_mutex_t mutex; //for protecting list
	pthread_cond_t full; //for main
	pthread_cond_t empty; //for worker
} list;

// **FUNCTIONS - Bounded Buffer (BB)
void bbInit ( struct bbList *l) {
	l->head = NULL;
	l->tail = NULL;
	l->count = 0;
}
void bbInsert ( struct bbList *l, struct bbNode *n) {
	if ( l->count == 0) {
		l->head = n;
		l->tail = n;
	} else {
		l->tail->next = n;
		l->tail = l->tail->next;
	}
	l->count++;
}
struct bbNode* bbRetrive( struct bbList *l) {
	struct bbNode *n;

	if ( l->count == 0)
		return NULL;

	n = l->head;
	l->head = l->head->next;
	l->count--;

	return n;
}

//GLOBAL VARIABLES! ---- 
int N = MAXTHREADS, M = MAXBUFSIZE;
item *root; //*last;
pthread_mutex_t llmutex;
//----------------------

//FUNCTION - Insertion sort for LL
void llInsert( item *ins, item *node){
	item *temp = ins->next;
	ins->next = node;
	node->next = temp;
}
int llDif( item *node1, item *node2){
	int dif = strcmp( node1->str, node2->str);
	if ( dif > 0 || (dif == 0 && node1->id > node2->id)) return 1; //larger
	if ( dif > 0 || (dif == 0 && node1->id == node2->id)) return -1; //same
	return 0; //smaller
}
void llSort( item *root, item *node) {
	if ( root->next == NULL) {
		root->next = node; return;
	}
	if ( llDif( root->next, node) == 1) {
		llInsert( root, node); return;
	}	
	if ( llDif( root->next, node) == -1) {
		return;
	}
	llSort( root->next, node);
}
void llInSort( item *node) {
	if ( root == NULL) {
		root = node; 
		return;
	}
	if ( llDif( root, node) == 1){
		node->next = root; root = node;
		return;
	}
	item *temp = root;
	llSort( temp, node);
}

void initFunc( int n, int m) {
	//The re adjustment of argument <n> (which should be 1<n<8)
	N = (( n > MAXTHREADS) ? MAXTHREADS : n);
	//The re adjustment of argument <m> (which should be 1<n<1000)
	M = (( m > MAXBUFSIZE) ? MAXBUFSIZE : m);

	root = NULL;
}

//Worker function for threads
void* Worker ( void *BBLL) {
	struct bbList *buf = ( struct bbList *) BBLL;
	item *temp;

	while (1) {
		//CRITICAL SECTION ===================
		pthread_mutex_lock( &buf->mutex);
		
		while ( buf->count == 0)
			pthread_cond_wait( &buf->empty, &buf->mutex);

		temp = bbRetrive( buf);

		if ( buf->count == M)
			pthread_cond_signal( &buf->full);

		pthread_mutex_unlock( &buf->mutex);
		//====================================

		if ( temp == NULL) {
			printf( "Worker | Item was null!\n"); exit(1);
		}
		if ( temp->id == TERMINATE) 
			break;
		temp->next = NULL;

		//CRITICAL SECTION LLSORTING ===========
		pthread_mutex_lock( &llmutex);

		llInSort( temp);

		pthread_mutex_unlock( &llmutex);
		//======================================
		//printf ( "Worker | Taken item: %s, lineno: %d\n", temp->str, temp->id);
	}
	//printf( "Worker | Terminating.. \n");
	pthread_exit( NULL);
}

int main ( int argc, char *argv[]) {
	// Time Measurement Variables
	// static struct timeval tm1;
	// gettimeofday(&tm1, NULL);

	//Command Line Argument passing (ts_indexgen <n> <m> <infile> <outfile> )
	if( argc == 5) {
    	printf("Main | The arguments supplied are %s, %s, %s, %s\n", 
    		argv[1], argv[2], argv[3], argv[4]);
    	//Initializing number of Threads and Buffer Size
    	initFunc( atoi(argv[1]), atoi(argv[2]));
   	}
   	else if( argc > 5 ) {
    	printf("Main | Too many arguments supplied.\n"); exit(0);
   	}
   	else {
		printf("Main | 4 argument expected.\n"); exit(0);
   	}//----------------------------------------------------------------------

	//Openning the input file in read-only mode
	FILE *ptr_file;
	ptr_file = fopen( argv[3], "r");
	if (!ptr_file) {
		perror( "Main | can't open the input file");		
		return 1;
	}//----------------------------------------

	//INITIALIZATIONS ------------------------
	list *BBLL[N]; //BoundedBufferLinkedList[N]
	int i;
	for ( i = 0; i < N; i++) {
		BBLL[i] = ( list *) malloc ( sizeof ( list));
		bbInit( BBLL[i]);
	}

	int lineno = 0;
	char *word = (char *) malloc( MAXWORDSIZE);
	char line[MAXLINESIZE];
	//-----------------------------------------

	//pThread - Worker CREATION ---------------
	pthread_t threads[N];
	int rc;
	int t;
	for ( t = 0; t < N; t++) {
		//printf( "Creating thread: %d\n", t);
		rc = pthread_create( &threads[t], NULL, Worker, ( void *) BBLL[(int)t]);
		if ( rc != 0)
			printf( "Error while creating thread!\n");
	}//----------------------------------------

	/*Read the input text file, word by word, convert words to lowercase,
	put the word to the corresponding bounded buffer*/
	while (	fgets( line, MAXLINESIZE, ptr_file)) {
		lineno++;
		word = strtok(line, " \n\t");
		while ( word != NULL) {
			//Lowercase conversion
			int const length = strlen( word);
			char *lower = (char*) malloc (length);
			lower[length+1] = 0;
			for ( i = 0; i < length; i++) 
				lower[i] = tolower(word[i]);

			//Looking to first char of the word, Deciding the worker number (qid)
			int qid = ((int) ( (lower[0] - 97) / (26 / N)));
			qid = (qid > N-1) ? N-1 : qid;

			//Put the word to the bounded buffer
			item *pass = malloc( sizeof( item));
			strcpy( pass->str, lower); pass->id = lineno;

			//CRITICAL SECTION ============================
			pthread_mutex_lock( &BBLL[qid]->mutex); 

			bbInsert( BBLL[qid], pass);
			while ( BBLL[qid]->count == M+1)
				pthread_cond_wait( &BBLL[qid]->full, &BBLL[qid]->mutex );

			if ( BBLL[qid]->count == 1)
				pthread_cond_signal( &BBLL[qid]->empty);
			
			pthread_mutex_unlock( &BBLL[qid]->mutex);
			//=============================================
			word = strtok( NULL, " \n\t");
		}
	}//----------------------------------------------------------------
	fclose( ptr_file);
	
	item *temp = ( item *) malloc ( sizeof ( item));
	temp->id = TERMINATE; strcpy ( temp->str, "TERMINATE");

	//Waiting for Worker TERMINATIONS --------------
	for ( t = 0; t < N; t++) {
	
		//CRITICAL SECTION ============================
		pthread_mutex_lock( &BBLL[t]->mutex); 

		bbInsert( BBLL[t], temp);
		while ( BBLL[t]->count == M+1)
			pthread_cond_wait( &BBLL[t]->full, &BBLL[t]->mutex );
	
		if ( BBLL[t]->count == 1)
			pthread_cond_signal( &BBLL[t]->empty);
		
		pthread_mutex_unlock( &BBLL[t]->mutex);
		//=============================================
		pthread_join(threads[t], NULL);
	}//---------------------------------------------

	//Generating <output> index file
	ptr_file = fopen( argv[4], "w+"); //<output>

	fprintf( ptr_file, "%s %d", root->str, root->id);
	item *cur;
	while ( root->next != NULL) {
		if ( strcmp( root->str, root->next->str) == 0)
			fprintf( ptr_file, ", %d", root->next->id);
		else
			fprintf( ptr_file, "\r\n%s %d", root->next->str, root->next->id);
		cur = root;
		root = root->next;
		free(cur);
	}
	fprintf( ptr_file, "\r\n");
	//-----------------------------------------
	fclose( ptr_file);

	//Measuring time!--------------------------
    // struct timeval tm2;
    // gettimeofday(&tm2, NULL);

    // unsigned long long m = 1000 * (tm2.tv_sec - tm1.tv_sec) + (tm2.tv_usec - tm1.tv_usec) / 1000;
    // printf("%llu ms\n", m);
    //-----------------------------------------

return 0;
}