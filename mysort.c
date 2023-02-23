#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>
#include <sys/stat.h>
#include<pthread.h>
#include<string.h>

#define USE "./mysort <input file> <output file> <number of threads>"
#define BUFFER_SIZE 100
#define MAX_LEN 101 // This is the default size of every string 

int number_of_lines, part=0;
char **data = NULL;
int numThreads, number_per_thread;

long int get_file_size(char *file_name)
{
	int id;
	struct stat status; // Gets file status
	id = stat(file_name, &status);
	if (id == -1)
	{
		printf("File Size Error!");
	}
	return status.st_size;
}

void Merge(long int low,long int mid,long int high) //Merging the Array Function
{
    long int nL= mid-low+1;
    long int nR= high-mid;

    char** L=malloc(sizeof(char *)*nL);
    char** R=malloc(sizeof(char *)*nR);
     long int i;
    for(i=0;i<nL;i++)
    {
        L[i]=(char*)malloc((MAX_LEN));
        strcpy(L[i],data[low+i]);
    }
    for(i=0;i<nR;i++)
    {
        R[i]=(char*)malloc((MAX_LEN));
        strcpy(R[i],data[mid+i+1]);
    }
    long int j=0,k;
    i=0;
    k=low;
    while(i<nL&&j<nR)
    {
        if(strcmp(L[i],R[j])<0)strcpy(data[k++],L[i++]);
        else strcpy(data[k++],R[j++]);
    }
    while(i<nL)strcpy(data[k++],L[i++]);
    while(j<nR)strcpy(data[k++],R[j++]);
	for(i=0;i<nL;i++)
    {
        free(L[i]);
    }
    free(L);
    for(i=0;i<nR;i++)
    {
        free(R[i]);
    }
    free(R);
}


void MergeSort(long int low,long int high) //Main MergeSort function
{
    if(low<high)
    {
        long int mid = low + (high - low) / 2;
        MergeSort(low,mid);
        MergeSort(mid+1,high);
        Merge(low,mid,high);
    }
}

void* merge_sort(void* arg)
{
    // which part out of 4 parts
    int thread_part = part++;
 
    // calculating low and high
    long int low = thread_part * (number_of_lines /numThreads);
    long int high = (thread_part + 1) * (number_of_lines / numThreads) - 1;
 
    // evaluating mid point
    long int mid = low + (high - low) / 2;
    if (low < high) {
        MergeSort(low, mid);
        MergeSort(mid + 1, high);
        Merge(low, mid, high);
    }
}

void merge_sections_of_array(int number, int aggregation) {
    for(int i = 0; i < number; i = i + 2) {
        long int left = i * (number_per_thread* aggregation);
        long int right = ((i + 2) * number_per_thread* aggregation) - 1;
        long int middle = left + (number_per_thread* aggregation) - 1;
        if (right >= number_of_lines) {
            right = number_of_lines - 1;
        }
        printf("\n left: %ld mid: %ld right: %ld",left,middle,right);
        Merge(left, middle, right);
        printf("\n%d done",i);
    }
    if (number / 2 >= 1) {
        merge_sections_of_array(number / 2, aggregation * 2);
    }
}

// TODO implement external sort
void mysort(char* inputFile, char* outputFile)
{
	long int file_size = get_file_size(inputFile);
	number_of_lines = file_size/100;
	
	FILE* fin;

	fin = fopen(inputFile, "r");
	if (fin == NULL) {
		fprintf(stderr, "fopen(%s) failed", inputFile);
		return;
	}

	data = (char**)malloc(number_of_lines*sizeof(char *));
	for (long int i = 0; i < number_of_lines; i++)
		data[i] = (char*)malloc(MAX_LEN);

	long int line = 0;
	while (!feof(fin) && !ferror(fin))
		if (fgets(data[line], MAX_LEN, fin) != NULL)
			line++;

	fclose(fin);
	
	pthread_t threads[numThreads];
	// creating 4 threads
	long int size = line;
	
	for (int i = 0; i < numThreads; i++)
        pthread_create(&threads[i], NULL, merge_sort,
                                        (void*)NULL);
 
    // joining all 4 threads
    for (int i = 0; i < numThreads; i++)
        pthread_join(threads[i], NULL);
    printf("\nThreads Joined\n");
    
    //Merge(0, (number_of_lines / 2 - 1) / 2, number_of_lines / 2 - 1);
    //Merge(number_of_lines / 2, number_of_lines/2 + (number_of_lines-1-number_of_lines/2)/2, number_of_lines - 1);
    //Merge(0, (number_of_lines - 1)/2, number_of_lines - 1);
    number_per_thread = number_of_lines/numThreads;
    int number = numThreads;
    printf("Final Merging started\n");
    merge_sections_of_array(number, 1);
    printf("Final Merging End\n");
        
	FILE *outfile = fopen(outputFile, "w");
	printf("Output File Opened\n");
	for(long int i=0; i<size; i++)
		fputs(data[i],outfile);
	fclose(outfile);
	printf("Output File closed\n");

	for (long int i = 0; i < number_of_lines; i++)
		free(data[i]);
	free(data);
}

int main(int argc, char** argv) {
    char* inputFile;
    char* outputFile;
    
    struct timeval start, end;
    double executionTime;

    if (argc != 4) {
        fprintf(stderr, USE);
        return 1;
    }

    // Read arguments
    inputFile = argv[1];
    outputFile = argv[2];
    numThreads = atoi(argv[3]);

    // Execute sort and measure execution time
    gettimeofday(&start, NULL);
    mysort(inputFile, outputFile);
    gettimeofday(&end, NULL);
    executionTime = ((double) end.tv_sec - (double) start.tv_sec)
            + ((double) end.tv_usec - (double) start.tv_usec) / 1000000.0;
    
    printf("input file: %s\n", inputFile);
    printf("output file: %s\n", outputFile);
    printf("number of threads: %d\n", numThreads);
    printf("execution time: %lf\n", executionTime);

    return 0;
}
