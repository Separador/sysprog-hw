/* Program reads integers from multiple (N) input files,
 * sorts them using self-made coroutines
 * and merges sorted values in single output file.
 * Each coroutine runs for T/N msecs and switches to the other,
 * where T - time latency.
 * Each input file contains at most FILE_LENGTH elements.
 *
 * INPUT:   switch latency (T) and N file names to sort
 * OUTPUT:  writes resulting sequence to output.txt file
 *          outputs overall and each corutine sorting time in secs */

#include <stdio.h>
#include <stdlib.h>
#include <ucontext.h>
#include <signal.h>
#include <sys/mman.h>
#include <stdbool.h>
#include <time.h>

#define handle_error(msg) \
   do { perror(msg); exit(EXIT_FAILURE); } while (0)

#define stack_size 1024 * 1024
#define FILE_LENGTH 10000    /* number of elements in a file */
enum errors {INPUT_PARAMERR, FILEOPENERR, FILEREADERR};

int shed = 0;
static ucontext_t uctx_main;

struct list {
    struct list *next, *prev;
};

typedef struct {
    struct list node;		/* points to previous
				 * and next active coroutine */
    ucontext_t context;
    double time;		/* coroutine time */
    int id;
} coroutine;

static struct coro {
    coroutine *pool;
    coroutine *current;
    double timeslice;		/* coroutine timeslice in msecs */
    int num;
    clock_t clk;		/* time counter assistant */
} coros;

void connect_nodes(struct list *n1, struct list *n2)
{
    if (n1 == NULL || n2 == NULL)
	return;
    n1->next = n2;
    n2->prev = n1;
}

void remove_node(struct list *n)
{
    if (n == NULL)
	return;
    connect_nodes(n->prev, n->next);
    coros.current = (coroutine *)n->next;
    n->prev = NULL;
    n->next = NULL;
}

static void *
allocate_stack_sig()
{
        void *stack = malloc(stack_size);
        stack_t ss;
        ss.ss_sp = stack;
        ss.ss_size = stack_size;
        ss.ss_flags = 0;
        sigaltstack(&ss, NULL);
        return stack;
}

static void *
allocate_stack_mmap()
{
        return mmap(NULL, stack_size, PROT_READ | PROT_WRITE | PROT_EXEC,
                    MAP_ANON | MAP_PRIVATE, -1, 0);
}

static void *
allocate_stack_mprot()
{
        void *stack = malloc(stack_size);
        mprotect(stack, stack_size, PROT_READ | PROT_WRITE | PROT_EXEC);
        return stack;
}

enum stack_type {
        STACK_MMAP,
        STACK_SIG,
        STACK_MPROT
};

/**
 * Use this wrapper to choose your favourite way of stack
 * allocation.
 */
static void *
allocate_stack(enum stack_type t)
{
        switch(t) {
        case STACK_MMAP:
                return allocate_stack_mmap();
        case STACK_SIG:
                return allocate_stack_sig();
        case STACK_MPROT:
                return allocate_stack_mprot();
        }
    return NULL;
}

/* reads file specified by fname and fills vals;
 * returns number of read vals */
int readfile(const char *fname, int *vals)
{
    FILE *f = fopen(fname, "r");
    if (f == NULL)
        exit(FILEOPENERR);
    int length = 0;
    while (fscanf(f, "%d", vals++) == 1) {
        length++;
    }
    fclose(f);
    return length;
}

/* writes integers from array vals to file specified by fname;
 * returns number of written vals */
int writefile(const char *fname, const int *a, int length)
{
    FILE *f = fopen(fname, "w");
    if (f == NULL)
        exit(FILEOPENERR);
    int i = 0;
    for (; i < length; i++) {
        fprintf(f, "%d", *a++);
        if (i + 1 != length)
            fprintf(f, " ");
    }
    fclose(f);
    return i;
}

/* schedules next active coroutine */
void schedule_coroutines()
{
    if (!shed) {
	/* printf("no sched\n"); */
        return;
    }
    clock_t clk = clock() - coros.clk;
    double time_passed = clk/(double)CLOCKS_PER_SEC;
    /* printf("%d\n", clk); */
    /* switch if timeslice is exceeded */
    if (coros.timeslice > time_passed) {
	/* return; */
    }
    /* printf("%g %g\n", coros.timeslice, time_passed); */
    coros.current->time += time_passed;
    coroutine *prev = coros.current;
    coros.clk = clock();    
    coros.current = (coroutine *)coros.current->node.next;
    if (swapcontext(&prev->context, &coros.current->context) == -1)
	handle_error("swapcontext");
    coros.current = prev;
}

/* reorders values in arrays a and b in ascending order */
void merge(int *a, int *b, int alen, int blen)
{
    int i = 0, j = 0, *p = NULL, length = alen + blen;
    schedule_coroutines();
    int *temp = calloc(length, sizeof(int));
    schedule_coroutines();
    /* fill temp */
    while (i < alen && j < blen) {
        p = (a[i] < b[j])? a+i : b+j;
        schedule_coroutines();
        *temp++ = *p;
        schedule_coroutines();
        (a[i] < b[j])? i++ : j++;
        schedule_coroutines();
    }
    schedule_coroutines();
    p = (i == alen)? b + j : a + i;
    schedule_coroutines();
    int remains = length - i - j;
    schedule_coroutines();
    while (remains > 0) {
        schedule_coroutines();
        *temp++ = *p++;
        schedule_coroutines();
        remains--;
        schedule_coroutines();
    }
    schedule_coroutines();
    temp -= length;     /* return to start of temp */
    schedule_coroutines();
    for (i = 0; i < alen; i++) {
        schedule_coroutines();
        *a++ = *temp++;
        schedule_coroutines();
    }
    for (i = 0; i < blen; i++) {
        schedule_coroutines();
        *b++ = *temp++;
        schedule_coroutines();
    }
    free(temp-length);
    schedule_coroutines();
}

/* sorts arrays vals */
void sort(int *vals, int len)
{
    schedule_coroutines();
    if (len == 1) {
        return;
    }
    else if (len == 2) {
        schedule_coroutines();
        if(*vals > *(vals + 1)) {
            int temp = *vals;
            schedule_coroutines();
            *vals = *(vals+1);
            schedule_coroutines();
            *(vals+1) = temp;
            schedule_coroutines();
        }
    } else {
        schedule_coroutines();
        int half = len/2;
        schedule_coroutines();
        sort(vals, half);
        schedule_coroutines();
        sort(vals + half, len - half);
        schedule_coroutines();
        merge(vals, vals + half, half, len - half);
        schedule_coroutines();
    }
}

void mergefiles(int *a, int start, int end, int *lens)
{
    if (end - start == 1) {
        return;
    } else if (end - start == 2) {
        merge(a, a + lens[start], lens[start], lens[start+1]);
    } else {
        int half = (start + end)/2,
            llen = 0, rlen = 0;
        for (int i = start; i < half; i++)
            llen += lens[i];
        mergefiles(a, start, half, lens);
        mergefiles(a + llen, half, end, lens);
        for (int i = half; i < end; i++)
            rlen += lens[i];
        merge(a, a + llen, llen, rlen);
    }
}

void setup_coroutines(const int *files, int fc, const int *lens)
{
    coros.pool = (coroutine *)calloc(fc, sizeof(coroutine));
    coros.num = fc;
    int shift = 0;
    for (int i = 0; i < coros.num; i++) {
        char *func_stack = allocate_stack(STACK_SIG);
        if (getcontext(&coros.pool[i].context) == -1)
            handle_error("getcontext");
        coros.pool[i].context.uc_stack.ss_sp = func_stack;
        coros.pool[i].context.uc_stack.ss_size = stack_size;
	if (i == 0) {
	    connect_nodes(&coros.pool[coros.num-1].node,
			  &coros.pool[i].node);
	} else {
	    connect_nodes(&coros.pool[i-1].node, 
			  &coros.pool[i].node);
	}
        /* all coroutines shall return to main
         * and wait for other if needed */
        coros.pool[i].context.uc_link = &uctx_main;
        makecontext(&coros.pool[i].context, sort, 2, files + shift, lens[i]);
	shift += lens[i];
	coros.pool[i].id = i;
    }
    coros.current = &coros.pool[0];
}

int readallfiles(const char **filenames, int *files, int fc, int *lens)
{
    int totallen = 0,
        len = 0,
        *readpos = files;       /* pos in array where to read */
    for (int i = 0; i < fc; i++) {
        len = readfile(filenames[i], readpos);
        if (len == 0)
            exit(FILEREADERR);
        lens[i] = len;
        totallen += len;
        readpos += len;
    }
    return totallen;
 }

int main(int argc, const char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: sort <time latency> <filename1>"
                "[<filename2> ...]\n");
        exit(INPUT_PARAMERR);
    }
    int fc = argc - 2,                   /* file count */
        *lens = calloc(fc, sizeof(int)), /* array of lengths */
        *files = calloc(fc * FILE_LENGTH, sizeof(int)),
        totallen = readallfiles(argv+2, files, fc, lens);
    /* multiply divisor by 1000 to get msecs */
    coros.timeslice = atoi(argv[1])/(double)(1000*fc);
    ucontext_t uctx_temp;
    double ttime = clock();     /* total sorting time */
    if (fc == 1) {
        sort(files, lens[0]);
    } else {
	setup_coroutines(files, fc, lens);
        /* start coroutines */
	shed = 1;
        coros.clk = clock();
        if (swapcontext(&uctx_main, &coros.current->context) == -1)
            handle_error("swapcontext");
	if (&coros.current->node != coros.current->node.next) {
	    remove_node((struct list *)coros.current);
	    if (swapcontext(&uctx_temp, &coros.current->context) == -1)
		handle_error("swapcontext");
	}
    }
    shed = 0;
    printf("merge\n");
    mergefiles(files, 0, fc, lens);
    ttime = (clock() - ttime);// / (double)CLOCKS_PER_SEC;
    if (fc > 1) {
        for (int i = 0; i < coros.num; i++)
            printf("Coroutine #%d sorting time: %.6f sec\n", 
		   i, coros.pool[i].time);
	for (int i = 0; i < coros.num; i++)
	    free(coros.pool[i].context.uc_stack.ss_sp);
    }
    printf("Overall sorting time: %.6f sec\n", ttime);
    writefile("output.txt", files, totallen);
    free(coros.pool);
    free(files);
    return 0;
}

