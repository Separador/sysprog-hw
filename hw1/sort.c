/* Program reads integers from multiple (N) input files,
 * sorts them using self-made coroutines
 * and merges sorted values in single output file.
 * Each coroutine runs for T/N msecs and switches to the other,
 * where T - target latency for all coroutines.
 *
 * INPUT:   target latency (T) in msecs and N file names to sort
 *          usage: sort <T> <filename1>[<filename2> ...]
 * OUTPUT:  writes resulting sequence to output.txt file
 *          outputs overall and each corutine sorting time in secs */

#include <stdio.h>
#include <stdlib.h>
#include <ucontext.h>
#include <signal.h>
#include <sys/mman.h>
#include <stdbool.h>
#include <time.h>

#define handle_error(msg)                           \
    do { perror(msg); exit(EXIT_FAILURE); } while (0)

#define stack_size 1024 * 1024
#define MIN_BUF 16    /* min allocated length for file */
enum errors {INPUT_PARAMERR, FILEOPENERR, FILEREADERR};

static ucontext_t uctx_main;

struct list {
    struct list *next, *prev;
};

typedef struct {
    struct list node;           /* points to previous
                                 * and next active coroutine */
    ucontext_t context;
    double time;                /* coroutine time in secs */
    char *stack;                /* stack ptr for free() */
    int id;
} coroutine;

static struct coro {
    coroutine *pool;
    coroutine *current;
    double timeslice;           /* coroutine timeslice in secs */
    int num;
    clock_t clk;                /* time counter assistant */
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

static void * allocate_stack_sig()
{
    void *stack = malloc(stack_size);
    stack_t ss;
    ss.ss_sp = stack;
    ss.ss_size = stack_size;
    ss.ss_flags = 0;
    sigaltstack(&ss, NULL);
    return stack;
}

static void * allocate_stack_mmap()
{
    return mmap(NULL, stack_size, PROT_READ | PROT_WRITE | PROT_EXEC,
            MAP_ANON | MAP_PRIVATE, -1, 0);
}

static void * allocate_stack_mprot()
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
static void * allocate_stack(enum stack_type t)
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
    if (&coros.current->node == coros.current->node.next) {
        return;
    }
    clock_t clk = clock() - coros.clk;
    double time_passed = clk/(double)CLOCKS_PER_SEC;
    /* switch if timeslice is exceeded */
    if (coros.timeslice > time_passed) {
        return;
    }
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
    } else if (len == 2) {
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
        coros.pool[i].stack = func_stack;
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

/* reads integers from files 'filenames' into '*files' array;
 * allocates memory and resizes '*files' if needed;
 * returns overall integers read count */
int readallfiles(const char **filenames, int **files, int fc, int *lens)
{
    int shift = 0, len = 0, cap = 0;
    if (*files == NULL) {
        *files = realloc(*files, (cap + MIN_BUF) * sizeof(int));
        cap += MIN_BUF;
    }
    for (int i = 0; i < fc; i++) {
        FILE *f = fopen(filenames[i], "r");
        if (f == NULL)
            exit(FILEOPENERR);
        len = 0;
        while (fscanf(f, "%d", *files + shift + len) == 1) {
            len++;
            if (shift + len >= cap) {      /* not enough cap? double it */
                *files = realloc(*files, cap * 2 * sizeof(int));
                cap *= 2;
            }
        }
        fclose(f);
        lens[i] = len;
        shift += len;
    }
    return shift;
}

int main(int argc, const char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "usage: sort <target latency> <filename1>"
                "[<filename2> ...]\n");
        exit(INPUT_PARAMERR);
    }
    int fc = argc - 2,                   /* file count */
        *lens = calloc(fc, sizeof(int)), /* array of lengths */
        *files = NULL,                   /* array of read integers */
        totallen = readallfiles(argv+2, &files, fc, lens);
    /* dividing input timeslice by 1000 to convert msecs into secs */
    coros.timeslice = atoi(argv[1])/(double)(1000*fc);
    ucontext_t uctx_temp;
    double ttime = clock();     /* total sorting time */
    if (fc == 1) {
        sort(files, lens[0]);
    } else {
        setup_coroutines(files, fc, lens);
        /* start coroutines */
        coros.clk = clock();
        if (swapcontext(&uctx_main, &coros.current->context) == -1)
            handle_error("swapcontext");
        /* return after coroutine finished file sort */
        if (&coros.current->node != coros.current->node.next) {
            remove_node((struct list *)coros.current);
            if (swapcontext(&uctx_temp, &coros.current->context) == -1)
                handle_error("swapcontext");
        }
    }
    mergefiles(files, 0, fc, lens);
    ttime = (clock() - ttime) / (double)CLOCKS_PER_SEC;
    if (fc > 1) {
        for (int i = 0; i < coros.num; i++)
            printf("Coroutine #%d sorting time: %.6f sec\n", 
                    i, coros.pool[i].time);
        for (int i = 0; i < coros.num; i++)
            free(coros.pool[i].stack);
    }
    printf("Overall sorting time: %.6f sec\n", ttime);
    writefile("output.txt", files, totallen);
    free(coros.pool);
    free(files);
    return 0;
}
