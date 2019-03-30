/* Program reads integers from multiple input files,
 * sorts them using self-made corutines
 * and merges sorted values in single output file.
 * Each input file contains at most FILE_LENGTH elements.
 *
 * INPUT:   file names to sort
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
static ucontext_t uctx_main, uctx_temp, *uctx_coro = NULL;
static volatile int coro_num = 0,    /* overall number of coroutines */
    coro_cur = 0;                    /* current coroutine id */   
static int active_coroutine = 0;
static bool sched = false,  /* should schedule coroutines or not */
    *coro_active;                /* coroutine active status */
static double *coro_time = NULL, coro_cur_time = 0;


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
    if (!sched)
        return;
    int coro_old = coro_cur;
    coro_cur_time = clock() - coro_cur_time;
    coro_time[coro_old] += coro_cur_time / (double)CLOCKS_PER_SEC;
    coro_cur = (coro_old + 1) % coro_num;
    while (!coro_active[coro_cur]) {
        coro_cur = (coro_cur + 1) % coro_num;
    }
    coro_cur_time = clock();
    if (swapcontext(&uctx_coro[coro_old], &uctx_coro[coro_cur]) == -1)
        handle_error("swapcontext");
    coro_cur = coro_old;
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

void setup_coroutines(int *files, int fc, int *lens)
{
    uctx_coro = (ucontext_t *)calloc(fc, sizeof(ucontext_t));
    coro_active = (bool *)calloc(fc, sizeof(bool));
    coro_num = fc;
    active_coroutine = fc;
    coro_time = calloc(coro_num, sizeof(double));
    int shift = 0;
    for (int i = 0; i < coro_num; i++) {
        char *func_stack = allocate_stack(STACK_SIG);
        if (getcontext(&uctx_coro[i]) == -1)
            handle_error("getcontext");
        uctx_coro[i].uc_stack.ss_sp = func_stack;
        uctx_coro[i].uc_stack.ss_size = stack_size;
        /* all coroutines shall return to main
         * and wait for other if needed */
        uctx_coro[i].uc_link = &uctx_main;
        makecontext(&uctx_coro[i], sort, 2, files + shift, lens[i]);
        shift += lens[i];
        coro_active[i] = true;
    }
    coro_cur = 0;
    sched = true;
}

int readallfiles(char **filenames, int *files, int fc, int *lens)
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

int main(int argc, char **argv)
{
    if (argc < 2) {
        fprintf(stderr, "usage: sort <filename1>"
                "[<filename2> ...]\n");
        exit(INPUT_PARAMERR);
    }
    int fc = argc - 1,                   /* file count */
        *lens = calloc(fc, sizeof(int)), /* array of lengths */
        *files = calloc(fc * FILE_LENGTH, sizeof(int)),
        totallen = readallfiles(argv+1, files, fc, lens);
    double ttime = clock();     /* total sorting time */
    
    if (fc == 1) {
        sort(files, lens[0]);
    } else {
        setup_coroutines(files, fc, lens);
        /* start coroutines */
        coro_cur_time = clock();
        if (swapcontext(&uctx_main, &uctx_coro[coro_cur]) == -1)
            handle_error("swapcontext");
    }

    coro_active[coro_cur] = false;
    active_coroutine--;
    while (active_coroutine) {
        int next = coro_cur + 1;
        while (!coro_active[next])
            next = (next + 1) % coro_num;
        if (swapcontext(&uctx_temp, &uctx_coro[next]) == -1)
            handle_error("swapcontext");
    }
    sched = false;
    mergefiles(files, 0, fc, lens);
    ttime = (clock() - ttime) / (double)CLOCKS_PER_SEC;
    if (fc > 1) {
        for (int i = 0; i < coro_num; i++)
            printf("Coroutine #%d sorting time: %.6f sec\n", i, coro_time[i]);
    }
    printf("Overall sorting time: %.6f sec\n", ttime);
    writefile("output.txt", files, totallen);
    free(uctx_coro);
    free(files);
    free(coro_time);
    return 0;
}
