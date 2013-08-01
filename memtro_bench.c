#include <nitro.h>
#include <uthash/uthash.h>

#include "memtro.pb-c.h"

static int num_requests;
static uint64_t now;

pthread_cond_t starter_pistol;
pthread_mutex_t l_starter_pistol;

void *tick_worker(void *unused) {
    // sleep for 500 microseconds
    struct timespec ts = {0, 500000};
    struct timeval tv;

    while (1) {
        nanosleep(&ts, NULL);
        gettimeofday(&tv, NULL);
        now = (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
    }
    return NULL;
}

void *bench_worker(void *p) {
    char *loc = (char *)p;
    nitro_socket_t *sock = nitro_socket_connect(loc, NULL);

    if (!sock) {
        nitro_log_error("startup", "bind error: %s", nitro_errmsg(nitro_error()));
        exit(1);
    }

    Get gt = GET__INIT;
    gt.key = "foo";

    MemtroRequest req = MEMTRO_REQUEST__INIT;
    req.get = &gt;

    size_t sz = memtro_request__get_packed_size(&req);
    uint8_t *out = malloc(sz);
    memtro_request__pack(&req, out);

    nitro_frame_t *get_frame = nitro_frame_new_heap(out, sz);

    pthread_mutex_lock(&l_starter_pistol);
    pthread_cond_wait(&starter_pistol, &l_starter_pistol);
    pthread_mutex_unlock(&l_starter_pistol);

    int *times = malloc(num_requests * sizeof(int));

    int i;
    for (i=0; i < num_requests; ++i) {
        nitro_send(&get_frame, sock, NITRO_REUSE);
        times[i] = now;
    }

    for (i=0; i < num_requests; ++i) {
        nitro_frame_t *fr = nitro_recv(sock, 0);
        times[i] = now - times[i];
        nitro_frame_destroy(fr);
    }

    return times;
}

int int_compare(const void *p1, const void *p2) {
    int i1 = *((int *)p1);
    int i2 = *((int *)p2);

    if (i1 < i2) return -1;
    if (i1 == i2) return 0;
    return 1;
}

double tod_delta(struct timeval *tv1, struct timeval *tv2) {
    double d1 = tv1->tv_sec + ((double)tv1->tv_usec / 1000000.0);
    double d2 = tv2->tv_sec + ((double)tv2->tv_usec / 1000000.0);

    return d2 - d1;
}

int main(int argc, char **argv) {

    if (argc != 4) {
        nitro_log_error("startup", "three arguments required");
        nitro_log_error("usage", "memtro PORT WORKERS REQUESTS");
        exit(1);
    }

    errno = 0;
    int port = strtol(argv[1], NULL, 10);
    int num_threads = strtol(argv[2], NULL, 10);
    num_requests = strtol(argv[3], NULL, 10);

    if (errno) {
        nitro_log_error("startup", "invalid argument (non-integer)");
        nitro_log_error("usage", "memtro_bench PORT WORKERS REQUESTS");
        exit(1);
    }
    assert(num_threads > 0 && num_threads < 500);

    pthread_mutex_init(&l_starter_pistol, NULL);
    pthread_cond_init(&starter_pistol, NULL);

    pthread_t *threads = calloc(num_threads, sizeof(pthread_t));

    char loc_buf[50] = {0};
    snprintf(loc_buf, 50, "tcp://172.16.26.130:%d", port);

    nitro_runtime_start();
    nitro_enable_stats();

    pthread_t ticker;
    pthread_create(&ticker, NULL, tick_worker, NULL);

    int i;
    for (i=0; i < num_threads; ++i) {
        pthread_create(&threads[i], NULL, bench_worker, loc_buf);
    }

    nitro_log_info("startup", "all %d threads benchmarking", num_threads);

    void *pi;
    int **save_results = malloc(num_threads * sizeof(int *));
    sleep(1);

    struct timeval bstart, bstop;
    gettimeofday(&bstart, NULL);
    pthread_cond_broadcast(&starter_pistol);
    int bwrite = write(2, "threads finishing: ", strlen("threads finishing: "));

    for (i=0; i < num_threads; ++i) {
        pthread_join(threads[i], &pi);
        int *single = (int *)pi;
        save_results[i] = single;
        int bwrite = write(2, ".", 1);
        (void)bwrite;
    }
    gettimeofday(&bstop, NULL);
    bwrite = write(2, "\n", 1);
    (void)bwrite;

    int *results = malloc(num_threads * num_requests * sizeof(int));

    for (i=0; i < num_threads; ++i) {
        memcpy(results + (i * num_requests), save_results[i], num_requests * sizeof(int));
        free(save_results[i]);
    }

    int length = num_threads * num_requests;
    qsort(results, length, sizeof(int), int_compare);

    uint64_t tot = 0;

    for (i=0; i < length; ++i) {
        tot += results[i];
    }

    int ten_inc = length / 10;
    int hundred_inc = length / 100;
    int thousand_inc = length / 1000;
    double delt = tod_delta(&bstart, &bstop);

    printf("%d requests in %.3fs (%.2f req/s)\n",
        length,  delt, length / delt);

    printf("mean.....%.2fms\n", (double)tot / length);
    printf("0th......%dms\n", results[0]);
    printf("10th.....%dms\n", results[ten_inc * 1]);
    printf("20th.....%dms\n", results[ten_inc * 2]);
    printf("30th.....%dms\n", results[ten_inc * 3]);
    printf("40th.....%dms\n", results[ten_inc * 4]);
    printf("50th.....%dms\n", results[ten_inc * 5]);
    printf("60th.....%dms\n", results[ten_inc * 6]);
    printf("70th.....%dms\n", results[ten_inc * 7]);
    printf("80th.....%dms\n", results[ten_inc * 8]);
    printf("90th.....%dms\n", results[ten_inc * 9]);
    printf("95th.....%dms\n", results[95 * hundred_inc]);
    printf("99th.....%dms\n", results[99 * hundred_inc]);
    printf("99.5th...%dms\n", results[995 * thousand_inc]);
    printf("99.9th...%dms\n", results[999 * thousand_inc]);
    printf("100th....%dms\n", results[length - 1]);

    return 0;
}
