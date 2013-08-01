#include <nitro.h>
#include <uthash/uthash.h>

#include "memtro.pb-c.h"

typedef struct stored_value {
    char *key;
    uint8_t *value;
    size_t val_sz;

    UT_hash_handle hh;
} stored_value;

static pthread_rwlock_t store_lock;
static stored_value *db;

uint8_t *handle_get(Get *get, size_t *rsz) {
    GetResponse gr = GET_RESPONSE__INIT;

    stored_value *hit = NULL;
    int len = strlen(get->key);
//    pthread_rwlock_rdlock(&store_lock);
    HASH_FIND(hh, db, get->key, len,  hit);
 //   pthread_rwlock_unlock(&store_lock);

    if (hit) {
        gr.has_value = 1;
        gr.value.data = hit->value;
        gr.value.len = hit->val_sz;
    }

    *rsz = get_response__get_packed_size(&gr);
    uint8_t *resp = malloc(*rsz);
    get_response__pack(&gr, resp);
    return resp;
}

uint8_t *handle_put(Put *put, size_t *rsz) {
    PutResponse pr = PUT_RESPONSE__INIT;

    int len = strlen(put->key);
    pthread_rwlock_wrlock(&store_lock);

    stored_value *hit = NULL;
    HASH_FIND(hh, db, put->key, len,  hit);

    if (hit) {
        pr.is_new = 0;

    } else {
        pr.is_new = 1;
        hit = calloc(1, sizeof(stored_value));
        hit->key = put->key;
        hit->value = put->value.data;
        hit->val_sz = put->value.len;

        /* prevent removal when envelope is cleared */
        put->key = NULL;
        put->value.data = NULL;

        HASH_ADD_KEYPTR(hh, db, hit->key, len, hit);
    }

    pthread_rwlock_unlock(&store_lock);

    *rsz = put_response__get_packed_size(&pr);
    uint8_t *resp = malloc(*rsz);
    put_response__pack(&pr, resp);
    return resp;
}

void *cache_worker(void *p) {
    nitro_socket_t *sock = (nitro_socket_t *)p;

    while (1) {
        nitro_frame_t *fr = nitro_recv(sock, 0);

        MemtroRequest *req = memtro_request__unpack(NULL,
            nitro_frame_size(fr), nitro_frame_data(fr));

        size_t rsz;
        uint8_t *odata = NULL;
        if (req->get) {
            odata = handle_get(req->get, &rsz);
        } else {
            odata = handle_put(req->put, &rsz);
        }

        if (odata) {
            nitro_frame_t *fout = nitro_frame_new_heap(
                odata, rsz);
            int r = nitro_reply(fr, &fout, sock, 0);
            assert(!r && fout == NULL);
        }

        memtro_request__free_unpacked(req, NULL);
        nitro_frame_destroy(fr);
    }

    return NULL;
}

int main(int argc, char **argv) {

    if (argc != 3) {
        nitro_log_error("startup", "three arguments required");
        nitro_log_error("usage", "memtro PORT WORKERS");
        exit(1);
    }

    errno = 0;
    int port = strtol(argv[1], NULL, 10);
    int num_threads = strtol(argv[2], NULL, 10);

    if (errno) {
        nitro_log_error("startup", "invalid argument (non-integer)");
        nitro_log_error("usage", "memtro PORT WORKERS");
        exit(1);
    }
    assert(num_threads > 0 && num_threads < 500);

    pthread_t *threads = calloc(num_threads, sizeof(pthread_t));

    char loc_buf[50] = {0};
    snprintf(loc_buf, 50, "tcp://*:%d", port);

    nitro_runtime_start();
    nitro_enable_stats();

    nitro_socket_t *sock = nitro_socket_bind(loc_buf, NULL);

    if (!sock) {
        nitro_log_error("startup", "bind error: %s", nitro_errmsg(nitro_error()));
        exit(1);
    }

    pthread_rwlock_init(&store_lock, NULL);

    int i;
    for (i=0; i < num_threads; ++i) {
        pthread_create(&threads[i], NULL, cache_worker, sock);
    }

    nitro_log_info("startup", "all %d threads online; system ready", num_threads);

    void *p;
    for (i=0; i < num_threads; ++i) {
        pthread_join(threads[i], &p);
        assert(0); // should never exit
    }

    return 0;
}
