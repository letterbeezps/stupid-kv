#ifndef STUPID_KV_H
#define STUPID_KV_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ---------------------------------------------------------------------
 * Return codes
 *
 * Every function returns int32_t (except constructors / accessors).
 *   SK_OK          success
 *   SK_NOT_FOUND   get(): key absent (not an error)
 *   negative       error; a human-readable message is stored into the
 *                  trailing `char **err_out` out-parameter when present.
 *                  The message must be released with sk_free_string().
 * ------------------------------------------------------------------- */
enum {
    SK_OK = 0,
    SK_NOT_FOUND = 1,

    SK_NULL_ARG = -1,
    SK_TX_CLOSED = -2,
    SK_TX_NOT_WRITABLE = -3,
    SK_WRITE_CONFLICT = -4,
    SK_READ_CONFLICT = -5,
    SK_ALREADY_EXISTS = -6,
    SK_COMMIT_NOT_PERSISTED = -7,
    SK_IO = -8,
    SK_PANIC = -99,
};

/* Opaque handles. */
typedef struct sk_database sk_database;
typedef struct sk_tx sk_tx;

/* ---------------------------------------------------------------------
 * Options
 *
 * "0 means default" for all numeric fields; -1 means default for the
 * int32_t tri-state booleans (-1 default, 0 off, 1 on).
 * ------------------------------------------------------------------- */
typedef struct {
    uint64_t gc_interval_ms;         /* 0 = default (500ms)  */
    uint64_t cleanup_interval_ms;    /* 0 = default (1s)     */
    uint64_t gc_full_scan_frequency; /* 0 = default (20)     */
    uint64_t pool_size;              /* 0 = default (512)    */
    uint64_t reset_threshold;        /* 0 = default (100)    */
    int32_t enable_cleanup;          /* -1 default (on)      */
    int32_t enable_gc;               /* -1 default (on)      */
} sk_db_options;

typedef struct {
    const char *base_path;           /* required, must not be NULL */
    const char *snapshot_path;       /* optional override, may be NULL */
    const char *aol_path;            /* optional override, may be NULL */
    int32_t snapshot_mode;           /* 0 = never, 1 = interval    */
    uint64_t snapshot_interval_ms;   /* used when snapshot_mode==1 */
    int32_t aol_mode;                /* 0 = never, 1 = sync, 2 = async */
    int32_t fsync_mode;              /* 0 = never, 1 = every append, 2 = interval */
    uint64_t fsync_interval_ms;      /* used when fsync_mode==2    */
    int32_t compression;             /* 0 = none, 1 = lz4          */
} sk_persist_options;

/* ---------------------------------------------------------------------
 * Database lifecycle
 * ------------------------------------------------------------------- */

/* Create an in-memory database. Never returns NULL. */
sk_database *sk_db_new(void);

/* Create a database with runtime options. Never returns NULL.
 * `opts` may be NULL (equivalent to sk_db_new). */
sk_database *sk_db_new_with_options(const sk_db_options *opts);

/* Create a database with snapshot + AOL persistence.
 * Returns NULL on I/O failure; *err_out holds the message. */
sk_database *sk_db_new_with_persistence(const sk_db_options *opts,
                                        const sk_persist_options *popts,
                                        char **err_out);

/* Destroy the database. Pending open transactions must be freed first
 * (transactions own an Arc to the shared state, but the handle `db`
 * itself is consumed here). */
void sk_db_free(sk_database *db);

/* Begin a transaction. write != 0 for a read-write transaction. */
sk_tx *sk_db_tx_begin(sk_database *db, int32_t write);

/* Trigger a full snapshot manually (same atomic protocol as the
 * background worker). No-op SK_OK when persistence is disabled. */
int32_t sk_db_snapshot(sk_database *db, char **err_out);

/* ---------------------------------------------------------------------
 * Transaction
 *
 * The sk_tx handle is internally synchronized (Rust Mutex) and may be
 * used from multiple goroutines concurrently.
 * ------------------------------------------------------------------- */

/* Drop the handle. An open transaction is automatically cancelled
 * (Rust Drop semantics), mirroring Python's "drop auto-cancels". */
void sk_tx_free(sk_tx *tx);

/* Oracle timestamp captured at transaction start. 0 if closed. */
uint64_t sk_tx_version(sk_tx *tx);

/* 1 if the transaction has been committed/cancelled, else 0. */
int32_t sk_tx_is_closed(sk_tx *tx);

/* Switch isolation level in place. Both mirror the Rust builder APIs
 * (`with_snapshot_isolation` / `with_serializable_snapshot_isolation`). */
int32_t sk_tx_with_snapshot_isolation(sk_tx *tx);
int32_t sk_tx_with_serializable_snapshot_isolation(sk_tx *tx);

/* Read the value for `key`. On SK_OK, *val_out points to a heap buffer
 * of *val_len bytes allocated by Rust; copy it and release with
 * sk_free_value(). Returns SK_NOT_FOUND (not an error) when absent. */
int32_t sk_tx_get(sk_tx *tx, const uint8_t *key, size_t key_len,
                  uint8_t **val_out, size_t *val_len, char **err_out);

/* Key existence at the transaction's snapshot. *out receives 0/1. */
int32_t sk_tx_exists(sk_tx *tx, const uint8_t *key, size_t key_len,
                     int32_t *out, char **err_out);

/* Insert or update. */
int32_t sk_tx_set(sk_tx *tx, const uint8_t *key, size_t key_len,
                  const uint8_t *val, size_t val_len, char **err_out);

/* Insert only if absent; SK_ALREADY_EXISTS otherwise. */
int32_t sk_tx_put(sk_tx *tx, const uint8_t *key, size_t key_len,
                  const uint8_t *val, size_t val_len, char **err_out);

/* Delete a key. */
int32_t sk_tx_del(sk_tx *tx, const uint8_t *key, size_t key_len,
                  char **err_out);

/* Commit / cancel. Idempotent-safe: committing a closed tx returns
 * SK_TX_CLOSED. */
int32_t sk_tx_commit(sk_tx *tx, char **err_out);
int32_t sk_tx_cancel(sk_tx *tx, char **err_out);

/* ---------------------------------------------------------------------
 * Memory management
 * ------------------------------------------------------------------- */

/* Free a value buffer returned by sk_tx_get(). */
void sk_free_value(uint8_t *ptr, size_t len);

/* Free an error message returned through `char **err_out`. */
void sk_free_string(char *ptr);

#ifdef __cplusplus
}
#endif

#endif /* STUPID_KV_H */
