#ifndef REPLICA_SQLITE_BRIDGE_H
#define REPLICA_SQLITE_BRIDGE_H
#include <stdint.h>
#include "sqlite3.h"
/* Opslagcallbacks van één VFS, met opslaghandvatten in plaats van C-bestandspointers.
 * De standaard-VFS `replica` heeft er één (die van de engine); elke verbinding
 * uit replica_sqlite_open_with een eigen kopie in haar eigen VFS. */
typedef struct {
 void *context;
 int (*open)(void *, const char *, int, uint32_t *);
 int (*close)(void *, uint32_t);
 int (*read)(void *, uint32_t, uint64_t, unsigned char *, int);
 int (*write)(void *, uint32_t, uint64_t, const unsigned char *, int);
 int (*truncate)(void *, uint32_t, uint64_t);
 int (*sync)(void *, uint32_t, int);
 int (*size)(void *, uint32_t, uint64_t *);
 int (*remove)(void *, const char *, int);
 int (*exists)(void *, const char *, int *);
 int (*random)(void *, unsigned char *, int);
 int (*time)(void *, int64_t *);
 int (*cooperate)(void *);
} ReplicaStorage;
int replica_sqlite_init(void *heap, int bytes, int page_size, const ReplicaStorage *storage);
void replica_sqlite_end(void);
int replica_sqlite_open(const char *path, sqlite3 **db);
int replica_sqlite_open_read_only(const char *path, sqlite3 **db);
/* Een verbinding met een eigen VFS (`replica-<n>`) over deze callbacks. */
int replica_sqlite_open_with(const char *path, const ReplicaStorage *storage, int readonly, sqlite3 **db);
void replica_sqlite_close(sqlite3 *db);
#endif
