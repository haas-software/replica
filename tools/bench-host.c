/* De lat op de host: dezelfde stappen als apps/sqlite-bench/src/steps.rs, maar
 * met SQLite's eigen unix-VFS op het bestandssysteem van de Mac (of Linux).
 * Dezelfde pragma's als bridge.c (journal_mode, synchronous, locking_mode),
 * zodat het verschil met QEMU en het ijzer alleen de VFS en de kern is.
 * Bouwen en draaien: tools/bench-host.sh [MiB]. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include "sqlite3.h"

#define ROWS_PER_MIB 5000
#define COMMITS 200
#define POINTS 2000
#define RANGES 100
#define KINDS 8
#define GROUPS 1024

static sqlite3 *db;
static unsigned long long rng = 0x9e3779b97f4a7c15ULL;
static unsigned long long rows;

static unsigned long long next(void) {
 rng ^= rng << 13; rng ^= rng >> 7; rng ^= rng << 17; return rng;
}
static long long now_us(void) {
 struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
 return t.tv_sec * 1000000LL + t.tv_nsec / 1000;
}
static void fail(const char *what) {
 fprintf(stderr, "REPLICA_BENCH_FAIL %s: %s\n", what, sqlite3_errmsg(db));
 exit(1);
}
static void exec(const char *sql) {
 if (sqlite3_exec(db, sql, 0, 0, 0) != SQLITE_OK) fail(sql);
}
static sqlite3_stmt *prep(const char *sql) {
 sqlite3_stmt *s;
 if (sqlite3_prepare_v2(db, sql, -1, &s, 0) != SQLITE_OK) fail(sql);
 return s;
}
static void done(sqlite3_stmt *s) {
 if (sqlite3_step(s) != SQLITE_DONE) fail("step");
 sqlite3_reset(s);
}
static long long scalar(const char *sql) {
 sqlite3_stmt *s = prep(sql);
 if (sqlite3_step(s) != SQLITE_ROW) fail(sql);
 long long v = sqlite3_column_int64(s, 0);
 sqlite3_finalize(s);
 return v;
}
static void report(const char *name, long long start, unsigned long long n) {
 long long us = now_us() - start;
 if (us < 1) us = 1;
 printf("REPLICA_BENCH_%s us=%lld n=%llu per_s=%.0f\n", name, us, n, (double)n / ((double)us / 1e6));
}
static void open_db(const char *path) {
 if (sqlite3_open(path, &db) != SQLITE_OK) fail("open");
 /* Dezelfde cache als de bridge: drie achtste van 64 MiB. */
 exec("PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=DELETE; PRAGMA synchronous=EXTRA; PRAGMA cache_size=-24576");
}

int main(int argc, char **argv) {
 unsigned long long mib = argc > 1 ? strtoull(argv[1], 0, 10) : 64;
 const char *path = argc > 2 ? argv[2] : "bench.db";
 rows = (mib ? mib : 1) * ROWS_PER_MIB;
 remove(path);
 char journal[1024]; snprintf(journal, sizeof journal, "%s-journal", path); remove(journal);
 open_db(path);
 exec("CREATE TABLE item(id INTEGER PRIMARY KEY,grp INTEGER,ts INTEGER,amount REAL,label TEXT,payload BLOB);"
      "CREATE TABLE event(id INTEGER PRIMARY KEY,item INTEGER,kind INTEGER,note TEXT)");
 long long t = now_us();
 exec("BEGIN");
 {
  sqlite3_stmt *s = prep("INSERT INTO item VALUES(?1,?2,?3,?4,?5,?6)");
  unsigned char payload[128]; char label[32];
  for (unsigned long long i = 1; i <= rows; i++) {
   for (int b = 0; b < 128; b++) payload[b] = (unsigned char)next();
   snprintf(label, sizeof label, "item-%llu", i);
   sqlite3_bind_int64(s, 1, (sqlite3_int64)i);
   sqlite3_bind_int64(s, 2, (sqlite3_int64)(i % GROUPS));
   sqlite3_bind_int64(s, 3, (sqlite3_int64)(i * 7));
   sqlite3_bind_double(s, 4, (double)(i % 1000) / 4.0);
   sqlite3_bind_text(s, 5, label, -1, SQLITE_TRANSIENT);
   sqlite3_bind_blob(s, 6, payload, 128, SQLITE_TRANSIENT);
   done(s);
  }
  sqlite3_finalize(s);
 }
 exec("COMMIT");
 report("LOAD", t, rows);
 t = now_us(); exec("CREATE INDEX item_grp ON item(grp,ts)"); report("INDEX", t, rows);
 t = now_us();
 unsigned long long n = rows / 4;
 exec("BEGIN");
 {
  sqlite3_stmt *s = prep("INSERT INTO event VALUES(?1,?2,?3,?4)");
  char note[32];
  for (unsigned long long i = 1; i <= n; i++) {
   snprintf(note, sizeof note, "event %llu of kind %llu", i, i % KINDS);
   sqlite3_bind_int64(s, 1, (sqlite3_int64)i);
   sqlite3_bind_int64(s, 2, (sqlite3_int64)(1 + next() % rows));
   sqlite3_bind_int64(s, 3, (sqlite3_int64)(i % KINDS));
   sqlite3_bind_text(s, 4, note, -1, SQLITE_TRANSIENT);
   done(s);
  }
  sqlite3_finalize(s);
 }
 exec("COMMIT");
 exec("CREATE INDEX event_item ON event(item)");
 report("EVENTS", t, n);
 t = now_us();
 {
  sqlite3_stmt *s = prep("INSERT INTO event VALUES(?1,?2,?3,'commit')");
  for (unsigned long long i = 0; i < COMMITS; i++) {
   sqlite3_bind_int64(s, 1, (sqlite3_int64)(n + 1 + i));
   sqlite3_bind_int64(s, 2, (sqlite3_int64)(1 + i % rows));
   sqlite3_bind_int64(s, 3, KINDS);
   done(s);
  }
  sqlite3_finalize(s);
 }
 report("COMMITS", t, COMMITS);
 /* Heropenen: SQLite's paginacache leeg, zoals in de app. De cache van het
  * OS blijft wel warm; dat is de helft van "35% faster than the filesystem". */
 sqlite3_close(db);
 open_db(path);
 t = now_us();
 {
  sqlite3_stmt *s = prep("SELECT amount,label FROM item WHERE id=?1");
  for (int i = 0; i < POINTS; i++) {
   sqlite3_bind_int64(s, 1, (sqlite3_int64)(1 + next() % rows));
   if (sqlite3_step(s) != SQLITE_ROW) fail("point");
   sqlite3_reset(s);
  }
  sqlite3_finalize(s);
 }
 report("POINT", t, POINTS);
 t = now_us();
 scalar("SELECT count(*) FROM item WHERE amount>=0 AND length(label)>0");
 report("SCAN", t, rows);
 t = now_us();
 scalar("SELECT count(*) FROM (SELECT grp,count(*),sum(amount) FROM item NOT INDEXED GROUP BY grp)");
 report("GROUP", t, rows);
 t = now_us();
 {
  sqlite3_stmt *s = prep("SELECT count(*),sum(amount) FROM item WHERE grp=?1 AND ts BETWEEN ?2 AND ?3");
  long long span = 500LL * GROUPS * 7;
  for (int i = 0; i < RANGES; i++) {
   long long from = (long long)(next() % (rows * 7));
   sqlite3_bind_int64(s, 1, (sqlite3_int64)(next() % GROUPS));
   sqlite3_bind_int64(s, 2, from);
   sqlite3_bind_int64(s, 3, from + span);
   if (sqlite3_step(s) != SQLITE_ROW) fail("range");
   sqlite3_reset(s);
  }
  sqlite3_finalize(s);
 }
 report("RANGE", t, RANGES);
 t = now_us();
 {
  unsigned long long got = 0;
  sqlite3_stmt *s = prep("SELECT amount FROM item ORDER BY amount");
  while (sqlite3_step(s) == SQLITE_ROW) got++;
  sqlite3_finalize(s);
  if (got != rows) fail("sort");
  report("SORT", t, rows);
 }
 t = now_us();
 n = (unsigned long long)scalar("SELECT count(*) FROM event e JOIN item i ON i.id=e.item WHERE e.kind=3");
 report("JOIN", t, n);
 t = now_us();
 {
  sqlite3_stmt *s = prep("UPDATE item SET amount=amount+1 WHERE id BETWEEN 1 AND ?1");
  sqlite3_bind_int64(s, 1, (sqlite3_int64)(rows / 4));
  done(s);
  sqlite3_finalize(s);
 }
 report("UPDATE", t, (unsigned long long)sqlite3_changes64(db));
 t = now_us(); exec("DELETE FROM event WHERE kind=0"); report("DELETE", t, (unsigned long long)sqlite3_changes64(db));
 t = now_us();
 {
  sqlite3_stmt *s = prep("PRAGMA quick_check");
  if (sqlite3_step(s) != SQLITE_ROW || strcmp((const char *)sqlite3_column_text(s, 0), "ok")) fail("check");
  sqlite3_finalize(s);
 }
 report("CHECK", t, 1);
 sqlite3_close(db);
 return 0;
}
