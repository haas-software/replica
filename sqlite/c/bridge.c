/* De VFS bezit geen opslag: ieder byte gaat naar een Rust-eigenaar. SQLite's
 * eigen procesglobale runtime leeft uitsluitend tussen init en end.
 *
 * Er zijn twee soorten VFS. De standaard-VFS `replica` draagt de callbacks van
 * de engine zelf (replica_sqlite_init); `Engine::open` opent daarop één
 * verbinding. Elke verbinding uit replica_sqlite_open_with krijgt een eigen
 * VFS `replica-<n>` met een eigen kopie van haar callbacks in pAppData: zo
 * praat elke verbinding met haar eigen opslag-eigenaar (een eigen map, een
 * eigen stack). Geen callback leest nog een globale backend, behalve die van
 * de standaard-VFS zelf. */
#include "bridge.h"
#include <string.h>
#include <limits.h>
#ifndef SQLITE_TEMP_FILE_PREFIX
#define SQLITE_TEMP_FILE_PREFIX "etilqs_"
#endif

/* De naam van een tijdelijk bestand: het voorvoegsel van SQLite en 16 hexcijfers. */
#define TEMP_NAME 32
typedef struct {
 sqlite3_file base;
 ReplicaStorage *storage;
 uint32_t handle;
 int lock;
 /* Een tijdelijk bestand (sorter, temp-B-tree): de VFS verzint de naam en
  * verwijdert het bij sluiten (SQLITE_OPEN_DELETEONCLOSE), zoals unixOpen. */
 char temp[TEMP_NAME];
} ReplicaFile;
/* Eén verbinding met haar eigen VFS. Het geheugen komt uit SQLite's heap
 * (memsys5) en gaat terug bij het sluiten van de verbinding. */
typedef struct ReplicaVfs ReplicaVfs;
struct ReplicaVfs {
 sqlite3_vfs vfs;
 ReplicaStorage storage;
 sqlite3 *db;
 ReplicaVfs *next;
 char name[32];
};
static ReplicaStorage backend;
/* De ene verbinding op de standaard-VFS (Engine::open). */
static sqlite3 *connection;
/* Alle verbindingen met een eigen VFS, voor de opruiming in end. */
static ReplicaVfs *vfs_list;
/* Monotoon: een VFS-naam wordt nooit hergebruikt, ook niet na sluiten. */
static uint64_t vfs_count;
static int active;
/* De paginacache in pagina's: de pool uit replica_sqlite_init, en de
 * cache_size waarmee elke verbinding opent. */
static int cache_pages;

static ReplicaStorage *storage_of(sqlite3_vfs *vfs) {return (ReplicaStorage *)vfs->pAppData;}

static int file_close(sqlite3_file *file) {
 ReplicaFile *f=(ReplicaFile *)file;
 int rc=f->storage->close(f->storage->context,f->handle);
 if(rc==SQLITE_OK && f->temp[0])rc=f->storage->remove(f->storage->context,f->temp,0);
 f->base.pMethods=0;
 return rc;
}
static int file_read(sqlite3_file *file,void *buf,int n,sqlite3_int64 off) {
 ReplicaFile *f=(ReplicaFile *)file;
 if(off<0 || n<0)return SQLITE_IOERR_READ;
 return f->storage->read(f->storage->context,f->handle,(uint64_t)off,buf,n);
}
static int file_write(sqlite3_file *file,const void *buf,int n,sqlite3_int64 off) {
 ReplicaFile *f=(ReplicaFile *)file;
 if(off<0 || n<0)return SQLITE_IOERR_WRITE;
 return f->storage->write(f->storage->context,f->handle,(uint64_t)off,buf,n);
}
static int file_truncate(sqlite3_file *file,sqlite3_int64 size) {
 ReplicaFile *f=(ReplicaFile *)file;
 if(size<0)return SQLITE_IOERR_TRUNCATE;
 return f->storage->truncate(f->storage->context,f->handle,(uint64_t)size);
}
static int file_sync(sqlite3_file *file,int flags) {
 ReplicaFile *f=(ReplicaFile *)file;
 return f->storage->sync(f->storage->context,f->handle,flags);
}
static int file_size(sqlite3_file *file,sqlite3_int64 *size) {
 ReplicaFile *f=(ReplicaFile *)file;
 uint64_t n=0;
 int rc=f->storage->size(f->storage->context,f->handle,&n);
 if(rc==SQLITE_OK && n>INT64_MAX)return SQLITE_IOERR_FSTAT;
 *size=(sqlite3_int64)n;
 return rc;
}
/* Elke verbinding heeft een exclusieve eigenaar van haar map: geen tweede
 * verbinding of schrijver op dezelfde bestanden, dus het lockniveau leeft
 * alleen hier (geen OS-locks, geen I/O). */
static int file_lock(sqlite3_file *file,int level) {((ReplicaFile *)file)->lock=level;return SQLITE_OK;}
static int file_unlock(sqlite3_file *file,int level) {((ReplicaFile *)file)->lock=level;return SQLITE_OK;}
static int file_reserved(sqlite3_file *file,int *locked) {*locked=((ReplicaFile *)file)->lock>=SQLITE_LOCK_RESERVED;return SQLITE_OK;}
static int file_control(sqlite3_file *file,int op,void *arg) {
 if(op==SQLITE_FCNTL_LOCKSTATE){*(int *)arg=((ReplicaFile *)file)->lock;return SQLITE_OK;}
 return SQLITE_NOTFOUND;
}
/* HopFS schrijft in blokken van 4 KiB; een deel van een blok is daar een
 * lees-wijzig-schrijf. De journalkop wordt zo één heel blok. */
static int file_sector(sqlite3_file *file) {(void)file;return 4096;}
/* SAFE_APPEND: HopFS legt de boom (en dus de maat) pas vast na een flush van
 * de data, dus een bestand groeit nooit vóór zijn bytes er staan. SQLite
 * schrijft dan het aantal records niet meer in de journalkop en spaart per
 * commit één sync van het journal (syncJournal). */
static int file_characteristics(sqlite3_file *file) {(void)file;return SQLITE_IOCAP_SAFE_APPEND;}
static const sqlite3_io_methods methods={
 .iVersion=1,.xClose=file_close,.xRead=file_read,.xWrite=file_write,
 .xTruncate=file_truncate,.xSync=file_sync,.xFileSize=file_size,
 .xLock=file_lock,.xUnlock=file_unlock,.xCheckReservedLock=file_reserved,
 .xFileControl=file_control,.xSectorSize=file_sector,.xDeviceCharacteristics=file_characteristics
};
static int vfs_open(sqlite3_vfs *vfs,const char *name,sqlite3_file *file,int flags,int *out) {
 ReplicaFile *f=(ReplicaFile *)file;
 ReplicaStorage *s=storage_of(vfs);
 memset(f,0,sizeof(*f));
 if(flags & SQLITE_OPEN_WAL)return SQLITE_CANTOPEN;
 if(!name){
  /* Een tijdelijk bestand zonder naam (unixGetTempname): etilqs_ plus 16 hexcijfers
   * uit de entropie van de eigenaar, in dezelfde map als de database. De naam
   * staat in het bestand zelf, niet in een globale teller. */
  unsigned char r[8];
  if(!(flags & SQLITE_OPEN_DELETEONCLOSE) || s->random(s->context,r,sizeof r)!=sizeof r)return SQLITE_CANTOPEN;
  memcpy(f->temp,SQLITE_TEMP_FILE_PREFIX,7);
  for(int i=0;i<8;++i){f->temp[7+2*i]="0123456789abcdef"[r[i]>>4];f->temp[8+2*i]="0123456789abcdef"[r[i]&15];}
  name=f->temp;
 }else if(flags & SQLITE_OPEN_DELETEONCLOSE)return SQLITE_CANTOPEN;
 int rc=s->open(s->context,name,flags,&f->handle);
 if(rc!=SQLITE_OK)return rc;
 f->storage=s;
 f->base.pMethods=&methods;
 if(out)*out=flags;
 return SQLITE_OK;
}
static int vfs_delete(sqlite3_vfs *vfs,const char *name,int sync_dir) {ReplicaStorage *s=storage_of(vfs);return s->remove(s->context,name,sync_dir);}
static int vfs_access(sqlite3_vfs *vfs,const char *name,int flags,int *found) {ReplicaStorage *s=storage_of(vfs);(void)flags;return s->exists(s->context,name,found);}
static int vfs_path(sqlite3_vfs *vfs,const char *name,int n,char *out) {
 (void)vfs;
 size_t size=strlen(name);
 if(size>255 || size>=(size_t)n)return SQLITE_CANTOPEN;
 memcpy(out,name,size+1);
 return SQLITE_OK;
}
static int vfs_random(sqlite3_vfs *vfs,int n,char *out) {ReplicaStorage *s=storage_of(vfs);return s->random(s->context,(unsigned char *)out,n);}
static int vfs_sleep(sqlite3_vfs *vfs,int micros) {(void)vfs;(void)micros;return 0;}
static int vfs_time64(sqlite3_vfs *vfs,sqlite3_int64 *out) {
 ReplicaStorage *s=storage_of(vfs);int64_t millis=0;
 int rc=s->time(s->context,&millis);
 if(rc!=SQLITE_OK)return rc;
 if(millis>INT64_MAX-210866760000000LL || millis<0)return SQLITE_ERROR;
 *out=millis+210866760000000LL;return SQLITE_OK;
}
static int vfs_time(sqlite3_vfs *vfs,double *out) {
 sqlite3_int64 millis;
 int rc=vfs_time64(vfs,&millis);
 if(rc==SQLITE_OK)*out=(double)millis/86400000.0;
 return rc;
}
static sqlite3_vfs vfs={
 .iVersion=2,.szOsFile=sizeof(ReplicaFile),.mxPathname=255,.zName="replica",.pAppData=&backend,
 .xOpen=vfs_open,.xDelete=vfs_delete,.xAccess=vfs_access,.xFullPathname=vfs_path,
 .xRandomness=vfs_random,.xSleep=vfs_sleep,.xCurrentTime=vfs_time,.xCurrentTimeInt64=vfs_time64
};
/* Ook memdb-initialisatie zoekt de standaard-VFS tijdens initialize. */
int sqlite3_os_init(void){return sqlite3_vfs_register(&vfs,1);}
int sqlite3_os_end(void){return SQLITE_OK;}

int replica_sqlite_init(void *heap,int bytes,int page_size,const ReplicaStorage *storage) {
 if(active || bytes<65536 || !heap || !storage)return SQLITE_MISUSE;
 if(page_size<512 || page_size>65536 || (page_size&(page_size-1)))return SQLITE_MISUSE;
 /* De paginacache uit een eigen pool: in memsys5 (machten van twee) kost een
  * pagina van 4 KiB plus kop 8 KiB, in de pool precies wat hij is. Drie
  * achtste van de heap; de rest is memsys5 voor de sorter, statements en
  * schema. Elke verbinding opent met cache_size op de pool (open_on), dus
  * een app hoeft niets te zetten. GEMETEN 04-10: 24 MiB cache_size liet in
  * een heap van 64 MiB maar een paar MiB voor de sorter over, die daardoor
  * PMA's van enkele MiB schreef en in meer lagen moest mergen. */
 int hdr=0,rc=sqlite3_config(SQLITE_CONFIG_PCACHE_HDRSZ,&hdr);
 if(rc!=SQLITE_OK)return rc;
 /* Een slot per pagina van de paginamaat van de app (standaard 4 KiB): een
  * grotere pagina past niet in een slot en komt uit memsys5, tegen het dubbele;
  * met cache_size in slots van de pool liep die dan vol (NOMEM, GEMETEN
  * 15-11-2026: EasyFlor met 16 KiB-pagina's in een heap van 64 MiB). */
 int page=(page_size+hdr+7)&~7;
 /* De cache zestien pagina's kleiner dan de pool: pcache1 meldt "druk" zodra
  * er minder dan tien slots vrij zijn, en dan spilt de pager bij elke nieuwe
  * pagina één vuile pagina los (willekeurige volgorde, dus losse writes) in
  * plaats van alles op volgorde bij de commit. GEMETEN 04-10 op QEMU
  * (512 MiB): DELETE 253 naar 7.249 schrijfrondes met cache_size op de pool. */
 int slots=bytes/8*3/page;
 cache_pages=slots-16;
 int pool=slots*page;
 if(rc==SQLITE_OK)rc=sqlite3_config(SQLITE_CONFIG_PAGECACHE,heap,page,slots);
 /* De kleinste toewijzing 32 bytes: een sorterrecord van 20 bytes kostte met
  * 64 het drievoudige. De beheerbytes van memsys5 zijn dan 1/32 van de heap. */
 if(rc==SQLITE_OK)rc=sqlite3_config(SQLITE_CONFIG_HEAP,(char *)heap+pool,bytes-pool,32);
 /* Geen grote blokken: de sorter laat anders één buffer verdubbelen tot
  * cache_size, en in memsys5 (de grootste blokken) is er dan geen
  * aaneengesloten blok meer (NOMEM bij CREATE INDEX op 320.000 rijen, 04-10).
  * Per record alloceren past wel. */
 if(rc==SQLITE_OK)rc=sqlite3_config(SQLITE_CONFIG_SMALL_MALLOC,1);
 if(rc!=SQLITE_OK)return rc;
 backend=*storage;
 rc=sqlite3_initialize();
 if(rc==SQLITE_OK)rc=sqlite3_vfs_register(&vfs,1);
 if(rc!=SQLITE_OK){sqlite3_shutdown();memset(&backend,0,sizeof(backend));return rc;}
 /* Zonder deze grens geeft een volle heap NOMEM midden in een transactie
  * (gemeten 04-10: 160.000 rijen in een heap van 64 MiB); met de grens spilt
  * de sorter naar zijn tempbestand en geeft de paginacache pagina's terug
  * boven de pool, en gaat de transactie door. */
 sqlite3_soft_heap_limit64((bytes-pool)-(bytes-pool)/16);
 active=1;
 return SQLITE_OK;
}
/* Een opslag die er niet meer is: een verbinding die de Rust-kant vergat
 * (mem::forget) kan bij end nog open zijn terwijl haar eigenaar al weg is.
 * Haar callbacks wijzen dan hierheen, zodat het sluiten geen vrijgegeven
 * Rust-geheugen aanraakt. Lezen en schrijven falen, sluiten slaagt. */
static int dead_open(void *c,const char *n,int f,uint32_t *o){(void)c;(void)n;(void)f;(void)o;return SQLITE_CANTOPEN;}
static int dead_close(void *c,uint32_t h){(void)c;(void)h;return SQLITE_OK;}
static int dead_read(void *c,uint32_t h,uint64_t o,unsigned char *b,int n){(void)c;(void)h;(void)o;(void)b;(void)n;return SQLITE_IOERR_READ;}
static int dead_write(void *c,uint32_t h,uint64_t o,const unsigned char *b,int n){(void)c;(void)h;(void)o;(void)b;(void)n;return SQLITE_IOERR_WRITE;}
static int dead_truncate(void *c,uint32_t h,uint64_t n){(void)c;(void)h;(void)n;return SQLITE_IOERR_TRUNCATE;}
static int dead_sync(void *c,uint32_t h,int f){(void)c;(void)h;(void)f;return SQLITE_IOERR_FSYNC;}
static int dead_size(void *c,uint32_t h,uint64_t *n){(void)c;(void)h;*n=0;return SQLITE_IOERR_FSTAT;}
static int dead_remove(void *c,const char *n,int d){(void)c;(void)n;(void)d;return SQLITE_IOERR_DELETE;}
static int dead_exists(void *c,const char *n,int *f){(void)c;(void)n;*f=0;return SQLITE_IOERR_ACCESS;}
static int dead_random(void *c,unsigned char *b,int n){(void)c;(void)b;(void)n;return 0;}
static int dead_time(void *c,int64_t *o){(void)c;(void)o;return SQLITE_ERROR;}
static int dead_cooperate(void *c){(void)c;return SQLITE_INTERRUPT;}
static const ReplicaStorage dead={
 .context=0,.open=dead_open,.close=dead_close,.read=dead_read,.write=dead_write,
 .truncate=dead_truncate,.sync=dead_sync,.size=dead_size,.remove=dead_remove,
 .exists=dead_exists,.random=dead_random,.time=dead_time,.cooperate=dead_cooperate
};
/* Sluit één verbinding, ook met vergeten statements; de VFS blijft staan. */
static void close_raw(sqlite3 *db) {
 sqlite3_stmt *stmt;
 while((stmt=sqlite3_next_stmt(db,0))!=0)sqlite3_finalize(stmt);
 sqlite3_close(db);
}
/* Haalt een eigen VFS uit de lijst en geeft haar geheugen terug. Alleen
 * nadat haar verbinding gesloten is: SQLite leest pAppData tot het laatst. */
static void vfs_drop(ReplicaVfs *v) {
 for(ReplicaVfs **p=&vfs_list;*p;p=&(*p)->next){
  if(*p==v){*p=v->next;break;}
 }
 sqlite3_vfs_unregister(&v->vfs);
 sqlite3_free(v);
}
void replica_sqlite_close(sqlite3 *db) {
 if(!db)return;
 close_raw(db);
 if(connection==db){connection=0;return;}
 for(ReplicaVfs *v=vfs_list;v;v=v->next){
  if(v->db==db){vfs_drop(v);return;}
 }
}
void replica_sqlite_end(void) {
 if(!active)return;
 /* Wat nog open is, werd vergeten; haar eigenaar kan al weg zijn. */
 while(vfs_list){
  ReplicaVfs *v=vfs_list;
  v->storage=dead;
  close_raw(v->db);
  vfs_drop(v);
 }
 if(connection){close_raw(connection);connection=0;}
 sqlite3_vfs_unregister(&vfs);
 sqlite3_shutdown();
 memset(&backend,0,sizeof(backend));
 active=0;
}
/* De progresshandler krijgt de callbacks van zijn eigen verbinding mee. */
static int cooperate(void *p) {ReplicaStorage *s=p;return s->cooperate(s->context)!=SQLITE_OK;}
/* Opent en zet de pragma's van één eigenaar; bij een fout is *db gesloten en nul. */
static int open_on(const char *path,sqlite3 **db,int readonly,const char *vfs_name,ReplicaStorage *storage) {
 int flags=readonly?SQLITE_OPEN_READONLY:SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE;
 int rc=sqlite3_open_v2(path,db,flags|SQLITE_OPEN_NOMUTEX,vfs_name);
 if(rc!=SQLITE_OK){if(*db)sqlite3_close(*db);*db=0;return rc;}
 sqlite3_progress_handler(*db,1000,cooperate,storage);
 sqlite3_extended_result_codes(*db,1);
 sqlite3_db_config(*db,SQLITE_DBCONFIG_DEFENSIVE,1,0);
 sqlite3_db_config(*db,SQLITE_DBCONFIG_TRUSTED_SCHEMA,0,0);
 /* Eén eigenaar, dus EXCLUSIVE: SQLite houdt zijn slot en zijn journal open
  * en kijkt niet bij elke transactie opnieuw naar een heet journal, pagina 1
  * en de bestandsmaat (op QEMU 2 stat-rondes en een lees per puntread, 04-10).
  * Het journal wordt na een commit niet verwijderd maar zijn kop gewist,
  * met een sync; verwijderen plus de barrière van de map (EXTRA) spaart dat
  * uit. HopFS bewaart naamverwijdering pas na een bevestigde
  * directorybarrière; EXTRA vraagt die bij het sluiten nog steeds. */
 rc=sqlite3_exec(*db,"PRAGMA locking_mode=EXCLUSIVE; PRAGMA journal_mode=DELETE; PRAGMA synchronous=EXTRA",0,0,0);
 /* De paginacache zo groot als de pool (replica_sqlite_init); ook de PMA-maat
  * van de sorter volgt cache_size. Elke verbinding krijgt dezelfde grens op
  * dezelfde gedeelde pool (pcache1 in mode 2, zie lib.rs): de soft heap limit
  * en het recyclen van schone pagina's verdelen hem. */
 if(rc==SQLITE_OK){
  char sql[48];
  sqlite3_snprintf(sizeof sql,sql,"PRAGMA cache_size=%d",cache_pages);
  rc=sqlite3_exec(*db,sql,0,0,0);
 }
 /* Een lezer: ook SQL die zou schrijven faalt, vóór de VFS het weigert. */
 if(rc==SQLITE_OK && readonly)rc=sqlite3_exec(*db,"PRAGMA query_only=1",0,0,0);
 if(rc!=SQLITE_OK){close_raw(*db);*db=0;}
 return rc;
}
static int open_default(const char *path,sqlite3 **db,int readonly) {
 if(!active || connection)return SQLITE_MISUSE;
 int rc=open_on(path,db,readonly,"replica",&backend);
 if(rc==SQLITE_OK)connection=*db;
 return rc;
}
int replica_sqlite_open(const char *path,sqlite3 **db) {return open_default(path,db,0);}
int replica_sqlite_open_read_only(const char *path,sqlite3 **db) {return open_default(path,db,1);}
int replica_sqlite_open_with(const char *path,const ReplicaStorage *storage,int readonly,sqlite3 **db) {
 if(!active || !storage || !db)return SQLITE_MISUSE;
 *db=0;
 ReplicaVfs *v=sqlite3_malloc((int)sizeof(*v));
 if(!v)return SQLITE_NOMEM;
 memset(v,0,sizeof(*v));
 v->storage=*storage;
 v->vfs=vfs;
 v->vfs.pAppData=&v->storage;
 v->vfs.pNext=0;
 sqlite3_snprintf((int)sizeof v->name,v->name,"replica-%llu",(unsigned long long)++vfs_count);
 v->vfs.zName=v->name;
 int rc=sqlite3_vfs_register(&v->vfs,0);
 if(rc!=SQLITE_OK){sqlite3_free(v);return rc;}
 v->next=vfs_list;
 vfs_list=v;
 rc=open_on(path,db,readonly,v->name,&v->storage);
 if(rc!=SQLITE_OK){vfs_drop(v);return rc;}
 v->db=*db;
 return SQLITE_OK;
}
/* TRANSIENT kopieert bindings: geen geleende Rust-buffer blijft in C hangen. */
int replica_bind_text(sqlite3_stmt *s,int i,const unsigned char *p,int n){return sqlite3_bind_text(s,i,(const char *)p,n,SQLITE_TRANSIENT);}
int replica_bind_blob(sqlite3_stmt *s,int i,const unsigned char *p,int n){return sqlite3_bind_blob(s,i,p,n,SQLITE_TRANSIENT);}
