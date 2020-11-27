#include "inode_manager.h"
#include <time.h>

// disk layer -----------------------------------------

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf)
{
  memcpy(buf, blocks[id], BLOCK_SIZE);
  return;
}

void
disk::write_block(blockid_t id, const char *buf)
{
  memcpy(blocks[id], buf, BLOCK_SIZE);
  return;
}

// block layer -----------------------------------------

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  uint32_t id, start = 2 + BLOCK_NUM / BPB + INODE_NUM / IPB;

  /* select block */
  for (id = start; id < BLOCK_NUM; id++)
    if (using_blocks[id] == FREE) break;
  
  if (id == BLOCK_NUM) {
    printf("\t\tbm::alloc_block no more blocks!\n");
    exit(1);
  }

  /* set using_blocks */
  using_blocks[id] = ALLOC;

  /* write disk bitmap */
  // uint32_t b = BBLOCK(id), pos = id % BPB;
  // char buf[BLOCK_SIZE];
  // read_block(b, buf);
  // buf[pos / 8] = buf[pos / 8] | (0x1 << (pos % 8));
  // write_block(b, buf);

  return id;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */

  /* set using_blocks */
  using_blocks[id] = FREE;

  /* write disk bitmap */
  // uint32_t b = BBLOCK(id), pos = id % BPB;
  // char buf[BLOCK_SIZE];
  // read_block(b, buf);
  // buf[pos / 8] = buf[pos / 8] & ~(0x1 << (pos % 8));
  // write_block(b, buf);


  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    // printf("\t\tim::inode_manager  error! alloc first inode %d, should be 1\n", root_dir);
    exit(0);
  }
}

/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  int inum = INODE_NUM;
  struct inode *ino;
  char buf[BLOCK_SIZE], alloc;

  /* find block to alloc inode */
  bm->read_block(2, buf);
  for (int i = 1; i < INODE_NUM; i++)
  {
    alloc = (buf[(i + 10) / 8] >> ((i + 10) % 8)) & 0x1;
    if (!alloc) {inum = i; break;}
  }
  
  if (inum == INODE_NUM){ 
    printf("\t\tim::alloc_inode too much files\n");
    exit(0);
  }
  // printf("\tim::alloc_inode alloc inode %d\n", inum);

  /* write disk bitmap */
  buf[(inum + 10) / 8] = buf[(inum + 10) /8] | (0x1 << ((inum + 10) %8));
  bm->write_block(2, buf);

  /* put inode */
  time_t timer;
  time(&timer);
  uint32_t time_int = (uint32_t)timer;
  ino = (struct inode*)malloc(sizeof(struct inode));
  ino->type = (short) type;
  ino->atime = time_int;
  ino->mtime = time_int;
  ino->ctime = time_int;
  ino->size = 0;
  put_inode(inum, ino); 

  free(ino);

  return inum;
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  struct inode *ino;
  char buf[BLOCK_SIZE], alloc;
  ino = get_inode(inum);

  /* check if the inode is already a freed one */
  if (ino == NULL || ino->type == 0) return;

  /* clear inode */
  ino->type = 0;
  put_inode(inum, ino);
  
 /* write disk bitmap */
  bm->read_block(2, buf);
  alloc = buf[(inum + 10) /8];
  alloc = alloc & (~((0x1 << ((inum + 10) %8))));
  buf[(inum + 10) / 8] = alloc;
  bm->write_block(2, buf);
  
  free(ino);

  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino, *ino_disk;
  char buf[BLOCK_SIZE];

  // printf("\tim: get_inode %d\n", inum);

  if (inum < 0 || inum >= INODE_NUM) {
    printf("\t\tim::get_inode inum out of range\n");
    return NULL;
  }

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  // printf("%s:%d\n", __FILE__, __LINE__);

  ino_disk = (struct inode*)buf + inum%IPB;
  if (ino_disk->type == 0) {
    printf("\t\tim::get_inode inode not exist\n");
    return NULL;
  }

  ino = (struct inode*)malloc(sizeof(struct inode));
  *ino = *ino_disk;

  return ino;
}

void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  if (ino == NULL) return;
  // printf("\tim: put_inode %d, size = %d\n", inum, ino->size);

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

blockid_t *
inode_manager::get_blocks(uint32_t inum){

  /* get inode */
  struct inode *ino = get_inode(inum);
  if (ino == NULL) return NULL;

  uint32_t size = ino->size;
  uint32_t bcount = size / BLOCK_SIZE + (size % BLOCK_SIZE > 0);
  blockid_t *blocks = (blockid_t *)malloc(sizeof(blockid_t) * bcount);

  /* no indirect blocks */
  if (bcount < NDIRECT) {
    memcpy(blocks, ino->blocks, sizeof(blockid_t) * bcount);
    free(ino);
    return blocks;
  }

  /* indirect blocks */
  memcpy(blocks, ino->blocks, sizeof(blockid_t) * NDIRECT);
  char *buf = (char *)malloc(BLOCK_SIZE);
  bm->read_block(ino->blocks[NDIRECT], buf);
  memcpy(blocks + NDIRECT, buf, sizeof(blockid_t) * (bcount - NDIRECT));
  free(ino);
  return blocks;
}

void 
inode_manager::put_blocks(uint32_t inum, uint32_t bcount, 
  blockid_t *blocks){

  /* get inode */
  struct inode *ino = get_inode(inum);
  if (ino == NULL) return;  
  
  char *buf = (char *)malloc(BLOCK_SIZE);
  
  if (bcount < NDIRECT){  /* no indirect blocks */
    memcpy(ino->blocks, blocks, sizeof(blockid_t) * bcount); 
    ino->blocks[NDIRECT] = 0;
  }  
  else {  /* no indirect and indirect blocks */
    memcpy(ino->blocks, blocks, sizeof(blockid_t) * NDIRECT);
    memcpy(buf, blocks + NDIRECT, sizeof(blockid_t) * (bcount - NDIRECT));
    if (ino->blocks[NDIRECT] == 0)
      ino->blocks[NDIRECT] = bm->alloc_block();
    bm->write_block(ino->blocks[NDIRECT], buf);
  }
  put_inode(inum, ino);

  free(buf);
  return;
}

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_Out
   */

  struct inode *ino = get_inode(inum);
  if (ino == NULL){
    printf("\t\tim::read_file inode %d not exist\n", inum);
    *buf_out = NULL;
    *size = 0;
    return;
  }
    
  int rem_size = ino->size, i = 0, read_size;
  char *buf_total = (char *)malloc(rem_size);
  char *buf_block = (char *)malloc(BLOCK_SIZE);
  blockid_t *blocks = get_blocks(inum);

  *size = rem_size;
  *buf_out = buf_total;
  
  /* read blocks */
  while (rem_size > 0) {
    bm->read_block(blocks[i++], buf_block);
    read_size = rem_size > BLOCK_SIZE? BLOCK_SIZE: rem_size;
    memcpy(buf_total, buf_block, read_size);
    rem_size -= BLOCK_SIZE; buf_total += read_size;
  }

  /* modify atime */
  ino->atime = time(NULL);
  put_inode(inum, ino);
  
  /* free malloc space */
  free(buf_block);
  free(ino);
  free(blocks);

  return;
  
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  struct inode *ino = get_inode(inum);
  if (ino == NULL){
    printf("\t\tim::write_file inode %d not exist\n", inum);
    return;
  }

  if (size > (int)(MAXFILE * BLOCK_SIZE)){
    printf("\t\tim::write_file file too big!\n");
    return;
  }

  int old_size = ino->size, rem_size = size, write_size = 0, i = 0;
  int old_bcount = old_size / BLOCK_SIZE + (old_size % BLOCK_SIZE > 0);
  int new_bcount = size / BLOCK_SIZE + (size % BLOCK_SIZE > 0);
  char *buf_block = (char *)malloc(BLOCK_SIZE);
  blockid_t *old_blocks = get_blocks(inum);
  blockid_t *new_blocks = (blockid_t *)malloc(sizeof(blockid_t)*new_bcount);

  /* alloc new blocks or free old blocks */
  if (old_bcount < new_bcount) {
    memcpy(new_blocks, old_blocks, sizeof(blockid_t)*old_bcount);
    for (int n = old_bcount; n < new_bcount; n++)
      new_blocks[n] = bm->alloc_block();
  }
  else {
    memcpy(new_blocks, old_blocks, sizeof(blockid_t)*new_bcount);
    for (int n = new_bcount; n < old_bcount; n++) 
      bm->free_block(old_blocks[n]);

    if (old_bcount > NDIRECT && new_bcount < NDIRECT)
      bm->free_block(ino->blocks[NDIRECT]);
  }

  /* write blocks */
  while (rem_size > BLOCK_SIZE) {
    bm->write_block(new_blocks[i++], buf + write_size);
    write_size += BLOCK_SIZE;
    rem_size -= BLOCK_SIZE;
  }
  if (rem_size > 0) {
    memcpy(buf_block, buf + write_size, rem_size);
    bm->write_block(new_blocks[i], buf_block);
  }
  // printf("\t\tim::write_file after write file_size = %d\n", size);
  
  /* modify inode */
  ino->mtime = time(NULL);
  ino->atime = time(NULL);
  ino->ctime = time(NULL);
  ino->size = size;
  put_inode(inum, ino);
  put_blocks(inum, new_bcount, new_blocks);

  /* free malloc space */
  free(ino);
  free(buf_block);
  free(new_blocks);
  free(old_blocks);
  
  return;
}

void
inode_manager::getattr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  
  struct inode *ino = get_inode(inum);

  if (ino == NULL) {
    printf("\t\tim::getattr inode %d not exist\n", inum);
    return;
  }

  a.type = ino->type;
  a.size = ino->size;
  a.atime = ino->atime;
  a.mtime = ino->mtime;
  a.ctime = ino->ctime;
  
  free(ino);
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  struct inode *ino = get_inode(inum);

  if (ino == NULL) {
    printf("\t\tim::remove_file inode %d already removed\n", inum);
    return;
  }

  /* free blocks */
  int rem_size = ino->size, i = 0;
  while (rem_size > 0){
    bm->free_block(ino->blocks[i++]);
    rem_size -= BLOCK_SIZE;
  }
  
  /* free inode */
  free_inode(inum);
  free(ino);
  
  return;
}



// void
// inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
// {
//   /*
//    * your code goes here.
//    * note: read blocks related to inode number inum,
//    * and copy them to buf_Out
//    */

  
//   struct inode *ino = get_inode(inum);
//   if (ino == NULL){
//     printf("\t\tim::read_file inode %d not exist\n", inum);
//     *buf_out = NULL;
//     *size = 0;
//     return;
//   }
    
//   int rem_size = ino->size, i = 0, read_size;
//   char *buf_total = (char *)malloc((rem_size) * sizeof(char));
//   char *buf_block = (char *)malloc((BLOCK_SIZE) * sizeof(char));

//   *size = rem_size;
//   *buf_out = buf_total;
  
//   /* read blocks */
//   while (rem_size > 0) {
//     bm->read_block(ino->blocks[i++], buf_block);
//     read_size = rem_size > BLOCK_SIZE? BLOCK_SIZE: rem_size;
//     memcpy(buf_total, buf_block, read_size);
//     rem_size -= BLOCK_SIZE; buf_total += read_size;
//   }

//   /* modify atime */
//   ino->atime = time(NULL);
//   put_inode(inum, ino);
  
//   /* free malloc space */
//   free(buf_block);
//   free(ino);

//   return;
  
// }

// /* alloc/free blocks if needed */
// void
// inode_manager::write_file(uint32_t inum, const char *buf, int size)
// {
//   /*
//    * your code goes here.
//    * note: write buf to blocks of inode inum.
//    * you need to consider the situation when the size of buf 
//    * is larger or smaller than the size of original inode
//    */
//   struct inode *ino = get_inode(inum);
//   if (ino == NULL){
//     printf("\t\tim::write_file inode %d not exist\n", inum);
//     return;
//   }

//   if (size > MAXFILE * BLOCK_SIZE){
//     printf("\t\tim::write_file inode %d not exist\n", inum);
//     return;
//   }

//   int old_size = ino->size, rem_size = size, write_size = 0, i = 0;
//   int old_bcount = old_size / BLOCK_SIZE + (old_size % BLOCK_SIZE > 0);
//   int new_bcount = size / BLOCK_SIZE + (size % BLOCK_SIZE > 0);
//   char *buf_block = (char *)malloc((BLOCK_SIZE) * sizeof(char));

//   /* alloc new blocks or free blocks */
//   if (old_bcount < new_bcount) {
//     for (int n = old_bcount; n < new_bcount; n++){
//       ino->blocks[n] = bm->alloc_block();
//     }
//   }
//   else {
//     for (int n = new_bcount; n < old_bcount; n++) {
//       bm->free_block(ino->blocks[n]);
//       ino->blocks[n] = 0;
//     }
//   }

//   /* write blocks */
//   while (rem_size > BLOCK_SIZE) {
//     bm->write_block(ino->blocks[i++], buf + write_size);
//     write_size += BLOCK_SIZE;
//     rem_size -= BLOCK_SIZE;
//   }
//   if (rem_size > 0) {
//     mem
//     memcpy(buf_block, buf + write_size, rem_size);
//     bm->write_block(ino->blocks[i], buf_block);
//   }
  
//   /* modify inode */
//   ino->mtime = time(NULL);
//   ino->atime = time(NULL);
//   ino->ctime = time(NULL);
//   ino->size = size;
//   put_inode(inum, ino);

//   /* free malloc space */
//   free(ino);
//   free(buf_block);
  
//   return;
// }