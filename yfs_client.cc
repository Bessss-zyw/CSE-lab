// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client(extent_dst);
    // Lab2: Use lock_client_cache when you test lock_cache
    // lc = new lock_client(lock_dst);
    lc = new lock_client_cache(lock_dst);

    lc->acquire(1);
    if (ec->put(1, "") != extent_protocol::OK)
        printf("error init root dir\n"); // XYB: init root dir
    lc->release(1);
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

std::string
yfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

bool
yfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK || a.atime == 0) {
        printf("yc::isfile error getting attr\n");
        lc->release(inum);
        return false;
    }
    lc->release(inum);

    if (a.type == extent_protocol::T_FILE) {
        printf("yc::isfile: %lld is a file\n", inum);
        return true;
    } 

    printf("yc::isfile: %lld is not a file\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */

bool
yfs_client::isdir(inum inum)
{
    extent_protocol::attr a;

    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK || a.atime == 0) {
        printf("yc::isdir error getting attr\n");
        lc->release(inum);
        return false;
    }
    lc->release(inum);

    if (a.type == extent_protocol::T_DIR) {
        printf("yc::isdir: %lld is a dir\n", inum);
        return true;
    } 
    printf("yc::isdir: %lld is not a dir\n", inum);
    return false;
}


bool
yfs_client::issymlink(inum inum)
{
    extent_protocol::attr a;

    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK || a.atime == 0) {
        printf("yc::issymlink error getting attr\n");
        lc->release(inum);
        return false;
    }
    lc->release(inum);

    if (a.type == extent_protocol::T_SYMLINK) {
        printf("yc::issymlink: %lld is a link\n", inum);
        return true;
    } 
    printf("yc::issymlink: %lld is not a link\n", inum);
    return false;
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;

    printf("yc::getfile %016llx\n", inum);
    extent_protocol::attr a;
    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK || a.atime == 0) {
        r = IOERR;
        goto release;
    }

    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    printf("yc::getfile %016llx -> size %llu\n", inum, fin.size);

release:
    lc->release(inum);
    return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;

    printf("yc::getdir %016llx\n", inum);
    extent_protocol::attr a;
    lc->acquire(inum);
    if (ec->getattr(inum, a) != extent_protocol::OK || a.atime == 0) {
        r = IOERR;
        goto release;
    }
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;
    printf("yc::getdir %016llx done\n", inum);

release:
    lc->release(inum);
    return r;
}


#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

// Only support set size of attr
int
yfs_client::setattr(inum ino, size_t size)
{
    int r = OK;
    std::string content;

    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    printf("yc::setattr %016llx to size %lu\n", ino, size);

    lc->acquire(ino);
    if (ec->get(ino, content) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    content.resize(size);

    if (ec->put(ino, content) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    
release:
    lc->release(ino);
    return r;
}

int
yfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    bool found = false;
    std::string content; 
    struct dirent_c entry;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    printf("yc::create parent %llu\n", parent);

    /* if there is a dup name file in the parent dir */
    lc->acquire(parent);
    if(__lookup(parent, name, found, ino_out) == OK && found == true) {
        printf("yc::create dup name!\n");
        r = IOERR;
        goto release;
    }

    /* create new file */
    lc->acquire(0);
    if (ec->create(extent_protocol::T_FILE, ino_out) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    /* write new entry */
    if (ec->get(parent, content) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
        
    entry.inum = ino_out;
    entry.length = strlen(name);
    memcpy(entry.name, name, entry.length);    
     
    // printf("yc::create parent %llu before content = %lu\n", parent, content.size());
    content.append((char *)&entry, sizeof(dirent_c));
    if (ec->put(parent, content) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

release:
    lc->release(0);
    lc->release(parent);
    return r;
}

int
yfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    bool found = false;
    std::string content; 
    struct dirent_c entry;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    printf("yc::mkdir parent %llu\n", parent);

    /* if there is a dup name file in the parent dir */
    lc->acquire(parent);
    if(__lookup(parent, name, found, ino_out) == OK && found == true) {
        printf("yc::mkdir dup name!\n");
        r = IOERR;
        goto release;
    }

    /* create new dir */
    lc->acquire(0);
    if (ec->create(extent_protocol::T_DIR, ino_out) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    /* write new entry */
    if (ec->get(parent, content) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
        
    entry.inum = ino_out;
    entry.length = strlen(name);
    memcpy(entry.name, name, entry.length);    
    content.append((char *)&entry, sizeof(dirent_c));
    if (ec->put(parent, content) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

release:
    lc->release(0);
    lc->release(parent);
    return r;
}

int
yfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    lc->acquire(parent);
    r = __lookup(parent, name, found, ino_out);
    lc->release(parent);
    return r;
}

int
yfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;
    lc->acquire(dir);
    r = __readdir(dir, list);
    lc->release(dir);   
    return r;
}

int
yfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    std::string content;
    size_t size_origin;

    /*
     * your code goes here.
     * note: read using ec->get().
     */
    printf("yc::read %016llx, size %lu, off = %ld\n", ino, size, off);

    lc->acquire(ino);
    if (ec->get(ino, content) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    size_origin = content.size();
    printf("yc::read file_size %lu\n", size_origin);    
    
    if ((uint32_t)off >= size_origin) {
        printf("yc::read error! offset out of file!\n");
        r = IOERR;
        goto release;
    }
    size = size > (size_origin - off)? (size_origin - off):size;
    data.assign(content, off, size);

release:
    lc->release(ino);
    return r;
}

int
yfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;
    std::string content;
    size_t size_origin;

    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    printf("yc::write %016llx, size %lu, off = %ld\n", ino, size, off);
    lc->acquire(ino);
    if (ec->get(ino, content) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    size_origin = content.size();
    if ((uint32_t)off > size_origin){
        content.append((size_t)off - size_origin, '\0');
        bytes_written = (size_t)off - size_origin + size;
    }
    else
        bytes_written = size;
    
    content.replace(off, size, std::string(data, size));
    if (ec->put(ino, content) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    
release:
    lc->release(ino);
    return r;
}

int 
yfs_client::unlink(inum parent,const char *name)
{
    int r = OK;
    bool found;
    inum ino;
    std::list<dirent> entries;
    std::list<dirent>::iterator it;
    std::string buf;

    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    printf("yc::unlink %016llx, name = %s\n", parent, name);

    /* if no such file */
    lc->acquire(parent);
    if (__lookup(parent, name, found, ino) != extent_protocol::OK || !found) {
        printf("yc::unlink no such file!\n");
        r = IOERR;
        goto release;
    }

    /* remove file */
    lc->acquire(0);
    if (ec->remove(ino) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    /* remove entry in parent dir */
    if (__readdir(parent, entries) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    for (it = entries.begin(); it != entries.end(); ++it) {
        if (it->inum != ino) {
            dirent_c entry;
            entry.inum = it->inum;
            entry.length = it->name.size();
            memcpy(entry.name, it->name.data(), entry.length); 
            buf.append((char *)(& entry), sizeof(dirent_c));  
        }
    }

    if (ec->put(parent, buf) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

release:
    lc->release(0);
    lc->release(parent);
    return r;
}


int 
yfs_client::readlink(inum ino, std::string &buf){
    int r = OK;
    printf("yc::readlink ino %llu\n", ino);

    lc->acquire(ino);
    if(ec->get(ino, buf) != extent_protocol::OK){
        r = IOERR;
        goto release;
    }
release:
    lc->release(ino);
    return r;
}


int 
yfs_client::symlink(inum parent, const char *name, const char *link, inum &ino_out){
    int r = OK;
    bool found = false;
    std::string content; 
    struct dirent_c entry;
    inum id;

    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    printf("yc::symlink parent %llu, name = %s, link = %s\n", parent, name, link);

    /* if there is a dup name file in the parent dir */
    lc->acquire(parent);
    if(__lookup(parent, name, found, id) == OK && found) {
        r = EXIST;
        goto release;
    }

    /* create new symlink */
    lc->acquire(0);
    if (ec->create(extent_protocol::T_SYMLINK, ino_out) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    /* write link */
    if(ec->put(ino_out, std::string(link)) != extent_protocol::OK){
        r = IOERR;
        goto release;
    }

    /* write new entry */
    if (ec->get(parent, content) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    entry.inum = ino_out;
    entry.length = strlen(name);
    memcpy(entry.name, name, entry.length);    
     
    // printf("yc::create parent %llu before content = %lu\n", parent, content.size());
    content.append((char *)&entry, sizeof(dirent_c));
    if (ec->put(parent, content) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

release:
    lc->release(0);
    lc->release(parent);
    return r;
}


int
yfs_client::__lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    found = false;
    std::list<dirent> list;
    extent_protocol::attr a;

    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    printf("yc::lookup parent %llu, name = %s\n", parent, name);

    /* if parent is not a dir */
    if (ec->getattr(parent, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    assert(a.type == extent_protocol::T_DIR);
    
    /* get dir entry list */
    __readdir(parent, list);

    /* read and scan dirent table */
    while (list.size() > 0) {
        struct dirent entry = list.front();
        list.pop_front();

        if(!entry.name.compare(name)) {
            found = true;
            ino_out = entry.inum;
            return r;
        }
    }
    
release:
    return r;
}

int
yfs_client::__readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;
    std::string content;
    extent_protocol::attr a;
    const char *ptr;
    int count, size_d;

    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    printf("yc::readdir dir %llu\n", dir);

    /* if parent is not a dir */
    if (ec->getattr(dir, a) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    assert(a.type == extent_protocol::T_DIR);
    
    /* get dir content */
    if (ec->get(dir, content) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }

    /* transfer content into dirent list */
    size_d = sizeof(dirent_c);
    count = content.size()/size_d;
    ptr = content.c_str();
    
    for (int i = 0; i < count; i++) {
        struct dirent_c entry_c;
        struct dirent entry;
        
        memcpy(&entry_c, ptr + i * size_d, size_d);
        entry.inum = entry_c.inum;
        entry.name.assign(entry_c.name, entry_c.length);

        list.push_back(entry);
    }
    

release:
    return r;
}
