// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"

#define DEBUG 0

extent_server::extent_server() 
{
  im = new inode_manager();
}

extent_server::~extent_server() 
{
  for(std::map<extent_protocol::extentid_t, extent *>::iterator it = extents.begin(); it != extents.end(); it++)
		delete it->second;
}

int extent_server::create(std::string id, uint32_t type, extent_protocol::extentid_t &eid)
{
  eid = im->alloc_inode(type);
  if (DEBUG) printf("\textent_server: create inode %lld\n", eid);

  // if id is not in cached set, add it 
  addextent(eid, id);

  return extent_protocol::OK;
}

int extent_server::put(std::string id, extent_protocol::extentid_t eid, std::string buf, int &)
{
  if (DEBUG) printf("\textent_server: put %lld\n", eid);
  eid &= 0x7fffffff;
  
  const char * cbuf = buf.c_str();
  int size = buf.size();
  im->write_file(eid, cbuf, size);

  // notify the cached clients except id
  notify(eid, id);

  // if id is not in cached set, add it 
  addextent(eid, id);
  
  return extent_protocol::OK;
}

int extent_server::get(std::string id, extent_protocol::extentid_t eid, std::string &buf)
{
  if (DEBUG) printf("\textent_server: get %lld\n", eid);

  eid &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  im->read_file(eid, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  // if id is not in cached set, add it 
  addextent(eid, id);

  return extent_protocol::OK;
}

int extent_server::getattr(std::string id, extent_protocol::extentid_t eid, extent_protocol::attr &a)
{
  if (DEBUG) printf("\textent_server: getattr %lld\n", eid);

  eid &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  im->getattr(eid, attr);
  a = attr;

  // if id is not in cached set, add it 
  addextent(eid, id);

  return extent_protocol::OK;
}

int extent_server::remove(std::string id, extent_protocol::extentid_t eid, int &)
{
  if (DEBUG) printf("\textent_server: remove %lld\n", eid);

  eid &= 0x7fffffff;
  im->remove_file(eid);

  // notify the cached clients except id
  notify(eid, id);
  extent_t * info = extents[eid];
  delete info;
  extents[eid] = NULL;
 
  return extent_protocol::OK;
}


void extent_server::addextent(extent_protocol::extentid_t eid, std::string id) {
  if (DEBUG) printf("\textent_server: addextent id %s to extent %lld\n", id.data(), eid);
  // if (extents.find(eid) == extents.end()) {
  if (extents[eid] == NULL) {
    extents[eid] = new extent();
  }
  extent_t * info = extents[eid];
  info->cached_cids.insert(id);
  if (DEBUG) printf("\textent_server: addextent finished\n");
}

void extent_server::notify(extent_protocol::extentid_t eid, std::string except) {
  if (DEBUG) printf("\textent_server: notify %lld except %s\n", eid, except.data());
  
  int r;
  extent_t * info = extents[eid];
  if (info == NULL) return;
  for (std::set<std::string>::iterator it=info->cached_cids.begin(); it!=info->cached_cids.end(); ++it){
    if (*it == except) continue;
    handle(*it).safebind()->call(extent_protocol::pull, eid, r);
  }
}
