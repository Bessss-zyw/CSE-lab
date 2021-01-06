// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>
#include "rpc.h"

#define DEBUG 0

int extent_client::last_port = 0;

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }

  // extent_client with cache
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10)) + 100;
  const char *hname;
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(extent_protocol::pull, this, &extent_client::pull_handler);
  // rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);
}

extent_client::~extent_client()
{
	for(std::map<extent_protocol::extentid_t, extent_file *>::iterator it = cachemap.begin(); it != cachemap.end(); it ++)
		delete it->second;
}


extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = cl->call(extent_protocol::create, id, type, eid);
  VERIFY (ret == extent_protocol::OK);

  // add it into cache
  struct extent_file *info = (struct extent_file *)get_extent_info(eid);
  info->attr.type = type;
  info->need_get = false;
  info->need_getattr = false;

  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here

  // if data is dirty, get from rpc
  struct extent_file *info = (struct extent_file *)get_extent_info(eid);
  if (info->need_get) {
    ret = cl->call(extent_protocol::get, id, eid, buf);
    VERIFY (ret == extent_protocol::OK);
    info->need_get = false;
    info->buf.assign(buf);
  }
  else
    buf.assign(info->buf);

  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here

  // if data is dirty, get from rpc
  struct extent_file *info = (struct extent_file *)get_extent_info(eid);
  if (info->need_getattr) {
    ret = cl->call(extent_protocol::getattr, id, eid, attr);
    VERIFY (ret == extent_protocol::OK);
    info->need_getattr = false;
    info->attr = attr;
  }
  else
    attr = info->attr;


  
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  int r;
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = cl->call(extent_protocol::put, id, eid, buf, r);
  VERIFY (ret == extent_protocol::OK);

  // add it into cache
  struct extent_file *info = (struct extent_file *)get_extent_info(eid);
  info->need_get = false;
  info->need_getattr = false;
  info->buf.assign(buf);
  info->attr.size = buf.size();
  info->attr.atime = time(NULL);
  info->attr.mtime = time(NULL);
  info->attr.ctime = time(NULL);

  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  int r = 0;
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here
  ret = cl->call(extent_protocol::remove, id, eid, r);
  VERIFY (ret == extent_protocol::OK);

  struct extent_file *info = cachemap[eid];
  if (info != NULL) {
    delete info;
    cachemap[eid] = NULL;
  }

  return ret;
}




void * 
extent_client::get_extent_info(extent_protocol::extentid_t eid) {
  // if no such file in map, create one
  if (cachemap[eid] == NULL) {
    cachemap[eid] = new extent_file();
  }

  // get file from map/from rpc
  struct extent_file *info = cachemap[eid];
  VERIFY(info != NULL);
  return info;
}



extent_protocol::status
extent_client::pull_handler(extent_protocol::extentid_t eid, int &)
{
  extent_protocol::status ret = extent_protocol::OK;
  // Your lab2 part1 code goes here

  struct extent_file *info = (struct extent_file *)get_extent_info(eid);
  // VERIFY (info != NULL);

  info->need_get = true;
  info->need_getattr = true;

  return ret;
}

