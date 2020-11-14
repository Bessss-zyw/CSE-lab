// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


int lock_client_cache::last_port = 0;

lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  // tprintf("lock_client_cache start\n");
  srand(time(NULL)^last_port);
  rlock_port = ((rand()%32000) | (0x1 << 10));
  const char *hname;
  // VERIFY(gethostname(hname, 100) == 0);
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlock_port;
  id = host.str();
  last_port = rlock_port;
  rpcs *rlsrpc = new rpcs(rlock_port);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  pthread_mutex_init(&mutex, NULL);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  // tprintf("lock_client_cache acquire lid = %llu begin\n", lid);
  int ret = lock_protocol::OK;
  // Your lab2 part3 code goes here
  pthread_mutex_lock(&mutex);

  // get lock_info, if no such lock, create one
  if (lockmap.find(lid) == lockmap.end()) 
    lockmap[lid] = new lock_info();
  lock_info *info = lockmap[lid];
  thread_cond *thread_new = new thread_cond();
  info->tids.push_back(thread_new);
  // tprintf("lock_client_cache acquire lid = %llu size = %lu stat = %d\n", lid, info->tids.size(), info->stat);
  
  if (info->tids.size() == 1) { // if only one thread 
    switch (info->stat) {
    case NONE:  // get lock from server
      return __get_rpc_lock(lid, info, thread_new);
    case FREE:  // get lock directly from client
      // tprintf("lock_client_cache acquire lock %llu get!\n", lid);
      info->stat = LOCKED;
      pthread_mutex_unlock(&mutex);
      return ret;
    case RELEASING:    // wait for cv and get lock
      pthread_cond_wait(&thread_new->cond, &mutex);
      return __get_rpc_lock(lid, info, thread_new);
    default:
      assert(0);
    }
  }
  else {  // if more than one thread
    pthread_cond_wait(&thread_new->cond, &mutex);
    switch (info->stat) {
    case FREE: case LOCKED:  // grant lock and return OK
      // tprintf("lock_client_cache acquire lock %llu get!\n", lid);
      info->stat = LOCKED;  
      pthread_mutex_unlock(&mutex);
      return ret;
    case NONE:               // wait for cv and get lock
      return __get_rpc_lock(lid, info, thread_new);
    default: 
      assert(0);
    }
  }
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  // tprintf("lock_client_cache release lid = %llu\n", lid);
  int ret = lock_protocol::OK, r = 0;
  // Your lab2 part3 code goes here
  pthread_mutex_lock(&mutex);

  // get lock_info 
  lock_info *info = lockmap[lid];
  assert(info);

  // release the thread from thread list
  // tprintf("lock_client_cache release lid = %llu size = %lu\n", lid, info->tids.size());
  delete info->tids.front();
  info->tids.pop_front();
  // tprintf("lock_client_cache release lid = %llu thread released size = %lu\n", lid, info->tids.size());

  // set lock state
  if (info->msg == REVOKE) { 
    info->stat = RELEASING;
    pthread_mutex_unlock(&mutex);
    ret = cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&mutex);
    info->stat = NONE;
    info->msg = NOMSG;
  }
  else info->stat = FREE;

  // need to cv the first of the thread list
  if (!info->tids.empty()) {  
    if (info->stat == FREE) info->stat = LOCKED;
    pthread_cond_signal(&info->tids.front()->cond);
  }

  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &r)
{
  // tprintf("lock_client_cache revoke handler lid = %llu\n", lid);
  int ret = rlock_protocol::OK;
  // Your lab2 part3 code goes here
  pthread_mutex_lock(&mutex);

  // get lock_info 
  lock_info *info = lockmap[lid];
  assert(info);

  // check lock state
  if (info->stat == FREE) { // return lock to server 
    info->stat = RELEASING;
    pthread_mutex_unlock(&mutex);
    ret = cl->call(lock_protocol::release, lid, id, r);
    pthread_mutex_lock(&mutex);
    info->stat = NONE;

    // need to cv the first of the thread list
    if (!info->tids.empty())
      pthread_cond_signal(&info->tids.front()->cond);
  }
  else
    info->msg = REVOKE;

  pthread_mutex_unlock(&mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &r)
{
  // tprintf("lock_client_cache retry_handler lid = %llu\n", lid);
  int ret = rlock_protocol::OK;
  // Your lab2 pa rt3 code goes here
  pthread_mutex_lock(&mutex);

  // get lock_info
  lock_info *info = lockmap[lid];
  assert(info);
  
  // cv the first of the thread list
  info->msg = RETRY;
  pthread_cond_signal(&info->tids.front()->cond);

  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_client_cache::__get_rpc_lock (lock_protocol::lockid_t lid,
                                  lock_info *info, thread_cond * thread) 
{
  // tprintf("lock_client_cache __get_rpc_lock lid = %llu\n", lid);
  int ret = lock_protocol::OK, r = 0;

  assert(info->stat != FREE);
  info->stat = ACQUIRING;

  while (info->stat == ACQUIRING) {
    pthread_mutex_unlock(&mutex);
    ret = cl->call(lock_protocol::acquire, lid, id, r);
    pthread_mutex_lock(&mutex);
    
    if (ret == lock_protocol::OK) { // if get lock successfully
      // tprintf("lock_client_cache __get_rpc_lock lock %llu get from server!\n", lid);
      info->stat = LOCKED;
      pthread_mutex_unlock(&mutex);
      return ret;
    }
    else {
      if (info->msg == NOMSG) pthread_cond_wait(&thread->cond, &mutex);
      if (info->msg == RETRY) info->msg = NOMSG;
      else if (info->msg == REVOKE) assert(0);
    }
  }
  
  assert(0);
  return ret;
}

lock_client_cache::~lock_client_cache() {
  // tprintf("lock_client_cache over\n");
  pthread_mutex_lock(&mutex);
  for (std::map<lock_protocol::lockid_t, lock_info *>::iterator iter = lockmap.begin(); iter != lockmap.end(); iter++)
    delete iter->second;
  pthread_mutex_unlock(&mutex);
}