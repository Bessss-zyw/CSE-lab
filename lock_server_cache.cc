// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

lock_server_cache::lock_server_cache()
{
  VERIFY(pthread_mutex_init(&mutex, NULL) == 0);
}

int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, int &r)
{
  // tprintf("lock_server_cache acquire lid = %llu begin\n", lid);
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab2 part3 code goes here
  pthread_mutex_lock(&mutex);

  // no such lock yet, create one
  if (lockmap.find(lid) == lockmap.end()) {
    // tprintf("lock_server_cache acquire add new lock\n");
    lock_info * lock_new = new lock_info();
    lockmap[lid] = lock_new;
  }

  // get lock_info of this lock
  lock_info *lock = lockmap[lid];
  // tprintf("lock_server_cache acquire lid = %llu lock->stat = %d\n", lid, lock->stat);

  // operate according to state of this lock
  switch (lock->stat) {
  case NONE:  // give the lock to the client, return OK
    assert(lock->waiting_cids.empty());

    lock->stat = LOCKED;
    lock->holder = id;
    pthread_mutex_unlock(&mutex);
    break;

  case LOCKED:  // request lock holder, append waiting list, return RETRY
    assert(lock->waiting_cids.empty());
    assert(lock->holder.length());
    // assert(lock->holder != id);

    lock->stat = REVOKING;
    lock->waiting_cids.push_back(id);
    pthread_mutex_unlock(&mutex);
    handle(lock->holder).safebind()->call(rlock_protocol::revoke, lid, r);
    ret = lock_protocol::RETRY;
    break;

  case REVOKING:  // append waiting list, return RETRY
    // assert(!lock->waiting_cids.empty());
    assert(lock->holder.length());
    assert(lock->holder != id);

    lock->waiting_cids.push_back(id);
    pthread_mutex_unlock(&mutex);
    ret = lock_protocol::RETRY;
    break;

  case NOTIFYING: // append waiting list, return RETRY
    assert(!lock->holder.length());

    if (id == lock->candidate) {  // if candidate
      lock->candidate.clear();
      lock->stat = LOCKED;
      lock->holder = id;
      if (!lock->waiting_cids.empty()) {
        lock->stat = REVOKING;
        pthread_mutex_unlock(&mutex);
        handle(id).safebind()->call(rlock_protocol::revoke, lid, r);
      }
      else 
        pthread_mutex_unlock(&mutex);
      return lock_protocol::OK;
    }
    else {  // if not candidate
      lock->waiting_cids.push_back(id);
      pthread_mutex_unlock(&mutex);
      ret = lock_protocol::RETRY;
    }
    break;

  default:
    assert(0);
  }

  // tprintf("lock_server_cache acquire lid = %llu done!\n", lid);
  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, int &r)
{
  // tprintf("lock_server_cache release lid = %llu\n", lid);
  lock_protocol::status ret = lock_protocol::OK;
  // Your lab2 part3 code goes here
  pthread_mutex_lock(&mutex);

  // get lock_info of the lock
  lock_info *lock = lockmap[lid];
  lock->holder.clear();

  if (lock->waiting_cids.empty()) { // if waiting_list is empty, NONE
    lock->stat = NONE;
    pthread_mutex_unlock(&mutex);
    // tprintf("lock_server_cache release lid = %llu done!\n", lid);
    return ret;
  }
  else {  // if waiting_list is not empty, NOTIFYING
    std::string next = lock->waiting_cids.front();
    lock->waiting_cids.pop_front();
    lock->candidate = next;
    lock->stat = NOTIFYING;
    pthread_mutex_unlock(&mutex);
    handle(next).safebind()->call(rlock_protocol::retry, lid, r);
    // tprintf("lock_server_cache release lid = %llu done!\n", lid);
    return ret;
  }                             
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  // tprintf("stat request\n");
  r = nacquire;
  return lock_protocol::OK;
}

lock_server_cache::~lock_server_cache()
{
  pthread_mutex_lock(&mutex);
  for (std::map<lock_protocol::lockid_t, lock_info *>::iterator iter = lockmap.begin(); 
        iter != lockmap.end(); iter++)
    delete iter->second;
  pthread_mutex_unlock(&mutex);
}