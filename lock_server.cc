// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
  pthread_mutex_init(&mutex, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here

  printf("acquire request from clt %d\n", clt);
  
  pthread_mutex_lock(&mutex);
  if (locks.find(lid) == locks.end()) { // no such lock yet, build a new lock
    printf("no such lock %llu\n", lid);
    pthread_cond_t cond;
    pthread_cond_init(&cond, NULL);
    cons[lid] = cond;
  }
  else if (locks[lid]){ // this lock is locked
    printf("lock %llu locked\n", lid);
    while (locks[lid])
      pthread_cond_wait(&cons[lid], &mutex);
  }
  locks[lid] = true;
  pthread_mutex_unlock(&mutex);

  r = ret;
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2 part2 code goes here

  printf("release request from clt %d\n", clt);
  pthread_mutex_lock(&mutex);
  locks[lid] = false;
  pthread_cond_signal(&cons[lid]);
  pthread_mutex_unlock(&mutex);
  
  r = ret;
  return ret;
}