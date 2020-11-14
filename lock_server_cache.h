#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>
#include <map>
#include <list>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"
#include <set>


class lock_server_cache {
 private:
  enum state {
    NONE, 
    LOCKED,
    REVOKING,
    NOTIFYING
  };
  struct lock_info {
    state stat; 
    std::string holder; 
    std::string candidate; 
    std::list<std::string> waiting_cids; 

    lock_info() {
      stat = NONE;
    }
  };
  
  int nacquire;
  std::map<lock_protocol::lockid_t, lock_info*> lockmap;
  pthread_mutex_t mutex;
  
 public:
  lock_server_cache();
  ~lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
