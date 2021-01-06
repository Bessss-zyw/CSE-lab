// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "extent_server.h"

class extent_client {
 private:
  struct extent_file {
    bool need_get;
    bool need_getattr;
    std::string buf;
    extent_protocol::attr attr;
    // std::list<thread_cond *> tids;

    extent_file() {
      need_get = true;
      need_getattr = true;
      attr.size = 0;
      attr.type = 0;
      attr.atime = time(NULL);
      attr.mtime = time(NULL);
      attr.ctime = time(NULL);
      buf.clear();
    }
  };

  rpcc *cl;
  int rlock_port;
  std::string hostname;
  std::string id;
  std::map<extent_protocol::extentid_t, struct extent_file*> cachemap; 

 public:

  static int last_port;

  extent_client(std::string dst);
  ~extent_client();

  extent_protocol::status create(uint32_t type, extent_protocol::extentid_t &eid);
  extent_protocol::status get(extent_protocol::extentid_t eid, 
			                        std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				                          extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);

  void * get_extent_info(extent_protocol::extentid_t eid);
  extent_protocol::status pull_handler(extent_protocol::extentid_t eid, int &);
};

#endif