// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include <set>
#include "extent_protocol.h"
#include "inode_manager.h"

class extent_server {
 protected:
// #if 1
  typedef struct extent {
    std::set<std::string> cached_cids;
    extent() {};
  } extent_t;
  std::map <extent_protocol::extentid_t, extent_t *> extents;
// #endif
  inode_manager *im;

 public:
  extent_server();
  ~extent_server();

  int create(std::string id, uint32_t type, extent_protocol::extentid_t &eid);
  int put(std::string id, extent_protocol::extentid_t eid, std::string, int &);
  int get(std::string id, extent_protocol::extentid_t eid, std::string &);
  int getattr(std::string id, extent_protocol::extentid_t eid, extent_protocol::attr &);
  int remove(std::string id, extent_protocol::extentid_t eid, int &);

  void addextent(extent_protocol::extentid_t eid, std::string id);
  void notify(extent_protocol::extentid_t eid, std::string except);
};

#endif 







