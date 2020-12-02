#ifndef ydb_server_2pl_h
#define ydb_server_2pl_h

#include <string>
#include <map>
#include "extent_client.h"
#include "lock_protocol.h"
#include "lock_client.h"
#include "lock_client_cache.h"
#include "ydb_protocol.h"
#include "ydb_server.h"


class ydb_server_2pl: public ydb_server {
private:
	struct trans_lock {
		extent_protocol::extentid_t eid;
		char *oldContent;
		int writen; // the size of the oldContent
		struct trans_lock *next;
	};

	ydb_protocol::transaction_id current_transaction;
	std::map <ydb_protocol::transaction_id, struct trans_lock*> trans_record;
	
	// for dead lock detection
	std::map <ydb_protocol::transaction_id, extent_protocol::extentid_t> trans_req_lock;
	std::map <extent_protocol::extentid_t, ydb_protocol::transaction_id> lock_holder;

	void* check_lock(struct trans_lock* head, extent_protocol::extentid_t eid);
	bool check_dead_lock(ydb_protocol::transaction_id id, extent_protocol::extentid_t eid);
	void free_lock(struct trans_lock* head);

public:
	ydb_server_2pl(std::string, std::string);
	~ydb_server_2pl();
	ydb_protocol::status transaction_begin(int, ydb_protocol::transaction_id &);
	ydb_protocol::status transaction_commit(ydb_protocol::transaction_id, int &);
	ydb_protocol::status transaction_abort(ydb_protocol::transaction_id, int &);
	ydb_protocol::status get(ydb_protocol::transaction_id, const std::string, std::string &);
	ydb_protocol::status set(ydb_protocol::transaction_id, const std::string, const std::string, int &);
	ydb_protocol::status del(ydb_protocol::transaction_id, const std::string, int &);
};

#endif

