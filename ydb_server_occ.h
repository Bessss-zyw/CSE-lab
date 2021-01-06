#ifndef ydb_server_occ_h
#define ydb_server_occ_h

#include <string>
#include <map>
#include <set>
#include "extent_client.h"
#include "lock_protocol.h"
#include "lock_client.h"
#include "lock_client_cache.h"
#include "ydb_protocol.h"
#include "ydb_server.h"


class ydb_server_occ: public ydb_server {
private:
	struct record {
		extent_protocol::extentid_t eid;
		char *value;
		size_t size;
		struct record *next;
	};

	ydb_protocol::transaction_id current_transaction;
	std::map <ydb_protocol::transaction_id, struct record*> trans_read;
	std::map <ydb_protocol::transaction_id, struct record*> trans_write;

	void * check_record(struct record* head, extent_protocol::extentid_t eid);
	void * insert_record(struct record* head, extent_protocol::extentid_t eid, std::string value);
	void modify_record(struct record* item, std::string value);
	void free_list(struct record* head);

public:
	ydb_server_occ(std::string, std::string);
	~ydb_server_occ();
	ydb_protocol::status transaction_begin(int, ydb_protocol::transaction_id &);
	ydb_protocol::status transaction_commit(ydb_protocol::transaction_id, int &);
	ydb_protocol::status transaction_abort(ydb_protocol::transaction_id, int &);
	ydb_protocol::status get(ydb_protocol::transaction_id, const std::string, std::string &);
	ydb_protocol::status set(ydb_protocol::transaction_id, const std::string, const std::string, int &);
	ydb_protocol::status del(ydb_protocol::transaction_id, const std::string, int &);
};

#endif

