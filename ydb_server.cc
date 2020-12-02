#include "ydb_server.h"
#include "extent_client.h"

#define DEBUG 1

static long timestamp(void) {
	struct timeval tv;
	gettimeofday(&tv, NULL);
	return (tv.tv_sec*1000 + tv.tv_usec/1000);
}

static long hash_str(std::string str) {
	unsigned long __h = 0;
	for (size_t i = 0 ; i < str.size() ; i ++)
		__h = 5 * __h + str[i];
	__h %= 1024;
	return __h;
}

ydb_server::ydb_server(std::string extent_dst, std::string lock_dst) {
	ec = new extent_client(extent_dst);
	lc = new lock_client(lock_dst);
	//lc = new lock_client_cache(lock_dst);

	long starttime = timestamp();
	
	for(int i = 2; i < 1024; i++) {    // for simplicity, just pre alloc all the needed inodes
		extent_protocol::extentid_t id;
		ec->create(extent_protocol::T_FILE, id);
	}
	
	long endtime = timestamp();
	printf("time %ld ms\n", endtime-starttime);
}

ydb_server::~ydb_server() {
	delete lc;
	delete ec;
}


ydb_protocol::status ydb_server::transaction_begin(int, ydb_protocol::transaction_id &out_id) {    // the first arg is not used, it is just a hack to the rpc lib
	// no imply, just return OK
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::transaction_commit(ydb_protocol::transaction_id id, int &) {
	// no imply, just return OK
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::transaction_abort(ydb_protocol::transaction_id id, int &) {
	// no imply, just return OK
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::get(ydb_protocol::transaction_id id, const std::string key, std::string &out_value) {
	// lab3: your code here
	extent_protocol::extentid_t eid = hash_str(key);
	if (DEBUG) printf("ydb_server::get--eid = %llu, key = %s\n", eid, key.data());
	if (ec->get(eid, out_value) != extent_protocol::OK){
		return ydb_protocol::RPCERR;
	}
	if (DEBUG) printf("ydb_server::get--out_value = %s\n", out_value.data());
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::set(ydb_protocol::transaction_id id, const std::string key, const std::string value, int &) {
	// lab3: your code here

	extent_protocol::extentid_t eid = hash_str(key);
	if (DEBUG) printf("ydb_server::set--eid = %llu, key = %s, value = %s\n", eid, key.data(), value.data());
	if (ec->put(eid, value) != extent_protocol::OK){
		return ydb_protocol::RPCERR;
	}
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server::del(ydb_protocol::transaction_id id, const std::string key, int &) {
	// lab3: your code here

	extent_protocol::extentid_t eid = hash_str(key);
	if (DEBUG) printf("ydb_server::del--eid = %llu, key = %s\n", eid, key.data());
	if (ec->remove(eid) != extent_protocol::OK){
		return ydb_protocol::RPCERR;
	}
	return ydb_protocol::OK;
}

