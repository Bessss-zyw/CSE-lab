#include "ydb_server_occ.h"
#include "extent_client.h"

#define DEBUG 0
#define TRANSID_LOCK 2000
#define GLOBAL_LOCK 3000

static long hash_str(std::string str) {
	unsigned long __h = 0;
	for (size_t i = 0 ; i < str.size() ; i ++)
		__h = 5 * __h + str[i];
	__h %= 1024;
	return __h;
}

void * ydb_server_occ::check_record(struct record* head, extent_protocol::extentid_t eid) {
	struct record* temp = head;
	while (temp != NULL){
		if (temp->eid == eid) return temp;
		temp = temp->next;
	}
	return NULL;
}

void * ydb_server_occ::insert_record(struct record* head, extent_protocol::extentid_t eid, std::string value) {
	char *record_value = (char *)malloc(value.size());
	memcpy(record_value, value.data(), value.size());

	struct record *new_record = (struct record *)malloc(sizeof(*new_record));
	new_record->eid = eid;
	new_record->value = record_value;
	new_record->size = value.size();
	new_record->next = head;

	return new_record;
}

void ydb_server_occ::modify_record(struct record* item, std::string value) {
	if (item->value != NULL) free(item->value);

	item->value = (char *)malloc(value.size());
	memcpy(item->value, value.data(), value.size());
	item->size = value.size();
}

void ydb_server_occ::free_list(struct record* head) {
	struct record* temp;
	while (head != NULL){
		temp = head;
		head = head->next;
		free(temp);
	}
}


ydb_server_occ::ydb_server_occ(std::string extent_dst, std::string lock_dst) : ydb_server(extent_dst, lock_dst) {
	current_transaction = 1;
}

ydb_server_occ::~ydb_server_occ() {
	delete lc;
	delete ec;
}


ydb_protocol::status ydb_server_occ::transaction_begin(int, ydb_protocol::transaction_id &out_id) {    // the first arg is not used, it is just a hack to the rpc lib
	// lab3: your code here
	
	lc->acquire(TRANSID_LOCK);
	out_id = current_transaction++;
	if (DEBUG) printf("transaction_begin %d\n", out_id);
	lc->release(TRANSID_LOCK);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::transaction_commit(ydb_protocol::transaction_id id, int &) {
	// lab3: your code here
	lc->acquire(GLOBAL_LOCK);
	if (DEBUG) printf("transaction_commit %d\n", id);

	// check read set
	struct record * read_record = trans_read[id];
	std::string out_value(""), trans_value("");
	while (read_record != NULL) {
		trans_value.assign(read_record->value, read_record->size);
		if (ec->get(read_record->eid, out_value) != extent_protocol::OK){
			lc->release(GLOBAL_LOCK);  
			return ydb_protocol::RPCERR;
		}
		if (trans_value.compare(out_value)) {	// if not equal, abort
			lc->release(GLOBAL_LOCK);
			int a;
			transaction_abort(id, a);
			if (DEBUG) printf("transaction_commit: id = %d abort\n", id);
			return ydb_protocol::ABORT;
		}
		read_record = read_record->next;
	}
	if (DEBUG) printf("read set checked\n");

	// no read value changed, commit write
	struct record * write_record = trans_write[id];
	std::string write_value("");
	while (write_record != NULL) {
		if (DEBUG) printf("transaction_commit: id = %d, write eid = %llu, value = %s\n", id, write_record->eid, write_record->value);
		write_value.assign(write_record->value, write_record->size);
		if (ec->put(write_record->eid, write_value) != extent_protocol::OK){
			lc->release(GLOBAL_LOCK);  
			return ydb_protocol::RPCERR;
		}
		write_record = write_record->next;
	}

	lc->release(GLOBAL_LOCK);

	// free record list
	free_list(trans_write[id]);
	free_list(trans_read[id]);

	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::transaction_abort(ydb_protocol::transaction_id id, int &) {
	// lab3: your code here
	if (DEBUG) printf("transaction_abort %d\n", id);

	// free record list
	free_list(trans_write[id]);
	free_list(trans_read[id]);

	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::get(ydb_protocol::transaction_id id, const std::string key, std::string &out_value) {
	// lab3: your code here
	
	if (id < 0) return ydb_protocol::TRANSIDINV;	// check invalid transID

	// get hash value
	extent_protocol::extentid_t eid = hash_str(key);
	if (DEBUG) printf("get: id = %d, key = %s, eid = %llu\n", id, key.data(), eid);

	// check if have been write before
	struct record* write_record = (struct record*)check_record(trans_write[id], eid);
	if (write_record != NULL) {
		if (DEBUG) printf("get: write_record = %s\n", write_record->value);
		out_value.assign(write_record->value, write_record->size);
		return ydb_protocol::OK;
	}

	// check if have been read before
	struct record* read_record = (struct record*)check_record(trans_read[id], eid);
	if (read_record != NULL) {
		if (DEBUG) printf("get: read_record = %s\n", read_record->value);
		out_value.assign(read_record->value, read_record->size);
		return ydb_protocol::OK;
	}

	// if have not been write or read before 
	lc->acquire(GLOBAL_LOCK);
	if (ec->get(eid, out_value) != extent_protocol::OK){
		lc->release(GLOBAL_LOCK);  
		return ydb_protocol::RPCERR;
	}
	lc->release(GLOBAL_LOCK);
	if (DEBUG) printf("get: out_value = %s\n", out_value.data());

	// record read value
	struct record* new_record = (struct record*)insert_record(trans_read[id], eid, out_value);
	trans_read[id] = new_record;
	
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::set(ydb_protocol::transaction_id id, const std::string key, const std::string value, int &) {
	// lab3: your code here

	if (id < 0) return ydb_protocol::TRANSIDINV;	// check invalid transID

	// get hash value
	extent_protocol::extentid_t eid = hash_str(key);
	if (DEBUG) printf("set: id = %d, key = %s, eid = %llu, value = %s\n", id, key.data(), eid, value.data());

	// check if have been write before
	struct record* write_record = (struct record*)check_record(trans_write[id], eid);
	if (write_record != NULL) {
		modify_record(write_record, value);
		return ydb_protocol::OK;
	}

	// if have not been write before 
	struct record* new_record = (struct record*)insert_record(trans_write[id], eid, value);
	trans_write[id] = new_record;

	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_occ::del(ydb_protocol::transaction_id id, const std::string key, int &) {
	// lab3: your code here

	if (id < 0) return ydb_protocol::TRANSIDINV;	// check invalid transID
	
	// get hash value
	extent_protocol::extentid_t eid = hash_str(key);
	std::string value("");
	if (DEBUG) printf("set: id = %d, key = %s, eid = %llu, value = %s\n", id, key.data(), eid, value.data());

	// check if have been write before
	struct record* write_record = (struct record*)check_record(trans_write[id], eid);
	if (write_record != NULL) {
		modify_record(write_record, value);
		return ydb_protocol::OK;
	}

	// if have not been write before 
	struct record* new_record = (struct record*)insert_record(trans_write[id], eid, value);
	trans_write[id] = new_record;

	return ydb_protocol::OK;
}

