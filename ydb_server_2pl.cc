#include "ydb_server_2pl.h"
#include "extent_client.h"

#define DEBUG 0
#define DEBUG_LOCK 1
#define GLOBAL_LOCK 2000
#define UNWRITEN -1
#define NO_HOLDER 0
#define NO_REQ_LOCK 0

static long hash_str(std::string str) {
	unsigned long __h = 0;
	for (size_t i = 0 ; i < str.size() ; i ++)
		__h = 5 * __h + str[i];
	__h %= 1024;
	return __h;
}


ydb_server_2pl::ydb_server_2pl(std::string extent_dst, std::string lock_dst) : ydb_server(extent_dst, lock_dst) {
	current_transaction = 1;
}

ydb_server_2pl::~ydb_server_2pl() {
	delete lc;
	delete ec;
}


void* ydb_server_2pl::check_lock(struct trans_lock* head, extent_protocol::extentid_t eid) {
	struct trans_lock* temp = head;
	while (temp != NULL){
		if (temp->eid == eid) return temp;
		temp = temp->next;
	}
	return NULL;
}

bool ydb_server_2pl::check_dead_lock(ydb_protocol::transaction_id id, extent_protocol::extentid_t eid){
	ydb_protocol::transaction_id holder;
	extent_protocol::extentid_t waiting_lock = eid;
	// lc->acquire(GLOBAL_LOCK);
	while (1) {
		holder = lock_holder[waiting_lock];
		if (holder == NO_HOLDER) goto alive;
		else if (holder == id) goto dead;
		waiting_lock = trans_req_lock[holder];
		if (waiting_lock == NO_REQ_LOCK) goto alive;
	}
alive:
	// lc->release(GLOBAL_LOCK);
	return false;

dead:	
	// lc->release(GLOBAL_LOCK);
	if (DEBUG_LOCK) printf("dead lock detected while trans %d getting %llu...\n", id, eid);
	return true;
}


void ydb_server_2pl::free_lock(struct trans_lock* head) {
	struct trans_lock* temp;
	while (head != NULL){
		temp = head;
		head = head->next;
		lock_holder[temp->eid] = NO_HOLDER;
		lc->release(temp->eid);
		free(temp);
	}
}


 // the first arg is not used, it is just a hack to the rpc lib
ydb_protocol::status ydb_server_2pl::transaction_begin(int, ydb_protocol::transaction_id &out_id) {   
	// lab3: your code here
	if (DEBUG) printf("transaction_begin");
	lc->acquire(GLOBAL_LOCK);
	out_id = current_transaction++;
	if (DEBUG) printf(" %d\n", out_id);
	lc->release(GLOBAL_LOCK);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::transaction_commit(ydb_protocol::transaction_id id, int &) {
	// lab3: your code here
	if (DEBUG) printf("transaction_commit: id = %d\n", id);

	// check invalid transID
	if (id < 0) return ydb_protocol::TRANSIDINV;

	// free lock
	struct trans_lock* head = trans_record[id];
	free_lock(head);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::transaction_abort(ydb_protocol::transaction_id id, int &) {
	// lab3: your code here
	if (DEBUG) printf("transaction_abort: id = %d\n", id);

	// check invalid transID
	if (id < 0) return ydb_protocol::TRANSIDINV;

	// recover writen key-value pairs
	struct trans_lock* lock = trans_record[id];
	while (lock != NULL){
		if (lock->writen != UNWRITEN){
			std::string s(lock->oldContent, lock->writen);
			if (ec->put(lock->eid, s) != extent_protocol::OK){
				return ydb_protocol::RPCERR;
			}
		}
		lock = lock->next;
	}

	// free locks
	struct trans_lock* head = trans_record[id];
	free_lock(head);
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::get(ydb_protocol::transaction_id id, const std::string key, std::string &out_value) {
	// lab3: your code here
	if (DEBUG) printf("get: id = %d, key = %s\n", id, key.data());
	
	// check invalid transID
	if (id < 0) return ydb_protocol::TRANSIDINV;

	// get hash value
	extent_protocol::extentid_t eid = hash_str(key);

	// if haven't get the lock yet 
	if (!check_lock(trans_record[id], eid)) {	
		// get lock
		if (check_dead_lock(id, eid)) {
			int a;
			transaction_abort(id, a);
			return ydb_protocol::ABORT;
		}
		// lc->acquire(GLOBAL_LOCK);
		trans_req_lock[id] = eid;
		// lc->release(GLOBAL_LOCK);
		if (DEBUG_LOCK) printf("trans %d gettting %llu lock\n", id, eid);
		lc->acquire(eid);
		// lc->acquire(GLOBAL_LOCK);
		trans_req_lock[id] = NO_REQ_LOCK;
		lock_holder[eid] = id;
		// lc->release(GLOBAL_LOCK);


		// add lock to trans_record of this eid
		struct trans_lock *new_lock = (struct trans_lock *)malloc(sizeof(*new_lock));
		new_lock->eid = eid;
		new_lock->next = trans_record[id];
		new_lock->writen = UNWRITEN;
		trans_record[id] = new_lock;
	}

	if (ec->get(eid, out_value) != extent_protocol::OK){
		return ydb_protocol::RPCERR;
	}
	// if (DEBUG) printf("     out_value = %s\n", out_value.data());
	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::set(ydb_protocol::transaction_id id, const std::string key, const std::string value, int &) {
	// lab3: your code here
	if (DEBUG) printf("set: id = %d, key = %s, value = %s\n", id, key.data(), value.data());

	// check invalid transID
	if (id < 0) return ydb_protocol::TRANSIDINV;

	// get hash value
	extent_protocol::extentid_t eid = hash_str(key);
	struct trans_lock* lock = (struct trans_lock*)check_lock(trans_record[id], eid);

	// if haven't get the lock yet
	if (lock == NULL) {		
		// get lock
		if (check_dead_lock(id, eid)) {
			int a;
			transaction_abort(id, a);
			return ydb_protocol::ABORT;
		}
		// lc->acquire(GLOBAL_LOCK);
		trans_req_lock[id] = eid;
		// lc->release(GLOBAL_LOCK);
		if (DEBUG_LOCK) printf("trans %d gettting %llu lock\n", id, eid);
		lc->acquire(eid);
		// lc->acquire(GLOBAL_LOCK);
		trans_req_lock[id] = NO_REQ_LOCK;
		lock_holder[eid] = id;
		// lc->release(GLOBAL_LOCK);

		// add lock to trans_record of this eid
		struct trans_lock *new_lock = (struct trans_lock *)malloc(sizeof(*new_lock));
		new_lock->eid = eid;
		new_lock->next = trans_record[id];

		// get old value
		std::string oldContent;
		if (ec->get(eid, oldContent) != extent_protocol::OK){
			free(new_lock);
			lock_holder[eid] = NO_HOLDER;
			lc->release(eid);
			return ydb_protocol::RPCERR;
		}
		new_lock->writen = oldContent.size();
		new_lock->oldContent = (char *)malloc(oldContent.size());
		memcpy(new_lock->oldContent, oldContent.c_str(), oldContent.size());
		// if (DEBUG) printf("set: get old value = %s\n", oldContent.data());

		trans_record[id] = new_lock;
	}
	else if (lock->writen == UNWRITEN){	// if have lock but haven't write before
		// get old value
		std::string oldContent;
		if (ec->get(eid, oldContent) != extent_protocol::OK){
			return ydb_protocol::RPCERR;
		}
		lock->writen = oldContent.size();
		lock->oldContent = (char *)malloc(oldContent.size());
		memcpy(lock->oldContent, oldContent.data(), oldContent.size());
	}

	if (ec->put(eid, value) != extent_protocol::OK){
		return ydb_protocol::RPCERR;
	}

	return ydb_protocol::OK;
}

ydb_protocol::status ydb_server_2pl::del(ydb_protocol::transaction_id id, const std::string key, int &) {
	// lab3: your code here
	if (DEBUG) printf("del: id = %d, key = %s\n", id, key.data());

	// check invalid transID
	if (id < 0) return ydb_protocol::TRANSIDINV;

	// get hash value
	extent_protocol::extentid_t eid = hash_str(key);
	struct trans_lock* lock = (struct trans_lock*)check_lock(trans_record[id], eid);

	// if haven't get the lock yet
	if (lock == NULL) {		
		// get lock
		if (check_dead_lock(id, eid)) {
			int a;
			transaction_abort(id, a);
			return ydb_protocol::ABORT;
		}
		// lc->acquire(GLOBAL_LOCK);
		trans_req_lock[id] = eid;
		// lc->release(GLOBAL_LOCK);
		if (DEBUG_LOCK) printf("trans %d gettting %llu lock\n", id, eid);
		lc->acquire(eid);
		// lc->acquire(GLOBAL_LOCK);
		trans_req_lock[id] = NO_REQ_LOCK;
		lock_holder[eid] = id;
		// lc->release(GLOBAL_LOCK);

		// add lock to trans_record of this eid
		struct trans_lock *new_lock = (struct trans_lock *)malloc(sizeof(*new_lock));
		new_lock->eid = eid;
		new_lock->next = trans_record[id];

		// get old value
		std::string oldContent;
		if (ec->get(eid, oldContent) != extent_protocol::OK){
			free(new_lock);
			lock_holder[eid] = NO_HOLDER;
			lc->release(eid);
			return ydb_protocol::RPCERR;
		}
		new_lock->writen = oldContent.size();
		new_lock->oldContent = (char *)malloc(oldContent.size());
		memcpy(new_lock->oldContent, oldContent.data(), oldContent.size());
		// if (DEBUG) printf("del: get old value = %s\n", oldContent.data());
		
		trans_record[id] = new_lock;
	}
	else if (!lock->writen){	// if have lock but haven't write before
		// get old value
		std::string oldContent;
		if (ec->get(eid, oldContent) != extent_protocol::OK){
			return ydb_protocol::RPCERR;
		}
		lock->writen = oldContent.size();
		lock->oldContent = (char *)malloc(oldContent.size());
		memcpy(lock->oldContent, oldContent.data(), oldContent.size());
	}

	if (ec->put(eid, std::string("")) != extent_protocol::OK){
		return ydb_protocol::RPCERR;
	}

	return ydb_protocol::OK;
}

