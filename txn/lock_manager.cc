// Original Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
// Student 1: Feridun Mert Celebi (feridun.celebi@yale.edu)
// Student 2: Tihomir Elek (tihomir.elek@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"

LockManagerC::LockManagerC(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerC::WriteLock(Txn* txn, const Key& key) {
LockRequest lock_request(SHARED, txn);

  queue_making_mutex.Lock();

  // Add the new lock_request to lock_table_.
  if (!lock_table_.count(key)) {
    deque<LockRequest> *lock_queue = new deque<LockRequest>(1, lock_request);
    lockBucket* bucket = new lockBucket(lock_queue);
    //TODO: change back

    lock_table_[key] = bucket;
    queue_making_mutex.Unlock();

    (lock_table_[key]->key_mutex).Lock();
  }
  else {
    (lock_table_[key]->key_mutex).Lock();
    lock_table_[key]->list_of_locks->push_back(lock_request);
  }

  // Success for the lock acquisition of the transaction.
  if (lock_table_[key]->list_of_locks->front().txn_ == txn) {
    (lock_table_[key]->key_mutex).Unlock();
    return true;
  }
  (lock_table_[key]->key_mutex).Unlock();
  return false;
}

bool LockManagerC::ReadLock(Txn* txn, const Key& key) {
  // printf("trying the ReadLock\n");

  LockRequest lock_request(SHARED, txn);

  // Add the new lock_request to lock_table_.
  if (!lock_table_.count(key)) {
    // printf("trying the ReadLock2\n");
    deque<LockRequest> *new_lock_queue = new deque<LockRequest>(1, lock_request);
    lockBucket* bucket = new lockBucket(new_lock_queue);

    queue_making_mutex.Lock();
    lock_table_[key] = bucket;
    queue_making_mutex.Unlock();
    (lock_table_[key]->key_mutex).Lock();
  }
  else {
    if (lock_table_[key] == NULL){
      // printf("ssssssssssssssssss\n");
    }
    (lock_table_[key]->key_mutex).Lock();
    lock_table_[key]->list_of_locks->push_back(lock_request);

  }

  deque<LockRequest> *lock_queue = lock_table_[key]->list_of_locks;
  deque<LockRequest>::iterator lock = lock_queue->begin();
  for (; lock != lock_queue->end(); lock++) {
    if (lock->mode_ == EXCLUSIVE) {
      (lock_table_[key]->key_mutex).Unlock();
      return false;
    }
  }
  (lock_table_[key]->key_mutex).Unlock();
  return true;
}

void LockManagerC::Release(Txn* txn, const Key& key) {
  if (!lock_table_[key]){
    return;
  }

  deque<LockRequest> *lock_queue = lock_table_[key]->list_of_locks;
  deque<LockRequest>::iterator lock_req = lock_queue->begin();
  for (; lock_req != lock_queue->end(); lock_req++) {
     if (lock_req->txn_ == txn) {
      printf("pusi kurac\n");
      (lock_table_[key]->key_mutex).Lock();

      lock_queue->erase(lock_req);

      (lock_table_[key]->key_mutex).Unlock();
      break;
     }
  }
}

LockMode LockManagerC::Status(const Key& key, vector<Txn*>* owners) {
  return SHARED;
}
