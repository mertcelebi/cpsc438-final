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
  LockRequest request(EXCLUSIVE, txn);

  // Add the write lock to the queue of txns.
  if (lock_table_.count(key)) {
    lock_table_mutexs_[key]->Lock();
    lock_table_[key]->push_back(request);
    lock_table_mutexs_[key]->Unlock();
  }
  // Or create the entry in the table.
  else {
    create_entry_latch.Lock();
    Mutex* entry_mutex = new Mutex();
    lock_table_mutexs_[key] = key_mutex;
    deque<LockRequest> *lock_list = new deque<LockRequest>(1, request);
    lock_table_[key] = lock_list;
    create_entry_latch.Unlock();
  }

  // Update txn_waits_.
  lock_table_mutexs_[key]->Lock();
  if (lock_table_[key]->front().txn_ == txn) {
    lock_table_mutexs_[key]->Unlock();
    return true;
  }
  else {
    if (txn_waits_.count(txn)) {
      txn_waits_[txn] = 1;
    }
    else {
      txn_waits_[txn]++;
    }

    lock_table_mutexs_[key]->Unlock();
    return false;
  }
}

bool LockManagerC::ReadLock(Txn* txn, const Key& key) {
  LockRequest request(SHARED, txn);

  // Add the read lock to the queue of txns.
  if (lock_table_.count(key)) {
    lock_table_mutexs_[key]->Lock();
    lock_table_[key]->push_back(request);
    lock_table_mutexs_[key]->Unlock();
  }
  // Or create the entry in the table.
  else {
    create_entry_latch.Lock();
    Mutex* entry_mutex = new Mutex();
    lock_table_mutexs_[key] = key_mutex;
    deque<LockRequest> *lock_list = new deque<LockRequest>(1, request);
    lock_table_[key] = lock_list;
    create_entry_latch.Unlock();
  }

  // Update txn_waits hash table.
  lock_table_mutexs_[key]->Lock();

  deque<LockRequest> *lock_list = lock_table_[key];
  deque<LockRequest>::iterator lock;
  for (lock = lock_list->begin();; lock != lock_list->end(); lock++) {
    if (lock->mode_ == EXCLUSIVE) {
      if (txn_waits.count(txn)) {
        txn_waits[txn] = 1;
      }
      else {
        txn_waits[txn]++;
      }
    }
  }
  lock_table_mutexs_[key]->Unlock();

  return true;
}

void LockManagerC::Release(Txn* txn, const Key& key) {
  lock_table_mutexs_[key]->Lock();

  deque<LockRequest> *lock_list = lock_table_[key];
  deque<LockRequest>::iterator lock_i;
  for (lock_i = lock_list->begin(); lock_i != lock_list->end(); lock_i++) {
    // Transaction was found
    if (lock_i->txn_ == txn) {
      if ((i+1) != lock_list->end()) {
        bool start = lock_i == lock_list->begin();

        if (start && ((lock_i + 1)->mode_ == EXCLUSIVE)) {
          if (--txn_waits_[(lock_i + 1)->txn_] == 0) {
            ready_txns_->push_back((lock_i + 1)->txn_);
          }
        }

        if ((lock_i->mode_ == EXCLUSIVE) && ((start && (lock_i + 1)->mode_ == SHARED) ||
          ((!start && (lock_i - 1)->mode_ == SHARED) && ((lock_i + 1)->mode_ == SHARED)))) {
          deque<LockRequest>::iterator lock_j;
            for (lock_j = lock_i + 1; lock_j != lock_list->end() && lock_j->mode_ == SHARED; lock_j++) {
              if (--txn_waits_[lock_j->txn_] == 0) {
                ready_txns_->push_back(lock_j->txn_);
              }
            }
        }
      }
      lock_list->erase(i);
      break;
    }
  }

  lock_table_mutexs_[key]->Unlock();
}

LockMode LockManagerC::Status(const Key& key, vector<Txn*>* owners) {
  lock_table_mutexs_[key]->Lock();
  owners->clear();

  if (lock_table_[key]->lock_list->size() == 0) {
    lock_table_mutexs_[key]->Unlock();
    return UNLOCKED;
  }

  if (lock_table_[key]->lock_list->begin()->mode == EXCLUSIVE) {
    owners->push_back(lock_table_[key]->lock_list->begin()->txn_);
    lock_table_mutexs_[key]->Unlock();
    return EXCLUSIVE;
  }

  deque<LockRequest> *lock_list = lock_table_[key]->lock_list;
  deque<LockRequest>::iterator lock;
  for (lock = lock_list->begin(); lock != lock_list->end() && lock->mode_ == SHARED; lock++) {
    owners->push_back(lock->txn_);
  }

  lock_table_mutexs_[key]->Unlock();
  return SHARED;
}
