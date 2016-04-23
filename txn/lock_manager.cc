// Original Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
// Student 1: Feridun Mert Celebi (feridun.celebi@yale.edu)
// Student 2: Tihomir Elek (tihomir.elek@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "txn/lock_manager.h"

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  LockRequest lock_request(EXCLUSIVE, txn);

  // Add the new lockRequest to lock_table_.
  if (!lock_table_.count(key)) {
    deque<LockRequest> *lock_queue = new deque<LockRequest>(1, lock_request);
    lock_table_[key] = lock_queue;
  }
  else {
    lock_table_[key]->push_back(lock_request);
  }

  // Success for the lock acquisition of the transaction.
  if (lock_table_[key]->size() == 1) {
    return true;
  }
  else {
    if (!txn_waits_.count(txn)) {
      txn_waits_[txn]++;
    }
    return false;
  }
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Since Part 1A implements ONLY exclusive locks, calls to ReadLock can
  // simply use the same logic as 'WriteLock'.
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  bool locked;

  // Queue of locks with the given key.
  deque<LockRequest> *lock_queue = lock_table_[key];

  // Remove the transaction from lock_queue.
  deque<LockRequest>::iterator lock = lock_queue->begin();
  for (; lock != lock_queue->end(); lock++) {
    if (lock->txn_ == txn) {
      locked = (lock_queue->front().txn_ == txn);
      lock_queue->erase(lock);
      break;
    }
  }

  // Start the next txn if it acquired the lock.
  if (lock_queue->size() >= 1 && locked) {
    Txn *next = lock_queue->front().txn_;
    if (--txn_waits_[next] == 0) {
      ready_txns_->push_back(next);
    }
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  // Queue of locks with the given key.
  deque<LockRequest> *lock_queue = lock_table_[key];
  owners->clear();

  // Fill the owners vector.
  if (lock_queue->size()) {
    owners->push_back(lock_queue->begin()->txn_);
  }

  if (owners->empty()) {
    return UNLOCKED;
  }
  return EXCLUSIVE;
}

LockManagerB::LockManagerB(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerB::WriteLock(Txn* txn, const Key& key) {
  LockRequest lock_request(EXCLUSIVE, txn);

  // Add the new lock_request to lock_table_.
  if (!lock_table_.count(key)) {
    deque<LockRequest> *lock_queue = new deque<LockRequest>(1, lock_request);
    lock_table_[key] = lock_queue;
  }
  else {
    lock_table_[key]->push_back(lock_request);
  }

  // Success for the lock acquisition of the transaction.
  if (lock_table_[key]->front().txn_ == txn) {
    return true;
  }
  else {
    if (!txn_waits_.count(txn)) {
      txn_waits_[txn]++;
    }
    return false;
  }
}

bool LockManagerB::ReadLock(Txn* txn, const Key& key) {
  LockRequest lock_request(SHARED, txn);

  // Add the new lock_request to lock_table_.
  if (!lock_table_.count(key)) {
    deque<LockRequest> *lock_queue = new deque<LockRequest>(1, lock_request);
    lock_table_[key] = lock_queue;
  }
  else {
    lock_table_[key]->push_back(lock_request);
  }

  // Adjust txn_waits_ if lock is not acquired.
  deque<LockRequest> *lock_queue = lock_table_[key];
  deque<LockRequest>::iterator lock = lock_queue->begin();
  for (; lock != lock_queue->end(); lock++) {
    if (lock->mode_ == EXCLUSIVE) {
      if (!txn_waits_.count(txn)) {
        txn_waits_[txn]++;
      }
      return false;
    }
  }
  return true;
}

void LockManagerB::Release(Txn* txn, const Key& key) {
  // Queue of locks with the given key.
  deque<LockRequest> *lock_queue = lock_table_[key];
  deque<LockRequest>::iterator lock_i = lock_queue->begin();
  for (; lock_i != lock_queue->end(); lock_i++) {
    if (lock_i->txn_ == txn) {
      if ((lock_i + 1) != lock_queue->end()) {
        bool first_txn = lock_i == lock_queue->begin();

        if (first_txn && ((lock_i + 1)->mode_ == EXCLUSIVE)) {
          if (--txn_waits_[(lock_i + 1)->txn_] == 0) {
            ready_txns_->push_back((lock_i + 1)->txn_);
          }
        }

        if ((lock_i->mode_ == EXCLUSIVE) && ((first_txn && (lock_i + 1)->mode_ == SHARED) ||
          ((!first_txn && (lock_i - 1)->mode_ == SHARED) && ((lock_i + 1)->mode_ == SHARED)))) {
          deque<LockRequest>::iterator lock_j = lock_i + 1;
          for (; lock_j != lock_queue->end() && lock_j->mode_ == SHARED; lock_j++) {
            if (--txn_waits_[lock_j->txn_] == 0) {
              ready_txns_->push_back(lock_j->txn_);
            }
          }
        }
      }
      lock_queue->erase(lock_i);
      break;
    }
  }
}

LockMode LockManagerB::Status(const Key& key, vector<Txn*>* owners) {
  // Queue of locks with the given key.
  deque<LockRequest> *lock_queue = lock_table_[key];
  owners->clear();

  // UNLOCKED: nothing to be done.
  if (!lock_queue->size()) {
    return UNLOCKED;
  }

  // EXCLUSIVE: update owners with the txn that has the EXCLUSIVE lock.
  if (lock_queue->begin()->mode_ == EXCLUSIVE) {
    owners->push_back(lock_table_[key]->begin()->txn_);
    return EXCLUSIVE;
  }

  // SHARAED: update owners with the txns that have SHARED locks.
  deque<LockRequest>::iterator lock = lock_queue->begin();
  for (; lock != lock_queue->end() && lock->mode_ == SHARED; lock++) {
    owners->push_back(lock->txn_);
  }
  return SHARED;
}
