// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Christina Wallin (christina.wallin@yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
// Student 1: Feridun Mert Celebi (feridun.celebi@yale.edu)
// Student 2: Tihomir Elek (tihomir.elek@yale.edu)

#include "txn/txn_processor.h"
#include <stdio.h>
#include <set>
#include <algorithm>

#include "txn/lock_manager.h"

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT_ 6

TxnProcessor::TxnProcessor(CCMode mode)
    : mode_(mode), tp_(THREAD_COUNT_), next_unique_id_(1) {

  benchmark_complete = false;
  THREAD_COUNT = THREAD_COUNT_;
  threads_done = 0;
  global_txn_count = 0;

  lm_ = new LockManagerC(&ready_txns_);
}


TxnProcessor::~TxnProcessor() {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING)
    delete lm_;

  delete storage_;
}

void TxnProcessor::NewTxnRequest(Txn* txn) {
  // Atomically assign the txn a new number and add it to the incoming txn
  // requests queue.
  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  next_unique_id_++;
  txn_requests_.Push(txn);
  mutex_.Unlock();
}

Txn* TxnProcessor::GetTxnResult() {
  Txn* txn;
  while (!txn_results_.Pop(&txn)) {
    // No result yet. Wait a bit before trying again (to reduce contention on
    // atomic queues).
    sleep(0.000001);
  }
  return txn;
}

void TxnProcessor::ApplyWrites(Txn* txn) {
  // Write buffered writes out to storage.
  for (map<Key, Value>::iterator it = txn->writes_.begin();
       it != txn->writes_.end(); it++) {
    storage_->Write(it->first, it->second, txn->unique_id_);
  }
}


// --------------------------------------------
// Added for final project
void TxnProcessor::AddThreadTask(Txn *txn) {
  tp_.RunTask(new Method <TxnProcessor, void, Txn*>(
        this,
        &TxnProcessor::ExecuteTxnFancyLocking,
        txn));
}

void TxnProcessor::Start() {
  benchmark_started = true;
}

void TxnProcessor::Finish() {
  benchmark_complete = true;
}

void TxnProcessor::ExecuteTxnFancyLocking(Txn* txn) {

  thread_local static uint64 txn_count;

  if (benchmark_complete) {
    if (txn_count != 0) {

      threads_done_mutex.Lock();
      ++threads_done;
      threads_done_mutex.Unlock();

      txn_count_mutex.Lock();
      global_txn_count += txn_count;
      txn_count = 0;
      txn_count_mutex.Unlock();
    }
    return;
  }

  // printf("kurac 0\n");

  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); it++) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); it++) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->writes_[*it] = result;
  }


   bool blocked = false;
  // Request read locks.
  // TODO: LOCK THE TABLE
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); it++) {
    if (!lm_->ReadLock(txn, *it)) {
      blocked = true;
      // If readset_.size() + writeset_.size() > 1, and blocked, just abort
      if (txn->readset_.size() + txn->writeset_.size() > 1) {
        // Release all locks that already acquired

        for (set<Key>::iterator it_reads = txn->readset_.begin(); true; ++it_reads) {
          lm_->Release(txn, *it_reads);
          if (it_reads == it) {
            break;
          }
        }
        break;
      }
    }
  }

  if (blocked == false) {
    // Request write locks.
    for (set<Key>::iterator it = txn->writeset_.begin();
         it != txn->writeset_.end(); it++) {
      if (!lm_->WriteLock(txn, *it)) {
        blocked = true;
        // If readset_.size() + writeset_.size() > 1, and blocked, just abort
        if (txn->readset_.size() + txn->writeset_.size() > 1) {
          // Release all read locks that already acquired
          for (set<Key>::iterator it_reads = txn->readset_.begin(); it_reads != txn->readset_.end(); it_reads++) {
            lm_->Release(txn, *it_reads);
          }
          // Release all write locks that already acquired
          for (set<Key>::iterator it_writes = txn->writeset_.begin(); true; ++it_writes) {
            lm_->Release(txn, *it_writes);
            if (it_writes == it) {
              break;
            }
          }
          break;
        }
      }
    }
  }
  // If all read and write locks were immediately acquired, this txn is
  // ready to be executed. Else, just restart the txn
  if (blocked == false) {
    // Execute txn's program logic.
    txn->Run();
    lock_storage.Lock();
    ApplyWrites(txn);
    lock_storage.Unlock();
  } else if (blocked == true && (txn->writeset_.size() + txn->readset_.size() > 1)){
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    mutex_.Unlock();
  }
  if (benchmark_started) {
    ++txn_count;
  }

  // printf("about to Release\n");


  // Release read locks.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); it++) {
    lm_->Release(txn, *it);
  }
  // Release write locks.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); it++) {
    lm_->Release(txn, *it);
  }

}
