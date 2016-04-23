// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Christina Wallin (christina.wallin@yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
// Student 1: Feridun Mert Celebi (feridun.celebi@yale.edu)
// Student 2: Tihomir Elek (tihomir.elek@yale.edu)

#include "txn/txn_processor.h"
#include <stdio.h>
#include <set>

#include "txn/lock_manager.h"

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 8

TxnProcessor::TxnProcessor(CCMode mode)
    : mode_(mode), tp_(THREAD_COUNT), next_unique_id_(1) {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY)
    lm_ = new LockManagerA(&ready_txns_);
  else if (mode_ == LOCKING)
    lm_ = new LockManagerB(&ready_txns_);

  // Create the storage
  if (mode_ == MVCC) {
    storage_ = new MVCCStorage();
  } else {
    storage_ = new Storage();
  }

  storage_->InitStorage();

  // Start 'RunScheduler()' running.
  cpu_set_t cpuset;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  CPU_ZERO(&cpuset);
  CPU_SET(0, &cpuset);
  CPU_SET(1, &cpuset);
  CPU_SET(2, &cpuset);
  CPU_SET(3, &cpuset);
  CPU_SET(4, &cpuset);
  CPU_SET(5, &cpuset);
  CPU_SET(6, &cpuset);
  pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
  pthread_t scheduler_;
  pthread_create(&scheduler_, &attr, StartScheduler, reinterpret_cast<void*>(this));
}

void* TxnProcessor::StartScheduler(void * arg) {
  reinterpret_cast<TxnProcessor *>(arg)->RunScheduler();
  return NULL;
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

void TxnProcessor::ExecuteTxn(Txn* txn) {
  // Get the start time
  txn->occ_start_time_ = GetTime();

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
      txn->reads_[*it] = result;
  }

  // Execute txn's program logic.
  txn->Run();

  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);
}

void TxnProcessor::ExecuteTxnParallel(Txn *txn) {
  // Get the start time.
  txn->occ_start_time_ = GetTime();

  // Read everything in from readset.
  set<Key>::iterator read_key = txn->readset_.begin();
  for (; read_key != txn->readset_.end(); read_key++) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*read_key, &result)) {
      txn->reads_[*read_key] = result;
    }
  }

  // Read everything in from writeset.
  set<Key>::iterator write_key = txn->writeset_.begin();
  for (; write_key != txn->writeset_.end(); write_key++) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*write_key, &result)) {
      txn->reads_[*write_key] = result;
    }
  }

  // Execute txn's program logic.
  txn->Run();

  // Critical section.
  active_set_mutex_.Lock();
  set<Txn*> active_set_copy = active_set_.GetSet();
  active_set_.Insert(txn);
  active_set_mutex_.Unlock();

  bool validated = true;
  read_key = txn->readset_.begin();
  for (; read_key != txn->readset_.end(); read_key++) {
    if (storage_->Timestamp(*read_key) > txn->occ_start_time_) {
      validated = false;
      break;
    }
  }

  if (validated) {
    set<Key>::iterator write_key = txn->writeset_.begin();
    for (; write_key != txn->writeset_.end(); write_key++) {
      if (storage_->Timestamp(*write_key) > txn->occ_start_time_) {
        validated = false;
        break;
      }
    }
  }

  if (validated) {
    set<Txn*>::iterator active_txn = active_set_copy.begin();
    for (; active_txn != active_set_copy.end(); active_txn++) {

      // TODO: Tiho question.
      set<Key>::iterator write_key = txn->writeset_.begin();
      for (; write_key != txn->writeset_.end(); write_key++) {
        validated = validated && !(*active_txn)->writeset_.count(*write_key) &&
        !(*active_txn)->readset_.count(*write_key);

        if (!validated) {
          break;
        }
      }

      set<Key>::iterator read_key = txn->readset_.begin();
      for (; read_key != txn->readset_.end(); read_key++) {
        validated = validated && !(*active_txn)->writeset_.count(*read_key);

        if(!validated) {
          break;
        }
      }
    }
  }

  // Success.
  if (validated) {
    ApplyWrites(txn);
    active_set_.Erase(txn);
    txn->status_ = COMMITTED;
    txn_results_.Push(txn);
  }
  else {
    // Cleanup.
    active_set_.Erase(txn);
    txn->reads_.clear();
    txn->writes_.clear();
    txn->status_ = INCOMPLETE;

    // Reset.
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();
  }
}

void TxnProcessor::MVCCExecuteTxn(Txn* txn) {

  bool allPassed = true;
  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
         it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    storage_->Lock(*it);
    if (storage_->Read(*it, &result, txn->unique_id_)) {
      txn->reads_[*it] = result;
    }
    storage_->Unlock(*it);
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
      it != txn->writeset_.end(); ++it) {
    // Save each write result iff record exists in storage.
    Value result;
    storage_->Lock(*it);
    if (storage_->Read(*it, &result, txn->unique_id_)) {
      txn->writes_[*it] = result;
    }
    storage_->Unlock(*it);
  }

  // Execute txn's program logic.
  txn->Run();

  // CHECK IF I LOCK THIS PROPERLY, do I lock this in storage or in writes_?
  for (set<Key>::iterator it = txn->writeset_.begin();
      it != txn->writeset_.end(); ++it) {
    storage_->Lock(*it);
  }

  for (set<Key>::iterator it = txn->writeset_.begin();
      it != txn->writeset_.end(); ++it) {
    if (!storage_->CheckWrite(*it, txn->unique_id_)) {
      allPassed = false;
      break;
    }
  }
  if (allPassed) {
    ApplyWrites(txn);
    for (set<Key>::iterator it = txn->writeset_.begin();
        it != txn->writeset_.end(); ++it) {
      storage_->Unlock(*it);
    }
    // Return result to client.
    txn_results_.Push(txn);
  }
  else {
    // Unlock all keys in txn writeset_
    for (set<Key>::iterator it = txn->writeset_.begin();
        it != txn->writeset_.end(); ++it) {
      storage_->Unlock(*it);
    }
    // Cleanup
    txn->reads_.clear();
    txn->writes_.clear();
    txn->status_ = INCOMPLETE;

    // Restart
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock();
  }
}

void TxnProcessor::ApplyWrites(Txn* txn) {
  // Write buffered writes out to storage.
  for (map<Key, Value>::iterator it = txn->writes_.begin();
       it != txn->writes_.end(); it++) {
    storage_->Write(it->first, it->second, txn->unique_id_);
  }
}

void TxnProcessor::RunScheduler() {
  switch (mode_) {
    case SERIAL:                 RunSerialScheduler(); break;
    case LOCKING:                RunLockingScheduler(); break;
    case LOCKING_EXCLUSIVE_ONLY: RunLockingScheduler(); break;
    case OCC:                    RunOCCScheduler(); break;
    case P_OCC:                  RunOCCParallelScheduler(); break;
    case MVCC:                   RunMVCCScheduler();
  }
}

void TxnProcessor::RunSerialScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Get next txn request.
    if (txn_requests_.Pop(&txn)) {
      // Execute txn.
      ExecuteTxn(txn);

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }
  }
}

void TxnProcessor::RunLockingScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      bool blocked = false;
      // Request read locks.
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
        ready_txns_.push_back(txn);
      } else if (blocked == true && (txn->writeset_.size() + txn->readset_.size() > 1)){
        mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock();
      }
    }

    // Process and commit all transactions that have finished running.
    while (completed_txns_.Pop(&txn)) {
      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

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

      // Return result to client.
      txn_results_.Push(txn);
    }

    // Start executing all transactions that have newly acquired all their
    // locks.
    while (ready_txns_.size()) {
      // Get next ready txn from the queue.
      txn = ready_txns_.front();
      ready_txns_.pop_front();

      // Start txn running in its own thread.
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));

    }
  }
}

void TxnProcessor::RunOCCScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
        this,
        &TxnProcessor::ExecuteTxn,
        txn
      ));
    }

    // Validation phase.
    bool validated = true;
    while (completed_txns_.Pop(&txn)) {
      // Check for overlap in writeset.
      set<Key>::iterator write_key = txn->writeset_.begin();
      for (; write_key != txn->writeset_.end(); write_key++) {
        // If last modified > my start then invalid.
        if (storage_->Timestamp(*write_key) > txn->occ_start_time_) {
          validated = false;
          break;
        }
      }

      // Check for overlap in readset.
      set<Key>::iterator read_key = txn->readset_.begin();
      for (; read_key != txn->readset_.end(); read_key++) {
        // If last modified > my start then invalid.
        if (storage_->Timestamp(*read_key) > txn->occ_start_time_) {
          validated = false;
          break;
        }
      }

      // Commit/restart based on the validated boolean.
      if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      }
      else if (txn->Status() == COMPLETED_C) {
        if (validated) {
          ApplyWrites(txn);
          txn->status_ = COMMITTED;

          // Return result to client.
          txn_results_.Push(txn);
        }
        else {
          // Cleanup.
          txn->reads_.clear();
          txn->writes_.clear();
          txn->status_ = INCOMPLETE;

          // Reset.
          mutex_.Lock();
          txn->unique_id_ = next_unique_id_;
          next_unique_id_++;
          txn_requests_.Push(txn);
          mutex_.Unlock();
        }
      }
      else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }
    }
  }
}

void TxnProcessor::RunOCCParallelScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
        this,
        &TxnProcessor::ExecuteTxnParallel,
        txn
      ));
    }
  }
}

void TxnProcessor::RunMVCCScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
        this,
        &TxnProcessor::MVCCExecuteTxn,
        txn
      ));
    }
  }
}
