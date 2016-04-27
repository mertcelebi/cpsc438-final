// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)

#include "txn/txn_processor.h"

#include <vector>

#include "txn/txn_types.h"
#include "utils/testing.h"


int patient_table_size = 100;
int request_table_size = 100;
int provider_table_size = 14;
int admin_table_size = 4;
int docs_table_size = 150;
int database_size = patient_table_size +
                    request_table_size +
                    provider_table_size +
                    admin_table_size +
                    docs_table_size;

// Returns a human-readable string naming of the providing mode.
string ModeToString(CCMode mode) {
  switch (mode) {
    case LOCKING:                return " Locking C";
    default:                     return "INVALID MODE";
  }
}

class LoadGen {
 public:
  virtual ~LoadGen() {}
  virtual Txn* NewTxn() = 0;
};

class RMWLoadGen : public LoadGen {
 public:
  RMWLoadGen(int dbsize, int rsetsize, int wsetsize, double wait_time)
    : dbsize_(dbsize),
      rsetsize_(rsetsize),
      wsetsize_(wsetsize),
      wait_time_(wait_time) {
  }

  virtual Txn* NewTxn() {
    return new RMW(dbsize_, rsetsize_, wsetsize_, wait_time_);
  }

 private:
  int dbsize_;
  int rsetsize_;
  int wsetsize_;
  double wait_time_;
};

class RMWLoadGen2 : public LoadGen {
 public:
  RMWLoadGen2(int dbsize, int rsetsize, int wsetsize, double wait_time)
    : dbsize_(dbsize),
      rsetsize_(rsetsize),
      wsetsize_(wsetsize),
      wait_time_(wait_time) {
  }

  virtual Txn* NewTxn() {
    // 80% of transactions are READ only transactions and run for the full
    // transaction duration. The rest are very fast (< 0.1ms), high-contention
    // updates.
    if (rand() % 100 < 80)
      return new RMW(dbsize_, rsetsize_, 0, wait_time_);
    else
      return new RMW(dbsize_, 0, wsetsize_, 0);
  }

 private:
  int dbsize_;
  int rsetsize_;
  int wsetsize_;
  double wait_time_;
};

class PatientBankLoadGen : public LoadGen {
 public:
  PatientBankLoadGen(){
  }

  // Including this to get compiler to stop complaining; should directly call following
  // methods
  virtual Txn* NewTxn() {
    return NULL;
  }

  // Update (read & write) one record of the patient table (used repeatedly to simulate patient creation following an insert)
  virtual Txn *UpdatePatientTable() {
    int num_tables = 1;

    int patient_table = 0;

    int begin_ranges[num_tables];
    int end_ranges[num_tables];

    int readsetsizes[num_tables];
    int writesetsizes[num_tables];

    // .4ms
    double txn_duration = .0004;

    // patient table range
    begin_ranges[patient_table] = patient_table_begin_range;
    end_ranges[patient_table] = patient_table_end_range;

    // patient table ops
    readsetsizes[patient_table] = 1;
    writesetsizes[patient_table] = 1;

    return new RMW(begin_ranges, end_ranges, num_tables, readsetsizes,
                   writesetsizes, txn_duration);
  }

  // Arbitrary insert that takes 2ms
  virtual Txn *NewMediumInsertTxn() {
    int num_tables = 0;

    int begin_ranges[num_tables];
    int end_ranges[num_tables];

    int readsetsizes[num_tables];
    int writesetsizes[num_tables];

    // 2 ms
    double txn_duration = .002;

    return new RMW(begin_ranges, end_ranges, num_tables, readsetsizes,
                   writesetsizes, txn_duration);
  }

  // Read provider, read request table, read patient table, update request table
  virtual Txn *ProviderReadsAndUpdatesRequests() {
    int num_tables = 3;

    int provider_table = 0;
    int patient_table = 1;
    int request_table = 2;

    int begin_ranges[num_tables];
    int end_ranges[num_tables];

    int readsetsizes[num_tables];
    int writesetsizes[num_tables];

    // 1ms
    double txn_duration = .001;

    // provider table range
    begin_ranges[provider_table] = provider_table_begin_range;
    end_ranges[provider_table] = provider_table_end_range;

    // provider table ops
    readsetsizes[provider_table] = 1;
    writesetsizes[provider_table] = 0;

    // patient table range
    begin_ranges[patient_table] = patient_table_begin_range;
    end_ranges[patient_table] = patient_table_end_range;

    // patient table ops
    readsetsizes[patient_table] = 1;
    writesetsizes[patient_table] = 0;

    // request table range
    begin_ranges[request_table] = request_table_begin_range;
    end_ranges[request_table] = request_table_end_range;

    // request table ops
    readsetsizes[request_table] = 2;
    writesetsizes[request_table] = 2;


    return new RMW(begin_ranges, end_ranges, num_tables, readsetsizes,
                   writesetsizes, txn_duration);
  }

  // Read admin, read request table, read patient table, update request table
  virtual Txn *AdminReadsAndUpdatesRequests() {
    int num_tables = 3;

    int admin_table = 0;
    int patient_table = 1;
    int request_table = 2;

    int begin_ranges[num_tables];
    int end_ranges[num_tables];

    int readsetsizes[num_tables];
    int writesetsizes[num_tables];

    // 1ms
    double txn_duration = .001;

    // admin table range
    begin_ranges[admin_table] = admin_table_begin_range;
    end_ranges[admin_table] = admin_table_end_range;

    // admin table ops
    readsetsizes[admin_table] = 1;
    writesetsizes[admin_table] = 0;

    // patient table range
    begin_ranges[patient_table] = patient_table_begin_range;
    end_ranges[patient_table] = patient_table_end_range;

    // patient table ops
    readsetsizes[patient_table] = 1;
    writesetsizes[patient_table] = 0;

    // request table range
    begin_ranges[request_table] = request_table_begin_range;
    end_ranges[request_table] = request_table_end_range;

    // request table ops
    readsetsizes[request_table] = 2;
    writesetsizes[request_table] = 2;


    return new RMW(begin_ranges, end_ranges, num_tables, readsetsizes,
                   writesetsizes, txn_duration);
  }

  // Read a single record from patients
  virtual Txn *ReadPatients() {
    int num_tables = 1;

    int patient_table = 0;

    int begin_ranges[num_tables];
    int end_ranges[num_tables];

    int readsetsizes[num_tables];
    int writesetsizes[num_tables];

    // .5ms
    double txn_duration = .0005;

    // patient table range
    begin_ranges[patient_table] = patient_table_begin_range;
    end_ranges[patient_table] = patient_table_end_range;

    // patient table ops
    readsetsizes[patient_table] = 1;
    writesetsizes[patient_table] = 0;

    return new RMW(begin_ranges, end_ranges, num_tables, readsetsizes,
                   writesetsizes, txn_duration);
  }

  // Update two records from request_table
  virtual Txn *UpdateRequests() {
    int num_tables = 1;

    int request_table = 0;

    int begin_ranges[num_tables];
    int end_ranges[num_tables];

    int readsetsizes[num_tables];
    int writesetsizes[num_tables];

    // .4ms
    double txn_duration = .0004;

    // request table range
    begin_ranges[request_table] = request_table_begin_range;
    end_ranges[request_table] = request_table_end_range;

    // request table ops
    readsetsizes[request_table] = 2;
    writesetsizes[request_table] = 2;

    return new RMW(begin_ranges, end_ranges, num_tables, readsetsizes,
                   writesetsizes, txn_duration);
  }

  // Read one record from admins, read one from requests, read two from docs,
  // update docs table
  virtual Txn *AdminReadsAndUpdatesDocs() {
    int num_tables = 3;

    int admins_table = 0;
    int request_table = 1;
    int docs_table = 2;

    int begin_ranges[num_tables];
    int end_ranges[num_tables];

    int readsetsizes[num_tables];
    int writesetsizes[num_tables];

    // 1ms
    double txn_duration = .001;

    // admins table range
    begin_ranges[admins_table] = admin_table_begin_range;
    end_ranges[admins_table] = admin_table_end_range;

    // admins table ops
    readsetsizes[admins_table] = 1;
    writesetsizes[admins_table] = 0;

    // requests table range
    begin_ranges[request_table] = request_table_begin_range;
    end_ranges[request_table] = request_table_end_range;

    // requests table ops
    readsetsizes[request_table] = 1;
    writesetsizes[request_table] = 0;

    // docs table range
    begin_ranges[docs_table] = docs_table_begin_range;
    end_ranges[docs_table] = docs_table_end_range;

    // docs table ops
    readsetsizes[docs_table] = 2;
    writesetsizes[docs_table] = 2;

    return new RMW(begin_ranges, end_ranges, num_tables, readsetsizes,
                   writesetsizes, txn_duration);
  }

  // Read one record from patients, read one from requests, read two from docs
  virtual Txn *PatientReadsDocs() {
    int num_tables = 3;

    int patient_table = 0;
    int request_table = 1;
    int docs_table = 2;

    int begin_ranges[num_tables];
    int end_ranges[num_tables];

    int readsetsizes[num_tables];
    int writesetsizes[num_tables];

    // 1ms
    double txn_duration = .001;

    // patient table range
    begin_ranges[patient_table] = patient_table_begin_range;
    end_ranges[patient_table] = patient_table_end_range;

    // patient table ops
    readsetsizes[patient_table] = 1;
    writesetsizes[patient_table] = 0;

    // requests table range
    begin_ranges[request_table] = request_table_begin_range;
    end_ranges[request_table] = request_table_end_range;

    // requests table ops
    readsetsizes[request_table] = 1;
    writesetsizes[request_table] = 0;

    // docs table range
    begin_ranges[docs_table] = docs_table_begin_range;
    end_ranges[docs_table] = docs_table_end_range;

    // docs table ops
    readsetsizes[docs_table] = 2;
    writesetsizes[docs_table] = 0;

    return new RMW(begin_ranges, end_ranges, num_tables, readsetsizes,
                   writesetsizes, txn_duration);
  }


  // EXAMPLE
  virtual Txn *NewUpdatePatientProviderTxn() {
    int num_tables = 2;

    int provider_table = 0;
    int patient_table = 1;

    int begin_ranges[num_tables];
    int end_ranges[num_tables];

    int readsetsizes[num_tables];
    int writesetsizes[num_tables];

    // 10ms
    double txn_duration = .01;

    // provider table range
    begin_ranges[provider_table] = provider_table_begin_range;
    end_ranges[provider_table] = provider_table_end_range;

    // patient table ops
    readsetsizes[provider_table] = 2;
    writesetsizes[provider_table] = 1;

    // patient table range
    begin_ranges[patient_table] = patient_table_begin_range;
    end_ranges[patient_table] = patient_table_end_range;

    // patient table ops
    readsetsizes[patient_table] = 1;
    writesetsizes[patient_table] = 1;

    return new RMW(begin_ranges, end_ranges, num_tables, readsetsizes,
                   writesetsizes, txn_duration);
  }


  // EXAMPLE
  virtual Txn *NewSmallInsertTxn() {
    int num_tables = 0;

    int begin_ranges[num_tables];
    int end_ranges[num_tables];

    int readsetsizes[num_tables];
    int writesetsizes[num_tables];

    // 1 ms
    double txn_duration = .001;

    return new RMW(begin_ranges, end_ranges, num_tables, readsetsizes,
                   writesetsizes, txn_duration);
  }

  // EXAMPLE
  virtual Txn *NewLargeInsertTxn() {
    int num_tables = 0;

    int begin_ranges[num_tables];
    int end_ranges[num_tables];

    int readsetsizes[num_tables];
    int writesetsizes[num_tables];

    // 10 ms
    double txn_duration = .01;

    return new RMW(begin_ranges, end_ranges, num_tables, readsetsizes,
                   writesetsizes, txn_duration);
  }
  /*
  virtual Txn* NewTxn() {
    // 80% of transactions are READ only transactions and run for the full
    // transaction duration. The rest are very fast (< 0.1ms), high-contention
    // updates.
    if (rand() % 100 < 80)
      return new RMW(dbsize_, rsetsize_, 0, wait_time_);
    else
      return new RMW(dbsize_, 0, wsetsize_, 0);
  }
  */

 private:
  int patient_table_begin_range = 0;
  int patient_table_end_range = patient_table_begin_range + patient_table_size;

  int request_table_begin_range = patient_table_end_range;
  int request_table_end_range = request_table_begin_range + request_table_size;

  int provider_table_begin_range = request_table_end_range;
  int provider_table_end_range = provider_table_begin_range + provider_table_size;

  int admin_table_begin_range = provider_table_end_range;
  int admin_table_end_range = admin_table_begin_range + admin_table_size;

  int docs_table_begin_range = admin_table_end_range;
  int docs_table_end_range = docs_table_begin_range + docs_table_size;
};

void Benchmark(const vector<LoadGen*>& lg) {
  // deque<Txn*> doneTxns;

  // For each MODE...
  for (CCMode mode = LOCKING;
      mode <= LOCKING;
      mode = static_cast<CCMode>(mode+1)) {
    // Print out mode name.
    cout << ModeToString(mode) << flush;

    // For each experiment, run 3 times and get the average.
    for (uint32 exp = 0; exp < lg.size(); exp++) {
      double throughput[3];
      for (uint32 round = 0; round < 3; round++) {

        int txn_count = 0;

        //MINE
        // int txn_sent = 0;

        // Create TxnProcessor in next mode.
        TxnProcessor* p = new TxnProcessor(mode);

        // Number of transaction requests that can be active at any given time.
        int active_txns = (p->THREAD_COUNT) * 10;

        // Start specified number of txns running.
        for (int i = 0; i < active_txns; i++) {
          // for each thread in thread pool
          // Txn *txn = lg[exp]->NewTxn();
          p->AddThreadTask(lg[exp]->NewTxn());
          // txn_sent++;
        }

        // Record start time.
        p->Start();
        double start = GetTime();



        sleep(2);
        // Keep 100 active txns at all times for the first full second.
        // while (GetTime() < start + 1) {
          // Txn* txn = p->GetTxnResult();
          // doneTxns.push_back(txn);
          // txn_count++;
          // p->NewTxnRequest(lg[exp]->NewTxn());
          // txn_sent++;
        // }

        // Wait for all of them to finish.
        // for (int i = 0; i < active_txns; i++) {
        //   Txn* txn = p->GetTxnResult();
        //   doneTxns.push_back(txn);
        //   txn_count++;
        //   // printf("Recieved %i results out of %i requests sent\n", txn_count, txn_sent);
        //   // printf("active_txns = %i", active_txns);
        // }

        // printf("Ending benchmark");

        // Record end time.
        p->Finish();
        double end = GetTime();

        bool threads_finished = false;
        do {
          (p->threads_done_mutex).Lock();
          threads_finished = (p->threads_done) == p->THREAD_COUNT;

          // printf("threads_done = %i\n", p->threads_done);

          (p->threads_done_mutex).Unlock();
        } while (!threads_finished);

        (p->txn_count_mutex).Lock();
        txn_count = (p->global_txn_count);
        (p->txn_count_mutex).Unlock();

        throughput[round] = txn_count / (end-start);

        // doneTxns.clear();
        delete p;
      }

      // Print throughput
      cout << "\t" << (throughput[0] + throughput[1] + throughput[2]) / 3 << "\t" << flush;
    }

    cout << endl;
  }
}

void CreateNewPatient(TxnProcessor *p, PatientBankLoadGen *lg) {
  // First insert
  p->AddThreadTask(lg->NewMediumInsertTxn());

  // Now update the same patient table record 4 times
  Txn *upt = lg->UpdatePatientTable();
  p->AddThreadTask(upt);
  p->AddThreadTask(upt->clone());
  p->AddThreadTask(upt->clone());
  p->AddThreadTask(upt->clone());
}

void ProviderReadsAndUpdatesRequests(TxnProcessor *p, PatientBankLoadGen *lg) {
  p->AddThreadTask(lg->ProviderReadsAndUpdatesRequests());
}

void AdminReadsAndUpdatesRequests(TxnProcessor *p, PatientBankLoadGen *lg) {
  p->AddThreadTask(lg->AdminReadsAndUpdatesRequests());
}

void PatientCreatesRequest(TxnProcessor *p, PatientBankLoadGen *lg) {
  // Read from patients then "insert" request
  p->AddThreadTask(lg->ReadPatients());
  p->AddThreadTask(lg->NewMediumInsertTxn());

  // Update request twice
  Txn *ur = lg->UpdateRequests();
  p->AddThreadTask(ur);
  p->AddThreadTask(ur->clone());
}

void AdminReadsAndUpdatesDocs(TxnProcessor *p, PatientBankLoadGen *lg) {
  p->AddThreadTask(lg->AdminReadsAndUpdatesDocs());
}

void PatientReadsDocs(TxnProcessor *p, PatientBankLoadGen *lg) {
  p->AddThreadTask(lg->PatientReadsDocs());
}

void BenchmarkPatientCreationWhileAdminsRun(const vector<PatientBankLoadGen*>& lg) {
  CCMode mode = OCC;

  // Print out mode name.
  cout << ModeToString(mode) << flush;

  // For each experiment, run 3 times and get the average.
  for (uint32 exp = 0; exp < lg.size(); exp++) {
    double throughput[3];
    for (uint32 round = 0; round < 3; round++) {
      int txn_count = 0;

      // Create TxnProcessor in next mode.
      TxnProcessor* p = new TxnProcessor(mode);

      // Number of transaction requests that can be active at any given time.
      int active_txns = (p->THREAD_COUNT) * 20;

      // Start specified number of txns running.
      for (int i = 0; i < active_txns; ++i) {
        // 5 admin reads/updates for every new patient created
        for (int j = 0; j < 5; ++j) {
          AdminReadsAndUpdatesRequests(p, lg[exp]);
        }
        CreateNewPatient(p, lg[exp]);
      }

      // Record start time.
      p->Start();
      double start = GetTime();

      sleep(2);

      // Record end time.
      p->Finish();
      double end = GetTime();

      bool threads_finished = false;
      do {
        (p->threads_done_mutex).Lock();
        threads_finished = (p->threads_done) == p->THREAD_COUNT;
        (p->threads_done_mutex).Unlock();
      } while (!threads_finished);

      (p->txn_count_mutex).Lock();
      txn_count = (p->global_txn_count);
      (p->txn_count_mutex).Unlock();

      throughput[round] = txn_count / (end-start);

      delete p;
    }

    // Print throughput
    cout << "\t" << (throughput[0] + throughput[1] + throughput[2]) / 3 << "\t" << flush;
  }

  cout << endl;
}

void BenchmarkRequestCreationWhileAdminsRun(const vector<PatientBankLoadGen*>& lg) {
  CCMode mode = OCC;

  // Print out mode name.
  cout << ModeToString(mode) << flush;

  // For each experiment, run 3 times and get the average.
  for (uint32 exp = 0; exp < lg.size(); exp++) {
    double throughput[3];
    for (uint32 round = 0; round < 3; round++) {
      int txn_count = 0;

      // Create TxnProcessor in next mode.
      TxnProcessor* p = new TxnProcessor(mode);

      // Number of transaction requests that can be active at any given time.
      int active_txns = (p->THREAD_COUNT) * 20;

      // Start specified number of txns running.
      for (int i = 0; i < active_txns; ++i) {
        // 5 admin reads/updates for every new request created
        for (int j = 0; j < 5; ++j) {
          AdminReadsAndUpdatesRequests(p, lg[exp]);
        }
        PatientCreatesRequest(p, lg[exp]);
      }

      // Record start time.
      p->Start();
      double start = GetTime();

      sleep(2);

      // Record end time.
      p->Finish();
      double end = GetTime();

      bool threads_finished = false;
      do {
        (p->threads_done_mutex).Lock();
        threads_finished = (p->threads_done) == p->THREAD_COUNT;
        (p->threads_done_mutex).Unlock();
      } while (!threads_finished);

      (p->txn_count_mutex).Lock();
      txn_count = (p->global_txn_count);
      (p->txn_count_mutex).Unlock();

      throughput[round] = txn_count / (end-start);

      delete p;
    }

    // Print throughput
    cout << "\t" << (throughput[0] + throughput[1] + throughput[2]) / 3 << "\t" << flush;
  }

  cout << endl;
}


void BenchmarkAdminsAndPatientsTouchDocs(const vector<PatientBankLoadGen*>& lg) {
  CCMode mode = OCC;

  // Print out mode name.
  cout << ModeToString(mode) << flush;

  // For each experiment, run 3 times and get the average.
  for (uint32 exp = 0; exp < lg.size(); exp++) {
    double throughput[3];
    for (uint32 round = 0; round < 3; round++) {
      int txn_count = 0;

      // Create TxnProcessor in next mode.
      TxnProcessor* p = new TxnProcessor(mode);

      // Number of transaction requests that can be active at any given time.
      int active_txns = (p->THREAD_COUNT) * 20;

      // Start specified number of txns running.
      for (int i = 0; i < active_txns; ++i) {
        // 3 admin reads/updates for every 1 patient read
        for (int j = 0; j < 3; ++j) {
          AdminReadsAndUpdatesDocs(p, lg[exp]);
        }
        PatientReadsDocs(p, lg[exp]);
      }

      // Record start time.
      p->Start();
      double start = GetTime();

      sleep(2);

      // Record end time.
      p->Finish();
      double end = GetTime();

      bool threads_finished = false;
      do {
        (p->threads_done_mutex).Lock();
        threads_finished = (p->threads_done) == p->THREAD_COUNT;
        (p->threads_done_mutex).Unlock();
      } while (!threads_finished);

      (p->txn_count_mutex).Lock();
      txn_count = (p->global_txn_count);
      (p->txn_count_mutex).Unlock();

      throughput[round] = txn_count / (end-start);

      delete p;
    }

    // Print throughput
    cout << "\t" << (throughput[0] + throughput[1] + throughput[2]) / 3 << "\t" << flush;
  }

  cout << endl;
}

void BenchmarkFullSimulation(const vector<PatientBankLoadGen*>& lg) {
  CCMode mode = OCC;

  // Print out mode name.
  cout << ModeToString(mode) << flush;

  // For each experiment, run 3 times and get the average.
  for (uint32 exp = 0; exp < lg.size(); exp++) {
    double throughput[3];
    for (uint32 round = 0; round < 3; round++) {
      int txn_count = 0;

      // Create TxnProcessor in next mode.
      TxnProcessor* p = new TxnProcessor(mode);

      // Number of transaction requests that can be active at any given time.
      int active_txns = (p->THREAD_COUNT) * 20;

      // Start specified number of txns running.
      for (int i = 0; i < active_txns; ++i) {
        // 4 admin reads/updates for every 1 anything else
        for (int j = 0; j < 3; ++j) {
          AdminReadsAndUpdatesRequests(p, lg[exp]);
          AdminReadsAndUpdatesDocs(p, lg[exp]);
        }
        CreateNewPatient(p, lg[exp]);
        ProviderReadsAndUpdatesRequests(p, lg[exp]);
        PatientCreatesRequest(p, lg[exp]);
        PatientReadsDocs(p, lg[exp]);
      }

      // Record start time.
      p->Start();
      double start = GetTime();

      sleep(2);

      // Record end time.
      p->Finish();
      double end = GetTime();

      bool threads_finished = false;
      do {
        (p->threads_done_mutex).Lock();
        threads_finished = (p->threads_done) == p->THREAD_COUNT;
        (p->threads_done_mutex).Unlock();
      } while (!threads_finished);

      (p->txn_count_mutex).Lock();
      txn_count = (p->global_txn_count);
      (p->txn_count_mutex).Unlock();

      throughput[round] = txn_count / (end-start);

      delete p;
    }

    // Print throughput
    cout << "\t" << (throughput[0] + throughput[1] + throughput[2]) / 3 << "\t" << flush;
  }

    cout << endl;
}

int main(int argc, char** argv) {
  cout << "\t\t\t    Average Transaction Duration" << endl;
  cout << "\t\t0.1ms\t\t1ms\t\t10ms";
  cout << endl;

  cpu_set_t cs;
  CPU_ZERO(&cs);
  CPU_SET(7, &cs);
  int ret = sched_setaffinity(0, sizeof(cs), &cs);
  if (ret) {
    perror("sched_setaffinity");
    assert(false);
  }

  // ADDING FOR SILO/LOCKING COMPARISON BENCHMARKING

  vector<PatientBankLoadGen*> pblg;

  cout << "^^ ignore above transaction durations, customized benchmarks follow..." << endl;

  cout << "PatientBank Benchmark 1: create new patients while admin reads patients & update requests" << endl;
  pblg.push_back(new PatientBankLoadGen());

  BenchmarkPatientCreationWhileAdminsRun(pblg);

  for (uint32 i = 0; i < pblg.size(); i++)
    delete pblg[i];
  pblg.clear();


  cout << "PatientBank Benchmark 2: patient creates requests while admin reads patients & updates requests" << endl;
  pblg.push_back(new PatientBankLoadGen());

  BenchmarkRequestCreationWhileAdminsRun(pblg);

  for (uint32 i = 0; i < pblg.size(); i++)
    delete pblg[i];
  pblg.clear();


  cout << "PatientBank Benchmark 3: admin reads & updates docs while patient reads docs" << endl;
  pblg.push_back(new PatientBankLoadGen());

  BenchmarkAdminsAndPatientsTouchDocs(pblg);

  for (uint32 i = 0; i < pblg.size(); i++)
    delete pblg[i];
  pblg.clear();


  cout << "PatientBank Benchmark 4: full simulation" << endl;
  pblg.push_back(new PatientBankLoadGen());

  BenchmarkFullSimulation(pblg);

  for (uint32 i = 0; i < pblg.size(); i++)
    delete pblg[i];
  pblg.clear();

  // REMOVE THIS TO RUN NORMAL BENCHMARKS
  exit(0);

  // END ADDING FOR SILO/LOCKING COMPARISON BENCHMARKING

  vector<LoadGen*> lg;

  cout << "'Low contention' Read only (5 records)" << endl;
  lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.0001));
  lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.001));
  lg.push_back(new RMWLoadGen(1000000, 5, 0, 0.01));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "'Low contention' Read only (20 records) " << endl;
  lg.push_back(new RMWLoadGen(1000000, 20, 0, 0.0001));
  lg.push_back(new RMWLoadGen(1000000, 20, 0, 0.001));
  lg.push_back(new RMWLoadGen(1000000, 20, 0, 0.01));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "'High contention' Read only (5 records)" << endl;
  lg.push_back(new RMWLoadGen(100, 5, 0, 0.0001));
  lg.push_back(new RMWLoadGen(100, 5, 0, 0.001));
  lg.push_back(new RMWLoadGen(100, 5, 0, 0.01));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "'High contention' Read only (20 records)" << endl;
  lg.push_back(new RMWLoadGen(100, 20, 0, 0.0001));
  lg.push_back(new RMWLoadGen(100, 20, 0, 0.001));
  lg.push_back(new RMWLoadGen(100, 20, 0, 0.01));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "Low contention read-write (5 records)" << endl;
  lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.0001));
  lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.001));
  lg.push_back(new RMWLoadGen(1000000, 0, 5, 0.01));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "Low contention read-write (10 records)" << endl;
  lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.0001));
  lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.001));
  lg.push_back(new RMWLoadGen(1000000, 0, 10, 0.01));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "High contention read-write (1 records)" << endl;
  lg.push_back(new RMWLoadGen(5, 0, 1, 0.0001));
  lg.push_back(new RMWLoadGen(5, 0, 1, 0.001));
  lg.push_back(new RMWLoadGen(5, 0, 1, 0.01));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "High contention read-write (5 records)" << endl;
  lg.push_back(new RMWLoadGen(100, 0, 5, 0.0001));
  lg.push_back(new RMWLoadGen(100, 0, 5, 0.001));
  lg.push_back(new RMWLoadGen(100, 0, 5, 0.01));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  cout << "High contention read-write (10 records)" << endl;
  lg.push_back(new RMWLoadGen(100, 0, 10, 0.0001));
  lg.push_back(new RMWLoadGen(100, 0, 10, 0.001));
  lg.push_back(new RMWLoadGen(100, 0, 10, 0.01));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();

  // 80% of transactions are READ only transactions and run for the full
  // transaction duration. The rest are very fast (< 0.1ms), high-contention
  // updates.
  cout << "High contention mixed read only/read-write " << endl;
  lg.push_back(new RMWLoadGen2(50, 30, 10, 0.0001));
  lg.push_back(new RMWLoadGen2(50, 30, 10, 0.001));
  lg.push_back(new RMWLoadGen2(50, 30, 10, 0.01));

  Benchmark(lg);

  for (uint32 i = 0; i < lg.size(); i++)
    delete lg[i];
  lg.clear();
}
