#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
  lsn_t checkpoint_lsn_{INVALID_LSN};
  ATT active_txns_{};
  KvDatabase persist_data_{};

  inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

  inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
 public:
  void Init(CheckPoint &last_checkpoint) {
    persist_lsn_ = last_checkpoint.checkpoint_lsn_;
    active_txns_ = std::move(last_checkpoint.active_txns_);
    data_ = std::move(last_checkpoint.persist_data_);
  }

  void RedoPhase() {
    auto iter = log_recs_.begin();
    // starts from checkpoint_lsn
    for (; iter != log_recs_.end() && iter->first < persist_lsn_; ++iter) {
    }
    for (; iter != log_recs_.end(); ++iter) {
      auto &log_rec = *(iter->second);
      active_txns_[log_rec.txn_id_] = log_rec.lsn_;
      switch (log_rec.type_) {
        case LogRecType::kInvalid:
          break;
        case LogRecType::kInsert:
          data_[log_rec.ins_key_] = log_rec.ins_val_;
          break;
        case LogRecType::kDelete:
          data_.erase(log_rec.del_key_);
          break;
        case LogRecType::kUpdate:
          data_.erase(log_rec.old_key_);
          data_[log_rec.new_key_] = log_rec.new_val_;
          break;
        case LogRecType::kBegin:
          break;
        case LogRecType::kCommit:
          active_txns_.erase(log_rec.txn_id_);
          break;
        case LogRecType::kAbort:
          Rollback(log_rec.txn_id_);
          active_txns_.erase(log_rec.txn_id_);
          break;
      }
    }
  }

  void UndoPhase() {
    for (const auto &[txn_id, _] : active_txns_) {
      Rollback(txn_id);
    }
    active_txns_.clear();
  }

  void Rollback(txn_id_t txn_id) {
    auto last_log_lsn = active_txns_[txn_id];
    while (last_log_lsn != INVALID_LSN) {
      auto log_rec = log_recs_[last_log_lsn];
      if (log_rec == nullptr) break;
      switch (log_rec->type_) {
        case LogRecType::kInsert:
          data_.erase(log_rec->ins_key_);
          break;
        case LogRecType::kDelete:
          data_[log_rec->del_key_] = log_rec->del_val_;
          break;
        case LogRecType::kUpdate:
          data_.erase(log_rec->new_key_);
          data_[log_rec->old_key_] = log_rec->old_val_;
          break;
        default:
          break;
      }
      last_log_lsn = log_rec->prev_lsn_;
    }
  }

  // used for test only
  void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

  // used for test only
  inline KvDatabase &GetDatabase() { return data_; }

 private:
  std::map<lsn_t, LogRecPtr> log_recs_{};
  lsn_t persist_lsn_{INVALID_LSN};
  ATT active_txns_{};
  KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
