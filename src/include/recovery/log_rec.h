#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
  kInvalid,
  kInsert,
  kDelete,
  kUpdate,
  kBegin,
  kCommit,
  kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

struct LogRec {
  LogRec() = default;

  LogRec(LogRecType type, lsn_t lsn, txn_id_t txn_id, lsn_t prev_lsn)
      : type_(type), lsn_(lsn), txn_id_(txn_id), prev_lsn_(prev_lsn) {}

  LogRecType type_{LogRecType::kInvalid};
  lsn_t lsn_{INVALID_LSN};
  txn_id_t txn_id_{INVALID_TXN_ID};
  lsn_t prev_lsn_{INVALID_LSN};
  /* used for insert only */
  KeyType ins_key_{};
  ValType ins_val_{};
  /* used for delete only */
  KeyType del_key_{};
  ValType del_val_{};
  /* used for update only */
  KeyType old_key_{};
  ValType old_val_{};
  KeyType new_key_{};
  ValType new_val_{};

  /* used for testing only */
  static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
  static lsn_t next_lsn_;

  static lsn_t GetAndUpdatePrevLSN(txn_id_t txn_id, lsn_t cur_lsn) {
    auto iter = prev_lsn_map_.find(txn_id);
    auto prev_lsn = INVALID_LSN;
    if (iter != prev_lsn_map_.end()) {
      prev_lsn = iter->second;
      iter->second = cur_lsn;
    } else {
      prev_lsn_map_.emplace(txn_id, cur_lsn);
    }
    return prev_lsn;
  }
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
  lsn_t lsn = LogRec::next_lsn_++;
  auto log = std::make_shared<LogRec>(LogRecType::kInsert, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
  log->ins_key_ = std::move(ins_key);
  log->ins_val_ = ins_val;
  return log;
}

static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
  lsn_t lsn = LogRec::next_lsn_++;
  auto log = std::make_shared<LogRec>(LogRecType::kDelete, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
  log->del_key_ = std::move(del_key);
  log->del_val_ = del_val;
  return log;
}

static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
  lsn_t lsn = LogRec::next_lsn_++;
  auto log = std::make_shared<LogRec>(LogRecType::kUpdate, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
  log->old_key_ = std::move(old_key);
  log->old_val_ = old_val;
  log->new_key_ = std::move(new_key);
  log->new_val_ = new_val;
  return log;
}

static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
  lsn_t lsn = LogRec::next_lsn_++;
  return std::make_shared<LogRec>(LogRecType::kBegin, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
}

static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
  lsn_t lsn = LogRec::next_lsn_++;
  return std::make_shared<LogRec>(LogRecType::kCommit, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
}

static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
  lsn_t lsn = LogRec::next_lsn_++;
  return std::make_shared<LogRec>(LogRecType::kAbort, lsn, txn_id, LogRec::GetAndUpdatePrevLSN(txn_id, lsn));
}

#endif  // MINISQL_LOG_REC_H
