#include "concurrency/lock_manager.h"

#include <iostream>

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"

void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

bool LockManager::LockShared(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  if (txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted) {
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockSharedOnReadUncommitted);
  }

  LockPrepare(txn, rid);

  LockRequestQueue &req_queue = lock_table_[rid];

  // emplace lock request to lock table
  req_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kShared);

  // wait for all write request finish
  if (req_queue.is_writing_) {
    req_queue.cv_.wait(
        lock, [&req_queue, txn]() -> bool { return txn->GetState() == TxnState::kAborted || !req_queue.is_writing_; });
  }

  // check txn aborted by deadlock detection
  CheckAbort(txn, req_queue);

  // add rid to txn's shared lock set
  txn->GetSharedLockSet().emplace(rid);

  // increase sharing count
  ++req_queue.sharing_cnt_;
  // grant lock to this txn
  auto iter = req_queue.GetLockRequestIter(txn->GetTxnId());
  iter->granted_ = LockMode::kShared;

  return true;
}

bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  LockPrepare(txn, rid);

  LockRequestQueue &req_queue = lock_table_[rid];

  // emplace lock request to lock table
  req_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kExclusive);

  // wait for all write request and read request finish
  if (req_queue.is_writing_ || req_queue.sharing_cnt_ > 0) {
    req_queue.cv_.wait(lock, [&req_queue, txn]() -> bool {
      return txn->GetState() == TxnState::kAborted || (!req_queue.is_writing_ && 0 == req_queue.sharing_cnt_);
    });
  }

  // check txn aborted by deadlock detection
  CheckAbort(txn, req_queue);

  // add rid to txn's exclusive lock set
  txn->GetExclusiveLockSet().emplace(rid);

  // mark request queue writing
  req_queue.is_writing_ = true;
  // grant lock to this txn
  auto iter = req_queue.GetLockRequestIter(txn->GetTxnId());
  iter->granted_ = LockMode::kExclusive;

  return true;
}

bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  if (txn->GetState() == TxnState::kShrinking) {
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
  }

  LockRequestQueue &req_queue = lock_table_[rid];

  if (req_queue.is_upgrading_) {
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kUpgradeConflict);
  }

  auto iter = req_queue.GetLockRequestIter(txn->GetTxnId());

  // already granted exclusive lock
  if (LockMode::kExclusive == iter->lock_mode_ && LockMode::kExclusive == iter->granted_) {
    return true;
  }

  // change lock mode before upgrading
  iter->lock_mode_ = LockMode::kExclusive;
  iter->granted_ = LockMode::kShared;

  // wait for all write request and read request finish
  // also hold shared lock before upgrading success or txn aborted
  if (req_queue.is_writing_ || req_queue.sharing_cnt_ > 1) {
    req_queue.is_upgrading_ = true;
    req_queue.cv_.wait(lock, [&req_queue, txn]() -> bool {
      //            std::cout << static_cast<int>(txn->GetState()) << std::endl;
      return txn->GetState() == TxnState::kAborted || (!req_queue.is_writing_ && 1 == req_queue.sharing_cnt_);
    });
  }

  // unmark upgrading flag if txn aborted
  if (txn->GetState() == TxnState::kAborted) {
    req_queue.is_upgrading_ = false;
  }
  CheckAbort(txn, req_queue);

  // erase rid from txn's shared lock set
  txn->GetSharedLockSet().erase(rid);
  // add rid to txn's exclusive lock set
  txn->GetExclusiveLockSet().emplace(rid);

  // update states
  --req_queue.sharing_cnt_;
  req_queue.is_upgrading_ = false;
  req_queue.is_writing_ = true;
  iter->granted_ = LockMode::kExclusive;

  return true;
}

bool LockManager::Unlock(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> lock(latch_);

  LockRequestQueue &req_queue = lock_table_[rid];

  // erase rid from txn's shared lock set and exclusive lock set
  txn->GetSharedLockSet().erase(rid);
  txn->GetExclusiveLockSet().erase(rid);

  auto iter = req_queue.GetLockRequestIter(txn->GetTxnId());
  auto lock_mode = iter->lock_mode_;

  // erase request from queue
  bool res = req_queue.EraseLockRequest(txn->GetTxnId());
  assert(res);

  // release lock at growing phase should change state to shrinking phase
  // there is no shrinking phase if isolation_level is READ_COMMITTED
  if (txn->GetState() == TxnState::kGrowing &&
      !(txn->GetIsolationLevel() == IsolationLevel::kReadCommitted && LockMode::kShared == lock_mode)) {
    txn->SetState(TxnState::kShrinking);
  }

  if (LockMode::kShared == lock_mode) {
    --req_queue.sharing_cnt_;
    req_queue.cv_.notify_all();
  } else {
    req_queue.is_writing_ = false;
    req_queue.cv_.notify_all();
  }

  return true;
}

void LockManager::LockPrepare(Txn *txn, const RowId &rid) {
  if (txn->GetState() == TxnState::kShrinking) {
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
  }
  if (lock_table_.find(rid) == lock_table_.end()) {
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }
}

void LockManager::CheckAbort(Txn *txn, LockManager::LockRequestQueue &req_queue) {
  if (txn->GetState() == TxnState::kAborted) {
    req_queue.EraseLockRequest(txn->GetTxnId());
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kDeadlock);
  }
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].insert(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].erase(t2); }

bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) {
  // reset all variables before DFS
  revisited_node_ = INVALID_TXN_ID;
  visited_set_.clear();
  std::stack<txn_id_t>().swap(visited_path_);

  // get all txns
  std::set<txn_id_t> txn_set;
  for (const auto &[t1, vec] : waits_for_) {
    txn_set.insert(t1);
    for (const auto &t2 : vec) {
      txn_set.insert(t2);
    }
  }

  // starts from each txn to search cycle
  for (const auto &start_txn_id : txn_set) {
    if (DFS(start_txn_id)) {
      newest_tid_in_cycle = revisited_node_;
      while (!visited_path_.empty() && revisited_node_ != visited_path_.top()) {
        newest_tid_in_cycle = std::max(newest_tid_in_cycle, visited_path_.top());
        visited_path_.pop();
      }
      return true;
    }
  }

  newest_tid_in_cycle = INVALID_TXN_ID;
  return false;
}

void LockManager::DeleteNode(txn_id_t txn_id) {
  waits_for_.erase(txn_id);

  auto *txn = txn_mgr_->GetTransaction(txn_id);

  for (const auto &row_id : txn->GetSharedLockSet()) {
    for (const auto &lock_req : lock_table_[row_id].req_list_) {
      if (lock_req.granted_ == LockMode::kNone) {
        RemoveEdge(lock_req.txn_id_, txn_id);
      }
    }
  }

  for (const auto &row_id : txn->GetExclusiveLockSet()) {
    for (const auto &lock_req : lock_table_[row_id].req_list_) {
      if (lock_req.granted_ == LockMode::kNone) {
        RemoveEdge(lock_req.txn_id_, txn_id);
      }
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval_);
    {
      std::unique_lock<std::mutex> l(latch_);
      std::unordered_map<txn_id_t, RowId> required_rec;
      // build dependency graph
      for (const auto &[row_id, lock_req_queue] : lock_table_) {
        for (const auto &lock_req : lock_req_queue.req_list_) {
          // ensure lock_req has no XLock or SLock
          if (lock_req.granted_ != LockMode::kNone) continue;
          required_rec[lock_req.txn_id_] = row_id;
          for (const auto &granted_req : lock_req_queue.req_list_) {
            // ensure granted_req has XLock or SLock
            if (LockMode::kNone == granted_req.granted_) continue;
            AddEdge(lock_req.txn_id_, granted_req.txn_id_);
          }
        }
      }
      // break each cycle
      txn_id_t txn_id = INVALID_TXN_ID;
      while (HasCycle(txn_id)) {
        auto *txn = txn_mgr_->GetTransaction(txn_id);
        DeleteNode(txn_id);
        txn->SetState(TxnState::kAborted);
        lock_table_[required_rec[txn_id]].cv_.notify_all();
      }
      waits_for_.clear();
    }
  }
}

bool LockManager::DFS(txn_id_t txn_id) {
  // node revisited
  if (visited_set_.find(txn_id) != visited_set_.end()) {
    revisited_node_ = txn_id;
    return true;
  }

  // set visited flag & record visited path
  visited_set_.insert(txn_id);
  visited_path_.push(txn_id);

  // recursive search sibling nodes
  for (const auto wait_for_txn_id : waits_for_[txn_id]) {
    if (DFS(wait_for_txn_id)) {
      return true;
    }
  }

  // dfs pop
  visited_set_.erase(txn_id);
  visited_path_.pop();

  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  for (const auto &[t1, sibling_vec] : waits_for_) {
    for (const auto &t2 : sibling_vec) {
      result.emplace_back(t1, t2);
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}
