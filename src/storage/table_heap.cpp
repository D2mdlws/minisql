#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 * use First Fit strategy to find a page to insert the tuple
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  for (auto page_id = first_page_id_; page_id != INVALID_PAGE_ID; page_id = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id))->GetNextPageId()) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
    if (page == nullptr)
      return false;
    page->WLatch();
    if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page_id, true);
      return true;
    }
    page->WUnlatch();
    // If the page is full, then create a new page.
    if (page->GetNextPageId() == INVALID_PAGE_ID) {
      page_id_t new_page_id;
      auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
      if (new_page == nullptr)
        return false;
      
      page->WLatch();
      new_page->WLatch();
      new_page->Init(new_page->GetPageId(), page_id, log_manager_, txn);
      new_page->SetPrevPageId(page_id);
      page->SetNextPageId(new_page->GetPageId());
      new_page->SetNextPageId(INVALID_PAGE_ID);
      
      page->WUnlatch();
      new_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(new_page_id, true);
    }
  }
  
  return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) { 
  // Step1: Find the page which contains the tuple.
  // Step2: Update the tuple in the page.
  // Step3: Unpin the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr)
    return false;
  page->WLatch();
  TablePage::UpdateStatus status = page->UpdateTuple(row, nullptr, schema_, txn, lock_manager_, log_manager_);
  if (status == TablePage::UpdateStatus::updateSuccess) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
    return true;
  } else if (status == TablePage::UpdateStatus::notEnoughSpace) {
    page->ApplyDelete(rid, txn, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
    if (!InsertTuple(row, txn))
      return false;
    return true;
  } else {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    return false;
  }
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  // Step3: Unpin the page.

  // find the page which contains the tuple
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    LOG(ERROR) << "Page not found" << std::endl;
    return;
  }
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) { 
  // Step1: Find the page which contains the tuple.
  // Step2: Read the tuple from the page.
  // Step3: Unpin the page.

  // Fetch func will pin a page
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if (page == nullptr) {
    LOG(ERROR) << "Page not found" << std::endl;
    return false;
  }
    
  page->RLatch();
  bool ret = page->GetTuple(row, schema_, txn, lock_manager_);
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false);
  if (!ret) {
    LOG(ERROR) << "GetTuple failed" << std::endl;
  }
  return ret; 
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) { 
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  if (page == nullptr) {
    return this->End();
  }
  RowId first_rid;
  page->RLatch();
  if (page->GetFirstTupleRid(&first_rid)) {
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(first_page_id_, false);
    return TableIterator(this, first_rid, txn);
  }
  page->RUnlatch();
  buffer_pool_manager_->UnpinPage(first_page_id_, false);
  return this->End();
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() { 
  RowId rid(INVALID_PAGE_ID, 0);
  return TableIterator(this, rid, nullptr); 
}
