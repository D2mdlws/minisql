#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) 
  : heap(table_heap), rid(rid), txn(txn) {}

TableIterator::TableIterator(const TableIterator &other) {
  heap = other.heap;
  rid = other.rid;
  txn = other.txn;
}

TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const {
  return rid == itr.rid;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(rid == itr.rid);
}

const Row &TableIterator::operator*() {
  auto page = reinterpret_cast<TablePage *>(heap->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    heap->buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    LOG(ERROR) << "Failed to fetch page.\n";
    return Row();
  }
  page->RLatch();
  Row *row;
  if (!page->GetTuple(row, heap->schema_, txn, heap->lock_manager_)) {
    page->RUnlatch();
    heap->buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    LOG(ERROR) << "Failed to get tuple.\n";
    return Row();
  }
  page->RUnlatch();
  heap->buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
  return *row;

}

Row *TableIterator::operator->() {
  auto page = reinterpret_cast<TablePage *>(heap->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page == nullptr) {
    heap->buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    LOG(ERROR) << "Failed to fetch page.\n";
    return nullptr;
  }
  page->RLatch();
  Row *row;
  if (!page->GetTuple(row, heap->schema_, txn, heap->lock_manager_)) {
    page->RUnlatch();
    heap->buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    LOG(ERROR) << "Failed to get tuple.\n";
    return nullptr;
  }
  page->RUnlatch();
  heap->buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
  return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  heap = itr.heap;
  rid = itr.rid;
  txn = itr.txn;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  RowId next_rid;
  auto page = reinterpret_cast<TablePage *>(heap->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  page->RLatch();
  if (page->GetNextTupleRid(rid, &next_rid)) {
    page->RUnlatch();
    rid = next_rid;
    heap->buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    return *this;
  } else { // no next tuple in this page
    page->RUnlatch();
    heap->buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    page_id_t next_page_id = page->GetNextPageId();
    while (next_page_id != INVALID_PAGE_ID) { // find next page
      page = reinterpret_cast<TablePage *>(heap->buffer_pool_manager_->FetchPage(next_page_id));
      page->RLatch();
      if (page->GetFirstTupleRid(&next_rid)) {
        page->RUnlatch();
        rid = next_rid;
        heap->buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
        return *this;
      }
      page->RUnlatch();
      heap->buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
      next_page_id = page->GetNextPageId();
    }
  }
  *this = heap->End();
  return *this;
  
}

// iter++
TableIterator TableIterator::operator++(int) { 
  TableIterator old(*this);
  ++(*this);
  return TableIterator(old);
}
