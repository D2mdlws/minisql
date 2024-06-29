#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, Row row, Txn *txn)
  : heap(table_heap), row(row), txn(txn) {}

TableIterator::TableIterator(const TableIterator &other) {
  heap = other.heap;
  row = other.row;
  txn = other.txn;
}

TableIterator::TableIterator() {
  heap = nullptr;
  row = Row();
  txn = nullptr;
}

TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const {
  return (heap == itr.heap) && (row.GetRowId() == itr.row.GetRowId());
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(operator==(itr));
}

const Row &TableIterator::operator*() {
  return row;
}

Row *TableIterator::operator->() {
  return &row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  heap = itr.heap;
  row = itr.row;
  txn = itr.txn;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  if (*this == heap->End()) {
    return *this;
  }
  RowId next_rid;
  auto page = reinterpret_cast<TablePage *>(heap->buffer_pool_manager_->FetchPage(row.GetRowId().GetPageId()));
  page->RLatch();
  if (page->GetNextTupleRid(row.GetRowId(), &next_rid)) {
    row.destroy();
    row.SetRowId(next_rid);
    page->GetTuple(&row, heap->schema_, txn, heap->lock_manager_);
    page->RUnlatch();
    heap->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return *this;
  } else { // no next tuple in this page

    page_id_t next_page_id = page->GetNextPageId();
    while (next_page_id != INVALID_PAGE_ID) { // find next page
      page->RUnlatch();
      heap->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
      page = reinterpret_cast<TablePage *>(heap->buffer_pool_manager_->FetchPage(next_page_id));
      page->RLatch();
      if (page->GetFirstTupleRid(&next_rid)) {
        row.destroy();
        row.SetRowId(next_rid);
        page->GetTuple(&row, heap->schema_, txn, heap->lock_manager_);
        page->RUnlatch();
        heap->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
        return *this;
      }
    }
  }
  page->RUnlatch();
  heap->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  *this = heap->End();
  return *this;

}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator old(*this);
  ++(*this);
  return TableIterator(old);
}
