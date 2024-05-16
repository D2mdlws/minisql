#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i); // frame_id
  }
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page : page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.

  if (page_id == INVALID_PAGE_ID ) {
    return nullptr;
  }

  if (page_table_.find(page_id) != page_table_.end()) { // 1.1
    frame_id_t frame_id = page_table_[page_id];
    replacer_->Pin(frame_id);
    pages_[frame_id].pin_count_++;
    return &pages_[frame_id];
  }

  if (free_list_.empty() && replacer_->Size() == 0) {
    return nullptr;
  } else
  {
    frame_id_t frame_id = TryToFindFreePage();  // 1.2
    if (frame_id == INVALID_PAGE_ID) {
      return nullptr;
    }
    if (pages_[frame_id].IsDirty()) { // 2
      FlushPage(pages_[frame_id].GetPageId());
      pages_[frame_id].is_dirty_ = false;
    }

    page_table_.erase(pages_[frame_id].GetPageId());
    page_table_.insert({page_id, frame_id});
    pages_[frame_id].page_id_ = page_id;
    pages_[frame_id].pin_count_ ++;
    pages_[frame_id].is_dirty_ = false;
    replacer_->Pin(frame_id);
    pages_[frame_id].ResetMemory();
    disk_manager_->ReadPage(page_id, pages_[frame_id].GetData());
    return &pages_[frame_id];
  }
  return nullptr;
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.

  size_t index = 0;
  for (index = 0; index < pool_size_; index++) {
    if (pages_[index].GetPinCount() == 0) {
      break;
    }
  }
  if (index == pool_size_) {
    return nullptr;
  }

  page_id = AllocatePage(); // 0

  bool from_free_list = false;
  if (page_id == INVALID_PAGE_ID ) { // 1
    return nullptr;
  }

  frame_id_t frame_id;
  frame_id = TryToFindFreePage(); // 2

  if (pages_[frame_id].IsDirty()) {
    FlushPage(pages_[frame_id].GetPageId());
    pages_[frame_id].is_dirty_ = false;
  }
  
  page_table_.erase(pages_[frame_id].GetPageId());
  page_table_.insert({page_id, frame_id});
  pages_[frame_id].page_id_ = page_id;
  pages_[frame_id].pin_count_ = 1;
  pages_[frame_id].is_dirty_ = false;
  replacer_->Pin(frame_id);
  pages_[frame_id].ResetMemory();
  return &(pages_[frame_id]);
  
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.

  DeallocatePage(page_id); // 0
  if (page_id == INVALID_PAGE_ID) {
    return true;
  }

  if (page_table_.find(page_id) == page_table_.end()) { // 1
    return true;
  }

  frame_id_t frame_id = page_table_[page_id]; // exists

  if (pages_[frame_id].pin_count_ != 0) { // 2
    return false;
  } 
  if (pages_[frame_id].IsDirty()) {
    FlushPage(page_id);
    pages_[frame_id].is_dirty_ = false;
  }
  page_table_.erase(page_id);
  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].pin_count_ = 0;
  pages_[frame_id].is_dirty_ = false;
  pages_[frame_id].ResetMemory();
  free_list_.emplace_back(frame_id);
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) { 
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }

  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  frame_id_t frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ == 0) {
    return false;
  }
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  

  return true; 
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) { 
  if (page_table_.find(page_id) == page_table_.end()) {
    return false;
  }
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].GetData());
  pages_[page_table_[page_id]].is_dirty_ = false;

  return true; 
}

frame_id_t BufferPoolManager::TryToFindFreePage() {
  if (!free_list_.empty()) {
    frame_id_t frame_id = free_list_.front();
    free_list_.pop_front();
    return frame_id;
  } else {
    frame_id_t frame_id;
    if (replacer_->Victim(&frame_id)) {
      return frame_id;
    }
  }
  return INVALID_PAGE_ID;

}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) { return disk_manager_->IsPageFree(page_id); }

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}