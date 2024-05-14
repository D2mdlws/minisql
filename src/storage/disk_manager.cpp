#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 * 搜索每一个bitmap，找到第一个空闲的extent块，在该块中写入一个page
 */
page_id_t DiskManager::AllocatePage() {

  DiskFileMetaPage* meta_page = reinterpret_cast<DiskFileMetaPage*>(meta_data_);
  uint32_t i;

  char buffer[PAGE_SIZE]; // 4k bytes
  page_id_t physical_page_id;
  BitmapPage<PAGE_SIZE>* bitmap;
  uint32_t page_offset;

  if (meta_page->GetAllocatedPages() >= MAX_VALID_PAGE_ID) {
    return INVALID_PAGE_ID;
  }
  
  for (i = 0; i < meta_page->GetExtentNums(); i++) {
    // i for extent id
    if (meta_page->GetExtentUsedPage(i) < BITMAP_SIZE) { // 当前extent还有空闲的page
      meta_page->num_allocated_pages_++;
      meta_page->extent_used_page_[i]++; // 当前extent已经使用的page数+1
      
      if (meta_page->extent_used_page_[i] == 1) {
        meta_page->num_extents_++; //如果该扩展之前没有被使用过，则增加扩展数量的计数器
      }

      physical_page_id = i * (BITMAP_SIZE + 1/*bitmap page is 4kB size*/) + 1/*meta data page*/; // write a bitmap page 
      ReadPhysicalPage(physical_page_id, buffer); //读取bitmap page

      bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(buffer);
      if (bitmap->AllocatePage(page_offset)) {
        WritePhysicalPage(physical_page_id, buffer);
        return i * BITMAP_SIZE + page_offset; // logical page id
      } else {
        LOG(ERROR) << "Failed to allocate page in bitmap" << std::endl;
      }
    }
  } // end for

  meta_page->num_allocated_pages_++;
  meta_page->extent_used_page_[i] = 1; // new extent
  meta_page->num_extents_++;

  physical_page_id = i * (BITMAP_SIZE + 1) + 1;

  ReadPhysicalPage(physical_page_id, buffer);
  bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(buffer);
  if (bitmap->AllocatePage(page_offset)) {
    WritePhysicalPage(physical_page_id, buffer);
    return i * BITMAP_SIZE + page_offset;
  } else {
    LOG(ERROR) << "Failed to allocate page in bitmap" << std::endl;
  }

  return INVALID_PAGE_ID;
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {

  DiskFileMetaPage* meta_page = reinterpret_cast<DiskFileMetaPage*>(meta_data_);
  uint32_t extent_id = logical_page_id / BITMAP_SIZE; 
  uint32_t page_offset;

  if (extent_id >= meta_page->GetExtentNums()) { // 0 ~ extent_num - 1
    LOG(ERROR) << "Invalid extent id" << std::endl;
    return;
  }

  char buffer[PAGE_SIZE];
  page_id_t physical_page_id = extent_id * (BITMAP_SIZE + 1) + 1;
  ReadPhysicalPage(physical_page_id, buffer);

  BitmapPage<PAGE_SIZE>* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(buffer);

  page_offset = MapPageId(logical_page_id) - physical_page_id - 1;
  
  if (bitmap->DeAllocatePage(page_offset)) {
    WritePhysicalPage(physical_page_id, buffer);
    meta_page->num_allocated_pages_--;
    meta_page->extent_used_page_[extent_id]--;
    if (meta_page->extent_used_page_[extent_id] == 0) {
      meta_page->num_extents_--;
    }
  } else {
    LOG(ERROR) << "Failed to deallocate page in bitmap" << std::endl;
  }
}

/**
 * TODO: Student Implement
 * logical_page_id: 0123  4567  89... 
 * logical_page_id / BITMAP_SIZE = extent_id indicate which extent the page is located
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {

  char buffer[PAGE_SIZE];
  DiskFileMetaPage* meta_page = reinterpret_cast<DiskFileMetaPage*>(meta_data_);
  uint32_t extent_id = logical_page_id / BITMAP_SIZE;
  page_id_t physical_page_id = extent_id * (BITMAP_SIZE + 1) + 1;
  uint32_t page_offset;

  if (extent_id >= meta_page->GetExtentNums()) {
    LOG(ERROR) << "Invalid extent id" << std::endl;
    return false;
  }
  page_offset = MapPageId(logical_page_id) - physical_page_id - 1;
  ReadPhysicalPage(physical_page_id, buffer);
  BitmapPage<PAGE_SIZE>* bitmap = reinterpret_cast<BitmapPage<PAGE_SIZE>*>(buffer);
  return bitmap->IsPageFree(page_offset);

  return false;
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) { // page_id_t in bit count
  page_id_t extent_id = logical_page_id / BITMAP_SIZE;
  page_id_t physical_extent_entry = 1 + extent_id * (BITMAP_SIZE + 1);
  page_id_t physical_page_id = physical_extent_entry + 1 + (logical_page_id % BITMAP_SIZE);
  return physical_page_id;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}