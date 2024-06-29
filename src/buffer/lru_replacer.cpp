#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) : num_pages_(num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (lru_list_.empty()) {
    return false;
  } else{
    unordered_set<int>::iterator it = lru_list_.begin();
    for (uint32_t i = 0 ; i < lru_list_.size(); i++, it++) {
      if (i == lru_list_.size() - 1) {
        *frame_id = *it;
        lru_list_.erase(it);
        return true;
      }
    }
  }
  return false;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  lru_list_.erase(frame_id);
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if (lru_list_.size() >= num_pages_) {
    return;
  }
  lru_list_.insert(frame_id);
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list_.size();
}