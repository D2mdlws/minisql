#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) { 
  // Check if the bitmap page is full
  if (page_allocated_ == GetMaxSupportedSize()) {
    return false;
  }

  SetPageAllocated(next_free_page_);
  page_offset = next_free_page_;
  page_allocated_++;
  // Find the next free page
  bool flag = false;
  for (uint32_t i = next_free_page_ + 1; i < GetMaxSupportedSize(); i++) {
    if (IsPageFree(i)) {
      flag = true;
      next_free_page_ = i;
      return true;
    }
  }
  if (!flag) {
    // If no free page is found, set next_free_page_ to the maximum value (8 * MAX_CHARS
    next_free_page_ = GetMaxSupportedSize();
    return true;
  } else {
    return false;
  }
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  // Check if the page is already free
  if (IsPageFree(page_offset)) {
    return false;
  }
  else {
    // Mark the page as free
    SetPageFree(page_offset);
    page_allocated_--;
    if (page_offset < next_free_page_) {
      next_free_page_ = page_offset;
    }
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  // Check if the page is free
  return IsPageFreeLow(page_offset / 8, page_offset % 8);

}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  // Check if the bit is 0
  return (bytes[byte_index] & (1 << (7 - bit_index))) == 0;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::SetPageAllocated(uint32_t page_offset) {
  // Set the bit to 1
  bytes[page_offset / 8] |= (1 << (7 - (page_offset % 8)));
  return true;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::SetPageFree(uint32_t page_offset) {
  // Set the bit to 0
  bytes[page_offset / 8] &= ~(1 << (7 - (page_offset % 8)));
  return true;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;