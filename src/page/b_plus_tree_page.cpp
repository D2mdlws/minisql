#include "page/b_plus_tree_page.h"
#include <cmath>
/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
/**
 * TODO: Student Implement
 */
bool BPlusTreePage::IsLeafPage() const {
  if(page_type_ == IndexPageType :: LEAF_PAGE){//是叶子节点
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
bool BPlusTreePage::IsRootPage() const {
  if(parent_page_id_ == INVALID_PAGE_ID){//是根节点
    return true;
  }
  return false;
}

/**
 * TODO: Student Implement
 */
void BPlusTreePage::SetPageType(IndexPageType page_type) {
  page_type_ = page_type;
}

int BPlusTreePage::GetKeySize() const {
  return key_size_;//当前索引属性的长度
}

void BPlusTreePage::SetKeySize(int size) {
  key_size_ = size;
}

/*
 * Helper methods to get/set size (number of key/value pairs stored in that
 * page)
 */
int BPlusTreePage::GetSize() const {
  return size_;//当前结点中存储Key-Value键值对的数量
}

void BPlusTreePage::SetSize(int size) {
  size_ = size;
}

void BPlusTreePage::IncreaseSize(int amount) {
  size_ += amount;
}

/*
 * Helper methods to get/set max size (capacity) of the page
 */
/**
 * TODO: Student Implement
 */
int BPlusTreePage::GetMaxSize() const {
  return max_size_;//当前结点最多能够容纳Key-Value键值对的数量
}

/**
 * TODO: Student Implement
 */
void BPlusTreePage::SetMaxSize(int size) {
  max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
/**
 * TODO: Student Implement
 */
int BPlusTreePage::GetMinSize() const {
  int tmp_min = 0;
  if(this->IsRootPage()){//根最少有两个孩子
    tmp_min = 2;
  }else if(this->IsLeafPage()){
    //叶节点最少容纳的K-V对数为(MaxSize-1)除以2所得结果的上取整
    //叶节点最多容纳的K-V对数为(MaxSize-1)
    tmp_min = int(ceil((max_size_ * 1.0 - 1) / 2));
  }else{
    //非根非叶节点最少容纳的K-V对数为MaxSize除以2所得结果的上取整
    //非根非叶节点最多容纳的K-V对数为MaxSize
    tmp_min = int(ceil((max_size_ * 1.0) / 2));
  }
  return tmp_min;
}

/*
 * Helper methods to get/set parent page id
 */
/**
 * TODO: Student Implement
 */
page_id_t BPlusTreePage::GetParentPageId() const {
  return parent_page_id_;
}

void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) {
  parent_page_id_ = parent_page_id;
}

/*
 * Helper methods to get/set self page id
 */
page_id_t BPlusTreePage::GetPageId() const {
  return page_id_;
}

void BPlusTreePage::SetPageId(page_id_t page_id) {
  page_id_ = page_id;
}

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) {
  lsn_ = lsn;
}
