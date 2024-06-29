#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
//键为属性，值为RowId
/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetKeySize(key_size);
  SetSize(0);
  SetPageType(IndexPageType :: LEAF_PAGE);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  //返回第一个Key大于等于传入key的位置（从0开始）
  int left = 0;
  int right = GetSize() - 1;
  int mid = 0;
  int tmp_res = 0;
  int res_index = -1;//处理传入key非常大的情况
  while(left <= right){
    mid = (left + right) / 2;
    tmp_res = KM.CompareKeys(KeyAt(mid), key);
    if(tmp_res < 0){
      left = mid + 1;
    }else if(tmp_res > 0){
      res_index = mid;
      right = mid - 1;
    }else{
      return mid;
    }
  }
  return res_index;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int tmp_size = GetSize();
  int target_index = KeyIndex(key, KM);//找到第一个
  if(target_index == -1){
    SetKeyAt(tmp_size, key);
    SetValueAt(tmp_size, value);//直接加到后面
  }else{
    if(KM.CompareKeys(KeyAt(target_index), key) == 0){//叶中已有
      return -1;//保持unique
    }
    //逐个后移
    PairCopy(PairPtrAt(target_index + 1), PairPtrAt(target_index), tmp_size - target_index);
    //插
    SetKeyAt(target_index, key);
    SetValueAt(target_index, value);
  }
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int tmp_size = GetSize();
  int tmp_num = tmp_size / 2;
  int tmp_start = 0;
  if(tmp_size % 2){
    tmp_start = tmp_size / 2 + 1;
  }else{
    tmp_start = tmp_size / 2;
  }
  recipient->CopyNFrom(PairPtrAt(tmp_start), tmp_num);
  SetSize(tmp_size - tmp_num);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  //谁调用就copy到谁那里
  int next_pos_index = GetSize();//下一个可放的位置
  PairCopy(PairPtrAt(next_pos_index), src, size);//包括src
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  //此时的value指RowId
  int left = 0;
  int right = GetSize() - 1;
  int mid = 0;
  int tmp_res = 0;
  while(left <= right){
    mid = (left + right) / 2;
    tmp_res = KM.CompareKeys(KeyAt(mid), key);
    if(tmp_res < 0){
      left = mid + 1;
    }else if(tmp_res > 0) {
      right = mid - 1;
    }else{
      value = ValueAt(mid);
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int tmp_size = GetSize();
  int left = 0;
  int right = tmp_size - 1;
  int mid = 0;
  int tmp_res = 0;
  int tmp_index = -1;
  while(left <= right){
    mid = (left + right) / 2;
    tmp_res = KM.CompareKeys(KeyAt(mid), key);
    if(tmp_res < 0){
      left = mid + 1;
    }else if(tmp_res > 0) {
      right = mid - 1;
    }else{
      tmp_index = mid;
      break;
    }
  }
  if(tmp_index == -1){//没找到
    return GetSize();//立即返回
  }
  //逐个前移
  PairCopy(PairPtrAt(tmp_index), PairPtrAt(tmp_index + 1), tmp_size - tmp_index - 1);
  SetSize(tmp_size - 1);
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  recipient->CopyNFrom(PairPtrAt(0), GetSize());//src是我
  recipient->SetNextPageId(GetNextPageId());//更新接收方的NextPageId
  SetSize(0);//把我的size设为0
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  int tmp_size = GetSize();
  recipient->CopyLastFrom(KeyAt(0), ValueAt(0));
  PairCopy(PairPtrAt(0), PairPtrAt(1), tmp_size - 1);//逐个前移，相当于删除第一个键值对
  SetSize(tmp_size - 1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  int tmp_size = GetSize();
  SetKeyAt(tmp_size, key);
  SetValueAt(tmp_size, value);
  SetSize(tmp_size + 1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  int tmp_size = GetSize();
  recipient->CopyFirstFrom(KeyAt(tmp_size - 1), ValueAt(tmp_size - 1));
  SetSize(tmp_size - 1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  int tmp_size = GetSize();
  PairCopy(PairPtrAt(1), PairPtrAt(0), tmp_size);//逐个后移，空出第0号位置
  SetKeyAt(0, key);
  SetValueAt(0, value);
  SetSize(tmp_size + 1);
}
