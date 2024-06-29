#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)//节点有效数据的起始位置
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetKeySize(key_size);
  SetSize(0);//目前还没有任何KEY-PAGE_ID对
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {//得到内部节点的第index（从0开始）号属性
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {//修改内部节点的第index（从0开始）号属性
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {//得到内部节点的第index（从0开始）号孩子页号
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {//修改内部节点的第index（从0开始）号孩子页号
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {//找出该孩子页号是该内部节点中的第几号（从0开始）
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {//从src到dest
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int left = 1;
  int right = GetSize() - 1;
  int mid = 0;
  int tmp_res = 0;
  int res_index = INVALID_PAGE_ID;
  while(left <= right){
    mid = (left + right) / 2;
    tmp_res = KM.CompareKeys(KeyAt(mid), key);
    if(tmp_res < 0){
      left = mid + 1;
    }else if(tmp_res > 0){
      right = mid - 1;
    }else{
      return ValueAt(mid);
    }
  }
  res_index = ValueAt(left - 1);
  return res_index;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  //填充新根
  //只在老根溢出须创建新根时调用
  SetSize(2);//现在有两个K-V对
  SetValueAt(0, old_value);
  SetKeyAt(1, new_key);
  SetValueAt(1,new_value);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int tmp_size = GetSize();
  int tmp_index = ValueIndex(old_value);
  GenericKey* tmp_key = nullptr;
  page_id_t tmp_value = 0;
  for(int i = tmp_size - 1; i > tmp_index; i--){//逐个后移
    tmp_key = KeyAt(i);
    tmp_value = ValueAt(i);
    SetKeyAt(i + 1, tmp_key);
    SetValueAt(i + 1, tmp_value);
  }
  //插
  SetKeyAt(tmp_index + 1, new_key);
  SetValueAt(tmp_index + 1, new_value);
  SetSize(tmp_size + 1);
  return tmp_size + 1;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int tmp_size = GetSize();
  int tmp_num = tmp_size / 2;
  int tmp_start = 0;
  if(tmp_size % 2){
    tmp_start = tmp_size / 2 + 1;
  }else{
    tmp_start = tmp_size / 2;
  }
  recipient->CopyNFrom(PairPtrAt(tmp_start), tmp_num, buffer_pool_manager);
  SetSize(tmp_size - tmp_num);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  //谁调用就copy到谁那里
  int next_pos_index = GetSize();
  PairCopy(PairPtrAt(next_pos_index), src, size);//包括src
  page_id_t tmp_value = 0;
  for(int i = 0; i < size; i++){
    tmp_value = ValueAt(next_pos_index + i);//已经copy过去了
    auto page = buffer_pool_manager->FetchPage(tmp_value);//tmp_value就是需要改父指针的孩子的页号
    if(page != nullptr){
      auto node = reinterpret_cast<BPlusTreePage*>(page->GetData());
      node->SetParentPageId(GetPageId());
      buffer_pool_manager->UnpinPage(tmp_value, true);
    }
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  int tmp_size = GetSize();
  GenericKey* tmp_key = nullptr;
  page_id_t tmp_value = 0;
  if(index >= 0 && index < tmp_size){//判断边界条件
    for(int i = index; i < tmp_size - 1; i ++){//逐个前移
      tmp_key = KeyAt(i + 1);
      tmp_value = ValueAt(i + 1);
      SetKeyAt(i, tmp_key);
      SetValueAt(i, tmp_value);
    }
    SetSize(tmp_size - 1);
  }
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  page_id_t tmp_value = ValueAt(0);//得到唯一的孩子页号
  SetSize(0);
  return tmp_value;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  //本函数不负责维护父节点
  SetKeyAt(0, middle_key);//把我的第0号无效Key设为middle key
  int tmp_size = this->GetSize();//我的K-V对数
  recipient->CopyNFrom(PairPtrAt(0), tmp_size, buffer_pool_manager);//src是我
  SetSize(0);//把我的size设为0
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  //本函数不负责维护父节点
  SetKeyAt(0, middle_key);//把我的第0号无效Key设为middle key
  recipient->CopyNFrom(PairPtrAt(0), 1, buffer_pool_manager);
  Remove(0);
  //该函数执行完后，我的第0号无效Key为父节点新的middle key，但还没有更新到父节点上
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int next_pos_index = GetSize();
  SetKeyAt(next_pos_index, key);
  SetValueAt(next_pos_index, value);
  auto page = buffer_pool_manager->FetchPage(value);
  if(page != nullptr){
    auto node = reinterpret_cast<BPlusTreePage*>(page->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value, true);
  }
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  //本函数不负责维护父节点
  int last_index = GetSize() - 1;
  recipient->SetKeyAt(0, middle_key);//把我的第0号无效Key设为middle key
  recipient->CopyFirstFrom(ValueAt(last_index), buffer_pool_manager);
  recipient->SetKeyAt(0, KeyAt(last_index));//现在我的第0号无效Key存的是父节点新的middle key
  Remove(last_index);
  //该函数执行完后，我的第0号无效Key为父节点新的middle key，但还没有更新到父节点上
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int tmp_size = GetSize();
  GenericKey* tmp_key = nullptr;
  page_id_t tmp_value = 0;
  for(int i = tmp_size - 1; i >= 0; i--){//逐个后移
    tmp_key = KeyAt(i);
    tmp_value = ValueAt(i);
    SetKeyAt(i + 1, tmp_key);
    SetValueAt(i + 1, tmp_value);
  }
  SetValueAt(0, value);//插
  auto page = buffer_pool_manager->FetchPage(value);
  if(page != nullptr){
    auto node = reinterpret_cast<BPlusTreePage*>(page->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value, true);
  }
  IncreaseSize(1);
}
