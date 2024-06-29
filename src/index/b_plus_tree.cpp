#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  auto page = buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID);//该页存储所有索引的索引号及对应的根节点的页号
  if(page != nullptr){
    page_id_t tmp_root_id;
    auto tmp_page = reinterpret_cast<IndexRootsPage *>(page);
    if(tmp_page->GetRootId(index_id, &tmp_root_id)){
      root_page_id_ = tmp_root_id;//得到B+树的根页号
    }else{
      root_page_id_ = INVALID_PAGE_ID;//没有这个索引
    }
    buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
  }
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  if(!IsEmpty()){
    auto page = buffer_pool_manager_->FetchPage(current_page_id);
    if(page != nullptr){
      auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
      if(node->IsLeafPage()){//是叶子页，下面再没有孩子页了
        auto tmp_leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
        buffer_pool_manager_->DeletePage(current_page_id);
      }else{
        auto tmp_internal_node = reinterpret_cast<InternalPage *>(page->GetData());
        for(int i = 0; i < tmp_internal_node->GetSize(); i++){
          Destroy(tmp_internal_node->ValueAt(i));//递归删除孩子页
        }
        buffer_pool_manager_->DeletePage(current_page_id);
      }
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
    }
  }
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  if(root_page_id_ == INVALID_PAGE_ID){//空索引
    return true;
  }
  auto tmp_page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto tmp_node = reinterpret_cast<BPlusTreePage *>(tmp_page->GetData());
  if(tmp_node->GetSize() == 0){
    buffer_pool_manager_->UnpinPage(root_page_id_, false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
  return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  if(!IsEmpty()){//索引不为空
    auto tmp_res_page = FindLeafPage(key, root_page_id_);
    auto tmp_leaf_page = buffer_pool_manager_->FetchPage(tmp_res_page->GetPageId());//固定该页
    auto tmp_leaf_node = reinterpret_cast<BPlusTreeLeafPage *>(tmp_leaf_page->GetData());
    RowId tmp_res;
    int found = 0;
    tmp_leaf_page->RLatch();
    if(tmp_leaf_node->Lookup(key, tmp_res, processor_)){//在叶子页中找到
      found = 1;
      result.emplace_back(tmp_res);
    }else{//在叶子页中未找到
      found = 0;
    }
    tmp_leaf_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(tmp_res_page->GetPageId(), false);//释放该页
    if(found){
      return true;
    }else{
      return false;
    }
  }else{//索引为空
    return false;
  }
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  if(IsEmpty()){
    StartNewTree(key, value);
    return true;
  }else{
    return InsertIntoLeaf(key, value, transaction);
  }
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  auto page = buffer_pool_manager_->NewPage(root_page_id_);//得到一个新的页
  if(page){
    auto node = reinterpret_cast<LeafPage *>(page->GetData());
    //预留一些空间以免异常
    leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId));
    internal_max_size_ = (PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (processor_.GetKeySize() + sizeof(RowId)) - 1;
    node->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
    node->Insert(key, value, processor_);
    UpdateRootPageId(1);//新建了一个索引，应该在索引根页中插入
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  }else{
    LOG(ERROR)<<"out of memory"<<std::endl;
  }
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  if(IsEmpty()){//索引为空
    return false;
  }
  auto tmp_res_page = FindLeafPage(key, root_page_id_);
  auto page = buffer_pool_manager_->FetchPage(tmp_res_page->GetPageId());//固定该页
  auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  page->WLatch();
  RowId target_rowid;
  if(leaf_node->Lookup(key, target_rowid, processor_)){
    //原来就有
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(tmp_res_page->GetPageId(), false);
    return false;
  }else{
    //原来没有，可以插入
    int leaf_current_size = leaf_node->Insert(key, value, processor_);
    if(leaf_current_size >= leaf_max_size_){//插入后要分裂
      auto new_sibling = Split(leaf_node, transaction);
      //维护分裂后父页数据
      auto new_sibling_key = new_sibling->KeyAt(0);
      InsertIntoParent(leaf_node, new_sibling_key, new_sibling, transaction);
    }
    //插入后不分裂
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(tmp_res_page->GetPageId(), true);
    return true;
  }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  //把传入页的数据分一半到新申请的页中
  //该函数未维护分裂后父页数据
  auto old_page = buffer_pool_manager_->FetchPage(node->GetPageId());//固定旧页
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(new_page_id);//得到新页
  if(new_page == nullptr){
    LOG(ERROR)<<"out of memory"<<std::endl;
    return nullptr;
  }else{
    auto new_node = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_node->SetPageType(IndexPageType::INTERNAL_PAGE);
    new_node->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), internal_max_size_);
    node->MoveHalfTo(new_node, buffer_pool_manager_);//把node的后半段移到new_node（一定为空）的后半段，故相当于前半段
    buffer_pool_manager_->UnpinPage(new_page_id, true);//释放旧页
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);//释放新页
    return new_node;//返回新建节点指针
  }
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  //把传入页的数据分一半到新申请的页中
  //该函数未维护分裂后父页数据
  auto old_page = buffer_pool_manager_->FetchPage(node->GetPageId());//固定旧页
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(new_page_id);//得到新页
  if(new_page == nullptr){
    LOG(ERROR)<<"out of memory"<<std::endl;
    return nullptr;
  }else{
    auto new_node = reinterpret_cast<LeafPage *>(new_page->GetData());
    new_node->SetPageType(IndexPageType::LEAF_PAGE);
    new_node->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), leaf_max_size_);//new_node和node性质一样
    node->MoveHalfTo(new_node);
    //把叶子页连起来
    new_node->SetNextPageId(node->GetNextPageId());
    node->SetNextPageId(new_page_id);
    buffer_pool_manager_->UnpinPage(node->GetPageId(), true);//释放旧页
    buffer_pool_manager_->UnpinPage(new_page_id, true);//释放新页
    return new_node;//返回新建节点指针
  }
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  if(old_node->IsRootPage()){//老根分裂了，须创建新根
    auto new_page = buffer_pool_manager_->NewPage(root_page_id_);
    auto new_root = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_root->SetPageType(IndexPageType::INTERNAL_PAGE);
    new_root->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(new_root->GetPageId());
    new_node->SetParentPageId(new_root->GetPageId());
    buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
    UpdateRootPageId(0);//属于根的更新，因为有老根
  }else{//老节点非根
    auto parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
    auto parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
    int parent_current_size = parent->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    if(parent_current_size > internal_max_size_){//父节点递归分裂
      auto new_parent_sibling = Split(parent, transaction);
      auto key = new_parent_sibling->KeyAt(0);
      InsertIntoParent(parent, key, new_parent_sibling, transaction);
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    }else{//父节点不递归分裂
      old_node->SetParentPageId(parent->GetPageId());
      new_node->SetParentPageId(parent->GetPageId());
      buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if(IsEmpty()){
    return;
  }
  auto tmp_res_page = FindLeafPage(key, root_page_id_);
  auto page = buffer_pool_manager_->FetchPage(tmp_res_page->GetPageId());//固定该页
  auto leaf_node = reinterpret_cast<LeafPage *>(page->GetData());
  page->WLatch();
  int leaf_node_old_size = leaf_node->GetSize();
  int leaf_node_current_size = leaf_node->RemoveAndDeleteRecord(key, processor_);
  if(leaf_node_current_size < leaf_node_old_size){//找到并删了
    page->WUnlatch();
    bool should_be_deleted = false;
    if(leaf_node_current_size < leaf_node->GetMinSize()){//需要合并或分配
      should_be_deleted = CoalesceOrRedistribute(leaf_node, transaction);//会处理DeletePage的情况
    }
    if(!should_be_deleted){
      buffer_pool_manager_->UnpinPage(tmp_res_page->GetPageId(), true);
    }
  }else{//原来就没有
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(tmp_res_page->GetPageId(), false);
  }
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  if(IsEmpty()){//空索引
    return false;
  }
  if(node->GetSize() >= node->GetMinSize()) {
    return false;
  }
  if(node->IsRootPage()){
    if(node->GetSize() == 1){//根节点只有一个K-V对且Key无效
      if(AdjustRoot(node)){//应删
        buffer_pool_manager_->DeletePage(node->GetPageId());
        return true;
      }
    }
    return false;
  }
  auto parent_node_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent_node = reinterpret_cast<InternalPage *>(parent_node_page->GetData());
  int node_index = parent_node->ValueIndex(node->GetPageId());
  if(node_index == 0){//只有右兄弟
    auto sibling_node = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_node->ValueAt(1)));
    if(node->IsLeafPage() ? ((sibling_node->GetSize() + node->GetSize()) < node->GetMaxSize()) : ((sibling_node->GetSize() + node->GetSize()) <= node->GetMaxSize())){
      //可合并
      bool should_be_deleted = false;
      should_be_deleted = Coalesce(node, sibling_node, parent_node, 1, transaction);//会处理DeletePage的情况
      if(!should_be_deleted){
        buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);
      }
      return false;//node未被删除
    }else{
      //须分配
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(), false);
      Redistribute(sibling_node, node, 0);//维护父节点数据
      buffer_pool_manager_->UnpinPage(parent_node->ValueAt(1), true);
      return false;//node未被删除
    }
  }
  if(node_index == (parent_node->GetSize() - 1)){
    auto sibling_node = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_node->ValueAt(parent_node->GetSize() - 2)));
    if(node->IsLeafPage() ? ((sibling_node->GetSize() + node->GetSize()) < node->GetMaxSize()) : ((sibling_node->GetSize() + node->GetSize()) <= node->GetMaxSize())){
      //可合并
      bool should_be_deleted = false;
      should_be_deleted = Coalesce(sibling_node, node, parent_node, node_index, transaction);//会处理DeletePage的情况
      if(!should_be_deleted){
        buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);
      }
      buffer_pool_manager_->UnpinPage(parent_node->ValueAt(parent_node->GetSize() - 2), true);
      return true;//node被删除
    }else{
      //须分配
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(), false);
      Redistribute(sibling_node, node, 1);
      buffer_pool_manager_->UnpinPage(sibling_node->GetPageId(), true);
      return false;//node未被删除
    }
  }
  auto sibling_node_1 = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_node->ValueAt(node_index - 1)));
  auto sibling_node_2 = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_node->ValueAt(node_index + 1)));
  if(node->IsLeafPage() ? ((sibling_node_1->GetSize() + node->GetSize()) < node->GetMaxSize()) : ((sibling_node_1->GetSize() + node->GetSize()) <= node->GetMaxSize())){
    buffer_pool_manager_->UnpinPage(sibling_node_2->GetPageId(), false);
    //可合并
    bool should_be_deleted = false;
    should_be_deleted = Coalesce(sibling_node_1, node, parent_node, node_index, transaction);//会处理DeletePage的情况
    if(!should_be_deleted){
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(sibling_node_1->GetPageId(), true);
    return true;//node被删除
  }else if(node->IsLeafPage() ? ((sibling_node_2->GetSize() + node->GetSize()) < node->GetMaxSize()) : ((sibling_node_2->GetSize() + node->GetSize()) <= node->GetMaxSize())){
    buffer_pool_manager_->UnpinPage(sibling_node_1->GetPageId(), false);
    //可合并
    bool should_be_deleted = false;
    should_be_deleted = Coalesce(node, sibling_node_2, parent_node, node_index + 1, transaction);//会处理DeletePage的情况
    if(!should_be_deleted){
      buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);
    }
    return false;//node未被删除
  }else{
    buffer_pool_manager_->UnpinPage(sibling_node_2->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(node->GetParentPageId(), false);
    Redistribute(sibling_node_1, node, 1);
    buffer_pool_manager_->UnpinPage(sibling_node_1->GetPageId(), true);
    return false;
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index, Txn *transaction) {
  node->MoveAllTo(neighbor_node);//把node中的数据全部接到neighbor_node后面
  buffer_pool_manager_->DeletePage(node->GetPageId());
  parent->Remove(index);//更新父节点数据
  if(parent->GetSize() >= parent->GetMinSize()){
    return false;
  }else{//递归处理父节点过小情况
    return CoalesceOrRedistribute(parent, transaction);
  }
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index, Txn *transaction) {
  node->MoveAllTo(neighbor_node, parent->KeyAt(index), buffer_pool_manager_);
  buffer_pool_manager_->DeletePage(node->GetPageId());
  parent->Remove(index);
  if(parent->GetSize() >= parent->GetMinSize()){
    return false;
  }else{
    return CoalesceOrRedistribute(parent, transaction);
  }
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index){//index是标志neighbor_node和node谁先谁后的
  if(index){//neighbor_node在前，node在后
    neighbor_node->MoveLastToFrontOf(node);//要改父节点
    auto parent_node_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto parent_node = reinterpret_cast<InternalPage *>(parent_node_page->GetData());
    parent_node->SetKeyAt(parent_node->ValueIndex(node->GetPageId()), node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  }else{//neighbor_node在后，node在前
    neighbor_node->MoveFirstToEndOf(node);//要改父节点
    auto parent_node_page = buffer_pool_manager_->FetchPage(neighbor_node->GetParentPageId());
    auto parent_node = reinterpret_cast<InternalPage *>(parent_node_page->GetData());
    parent_node->SetKeyAt(parent_node->ValueIndex(neighbor_node->GetPageId()), neighbor_node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  }
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index){
  if(index){//neighbor_node在前，node在后
    auto parent_node_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto parent_node = reinterpret_cast<InternalPage *>(parent_node_page->GetData());
    neighbor_node->MoveLastToFrontOf(node, parent_node->KeyAt(parent_node->ValueIndex(node->GetPageId())), buffer_pool_manager_);
    parent_node->SetKeyAt(parent_node->ValueIndex(node->GetPageId()), node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  }else{//neighbor_node在后，node在前
    auto parent_node_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
    auto parent_node = reinterpret_cast<InternalPage *>(parent_node_page->GetData());
    neighbor_node->MoveFirstToEndOf(node, parent_node->KeyAt(parent_node->ValueIndex(neighbor_node->GetPageId())), buffer_pool_manager_);
    parent_node->SetKeyAt(parent_node->ValueIndex(neighbor_node->GetPageId()), neighbor_node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node){
  if(old_root_node->GetSize() == 1){
    if(old_root_node->IsLeafPage()){//整个索引就剩下一个键值
      return false;//这种情况是正常的，不应删
    }else{//应删
      auto tmp_root_node = reinterpret_cast<InternalPage *>(old_root_node);
      page_id_t root_page_id = tmp_root_node->RemoveAndReturnOnlyChild();
      auto new_root_node_page = buffer_pool_manager_->FetchPage(root_page_id);
      auto new_root_node = reinterpret_cast<BPlusTreePage *>(new_root_node_page->GetData());
      new_root_node->SetParentPageId(INVALID_PAGE_ID);//变成根了
      root_page_id_ = new_root_node->GetPageId();//新的根页号
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      UpdateRootPageId(0);
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  auto tmp_res_page = FindLeafPage(nullptr, root_page_id_, true);
  auto leaf_node_page = buffer_pool_manager_->FetchPage(tmp_res_page->GetPageId());//固定该页
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_node_page->GetData());
  buffer_pool_manager_->UnpinPage(tmp_res_page->GetPageId(), false);
  return IndexIterator(leaf_node->GetPageId(), buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  auto tmp_res_page = FindLeafPage(key, root_page_id_, false);
  auto leaf_node_page = buffer_pool_manager_->FetchPage(tmp_res_page->GetPageId());//固定该页
  auto leaf_node = reinterpret_cast<LeafPage *>(leaf_node_page->GetData());
  buffer_pool_manager_->UnpinPage(tmp_res_page->GetPageId(), false);
  return IndexIterator(leaf_node->GetPageId(), buffer_pool_manager_, leaf_node->KeyIndex(key, processor_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator(INVALID_PAGE_ID, buffer_pool_manager_, 0);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  auto tmp_page = buffer_pool_manager_->FetchPage(page_id);
  auto tmp_node = reinterpret_cast<BPlusTreePage *>(tmp_page->GetData());
  tmp_page->RLatch();
  if(tmp_node->IsLeafPage()){//传入的page_id对应的就是叶子页
    tmp_page->RUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, false);
    return tmp_page;
  }
  //传入的page_id对应的是内部页
  auto tmp_internal_node = reinterpret_cast<InternalPage *>(tmp_node);
  //考虑leftMost
  page_id_t tmp_child_id = (leftMost ? tmp_internal_node->ValueAt(0) : tmp_internal_node->Lookup(key, processor_));
  tmp_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return FindLeafPage(key, tmp_child_id, leftMost);//递归查找
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  auto header_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  if(insert_record){
    header_page->Insert(index_id_, root_page_id_);
  }else{
    header_page->Update(index_id_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

