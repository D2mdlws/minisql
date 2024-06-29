#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  return 4 + 4 + 4 + table_meta_pages_.size() * 8 + index_meta_pages_.size() * 8;
}

CatalogMeta::CatalogMeta() {
}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {

  if (init) {
    // page_id_t catalog_page_id_new;
    // auto page = buffer_pool_manager_->NewPage(catalog_page_id_new);
    // if (catalog_page_id_new != CATALOG_META_PAGE_ID) {
    //   LOG(ERROR) << "catalog meta page id: "<< catalog_page_id_new << std::endl;
    //   LOG(ERROR) << "Catalog page id is not correct";
    //
    // }
    catalog_meta_ = CatalogMeta::NewInstance(); // create a new catalog meta page
    next_table_id_ = 0;
    next_index_id_ = 0;
    return;
  } else {
    // load catalog meta page
    Page *catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    char* buf = catalog_meta_page->GetData();
    catalog_meta_ = CatalogMeta::DeserializeFrom(buf);
    // buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);


    // load table meta pages
    for (auto iter : catalog_meta_->table_meta_pages_) {
      LoadTable(iter.first, iter.second);
    }
    for (auto iter : catalog_meta_->index_meta_pages_) {
      LoadIndex(iter.first, iter.second);
    }
    next_index_id_ = catalog_meta_->GetNextIndexId();
    next_table_id_ = catalog_meta_->GetNextTableId();
  }
  FlushCatalogMetaPage();
}


CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {

  if (table_names_.find(table_name) != table_names_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }

  page_id_t page_id = 0;

  // create a new page to store table metadata
  Page* page = buffer_pool_manager_->NewPage(page_id);
  LOG(INFO) << "in CreateTable Function NEWPAGE_id " << page_id << std::endl;

  Schema *deepCopySchema = Schema::DeepCopySchema(schema);
  table_id_t table_id = next_table_id_;
  next_table_id_++;

  // create table heap
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, deepCopySchema, txn, log_manager_, lock_manager_);
  // create table metadata
  TableMetadata *table_meta = TableMetadata::Create(next_table_id_, table_name,table_heap->GetFirstPageId() , deepCopySchema);

  // createable info
  table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);

  // add to tables and table_names in catalog manager
  table_names_.emplace(table_name, next_table_id_);
  tables_.emplace(next_table_id_, table_info);
  // update catalog meta
  catalog_meta_->table_meta_pages_.emplace(next_table_id_, page_id);

  // write table metadata to page
  char *table_metadata_buf = page->GetData();
  table_meta->SerializeTo(table_metadata_buf);
  buffer_pool_manager_->UnpinPage(page_id, true);
  buffer_pool_manager_->FlushPage(page_id);

  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto itr = table_names_.find(table_name);
  if(itr == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  ASSERT(tables_.count(itr->second) > 0, "Find table in table_names_ but not in tables_");
  table_info = tables_[itr->second];
  return DB_SUCCESS;

}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {

  if (tables_.empty()) {
    return DB_TABLE_NOT_EXIST;
  }
  for (auto iter : tables_) {
    tables.push_back(iter.second);
  }
  ASSERT(tables_.size() == tables.size(), "All TableInfo should be in vector.");
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  if (index_names_.find(table_name) != index_names_.end() && index_names_[table_name].find(index_name) != index_names_[table_name].end()) {
    return DB_INDEX_ALREADY_EXIST;
  }


  // create index key_map_
  std::vector<uint32_t> key_map;
  for (auto &key : index_keys) {
    index_id_t index_id;
    if (tables_[table_names_[table_name]]->GetSchema()->GetColumnIndex(key, index_id) == DB_COLUMN_NAME_NOT_EXIST) {
      return DB_COLUMN_NAME_NOT_EXIST;
    }
    key_map.push_back(index_id);
  }

  // try to get schema from table_info
  Schema *key_schema = Schema::ShallowCopySchema(tables_[table_names_[table_name]]->GetSchema(), key_map);

  index_id_t index_id = next_index_id_;
  next_index_id_ ++;

  // create a new page to store index metadata
  page_id_t page_id = 0;
  Page* page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr) {
    return DB_FAILED;
  }

  // create index metadata
  IndexMetadata *index_meta = IndexMetadata::Create(next_index_id_, index_name, table_names_[table_name], key_map);
  // create index info
  index_info = IndexInfo::Create();
  index_info->Init(index_meta, tables_[table_names_[table_name]], buffer_pool_manager_);

  // write to page
  index_meta->SerializeTo(page->GetData());


  // add to indexes and index_names
  index_names_[table_name][index_name] = next_index_id_;
  indexes_[next_index_id_] = index_info;

  catalog_meta_->index_meta_pages_.emplace(next_index_id_, page_id);
  buffer_pool_manager_->UnpinPage(page_id, true);
  // update catalog meta
  buffer_pool_manager_->FlushPage(page_id);

  FlushCatalogMetaPage();

  return DB_SUCCESS;

}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const { // const identifier, cannot use [] operator

  if (index_names_.find(table_name) == index_names_.end() || index_names_.at(table_name).find(index_name) == index_names_.at(table_name).end()){
    return DB_INDEX_NOT_FOUND;
  }
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  index_id_t index_id = index_names_.at(table_name).at(index_name);
  if(indexes_.count(index_id) == 0) {
    return DB_FAILED;
  }
  index_info = indexes_.at(index_names_.at(table_name).at(index_name));


  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  if (index_names_.find(table_name) == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;
  }
  for (auto iter : index_names_.at(table_name)) {
    indexes.push_back(indexes_.at(iter.second));
  }
  if (indexes.empty()) {
    return DB_INDEX_NOT_FOUND;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t table_id = table_names_[table_name];
  if (DropTable(table_id) == DB_SUCCESS) {
    return DB_SUCCESS;
  }

  return DB_FAILED;
}

dberr_t CatalogManager::DropTable(table_id_t table_id) {
  if (tables_.find(table_id) == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  TableInfo *table_info_tobe_deleted = nullptr;
  // get table info
  auto error_type = GetTable(table_id, table_info_tobe_deleted);
  if (error_type != DB_SUCCESS) {
    return error_type;
  }

  // delete table meta pag
  auto page_id = catalog_meta_->table_meta_pages_[table_id];
  auto root_page_id = tables_[table_id]->GetRootPageId();
  auto table_heap = table_info_tobe_deleted->GetTableHeap();

  table_heap->FreeTableHeap();

  // delete table info
  LOG(INFO) << page_id << " in DropTable Function" << std::endl;

  catalog_meta_->DeleteTableMetaPage(buffer_pool_manager_, table_id);
  table_names_.erase(table_info_tobe_deleted->GetTableName());
  tables_.erase(table_id);


  delete table_info_tobe_deleted;

  // update catalog meta
  FlushCatalogMetaPage();
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  if (index_names_[table_name].find(index_name) == index_names_[table_name].end()) {
    return DB_INDEX_NOT_FOUND;
  }

  IndexInfo *index_info_tobe_deleted = nullptr;

  auto error_num = GetIndex(table_name, index_name, index_info_tobe_deleted);
  if (error_num != DB_SUCCESS) {
    return error_num;
  }

  auto bpindex = dynamic_cast<BPlusTreeIndex*>(index_info_tobe_deleted->GetIndex());
  auto bptree = bpindex->GetContainer();
  auto root_page_id = bptree.GetRootPageId();
  bptree.Destroy(root_page_id);


  index_id_t index_id = index_names_[table_name][index_name];
  catalog_meta_->DeleteIndexMetaPage(buffer_pool_manager_, index_id);

  index_names_[table_name].erase(index_name);
  indexes_.erase(index_id);
        delete index_info_tobe_deleted;
  FlushCatalogMetaPage();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  // meta_page->WLatch();
  catalog_meta_->SerializeTo(meta_page->GetData()); // write to meta page

  // buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
  dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  if (tables_.find(table_id) != tables_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }
  // table = tableinfo = tablemeta + tableheap

  TableMetadata *table_meta = nullptr;
  Page* page = buffer_pool_manager_->FetchPage(page_id);
  TableMetadata::DeserializeFrom(page->GetData(), table_meta);
  ASSERT(table_meta != nullptr, "Unable to deserialize table_meta_data");
  buffer_pool_manager_->UnpinPage(page_id, false);

  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), table_meta->GetSchema(), log_manager_, lock_manager_);
  TableInfo *table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);

  // add to tables and table_names and update catalog meta
  table_names_.insert({table_info->GetTableName(), table_info->GetTableId()});
  tables_.insert({table_info->GetTableId(), table_info});

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  if (indexes_.find(index_id) != indexes_.end()) {
    return DB_INDEX_ALREADY_EXIST;
  }
  // index = indexinfo = indexmeta + tableinfo + key_schema + index

  IndexMetadata *index_meta = nullptr;
  Page* page = buffer_pool_manager_->FetchPage(page_id);
  char* buf = page->GetData();
  IndexMetadata::DeserializeFrom(buf, index_meta);
  buffer_pool_manager_->UnpinPage(page_id, false);
  auto table_name = tables_[index_meta->GetTableId()]->GetTableName();
  index_names_[table_name].emplace(index_meta->GetIndexName(), index_id);

  table_id_t table_id = index_meta->GetTableId();

  TableInfo *table_info = tables_[table_id];
  IndexInfo *index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  indexes_.emplace(index_id, index_info);

  catalog_meta_->index_meta_pages_.emplace(index_id, page_id);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if (tables_.find(table_id) == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_[table_id];
  return DB_SUCCESS;
}