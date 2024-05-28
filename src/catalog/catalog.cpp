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

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
//    ASSERT(false, "Not Implemented yet");
  if (init) {
    catalog_meta_ = CatalogMeta::NewInstance(); // create a new catalog meta page
    // FlushCatalogMetaPage();
  } else {
    // load catalog meta page
    catalog_meta_ = CatalogMeta::DeserializeFrom(buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID)->GetData());

    // load table meta pages
    for (auto iter : catalog_meta_->table_meta_pages_) {
      TableMetadata *table_meta;
      TableMetadata::DeserializeFrom(buffer_pool_manager_->FetchPage(iter.second)->GetData(), table_meta);

      TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), table_meta->GetSchema(), log_manager_, lock_manager_);
      TableInfo *table_info = TableInfo::Create();
      table_info->Init(table_meta, table_heap);
      tables_.emplace(iter.first, table_info);
      table_names_.emplace(table_meta->GetTableName(), iter.first);
    }

    // load index meta pages
    for (auto iter : catalog_meta_->index_meta_pages_) {
      IndexMetadata *index_meta;
      IndexMetadata::DeserializeFrom(buffer_pool_manager_->FetchPage(iter.second)->GetData(), index_meta);

      // create indexInfo 1. meta_data 2. table_info 3. key_schema 4. index
      
      IndexInfo *index_info = IndexInfo::Create();
      index_info->Init(index_meta, tables_[index_meta->GetTableId()], buffer_pool_manager_);
      indexes_.emplace(iter.first, index_info);
      
      if (index_names_.find(tables_[index_meta->GetTableId()]->GetTableName()) != index_names_.end()) {
        index_names_.emplace(index_meta->GetIndexName(), iter.first);
      } else {
        index_names_[tables_[index_meta->GetTableId()]->GetTableName()].emplace(index_meta->GetIndexName(), iter.first);
      }
    }

  }
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
  Page* page = buffer_pool_manager_->NewPage(page_id); // create a new page to store table metadata
  Schema *deepCopySchema = Schema::DeepCopySchema(schema);

  // create table metadata
  TableMetadata *table_meta = TableMetadata::Create(catalog_meta_->GetNextTableId(), table_name, page_id, deepCopySchema);
  // create table heap
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, deepCopySchema, txn, log_manager_, lock_manager_);

  // write to page
  page->WLatch();
  table_meta->SerializeTo(page->GetData());
  page->WUnlatch();
  // unpin page
  buffer_pool_manager_->UnpinPage(page_id, true);

  // create table info
  table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);

  // add to tables and table_names
  tables_.emplace(table_meta->GetTableId(), table_info);
  table_names_.emplace(table_name, table_meta->GetTableId());
  // update catalog meta

  catalog_meta_->table_meta_pages_.emplace(table_meta->GetTableId(), page_id);
  Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  meta_page->WLatch();
  catalog_meta_->SerializeTo(meta_page->GetData()); // write to meta page
  meta_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_[table_names_[table_name]];
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
    tables_[table_names_[table_name]]->GetSchema()->GetColumnIndex(key, index_id);
    key_map.push_back(index_id);
  }

  page_id_t page_id = 0;
  Page* page = buffer_pool_manager_->NewPage(page_id); // create a new page to store index metadata

  // create index metadata
  IndexMetadata *index_meta = IndexMetadata::Create(catalog_meta_->GetNextIndexId(), index_name, table_names_[table_name], key_map);
  // create index info
  index_info = IndexInfo::Create();
  index_info->Init(index_meta, tables_[table_names_[table_name]], buffer_pool_manager_);
  
  // write to page
  page->WLatch();
  index_meta->SerializeTo(page->GetData());
  page->WUnlatch();
  // unpin page
  buffer_pool_manager_->UnpinPage(page_id, true);

  // add to indexes and index_names
  indexes_.emplace(index_meta->GetIndexId(), index_info);
  index_names_[table_name].emplace(index_name, index_meta->GetIndexId());
  catalog_meta_->index_meta_pages_.emplace(index_meta->GetIndexId(), page_id);

  // update catalog meta
  Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  meta_page->WLatch();
  catalog_meta_->SerializeTo(meta_page->GetData()); // write to meta page
  meta_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
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
  // delete table meta page
  buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_[table_id]);
  catalog_meta_->table_meta_pages_.erase(table_id);
  // delete table info
  delete tables_[table_id];
  tables_.erase(table_id);
  // delete table name
  for (auto iter = table_names_.begin(); iter != table_names_.end(); iter++) {
    if (iter->second == table_id) {
      table_names_.erase(iter);
      break;
    }
  }
  // update catalog meta
  Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  meta_page->WLatch();
  catalog_meta_->SerializeTo(meta_page->GetData()); // write to meta page
  meta_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
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

  index_id_t index_id = index_names_[table_name][index_name];

  buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_[index_id]);
  index_names_[table_name].erase(index_name);
  indexes_.erase(index_id);
  catalog_meta_->index_meta_pages_.erase(index_id);

  Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  meta_page->WLatch();
  catalog_meta_->SerializeTo(meta_page->GetData()); // write to meta page
  meta_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  meta_page->WLatch();
  catalog_meta_->SerializeTo(meta_page->GetData()); // write to meta page
  meta_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  if (buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID)) {
    return DB_SUCCESS;
  }
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  if (tables_.find(table_id) != tables_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }
  // table = tableinfo = tablemeta + tableheap

  TableMetadata *table_meta;
  Page* page = buffer_pool_manager_->FetchPage(page_id);
  TableMetadata::DeserializeFrom(page->GetData(), table_meta);
  buffer_pool_manager_->UnpinPage(page_id, false);

  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), table_meta->GetSchema(), log_manager_, lock_manager_);
  TableInfo *table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);

  // add to tables and table_names and update catalog meta
  tables_.emplace(table_id, table_info);
  table_names_.emplace(table_meta->GetTableName(), table_id); // 
  catalog_meta_->table_meta_pages_.emplace(table_id, page_id);

  Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  meta_page->WLatch();
  catalog_meta_->SerializeTo(meta_page->GetData()); // write to meta page
  meta_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

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

  IndexMetadata *index_meta;
  Page* page = buffer_pool_manager_->FetchPage(page_id);
  IndexMetadata::DeserializeFrom(page->GetData(), index_meta);
  buffer_pool_manager_->UnpinPage(page_id, false);
  table_id_t table_id = index_meta->GetTableId();

  TableInfo *table_info = tables_[table_id];
  index_names_[table_info->GetTableName()].emplace(index_meta->GetIndexName(), index_id);
  IndexInfo *index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);
  indexes_.emplace(index_id, index_info);
  catalog_meta_->index_meta_pages_.emplace(index_id, page_id);

  Page *meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  meta_page->WLatch();
  catalog_meta_->SerializeTo(meta_page->GetData()); // write to meta page
  meta_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);


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