#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"

extern "C" {
  int yyparse(void);
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }

  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }

  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {

    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  if (current_db_.empty()) {
    cout << "No database!" << endl;
    return DB_FAILED;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  if (ast->type_ == kNodeSelect)
    delete planner.plan_->OutputSchema();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (db_name == current_db_)
    current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  if(current_db_.empty()){
    std::cout << "No database!" << endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  auto column_list = ast->child_->next_;
  vector<string> primary_keys;
  vector<string> unique_attributes;
  vector<string> column_names;
  vector<TypeId> column_types;
  vector<int> attribute_unique;
  vector<int> attribute_length;
  vector<int> column_ids;
  int cnt = 0;
  for(auto i = column_list->child_; i != nullptr; i = i->next_){
    if(i->type_ == kNodeColumnDefinition){
      attribute_unique.emplace_back(i->val_ != nullptr);
      column_names.emplace_back(i->child_->val_);
      column_ids.emplace_back(cnt++);
      string tmp_column_type = i->child_->next_->val_;
      if(tmp_column_type == "int"){
        column_types.emplace_back(kTypeInt);
        attribute_length.emplace_back(0);
      }else if(tmp_column_type == "float"){
        column_types.emplace_back(kTypeFloat);
        attribute_length.emplace_back(0);
      }else if(tmp_column_type == "char"){
        string tmp_length_string = i->child_->next_->child_->val_;
        if(tmp_length_string.find(".") != string :: npos){
          LOG(ERROR) << "Invalid char length: " << tmp_length_string;
          return DB_FAILED;
        }
        int tmp_length = atoi(i->child_->next_->child_->val_);
        if(tmp_length < 0){
          LOG(ERROR) << "Invalid char length: " << tmp_length;
          return DB_FAILED;
        }
        column_types.emplace_back(kTypeChar);
        attribute_length.emplace_back(tmp_length);
      }else{
        LOG(ERROR) << "Invalid column type: " << tmp_column_type;
        return DB_FAILED;
      }
    }else if(i->type_ == kNodeColumnList){
      for(auto j = i->child_; j != nullptr; j = j->next_){
        primary_keys.emplace_back(j->val_);
      }
    }
  }
  vector<Column *> columns;
  bool is_manage = false;
  for(int i = 0; i < column_names.size(); i++){
    if(find(primary_keys.begin(), primary_keys.end(), column_names[i]) != primary_keys.end()){
      if(column_types[i] == kTypeChar){
        Column* new_column = new Column(column_names[i], column_types[i], attribute_length[i], i, false, true);
        columns.push_back(new_column);
        is_manage = true;
      }else{
        Column* new_column = new Column(column_names[i], column_types[i], i, false, true);
        columns.push_back(new_column);
      }
    }else{
      if(column_types[i] == kTypeChar){
        Column* new_column = new Column(column_names[i], column_types[i], attribute_length[i], i, false, attribute_unique[i]);
        columns.push_back(new_column);
        is_manage = true;
        if(attribute_unique[i]){
          unique_attributes.push_back(column_names[i]);
        }
      }else{
        Column* new_column = new Column(column_names[i], column_types[i], i, false, attribute_unique[i]);
        columns.push_back(new_column);
      }
    }
  }
  Schema *schema = new Schema(columns, is_manage);
  TableInfo *table_info;
  dberr_t dberr = context->GetCatalog()->CreateTable(table_name, schema, context->GetTransaction(), table_info);
  if(dberr != DB_SUCCESS){
    return dberr;
  }
  IndexInfo *index_info;
  if(primary_keys.size()){
    dberr = context->GetCatalog()->CreateIndex(table_info->GetTableName(), table_name + "_PRIMARYKEY_INDEX", primary_keys, context->GetTransaction(), index_info, "bptree");
    if(dberr != DB_SUCCESS){
      return dberr;
    }
  }
  for(auto i : unique_attributes) {
    string index_name = "UNIQUE_" + i + "_" + "ON_" + table_name;
    dberr = context->GetCatalog()->CreateIndex(table_name, index_name, unique_attributes, context->GetTransaction(), index_info, "bptree");
    if(dberr != DB_SUCCESS){
      return dberr;
    }
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if(current_db_.empty()){
    std::cout << "No database!" << endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->val_;
  std::vector<IndexInfo *> indexes;
  dberr_t dberr = context->GetCatalog()->GetTableIndexes(table_name, indexes);
  if(dberr != DB_SUCCESS){
    return dberr;
  }
  for(auto tmp_index : indexes){
    dberr = context->GetCatalog()->DropIndex(table_name, tmp_index->GetIndexName());
    if(dberr != DB_SUCCESS){
      return dberr;
    }
  }
  dberr = context->GetCatalog()->DropTable(table_name);
  if(dberr != DB_SUCCESS){
    return dberr;
  }
  cout << "Drop table " << table_name << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if(current_db_.empty()){
    std::cout << "No database!" << endl;
    return DB_FAILED;
  }
  std::vector<TableInfo *> tables;
  dberr_t dberr = context->GetCatalog()->GetTables(tables);
  if(dberr != DB_SUCCESS){
    return dberr;
  }
  vector<vector<IndexInfo *>> indexes_on_tables;
  for(auto tmp_table : tables){
    string table_name = tmp_table->GetTableName();
    std::vector<IndexInfo *> indexes;
    dberr = context->GetCatalog()->GetTableIndexes(table_name, indexes);
    if(dberr != DB_SUCCESS){
      return dberr;
    }
    indexes_on_tables.emplace_back(indexes);
  }
  for(int i = 0; i < tables.size(); i++){
    std::cout << "@ table \"" << tables[i]->GetTableName() << "\", we have indexes: " << endl;
    for(auto tmp_index : indexes_on_tables[i]){
      cout << "    " << tmp_index->GetIndexName() << " on columns: ";
      for(auto tmp_column : tmp_index->GetIndexKeySchema()->GetColumns()){
        cout << " [ " << tmp_column->GetName() << " ] ";
      }
      cout << endl;
    }
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if(current_db_.empty()){
    std::cout << "No database!" << endl;
    return DB_FAILED;
  }
  string table_name = ast->child_->next_->val_;
  TableInfo *table_info;
  dberr_t dberr = context->GetCatalog()->GetTable(table_name, table_info);
  if(dberr != DB_SUCCESS){
    return dberr;
  }
  string index_name = ast->child_->val_;
  string index_type;
  vector<string> column_names;
  for(auto i = ast->child_->next_->next_->child_; i != nullptr; i = i->next_){
    column_names.emplace_back(i->val_);
  }
  if(ast->child_->next_->next_->next_ != nullptr){
    index_type = string(ast->child_->next_->next_->next_->child_->val_);
  }else{
    index_type = "bptree";
  }
  IndexInfo *index_info;
  dberr = context->GetCatalog()->CreateIndex(table_name, index_name, column_names, context->GetTransaction(), index_info, index_type);
  if(dberr != DB_SUCCESS){
    return dberr;
  }
  for(auto row = table_info->GetTableHeap()->Begin(context->GetTransaction()); row != table_info->GetTableHeap()->End(); row++){
    auto row_id = row->GetRowId();
    vector<Field> fields;
    for(auto tmp_column : index_info->GetIndexKeySchema()->GetColumns()){
      fields.emplace_back(*(row->GetField(tmp_column->GetTableInd())));
    }
    Row row_index(fields);
    dberr = index_info->GetIndex()->InsertEntry(row_index, row_id, context->GetTransaction());
    if(dberr != DB_SUCCESS){
      return dberr;
    }
  }
  std::cout << "Create index " << index_name << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if(current_db_.empty()){
    std::cout << "No database!" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  dberr_t dberr = context->GetCatalog()->GetTables(tables);
  if(dberr != DB_SUCCESS){
    return dberr;
  }
  string index_name = ast->child_->val_;
  string table_name = "";
  for(auto tmp_table : tables){
    IndexInfo *index_info;
    dberr = context->GetCatalog()->GetIndex(tmp_table->GetTableName(), index_name, index_info);
    if(dberr == DB_SUCCESS){
      table_name = tmp_table->GetTableName();
      break;
    }else{
      return dberr;
    }
  }
  if(table_name == ""){
    std::cout << "Index not found!" << endl;
    return DB_INDEX_NOT_FOUND;
  }
  dberr = context->GetCatalog()->DropIndex(table_name, index_name);
  if(dberr != DB_SUCCESS){
    return dberr;
  }
  std::cout << "Drop index " << index_name << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string file_name = ast->child_->val_;
  ifstream f(file_name, ios::in);
  std::vector<std::string> exec_file_cmds;
  std::vector<char> cmd;
  char tmp_char;
  if(f.is_open()){
    while(f.get(tmp_char)){
      cmd.emplace_back(tmp_char);
      if(tmp_char == ';'){
        f.get(tmp_char);
        string tmp_cmd = "";
        for(auto c : cmd){
          tmp_cmd += c;
        }
        exec_file_cmds.emplace_back(tmp_cmd);
        YY_BUFFER_STATE bp = yy_scan_string(tmp_cmd.c_str());
        yy_switch_to_buffer(bp);
        MinisqlParserInit();
        yyparse();
        auto result = Execute(MinisqlGetParserRootNode());
        MinisqlParserFinish();
        yy_delete_buffer(bp);
        yylex_destroy();
        ExecuteInformation(result);
        if(result == DB_QUIT){
          break;
        }
        cmd.clear();
      }
    }
    f.close();
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
  return DB_QUIT;
}
