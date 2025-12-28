#include "table/table.h"

#include "table/table_page.h"

namespace huadb {

Table::Table(BufferPool &buffer_pool, LogManager &log_manager, oid_t oid, oid_t db_oid, ColumnList column_list,
             bool new_table, bool is_empty)
    : buffer_pool_(buffer_pool),
      log_manager_(log_manager),
      oid_(oid),
      db_oid_(db_oid),
      column_list_(std::move(column_list)) {
  if (new_table || is_empty) {
    first_page_id_ = NULL_PAGE_ID;
  } else {
    first_page_id_ = 0;
  }
}

Rid Table::InsertRecord(std::shared_ptr<Record> record, xid_t xid, cid_t cid, bool write_log) {
  if (record->GetSize() > MAX_RECORD_SIZE) {
    throw DbException("Record size too large: " + std::to_string(record->GetSize()));
  }

  // 当 write_log 参数为 true 时开启写日志功能
  // 在插入记录时增加写 InsertLog 过程
  // 在创建新的页面时增加写 NewPageLog 过程
  // 设置页面的 page lsn
  // LAB 2 BEGIN
  if (write_log) {
    // InsertLog
  }

  // 使用 buffer_pool_ 获取页面
  std::unique_ptr<TablePage> current_page = nullptr;
  // 使用 TablePage 类操作记录页面

  // 如果 first_page_id_ 为 NULL_PAGE_ID，说明表还没有页面，需要创建新页面
  if (first_page_id_ == NULL_PAGE_ID) {
    current_page = std::make_unique<TablePage>(buffer_pool_.NewPage(db_oid_, oid_, 0));
    current_page->Init();
    first_page_id_ = 0;
  } else {
    current_page = std::make_unique<TablePage>(buffer_pool_.GetPage(db_oid_, oid_, first_page_id_));
  }

  // 遍历表的页面，判断页面是否有足够的空间插入记录，如果没有则通过 buffer_pool_ 创建新页面
  pageid_t current_page_id = first_page_id_;
  pageid_t next_page_id = NULL_PAGE_ID;
  while (current_page->GetFreeSpaceSize() < record->GetSize()) {
    next_page_id = current_page->GetNextPageId();
    if (next_page_id == NULL_PAGE_ID) {
      current_page_id = current_page_id + 1;
      current_page->SetNextPageId(current_page_id);
      current_page = std::make_unique<TablePage>(buffer_pool_.NewPage(db_oid_, oid_, current_page_id));
      // 创建新页面时需设置前一个页面的 next_page_id，并将新页面初始化
      current_page->Init();
    } else {
      current_page_id = next_page_id;
      current_page = std::make_unique<TablePage>(buffer_pool_.GetPage(db_oid_, oid_, current_page_id));
    }
  }

  // 找到空间足够的页面后，通过 TablePage 插入记录
  current_page->InsertRecord(record, xid, cid);
  // 返回插入记录的 rid
  auto rid = record->GetRid();
  // LAB 1 BEGIN
  return rid;
}

void Table::DeleteRecord(const Rid &rid, xid_t xid, bool write_log) {
  // 增加写 DeleteLog 过程
  // 设置页面的 page lsn
  // LAB 2 BEGIN

  // 使用 TablePage 操作页面
  // LAB 1 BEGIN
  std::unique_ptr<TablePage> current_page = std::make_unique<TablePage>(buffer_pool_.GetPage(db_oid_, oid_, rid.page_id_));
  current_page->DeleteRecord(rid.slot_id_, xid);
}

Rid Table::UpdateRecord(const Rid &rid, xid_t xid, cid_t cid, std::shared_ptr<Record> record, bool write_log) {
  DeleteRecord(rid, xid, write_log);
  return InsertRecord(record, xid, cid, write_log);
}

void Table::UpdateRecordInPlace(const Record &record) {
  auto rid = record.GetRid();
  auto table_page = std::make_unique<TablePage>(buffer_pool_.GetPage(db_oid_, oid_, rid.page_id_));
  table_page->UpdateRecordInPlace(record, rid.slot_id_);
}

pageid_t Table::GetFirstPageId() const { return first_page_id_; }

oid_t Table::GetOid() const { return oid_; }

oid_t Table::GetDbOid() const { return db_oid_; }

const ColumnList &Table::GetColumnList() const { return column_list_; }

}  // namespace huadb
