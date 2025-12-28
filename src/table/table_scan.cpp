#include "table/table_scan.h"

#include <iostream>

#include "table/table_page.h"

namespace huadb {

TableScan::TableScan(BufferPool &buffer_pool, std::shared_ptr<Table> table, Rid rid)
    : buffer_pool_(buffer_pool), table_(std::move(table)), rid_(rid) {}

std::shared_ptr<Record> TableScan::GetNextRecord(xid_t xid, IsolationLevel isolation_level, cid_t cid,
                                                 const std::unordered_set<xid_t> &active_xids) {
  // 根据事务隔离级别及活跃事务集合，判断记录是否可见
  // LAB 3 BEGIN

  // 每次调用读取一条记录
  // 读取时更新 rid_ 变量，避免重复读取
  // 扫描结束时，返回空指针
  std::shared_ptr<Record> record = nullptr;
  while (true) {
    // 注意处理扫描空表的情况（rid_.page_id_ 为 NULL_PAGE_ID）
    if (rid_.page_id_ == NULL_PAGE_ID) {
      rid_.slot_id_ = 0;
      break;
    }

    std::unique_ptr<TablePage> current_page =
        std::make_unique<TablePage>(buffer_pool_.GetPage(table_->GetDbOid(), table_->GetOid(), rid_.page_id_));

    record = current_page->GetRecord(rid_, table_->GetColumnList());
    if (record) {
      rid_.slot_id_++;
      // 不再返回已经删除的数据
      if (record->IsDeleted()) {
        continue;
      } else {
        break;
      }
    }

    // 下一页
    rid_.page_id_ = current_page->GetNextPageId();
    rid_.slot_id_ = 0;
  }

  // LAB 1 BEGIN
  return record;
}

}  // namespace huadb
