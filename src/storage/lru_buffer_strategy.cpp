#include "storage/lru_buffer_strategy.h"

namespace huadb {

void LRUBufferStrategy::Access(size_t frame_no) {
  // 缓存页面访问
  // LAB 1 BEGIN

  auto it = map.find(frame_no);
  if (it == map.end()) {
    // 不存在
    list.push_front(frame_no);
    map[frame_no] = list.begin();
  } else {
    // 存在
    list.splice(list.begin(), list, it->second);
  }
  return;
};

size_t LRUBufferStrategy::Evict() {
  // 缓存页面淘汰，返回淘汰的页面在 buffer pool 中的下标
  // LAB 1 BEGIN

  size_t value = list.back();
  list.pop_back();
  map.erase(value);

  return value;
}

}  // namespace huadb
