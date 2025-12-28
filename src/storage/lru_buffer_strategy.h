#pragma once
#include <unordered_map>
#include <list>
#include "storage/buffer_strategy.h"

namespace huadb {

class LRUBufferStrategy : public BufferStrategy {
 public:
  void Access(size_t frame_no) override;
  size_t Evict() override;
private:
  std::unordered_map<size_t, std::list<size_t>::iterator> map;
  std::list<size_t> list;
};

}  // namespace huadb
