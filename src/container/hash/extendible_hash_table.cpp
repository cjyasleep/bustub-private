//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  dir_.push_back(std::make_shared<Bucket>(bucket_size, 0));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  int index = IndexOf(key);
  return dir_[index]->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  int index = IndexOf(key);
  return dir_[index]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock<std::mutex> lock(latch_);
  // 这里用while的原因:
  // 这里假设桶大小为1 如果第一个桶满了 那么要进行第一次分裂
  // 第一次插入0 第二次插入2时 全局深度为1 第一个桶被0占用满了
  // 进行第一扩容时 dir_[1]指向新的桶 但是2 不能插入 全局深度为2
  // 所以要进行第二次扩容 全局深度变为3
  // 2就能够被插入dir_[2]指向的桶中
  // while循环进行是 判断dir_需不需要扩容 将满的桶分成两个新桶 并且重新分配指针
  // 并不意味着新插入的元素能够顺利插入重新分配后的两个桶中
  while (dir_[IndexOf(key)]->IsFull()) {
    int index = IndexOf(key);
    std::shared_ptr<Bucket> origin_bucket = dir_[index];
    if (origin_bucket->GetDepth() == GetGlobalDepthInternal()) {
      ++global_depth_;
      size_t cur_size = dir_.size();
      dir_.resize(cur_size << 1);
      for (size_t i = 0; i < cur_size; i++) {
        dir_[cur_size + i] = dir_[i];
      }
    }
    std::shared_ptr<Bucket> split_bucket0 = std::make_shared<Bucket>(bucket_size_, origin_bucket->GetDepth() + 1);
    std::shared_ptr<Bucket> split_bucket1 = std::make_shared<Bucket>(bucket_size_, origin_bucket->GetDepth() + 1);
    int mask = 1 << origin_bucket->GetDepth();
    for (const auto &it : origin_bucket->GetItems()) {
      if ((std::hash<K>()(it.first) & mask) != 0U) {
        split_bucket1->Insert(it.first, it.second);
      } else {
        split_bucket0->Insert(it.first, it.second);
      }
    }
    ++num_buckets_;
    for (size_t i = 0; i < dir_.size(); i++) {
      if (dir_[i] == origin_bucket) {
        if ((i & mask) != 0U) {
          dir_[i] = split_bucket1;
        } else {
          dir_[i] = split_bucket0;
        }
      }
    }
  }
  for (auto &it : dir_[IndexOf(key)]->GetItems()) {
    if (key == it.first) {
      it.second = value;
      return;
    }
  }
  dir_[IndexOf(key)]->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  return std::any_of(list_.begin(), list_.end(), [&key, &value](const auto &it) {
    if (it.first == key) {
      value = it.second;
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  return std::any_of(list_.begin(), list_.end(), [&key, this](auto &it) {
    if (it.first == key) {
      this->list_.remove(it);
      return true;
    }
    return false;
  });
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (IsFull()) {
    return false;
  }
  for (auto &it : list_) {
    if (it.first == key) {
      it.second = value;
      return false;
    }
  }
  list_.push_back({key, value});
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
