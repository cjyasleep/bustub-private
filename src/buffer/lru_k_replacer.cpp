//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  if (curr_size_ == 0) {
    return false;
  }
  for (auto it = history_list_.rbegin(); it != history_list_.rend(); it++) {
    auto frame = *it;
    if (is_evictable_[frame]) {
      *frame_id = frame;
      history_list_.erase(history_hash_[frame]);
      history_hash_.erase(frame);
      is_evictable_.erase(frame);
      access_count_.erase(frame);
      --curr_size_;
      return true;
    }
  }
  for (auto it = lru_k_list_.rbegin(); it != lru_k_list_.rend(); it++) {
    auto frame = *it;
    if (is_evictable_[frame]) {
      *frame_id = frame;
      lru_k_list_.erase(lru_k_hash_[frame]);
      lru_k_hash_.erase(frame);
      is_evictable_.erase(frame);
      access_count_.erase(frame);
      --curr_size_;
      return true;
    }
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw(std::exception());
  }
  ++access_count_[frame_id];
  if (access_count_[frame_id] < k_) {
    if (history_hash_.find(frame_id) == history_hash_.end()) {
      history_list_.push_front(frame_id);
      history_hash_[frame_id] = history_list_.begin();
    }
  } else if (access_count_[frame_id] == k_) {
    history_list_.erase(history_hash_[frame_id]);
    history_hash_.erase(frame_id);
    lru_k_list_.push_front(frame_id);
    lru_k_hash_[frame_id] = lru_k_list_.begin();
  } else {
    if (lru_k_hash_.find(frame_id) != lru_k_hash_.end()) {
      lru_k_list_.erase(lru_k_hash_[frame_id]);
      lru_k_hash_.erase(frame_id);
    }
    lru_k_list_.push_front(frame_id);
    lru_k_hash_[frame_id] = lru_k_list_.begin();
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  if (access_count_[frame_id] == 0U) {
    return;
  }
  if (is_evictable_[frame_id] && !set_evictable) {
    --curr_size_;
  }
  if (!is_evictable_[frame_id] && set_evictable) {
    ++curr_size_;
  }
  is_evictable_[frame_id] = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (access_count_[frame_id] == 0) {
    return;
  }
  if (!is_evictable_[frame_id] || frame_id > static_cast<frame_id_t>(replacer_size_)) {
    throw std::exception();
  }
  if (history_hash_.find(frame_id) != history_hash_.end()) {
    history_list_.erase(history_hash_[frame_id]);
    history_hash_.erase(frame_id);
  } else {
    lru_k_list_.erase(lru_k_hash_[frame_id]);
    lru_k_hash_.erase(frame_id);
  }
  --curr_size_;
  is_evictable_[frame_id] = false;
  access_count_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
