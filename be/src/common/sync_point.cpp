// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Most code of this file is copied from rocksdb SyncPoint.
// https://github.com/facebook/rocksdb

// clang-format off
#include "sync_point.h"

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

namespace doris {

struct SyncPoint::Data { // impl
public:
  Data() : enabled_(false) { }
  virtual ~Data() {}
  void process(const std::string& point, std::vector<std::any>&& cb_args);
  void load_dependency(const std::vector<SyncPointPair>& dependencies);
  void load_dependency_and_markers(
                                const std::vector<SyncPointPair>& dependencies,
                                const std::vector<SyncPointPair>& markers);
  bool predecessors_all_cleared(const std::string& point);
  void set_call_back(const std::string& point,
                    const std::function<void(std::vector<std::any>&&)>& callback);
  void clear_call_back(const std::string& point);
  void clear_all_call_backs();
  void enable_processing();
  void disable_processing();
  void clear_trace();
private:
  bool disable_by_marker(const std::string& point, std::thread::id thread_id);
private:
  // successor/predecessor map loaded from load_dependency
  std::unordered_map<std::string, std::vector<std::string>> successors_;
  std::unordered_map<std::string, std::vector<std::string>> predecessors_;
  std::unordered_map<std::string, std::function<void(std::vector<std::any>&&)>> callbacks_;
  std::unordered_map<std::string, std::vector<std::string>> markers_;
  std::unordered_map<std::string, std::thread::id> marked_thread_id_;
  std::mutex mutex_;
  std::condition_variable cv_;
  // sync points that have been passed through
  std::unordered_set<std::string> cleared_points_;
  std::atomic<bool> enabled_;
  int num_callbacks_running_ = 0;
};

SyncPoint* SyncPoint::get_instance() {
  static SyncPoint sync_point;
  return &sync_point;
}
SyncPoint::SyncPoint() : 
  impl_(new Data) {
}
SyncPoint:: ~SyncPoint() {
  delete impl_;
}
void SyncPoint::load_dependency(const std::vector<SyncPointPair>& dependencies) {
  impl_->load_dependency(dependencies);
}
void SyncPoint::load_dependency_and_markers(
                                const std::vector<SyncPointPair>& dependencies,
                                const std::vector<SyncPointPair>& markers) {
  impl_->load_dependency_and_markers(dependencies, markers);
}
void SyncPoint::set_call_back(const std::string& point,
                              const std::function<void(std::vector<std::any>&&)>& callback) {
  impl_->set_call_back(point, callback);
}
void SyncPoint::clear_call_back(const std::string& point) {
  impl_->clear_call_back(point);
}
void SyncPoint::clear_all_call_backs() {
  impl_->clear_all_call_backs();
}
void SyncPoint::enable_processing() {
  impl_->enable_processing();
}
void SyncPoint::disable_processing() {
  impl_->disable_processing();
}
void SyncPoint::clear_trace() {
  impl_->clear_trace();
}
void SyncPoint::process(const std::string& point, std::vector<std::any>&& cb_arg) {
  impl_->process(point, std::move(cb_arg));
}

// =============================================================================
// SyncPoint implementation
// =============================================================================

void SyncPoint::Data::load_dependency(
                               const std::vector<SyncPointPair>& dependencies) {
  std::lock_guard lock(mutex_);
  successors_.clear();
  predecessors_.clear();
  cleared_points_.clear();
  for (const auto& dependency : dependencies) {
    successors_[dependency.predecessor].push_back(dependency.successor);
    predecessors_[dependency.successor].push_back(dependency.predecessor);
  }
  cv_.notify_all();
}

/**
 * Markers are also dependency descriptions
 */
void SyncPoint::Data::load_dependency_and_markers(
                                const std::vector<SyncPointPair>& dependencies,
                                const std::vector<SyncPointPair>& markers) {
  std::lock_guard lock(mutex_);
  successors_.clear();
  predecessors_.clear();
  cleared_points_.clear();
  markers_.clear();
  marked_thread_id_.clear();
  for (const auto& dependency : dependencies) {
    successors_[dependency.predecessor].push_back(dependency.successor);
    predecessors_[dependency.successor].push_back(dependency.predecessor);
  }
  for (const auto& marker : markers) {
    successors_[marker.predecessor].push_back(marker.successor);
    predecessors_[marker.successor].push_back(marker.predecessor);
    markers_[marker.predecessor].push_back(marker.successor);
  }
  cv_.notify_all();
}

bool SyncPoint::Data::predecessors_all_cleared(const std::string& point) {
  for (const auto& pred : predecessors_[point]) {
    if (cleared_points_.count(pred) == 0) {
      return false;
    }
  }
  return true;
}

void SyncPoint::Data::clear_call_back(const std::string& point) {
  std::unique_lock lock(mutex_);
  callbacks_.erase(point);
}

void SyncPoint::Data::clear_all_call_backs() {
  std::unique_lock lock(mutex_);
  callbacks_.clear();
}

void SyncPoint::Data::process(const std::string& point, std::vector<std::any>&& cb_arg) {
  if (!enabled_) {
    return;
  }
  std::unique_lock lock(mutex_);
  auto thread_id = std::this_thread::get_id();
  auto marker_iter = markers_.find(point);
  // if current sync point is a marker
  // record it in marked_thread_id_ for all its successors
  if (marker_iter != markers_.end()) {
    for (auto& marked_point : marker_iter->second) {
      marked_thread_id_.emplace(marked_point, thread_id);
    }
  }
  // if current point is a marker's successor 
  if (disable_by_marker(point, thread_id)) {
    return;
  }
  while (!predecessors_all_cleared(point)) {
    cv_.wait(lock);
    if (disable_by_marker(point, thread_id)) {
      return;
    }
  }
  auto callback_pair = callbacks_.find(point);
  if (callback_pair != callbacks_.end()) {
    num_callbacks_running_++;
    auto callback = callback_pair->second; 
    mutex_.unlock();
    callback(std::move(cb_arg));
    mutex_.lock();
    num_callbacks_running_--;
  }
  cleared_points_.insert(point);
  cv_.notify_all();
}

bool SyncPoint::Data::disable_by_marker(const std::string& point,
                                        std::thread::id thread_id) {
  auto marked_point_iter = marked_thread_id_.find(point);
  return marked_point_iter != marked_thread_id_.end() // is a successor
          && thread_id != marked_point_iter->second;
}

void SyncPoint::Data::set_call_back(const std::string& point,
                                  const std::function<void(std::vector<std::any>&&)>& callback) {
  std::lock_guard lock(mutex_);
  callbacks_[point] = callback;
}

void SyncPoint::Data::clear_trace() {
  std::lock_guard lock(mutex_);
  cleared_points_.clear();
}

void SyncPoint::Data::enable_processing() {
  enabled_ = true;
}

void SyncPoint::Data::disable_processing() {
  enabled_ = false;
}

} // namespace doris
// clang-format on
// vim: et tw=80 ts=2 sw=2 cc=80:
