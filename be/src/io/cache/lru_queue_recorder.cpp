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

#include "io/cache/lru_queue_recorder.h"

#include "common/config.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/file_cache_common.h"

namespace doris::io {

void LRUQueueRecorder::record_queue_event(FileCacheType type, CacheLRULogType log_type,
                                          const UInt128Wrapper hash, const size_t offset,
                                          const size_t size) {
    if (_mgr->is_memory_storage() || config::file_cache_background_lru_dump_tail_record_num <= 0) {
        return;
    }
    auto record_drop = [&]() {
        _dropped_lru_log_count.fetch_add(1, std::memory_order_relaxed);
        if (_mgr->_lru_recorder_log_queue_dropped_metrics) {
            *(_mgr->_lru_recorder_log_queue_dropped_metrics) << 1;
        }
    };
    std::lock_guard<std::mutex> lru_log_lock(_mutex_lru_log);
    CacheLRULogQueue& log_queue = get_lru_log_queue(type);
    PendingMoveToBackMap& pending_move_to_back = get_pending_move_to_back_map(type);
    AccessKeyAndOffset key(hash, offset);
    if (log_type == CacheLRULogType::MOVETOBACK) {
        if (auto iter = pending_move_to_back.find(key); iter != pending_move_to_back.end()) {
            iter->second->size = size;
            log_queue.splice(log_queue.end(), log_queue, iter->second);
            ++(_lru_queue_update_cnt_from_last_dump[type]);
            return;
        }
    }
    if (_total_lru_log_queue_size >= _hard_cap) {
        record_drop();
        LOG_EVERY_N(WARNING, 60) << "Drop lru recorder log because hard cap is reached, hard_cap="
                                 << _hard_cap << " total_queue_size=" << _total_lru_log_queue_size;
        return;
    }
    CacheLRULogQueue::iterator log_iter;
    bool log_inserted = false;
    try {
        log_queue.emplace_back(log_type, hash, offset, size);
        log_iter = std::prev(log_queue.end());
        log_inserted = true;
        if (log_type == CacheLRULogType::MOVETOBACK) {
            auto [_, inserted] = pending_move_to_back.emplace(key, log_iter);
            DCHECK(inserted);
        }
    } catch (const std::exception& e) {
        if (log_inserted) {
            log_queue.erase(log_iter);
        }
        record_drop();
        LOG(WARNING) << "Failed to enqueue lru recorder log: " << e.what();
        return;
    } catch (...) {
        if (log_inserted) {
            log_queue.erase(log_iter);
        }
        record_drop();
        LOG(WARNING) << "Failed to enqueue lru recorder log: unknown error";
        return;
    }
    ++_lru_log_queue_size_by_type[static_cast<size_t>(type)];
    ++_total_lru_log_queue_size;
    ++(_lru_queue_update_cnt_from_last_dump[type]);
}

size_t LRUQueueRecorder::replay_queue_event(FileCacheType type, size_t max_events) {
    // we don't need the real cache lock for the shadow queue, but we do need a lock to prevent read/write contension
    std::lock_guard<std::mutex> lru_log_lock(_mutex_lru_log);
    return replay_queue_event_locked(type, max_events, lru_log_lock);
}

size_t LRUQueueRecorder::replay_queue_event_locked(FileCacheType type, size_t max_events,
                                                   std::lock_guard<std::mutex>& lru_log_lock) {
    // we don't need the real cache lock for the shadow queue, but we do need a lock to prevent read/write contension
    CacheLRULogQueue& log_queue = get_lru_log_queue(type);
    LRUQueue& shadow_queue = get_shadow_queue(type);
    PendingMoveToBackMap& pending_move_to_back = get_pending_move_to_back_map(type);

    size_t replayed = 0;
    while ((max_events == 0 || replayed < max_events) && !log_queue.empty()) {
        CacheLRULog log = log_queue.front();
        if (log.type == CacheLRULogType::MOVETOBACK) {
            pending_move_to_back.erase({log.hash, log.offset});
        }
        log_queue.pop_front();
        ++replayed;
        --_lru_log_queue_size_by_type[static_cast<size_t>(type)];
        --_total_lru_log_queue_size;
        try {
            switch (log.type) {
            case CacheLRULogType::ADD: {
                auto it = shadow_queue.get(log.hash, log.offset, lru_log_lock);
                if (it != std::list<LRUQueue::FileKeyAndOffset>::iterator()) {
                    shadow_queue.remove(it, lru_log_lock);
                }
                shadow_queue.add(log.hash, log.offset, log.size, lru_log_lock);
                break;
            }
            case CacheLRULogType::REMOVE: {
                auto it = shadow_queue.get(log.hash, log.offset, lru_log_lock);
                if (it != std::list<LRUQueue::FileKeyAndOffset>::iterator()) {
                    shadow_queue.remove(it, lru_log_lock);
                } else {
                    LOG(WARNING) << "REMOVE failed, doesn't exist in shadow queue";
                }
                break;
            }
            case CacheLRULogType::MOVETOBACK: {
                auto it = shadow_queue.get(log.hash, log.offset, lru_log_lock);
                if (it != std::list<LRUQueue::FileKeyAndOffset>::iterator()) {
                    shadow_queue.move_to_end(it, lru_log_lock);
                } else {
                    LOG(WARNING) << "MOVETOBACK failed, doesn't exist in shadow queue";
                }
                break;
            }
            case CacheLRULogType::RESIZE: {
                auto it = shadow_queue.get(log.hash, log.offset, lru_log_lock);
                if (it != std::list<LRUQueue::FileKeyAndOffset>::iterator()) {
                    shadow_queue.resize(it, log.size, lru_log_lock);
                } else {
                    LOG(WARNING) << "RESIZE failed, doesn't exist in shadow queue";
                }
                break;
            }
            default:
                LOG(WARNING) << "Unknown CacheLRULogType: " << static_cast<int>(log.type);
                break;
            }
        } catch (const std::exception& e) {
            LOG(WARNING) << "Failed to replay queue event: " << e.what();
        }
    }
    return replayed;
}

// we evaluate the diff between two queue by calculate how many operation is
// needed for transfer one to another (Levenshtein Distance)
// NOTE: HEAVY calculation with cache lock, only for debugging
void LRUQueueRecorder::evaluate_queue_diff(LRUQueue& base, std::string name,
                                           std::lock_guard<std::mutex>& cache_lock) {
    FileCacheType type = string_to_cache_type(name);
    LRUQueue& target = get_shadow_queue(type);
    size_t distance = target.levenshtein_distance_from(base, cache_lock);
    *(_mgr->_shadow_queue_levenshtein_distance) << distance;
    if (distance > 20) {
        LOG(WARNING) << name << " shadow queue is different from real queue";
    }
}

LRUQueue& LRUQueueRecorder::get_shadow_queue(FileCacheType type) {
    switch (type) {
    case FileCacheType::INDEX:
        return _shadow_index_queue;
    case FileCacheType::DISPOSABLE:
        return _shadow_disposable_queue;
    case FileCacheType::NORMAL:
        return _shadow_normal_queue;
    case FileCacheType::TTL:
        return _shadow_ttl_queue;
    default:
        LOG(WARNING) << "invalid shadow queue type";
        DCHECK(false);
    }
    return _shadow_normal_queue;
}

CacheLRULogQueue& LRUQueueRecorder::get_lru_log_queue(FileCacheType type) {
    switch (type) {
    case FileCacheType::INDEX:
        return _index_lru_log_queue;
    case FileCacheType::DISPOSABLE:
        return _disposable_lru_log_queue;
    case FileCacheType::NORMAL:
        return _normal_lru_log_queue;
    case FileCacheType::TTL:
        return _ttl_lru_log_queue;
    default:
        LOG(WARNING) << "invalid lru log queue type";
        DCHECK(false);
    }
    return _normal_lru_log_queue;
}

PendingMoveToBackMap& LRUQueueRecorder::get_pending_move_to_back_map(FileCacheType type) {
    return _pending_move_to_back_by_type[static_cast<size_t>(type)];
}

size_t LRUQueueRecorder::get_lru_queue_update_cnt_from_last_dump(FileCacheType type) {
    std::lock_guard<std::mutex> lru_log_lock(_mutex_lru_log);
    return get_lru_queue_update_cnt_from_last_dump_locked(type, lru_log_lock);
}

size_t LRUQueueRecorder::get_lru_queue_update_cnt_from_last_dump_locked(
        FileCacheType type, std::lock_guard<std::mutex>& /* lru_log_lock */) {
    return _lru_queue_update_cnt_from_last_dump[type];
}

void LRUQueueRecorder::reset_lru_queue_update_cnt_from_last_dump(FileCacheType type) {
    std::lock_guard<std::mutex> lru_log_lock(_mutex_lru_log);
    reset_lru_queue_update_cnt_from_last_dump_locked(type, lru_log_lock);
}

void LRUQueueRecorder::reset_lru_queue_update_cnt_from_last_dump_locked(
        FileCacheType type, std::lock_guard<std::mutex>& /* lru_log_lock */) {
    _lru_queue_update_cnt_from_last_dump[type] = 0;
}

void LRUQueueRecorder::subtract_lru_queue_update_cnt_from_last_dump_locked(
        FileCacheType type, size_t count, std::lock_guard<std::mutex>& /* lru_log_lock */) {
    auto& update_cnt = _lru_queue_update_cnt_from_last_dump[type];
    update_cnt = count >= update_cnt ? 0 : update_cnt - count;
}

size_t LRUQueueRecorder::get_lru_log_queue_size(FileCacheType type) {
    std::lock_guard<std::mutex> lru_log_lock(_mutex_lru_log);
    return _lru_log_queue_size_by_type[static_cast<size_t>(type)];
}

size_t LRUQueueRecorder::get_total_lru_log_queue_size() {
    std::lock_guard<std::mutex> lru_log_lock(_mutex_lru_log);
    return _total_lru_log_queue_size;
}

} // end of namespace doris::io
