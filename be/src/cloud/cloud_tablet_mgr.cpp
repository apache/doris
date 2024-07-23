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

#include "cloud/cloud_tablet_mgr.h"

#include <bthread/countdown_event.h>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/config.h"
#include "common/status.h"
#include "olap/lru_cache.h"
#include "runtime/memory/cache_policy.h"

namespace doris {
namespace {

// port from
// https://github.com/golang/groupcache/blob/master/singleflight/singleflight.go
template <typename Key, typename Val>
class SingleFlight {
public:
    SingleFlight() = default;

    SingleFlight(const SingleFlight&) = delete;
    void operator=(const SingleFlight&) = delete;

    using Loader = std::function<Val(const Key&)>;

    // Do executes and returns the results of the given function, making
    // sure that only one execution is in-flight for a given key at a
    // time. If a duplicate comes in, the duplicate caller waits for the
    // original to complete and receives the same results.
    Val load(const Key& key, Loader loader) {
        std::unique_lock lock(_call_map_mtx);

        auto it = _call_map.find(key);
        if (it != _call_map.end()) {
            auto call = it->second;
            lock.unlock();
            if (int ec = call->event.wait(); ec != 0) {
                throw std::system_error(std::error_code(ec, std::system_category()),
                                        "CountdownEvent wait failed");
            }
            return call->val;
        }
        auto call = std::make_shared<Call>();
        _call_map.emplace(key, call);
        lock.unlock();

        call->val = loader(key);
        call->event.signal();

        lock.lock();
        _call_map.erase(key);
        lock.unlock();

        return call->val;
    }

private:
    // `Call` is an in-flight or completed `load` call
    struct Call {
        bthread::CountdownEvent event;
        Val val;
    };

    std::mutex _call_map_mtx;
    std::unordered_map<Key, std::shared_ptr<Call>> _call_map;
};

SingleFlight<int64_t /* tablet_id */, std::shared_ptr<CloudTablet>> s_singleflight_load_tablet;

} // namespace

// tablet_id -> cached tablet
// This map owns all cached tablets. The lifetime of tablet can be longer than the LRU handle.
// It's also used for scenarios where users want to access the tablet by `tablet_id` without changing the LRU order.
// TODO(plat1ko): multi shard to increase concurrency
class CloudTabletMgr::TabletMap {
public:
    void put(std::shared_ptr<CloudTablet> tablet) {
        std::lock_guard lock(_mtx);
        _map[tablet->tablet_id()] = std::move(tablet);
    }

    void erase(CloudTablet* tablet) {
        std::lock_guard lock(_mtx);
        auto it = _map.find(tablet->tablet_id());
        // According to the implementation of `LRUCache`, `deleter` may be called after a tablet
        // with same tablet id insert into cache and `TabletMap`. So we MUST check if the tablet
        // instance to be erased is the same one in the map.
        if (it != _map.end() && it->second.get() == tablet) {
            _map.erase(it);
        }
    }

    std::shared_ptr<CloudTablet> get(int64_t tablet_id) {
        std::lock_guard lock(_mtx);
        if (auto it = _map.find(tablet_id); it != _map.end()) {
            return it->second;
        }
        return nullptr;
    }

    size_t size() { return _map.size(); }

    void traverse(std::function<void(const std::shared_ptr<CloudTablet>&)> visitor) {
        std::lock_guard lock(_mtx);
        for (auto& [_, tablet] : _map) {
            visitor(tablet);
        }
    }

private:
    std::mutex _mtx;
    std::unordered_map<int64_t, std::shared_ptr<CloudTablet>> _map;
};

// TODO(plat1ko): Prune cache
CloudTabletMgr::CloudTabletMgr(CloudStorageEngine& engine)
        : _engine(engine),
          _tablet_map(std::make_unique<TabletMap>()),
          _cache(std::make_unique<LRUCachePolicyTrackingManual>(
                  CachePolicy::CacheType::CLOUD_TABLET_CACHE, config::tablet_cache_capacity,
                  LRUCacheType::NUMBER, 0, config::tablet_cache_shards)) {}

CloudTabletMgr::~CloudTabletMgr() = default;

Result<std::shared_ptr<CloudTablet>> CloudTabletMgr::get_tablet(int64_t tablet_id,
                                                                bool warmup_data) {
    // LRU value type. `Value`'s lifetime MUST NOT be longer than `CloudTabletMgr`
    class Value : public LRUCacheValueBase {
    public:
        Value(const std::shared_ptr<CloudTablet>& tablet, TabletMap& tablet_map)
                : tablet(tablet), tablet_map(tablet_map) {}
        ~Value() override { tablet_map.erase(tablet.get()); }

        // FIXME(plat1ko): The ownership of tablet seems to belong to 'TabletMap', while `Value`
        // only requires a reference.
        std::shared_ptr<CloudTablet> tablet;
        TabletMap& tablet_map;
    };

    auto tablet_id_str = std::to_string(tablet_id);
    CacheKey key(tablet_id_str);
    auto* handle = _cache->lookup(key);
    if (handle == nullptr) {
        auto load_tablet = [this, &key,
                            warmup_data](int64_t tablet_id) -> std::shared_ptr<CloudTablet> {
            TabletMetaSharedPtr tablet_meta;
            auto st = _engine.meta_mgr().get_tablet_meta(tablet_id, &tablet_meta);
            if (!st.ok()) {
                LOG(WARNING) << "failed to tablet " << tablet_id << ": " << st;
                return nullptr;
            }

            auto tablet = std::make_shared<CloudTablet>(_engine, std::move(tablet_meta));
            auto value = std::make_unique<Value>(tablet, *_tablet_map);
            // MUST sync stats to let compaction scheduler work correctly
            st = _engine.meta_mgr().sync_tablet_rowsets(tablet.get(), warmup_data);
            if (!st.ok()) {
                LOG(WARNING) << "failed to sync tablet " << tablet_id << ": " << st;
                return nullptr;
            }

            auto* handle = _cache->insert(key, value.release(), 1, sizeof(CloudTablet),
                                          CachePriority::NORMAL);
            auto ret = std::shared_ptr<CloudTablet>(
                    tablet.get(), [this, handle](...) { _cache->release(handle); });
            _tablet_map->put(std::move(tablet));
            return ret;
        };

        auto tablet = s_singleflight_load_tablet.load(tablet_id, std::move(load_tablet));
        if (tablet == nullptr) {
            return ResultError(Status::InternalError("failed to get tablet {}", tablet_id));
        }
        return tablet;
    }

    CloudTablet* tablet_raw_ptr = reinterpret_cast<Value*>(_cache->value(handle))->tablet.get();
    auto tablet = std::shared_ptr<CloudTablet>(tablet_raw_ptr,
                                               [this, handle](...) { _cache->release(handle); });
    return tablet;
}

void CloudTabletMgr::erase_tablet(int64_t tablet_id) {
    auto tablet_id_str = std::to_string(tablet_id);
    CacheKey key(tablet_id_str.data(), tablet_id_str.size());
    _cache->erase(key);
}

void CloudTabletMgr::vacuum_stale_rowsets(const CountDownLatch& stop_latch) {
    LOG_INFO("begin to vacuum stale rowsets");
    std::vector<std::shared_ptr<CloudTablet>> tablets_to_vacuum;
    tablets_to_vacuum.reserve(_tablet_map->size());
    _tablet_map->traverse([&tablets_to_vacuum](auto&& t) {
        if (t->has_stale_rowsets()) {
            tablets_to_vacuum.push_back(t);
        }
    });
    int num_vacuumed = 0;
    for (auto& t : tablets_to_vacuum) {
        if (stop_latch.count() <= 0) {
            break;
        }

        num_vacuumed += t->delete_expired_stale_rowsets();
    }
    LOG_INFO("finish vacuum stale rowsets").tag("num_vacuumed", num_vacuumed);
}

std::vector<std::weak_ptr<CloudTablet>> CloudTabletMgr::get_weak_tablets() {
    std::vector<std::weak_ptr<CloudTablet>> weak_tablets;
    weak_tablets.reserve(_tablet_map->size());
    _tablet_map->traverse([&weak_tablets](auto& t) { weak_tablets.push_back(t); });
    return weak_tablets;
}

void CloudTabletMgr::sync_tablets(const CountDownLatch& stop_latch) {
    LOG_INFO("begin to sync tablets");
    int64_t last_sync_time_bound = ::time(nullptr) - config::tablet_sync_interval_s;

    auto weak_tablets = get_weak_tablets();

    // sort by last_sync_time
    static auto cmp = [](const auto& a, const auto& b) { return a.first < b.first; };
    std::multiset<std::pair<int64_t, std::weak_ptr<CloudTablet>>, decltype(cmp)>
            sync_time_tablet_set(cmp);

    for (auto& weak_tablet : weak_tablets) {
        if (auto tablet = weak_tablet.lock()) {
            if (tablet->tablet_state() != TABLET_RUNNING) {
                continue;
            }
            int64_t last_sync_time = tablet->last_sync_time_s;
            if (last_sync_time <= last_sync_time_bound) {
                sync_time_tablet_set.emplace(last_sync_time, weak_tablet);
            }
        }
    }

    int num_sync = 0;
    for (auto&& [_, weak_tablet] : sync_time_tablet_set) {
        if (stop_latch.count() <= 0) {
            break;
        }

        if (auto tablet = weak_tablet.lock()) {
            if (tablet->last_sync_time_s > last_sync_time_bound) {
                continue;
            }

            ++num_sync;
            auto st = tablet->sync_meta();
            if (!st) {
                LOG_WARNING("failed to sync tablet meta {}", tablet->tablet_id()).error(st);
                if (st.is<ErrorCode::NOT_FOUND>()) {
                    continue;
                }
            }

            st = tablet->sync_rowsets(-1);
            if (!st) {
                LOG_WARNING("failed to sync tablet rowsets {}", tablet->tablet_id()).error(st);
            }
        }
    }
    LOG_INFO("finish sync tablets").tag("num_sync", num_sync);
}

Status CloudTabletMgr::get_topn_tablets_to_compact(
        int n, CompactionType compaction_type, const std::function<bool(CloudTablet*)>& filter_out,
        std::vector<std::shared_ptr<CloudTablet>>* tablets, int64_t* max_score) {
    DCHECK(compaction_type == CompactionType::BASE_COMPACTION ||
           compaction_type == CompactionType::CUMULATIVE_COMPACTION);
    *max_score = 0;
    int64_t max_score_tablet_id = 0;
    // clang-format off
    auto score = [compaction_type](CloudTablet* t) {
        return compaction_type == CompactionType::BASE_COMPACTION ? t->get_cloud_base_compaction_score()
               : compaction_type == CompactionType::CUMULATIVE_COMPACTION ? t->get_cloud_cumu_compaction_score()
               : 0;
    };

    using namespace std::chrono;
    auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    auto skip = [now, compaction_type](CloudTablet* t) {
        if (compaction_type == CompactionType::BASE_COMPACTION) {
            return now - t->last_base_compaction_success_time_ms < config::base_compaction_freeze_interval_s * 1000;
        }
        // If tablet has too many rowsets but not be compacted for a long time, compaction should be performed
        // regardless of whether there is a load job recently.
        return now - t->last_cumu_no_suitable_version_ms < config::min_compaction_failure_interval_ms ||
               (now - t->last_load_time_ms > config::cu_compaction_freeze_interval_s * 1000
               && now - t->last_cumu_compaction_success_time_ms < config::cumu_compaction_interval_s * 1000
               && t->fetch_add_approximate_num_rowsets(0) < config::max_tablet_version_num / 2);
    };
    // We don't schedule tablets that are disabled for compaction
    auto disable = [](CloudTablet* t) { return t->tablet_meta()->tablet_schema()->disable_auto_compaction(); };

    auto [num_filtered, num_disabled, num_skipped] = std::make_tuple(0, 0, 0);

    auto weak_tablets = get_weak_tablets();
    std::vector<std::pair<std::shared_ptr<CloudTablet>, int64_t>> buf;
    buf.reserve(n + 1);
    for (auto& weak_tablet : weak_tablets) {
        auto t = weak_tablet.lock();
        if (t == nullptr) { continue; }

        int64_t s = score(t.get());
        if (s <= 0) { continue; }
        if (s > *max_score) {
            max_score_tablet_id = t->tablet_id();
            *max_score = s;
        }

        if (filter_out(t.get())) { ++num_filtered; continue; }
        if (disable(t.get())) { ++num_disabled; continue; }
        if (skip(t.get())) { ++num_skipped; continue; }

        buf.emplace_back(std::move(t), s);
        std::sort(buf.begin(), buf.end(), [](auto& a, auto& b) { return a.second > b.second; });
        if (buf.size() > n) { buf.pop_back(); }
    }

    LOG_EVERY_N(INFO, 1000) << "get_topn_compaction_score, n=" << n << " type=" << compaction_type
               << " num_tablets=" << weak_tablets.size() << " num_skipped=" << num_skipped
               << " num_disabled=" << num_disabled << " num_filtered=" << num_filtered
               << " max_score=" << *max_score << " max_score_tablet=" << max_score_tablet_id
               << " tablets=[" << [&buf] { std::stringstream ss; for (auto& i : buf) ss << i.first->tablet_id() << ":" << i.second << ","; return ss.str(); }() << "]"
               ;
    // clang-format on

    tablets->clear();
    tablets->reserve(n + 1);
    for (auto& [t, _] : buf) {
        tablets->emplace_back(std::move(t));
    }

    return Status::OK();
}

} // namespace doris
