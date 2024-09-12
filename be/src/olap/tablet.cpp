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

#include "olap/tablet.h"

#include <butil/logging.h>
#include <bvar/reducer.h>
#include <bvar/window.h>
#include <fmt/format.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/MasterService_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/types.pb.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>

#include <algorithm>
#include <atomic>
#include <boost/container/detail/std_fwd.hpp>
#include <roaring/roaring.hh>

#include "common/compiler_util.h" // IWYU pragma: keep
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <filesystem>
#include <iterator>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>

#include "agent/utils.h"
#include "common/config.h"
#include "common/consts.h"
#include "common/logging.h"
#include "common/signal_handler.h"
#include "common/status.h"
#include "exprs/runtime_filter.h"
#include "gutil/ref_counted.h"
#include "gutil/strings/substitute.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/path.h"
#include "io/fs/remote_file_system.h"
#include "io/io_common.h"
#include "olap/base_compaction.h"
#include "olap/base_tablet.h"
#include "olap/binlog.h"
#include "olap/cumulative_compaction.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/cumulative_compaction_time_series_policy.h"
#include "olap/delete_bitmap_calculator.h"
#include "olap/full_compaction.h"
#include "olap/memtable.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/olap_meta.h"
#include "olap/primary_key_index.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/indexed_column_reader.h"
#include "olap/rowset/vertical_beta_rowset_writer.h"
#include "olap/schema_change.h"
#include "olap/single_replica_compaction.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/tablet_schema.h"
#include "olap/txn_manager.h"
#include "olap/types.h"
#include "olap/utils.h"
#include "segment_loader.h"
#include "service/point_query_executor.h"
#include "tablet.h"
#include "util/bvar_helper.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/doris_metrics.h"
#include "util/pretty_printer.h"
#include "util/scoped_cleanup.h"
#include "util/stopwatch.hpp"
#include "util/threadpool.h"
#include "util/time.h"
#include "util/trace.h"
#include "util/uid_util.h"
#include "util/work_thread_pool.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/common/schema_util.h"
#include "vec/common/string_ref.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/jsonb/serialize.h"

namespace doris {
class TupleDescriptor;

namespace vectorized {
class Block;
} // namespace vectorized

using namespace ErrorCode;
using namespace std::chrono_literals;

using std::pair;
using std::string;
using std::vector;
using io::FileSystemSPtr;

namespace {

bvar::Adder<uint64_t> exceed_version_limit_counter;
bvar::Window<bvar::Adder<uint64_t>> exceed_version_limit_counter_minute(
        &exceed_version_limit_counter, 60);
bvar::Adder<uint64_t> cooldown_pending_task("cooldown_pending_task");
bvar::Adder<uint64_t> cooldown_processing_task("cooldown_processing_task");

void set_last_failure_time(Tablet* tablet, const Compaction& compaction, int64_t ms) {
    switch (compaction.compaction_type()) {
    case ReaderType::READER_CUMULATIVE_COMPACTION:
        tablet->set_last_cumu_compaction_failure_time(ms);
        return;
    case ReaderType::READER_BASE_COMPACTION:
        tablet->set_last_base_compaction_failure_time(ms);
        return;
    case ReaderType::READER_FULL_COMPACTION:
        tablet->set_last_full_compaction_failure_time(ms);
        return;
    default:
        LOG(FATAL) << "invalid compaction type " << compaction.compaction_name()
                   << " tablet_id: " << tablet->tablet_id();
    }
};

} // namespace

bvar::Adder<uint64_t> unused_remote_rowset_num("unused_remote_rowset_num");

WriteCooldownMetaExecutors::WriteCooldownMetaExecutors(size_t executor_nums)
        : _executor_nums(executor_nums) {
    for (size_t i = 0; i < _executor_nums; i++) {
        std::unique_ptr<PriorityThreadPool> pool;
        static_cast<void>(ThreadPoolBuilder("WriteCooldownMetaExecutor")
                                  .set_min_threads(1)
                                  .set_max_threads(1)
                                  .set_max_queue_size(std::numeric_limits<int>::max())
                                  .build(&pool));
        _executors.emplace_back(std::move(pool));
    }
}

void WriteCooldownMetaExecutors::stop() {
    for (auto& pool_ptr : _executors) {
        if (pool_ptr) {
            pool_ptr->shutdown();
        }
    }
}

void WriteCooldownMetaExecutors::WriteCooldownMetaExecutors::submit(TabletSharedPtr tablet) {
    auto tablet_id = tablet->tablet_id();

    {
        std::shared_lock rdlock(tablet->get_header_lock());
        if (!tablet->tablet_meta()->cooldown_meta_id().initialized()) {
            VLOG_NOTICE << "tablet " << tablet_id << " is not cooldown replica";
            return;
        }
        if (tablet->tablet_state() == TABLET_SHUTDOWN) [[unlikely]] {
            LOG_INFO("tablet {} has been dropped, don't do cooldown", tablet_id);
            return;
        }
    }
    {
        // one tablet could at most have one cooldown task to be done
        std::unique_lock<std::mutex> lck {_latch};
        if (_pending_tablets.count(tablet_id) > 0) {
            return;
        }
        _pending_tablets.insert(tablet_id);
    }

    auto async_write_task = [this, t = std::move(tablet)]() {
        {
            std::unique_lock<std::mutex> lck {_latch};
            _pending_tablets.erase(t->tablet_id());
        }
        auto s = t->write_cooldown_meta();
        if (s.ok()) {
            return;
        }
        if (!s.is<ABORTED>()) {
            LOG_EVERY_SECOND(WARNING)
                    << "write tablet " << t->tablet_id() << " cooldown meta failed because: " << s;
            submit(t);
            return;
        }
        VLOG_DEBUG << "tablet " << t->tablet_id() << " is not cooldown replica";
    };

    cooldown_pending_task << 1;
    _executors[_get_executor_pos(tablet_id)]->offer([task = std::move(async_write_task)]() {
        cooldown_pending_task << -1;
        cooldown_processing_task << 1;
        task();
        cooldown_processing_task << -1;
    });
}

Tablet::Tablet(StorageEngine& engine, TabletMetaSharedPtr tablet_meta, DataDir* data_dir,
               const std::string_view& cumulative_compaction_type)
        : BaseTablet(std::move(tablet_meta)),
          _engine(engine),
          _data_dir(data_dir),
          _is_bad(false),
          _last_cumu_compaction_failure_millis(0),
          _last_base_compaction_failure_millis(0),
          _last_full_compaction_failure_millis(0),
          _last_cumu_compaction_success_millis(0),
          _last_base_compaction_success_millis(0),
          _last_full_compaction_success_millis(0),
          _cumulative_point(K_INVALID_CUMULATIVE_POINT),
          _newly_created_rowset_num(0),
          _last_checkpoint_time(0),
          _cumulative_compaction_type(cumulative_compaction_type),
          _is_tablet_path_exists(true),
          _last_missed_version(-1),
          _last_missed_time_s(0) {
    if (_data_dir != nullptr) {
        _tablet_path = fmt::format("{}/{}/{}/{}/{}", _data_dir->path(), DATA_PREFIX,
                                   _tablet_meta->shard_id(), tablet_id(), schema_hash());
    }
}

bool Tablet::set_tablet_schema_into_rowset_meta() {
    bool flag = false;
    for (auto&& rowset_meta : _tablet_meta->all_mutable_rs_metas()) {
        if (!rowset_meta->tablet_schema()) {
            rowset_meta->set_tablet_schema(_tablet_meta->tablet_schema());
            flag = true;
        }
    }
    return flag;
}

Status Tablet::_init_once_action() {
    Status res = Status::OK();
    VLOG_NOTICE << "begin to load tablet. tablet=" << tablet_id()
                << ", version_size=" << _tablet_meta->version_count();

#ifdef BE_TEST
    // init cumulative compaction policy by type
    _cumulative_compaction_policy =
            CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(
                    _tablet_meta->compaction_policy());
#endif

    for (const auto& rs_meta : _tablet_meta->all_rs_metas()) {
        Version version = rs_meta->version();
        RowsetSharedPtr rowset;
        res = create_rowset(rs_meta, &rowset);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init rowset. tablet_id=" << tablet_id()
                         << ", schema_hash=" << schema_hash() << ", version=" << version
                         << ", res=" << res;
            return res;
        }
        _rs_version_map[version] = std::move(rowset);
    }

    // init stale rowset
    for (const auto& stale_rs_meta : _tablet_meta->all_stale_rs_metas()) {
        Version version = stale_rs_meta->version();
        RowsetSharedPtr rowset;
        res = create_rowset(stale_rs_meta, &rowset);
        if (!res.ok()) {
            LOG(WARNING) << "fail to init stale rowset. tablet_id:" << tablet_id()
                         << ", schema_hash:" << schema_hash() << ", version=" << version
                         << ", res:" << res;
            return res;
        }
        _stale_rs_version_map[version] = std::move(rowset);
    }

    return res;
}

Status Tablet::init() {
    return _init_once.call([this] { return _init_once_action(); });
}

// should save tablet meta to remote meta store
// if it's a primary replica
void Tablet::save_meta() {
    auto res = _tablet_meta->save_meta(_data_dir);
    CHECK_EQ(res, Status::OK()) << "fail to save tablet_meta. res=" << res
                                << ", root=" << _data_dir->path();
}

// Caller should hold _meta_lock.
Status Tablet::revise_tablet_meta(const std::vector<RowsetSharedPtr>& to_add,
                                  const std::vector<RowsetSharedPtr>& to_delete,
                                  bool is_incremental_clone) {
    LOG(INFO) << "begin to revise tablet. tablet_id=" << tablet_id();
    // 1. for incremental clone, we have to add the rowsets first to make it easy to compute
    //    all the delete bitmaps, and it's easy to delete them if we end up with a failure
    // 2. for full clone, we can calculate delete bitmaps on the cloned rowsets directly.
    if (is_incremental_clone) {
        CHECK(to_delete.empty()); // don't need to delete rowsets
        add_rowsets(to_add);
        // reconstruct from tablet meta
        _timestamped_version_tracker.construct_versioned_tracker(_tablet_meta->all_rs_metas());
    }

    Status calc_bm_status;
    std::vector<RowsetSharedPtr> base_rowsets_for_full_clone = to_add; // copy vector
    while (keys_type() == UNIQUE_KEYS && enable_unique_key_merge_on_write()) {
        std::vector<RowsetSharedPtr> calc_delete_bitmap_rowsets;
        int64_t to_add_min_version = INT64_MAX;
        int64_t to_add_max_version = INT64_MIN;
        for (auto& rs : to_add) {
            if (to_add_min_version > rs->start_version()) {
                to_add_min_version = rs->start_version();
            }
            if (to_add_max_version < rs->end_version()) {
                to_add_max_version = rs->end_version();
            }
        }
        Version calc_delete_bitmap_ver;
        if (is_incremental_clone) {
            // From the rowset of to_add with smallest version, all other rowsets
            // need to recalculate the delete bitmap
            // For example:
            // local tablet: [0-1] [2-5] [6-6] [9-10]
            // clone tablet: [7-7] [8-8]
            // new tablet:   [0-1] [2-5] [6-6] [7-7] [8-8] [9-10]
            // [7-7] [8-8] [9-10] need to recalculate delete bitmap
            calc_delete_bitmap_ver = Version(to_add_min_version, max_version_unlocked());
        } else {
            // the delete bitmap of to_add's rowsets has clone from remote when full clone.
            // only other rowsets in local need to recalculate the delete bitmap.
            // For example:
            // local tablet: [0-1]x [2-5]x [6-6]x [7-7]x [9-10]
            // clone tablet: [0-1]  [2-4]  [5-6]  [7-8]
            // new tablet:   [0-1]  [2-4]  [5-6]  [7-8] [9-10]
            // only [9-10] need to recalculate delete bitmap
            CHECK_EQ(to_add_min_version, 0) << "to_add_min_version is: " << to_add_min_version;
            calc_delete_bitmap_ver = Version(to_add_max_version + 1, max_version_unlocked());
        }

        if (calc_delete_bitmap_ver.first <= calc_delete_bitmap_ver.second) {
            calc_bm_status = capture_consistent_rowsets_unlocked(calc_delete_bitmap_ver,
                                                                 &calc_delete_bitmap_rowsets);
            if (!calc_bm_status.ok()) {
                LOG(WARNING) << "fail to capture_consistent_rowsets, res: " << calc_bm_status;
                break;
            }
            // FIXME(plat1ko): Use `const TabletSharedPtr&` as parameter
            auto self = _engine.tablet_manager()->get_tablet(tablet_id());
            CHECK(self);
            for (auto rs : calc_delete_bitmap_rowsets) {
                if (is_incremental_clone) {
                    calc_bm_status = update_delete_bitmap_without_lock(self, rs);
                } else {
                    calc_bm_status = update_delete_bitmap_without_lock(
                            self, rs, &base_rowsets_for_full_clone);
                    base_rowsets_for_full_clone.push_back(rs);
                }
                if (!calc_bm_status.ok()) {
                    LOG(WARNING) << "fail to update_delete_bitmap_without_lock, res: "
                                 << calc_bm_status;
                    break;
                }
            }
        }
        break; // while (keys_type() == UNIQUE_KEYS && enable_unique_key_merge_on_write())
    }

    DBUG_EXECUTE_IF("Tablet.revise_tablet_meta_fail", {
        auto ptablet_id = dp->param("tablet_id", 0);
        if (tablet_id() == ptablet_id) {
            LOG(INFO) << "injected revies_tablet_meta failure for tabelt: " << ptablet_id;
            calc_bm_status = Status::InternalError("fault injection error");
        }
    });

    // error handling
    if (!calc_bm_status.ok()) {
        if (is_incremental_clone) {
            delete_rowsets(to_add, false);
            LOG(WARNING) << "incremental clone on tablet: " << tablet_id() << " failed due to "
                         << calc_bm_status.msg() << ", revert " << to_add.size()
                         << " rowsets added before.";
        } else {
            LOG(WARNING) << "full clone on tablet: " << tablet_id() << " failed due to "
                         << calc_bm_status.msg() << ", will not update tablet meta.";
        }
        return calc_bm_status;
    }

    // full clone, calculate delete bitmap succeeded, update rowset
    if (!is_incremental_clone) {
        delete_rowsets(to_delete, false);
        add_rowsets(to_add);
        // reconstruct from tablet meta
        _timestamped_version_tracker.construct_versioned_tracker(_tablet_meta->all_rs_metas());

        // check the rowsets used for delete bitmap calculation is equal to the rowsets
        // that we can capture by version
        if (keys_type() == UNIQUE_KEYS && enable_unique_key_merge_on_write()) {
            Version full_version = Version(0, max_version_unlocked());
            std::vector<RowsetSharedPtr> expected_rowsets;
            auto st = capture_consistent_rowsets_unlocked(full_version, &expected_rowsets);
            DCHECK(st.ok()) << st;
            DCHECK_EQ(base_rowsets_for_full_clone.size(), expected_rowsets.size());
            if (st.ok() && base_rowsets_for_full_clone.size() != expected_rowsets.size())
                    [[unlikely]] {
                LOG(WARNING) << "full clone succeeded, but the count("
                             << base_rowsets_for_full_clone.size()
                             << ") of base rowsets used for delete bitmap calculation is not match "
                                "expect count("
                             << expected_rowsets.size() << ") we capture from tablet meta";
            }
        }
    }

    // clear stale rowset
    for (auto& [v, rs] : _stale_rs_version_map) {
        _engine.add_unused_rowset(rs);
    }
    _stale_rs_version_map.clear();
    _tablet_meta->clear_stale_rowset();
    save_meta();

    LOG(INFO) << "finish to revise tablet. tablet_id=" << tablet_id();
    return Status::OK();
}

Status Tablet::add_rowset(RowsetSharedPtr rowset) {
    DCHECK(rowset != nullptr);
    std::lock_guard<std::shared_mutex> wrlock(_meta_lock);
    SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
    // If the rowset already exist, just return directly.  The rowset_id is an unique-id,
    // we can use it to check this situation.
    if (_contains_rowset(rowset->rowset_id())) {
        return Status::OK();
    }
    // Otherwise, the version should be not contained in any existing rowset.
    RETURN_IF_ERROR(_contains_version(rowset->version()));

    RETURN_IF_ERROR(_tablet_meta->add_rs_meta(rowset->rowset_meta()));
    _rs_version_map[rowset->version()] = rowset;
    _timestamped_version_tracker.add_version(rowset->version());

    std::vector<RowsetSharedPtr> rowsets_to_delete;
    // yiguolei: temp code, should remove the rowset contains by this rowset
    // but it should be removed in multi path version
    for (auto& it : _rs_version_map) {
        if (rowset->version().contains(it.first) && rowset->version() != it.first) {
            CHECK(it.second != nullptr)
                    << "there exist a version=" << it.first
                    << " contains the input rs with version=" << rowset->version()
                    << ", but the related rs is null";
            rowsets_to_delete.push_back(it.second);
        }
    }
    std::vector<RowsetSharedPtr> empty_vec;
    RETURN_IF_ERROR(modify_rowsets(empty_vec, rowsets_to_delete));
    ++_newly_created_rowset_num;
    return Status::OK();
}

Status Tablet::modify_rowsets(std::vector<RowsetSharedPtr>& to_add,
                              std::vector<RowsetSharedPtr>& to_delete, bool check_delete) {
    // the compaction process allow to compact the single version, eg: version[4-4].
    // this kind of "single version compaction" has same "input version" and "output version".
    // which means "to_add->version()" equals to "to_delete->version()".
    // So we should delete the "to_delete" before adding the "to_add",
    // otherwise, the "to_add" will be deleted from _rs_version_map, eventually.
    //
    // And if the version of "to_add" and "to_delete" are exactly same. eg:
    // to_add:      [7-7]
    // to_delete:   [7-7]
    // In this case, we no longer need to add the rowset in "to_delete" to
    // _stale_rs_version_map, but can delete it directly.

    if (to_add.empty() && to_delete.empty()) {
        return Status::OK();
    }

    if (check_delete) {
        for (auto&& rs : to_delete) {
            if (auto it = _rs_version_map.find(rs->version()); it == _rs_version_map.end()) {
                return Status::Error<DELETE_VERSION_ERROR>(
                        "try to delete not exist version {} from {}", rs->version().to_string(),
                        tablet_id());
            } else if (rs->rowset_id() != it->second->rowset_id()) {
                return Status::Error<DELETE_VERSION_ERROR>(
                        "try to delete version {} from {}, but rowset id changed, delete rowset id "
                        "is {}, exists rowsetid is {}",
                        rs->version().to_string(), tablet_id(), rs->rowset_id().to_string(),
                        it->second->rowset_id().to_string());
            }
        }
    }

    bool same_version = true;
    std::sort(to_add.begin(), to_add.end(), Rowset::comparator);
    std::sort(to_delete.begin(), to_delete.end(), Rowset::comparator);
    if (to_add.size() == to_delete.size()) {
        for (int i = 0; i < to_add.size(); ++i) {
            if (to_add[i]->version() != to_delete[i]->version()) {
                same_version = false;
                break;
            }
        }
    } else {
        same_version = false;
    }

    std::vector<RowsetMetaSharedPtr> rs_metas_to_delete;
    for (auto& rs : to_delete) {
        rs_metas_to_delete.push_back(rs->rowset_meta());
        _rs_version_map.erase(rs->version());

        if (!same_version) {
            // put compaction rowsets in _stale_rs_version_map.
            _stale_rs_version_map[rs->version()] = rs;
        }
    }

    std::vector<RowsetMetaSharedPtr> rs_metas_to_add;
    for (auto& rs : to_add) {
        rs_metas_to_add.push_back(rs->rowset_meta());
        _rs_version_map[rs->version()] = rs;

        if (!same_version) {
            // If version are same, then _timestamped_version_tracker
            // already has this version, no need to add again.
            _timestamped_version_tracker.add_version(rs->version());
        }
        ++_newly_created_rowset_num;
    }

    _tablet_meta->modify_rs_metas(rs_metas_to_add, rs_metas_to_delete, same_version);

    if (!same_version) {
        // add rs_metas_to_delete to tracker
        _timestamped_version_tracker.add_stale_path_version(rs_metas_to_delete);
    } else {
        // delete rowset in "to_delete" directly
        for (auto& rs : to_delete) {
            LOG(INFO) << "add unused rowset " << rs->rowset_id() << " because of same version";
            if (rs->is_local()) {
                _engine.add_unused_rowset(rs);
            }
        }
    }
    return Status::OK();
}

void Tablet::add_rowsets(const std::vector<RowsetSharedPtr>& to_add) {
    if (to_add.empty()) {
        return;
    }
    std::vector<RowsetMetaSharedPtr> rs_metas;
    rs_metas.reserve(to_add.size());
    for (auto& rs : to_add) {
        _rs_version_map.emplace(rs->version(), rs);
        _timestamped_version_tracker.add_version(rs->version());
        rs_metas.push_back(rs->rowset_meta());
    }
    _tablet_meta->modify_rs_metas(rs_metas, {});
}

void Tablet::delete_rowsets(const std::vector<RowsetSharedPtr>& to_delete, bool move_to_stale) {
    if (to_delete.empty()) {
        return;
    }
    std::vector<RowsetMetaSharedPtr> rs_metas;
    rs_metas.reserve(to_delete.size());
    for (auto& rs : to_delete) {
        rs_metas.push_back(rs->rowset_meta());
        _rs_version_map.erase(rs->version());
    }
    _tablet_meta->modify_rs_metas({}, rs_metas, !move_to_stale);
    if (move_to_stale) {
        for (auto& rs : to_delete) {
            _stale_rs_version_map[rs->version()] = rs;
        }
        _timestamped_version_tracker.add_stale_path_version(rs_metas);
    } else {
        for (auto& rs : to_delete) {
            _timestamped_version_tracker.delete_version(rs->version());
            if (rs->is_local()) {
                _engine.add_unused_rowset(rs);
            }
        }
    }
}

RowsetSharedPtr Tablet::_rowset_with_largest_size() {
    RowsetSharedPtr largest_rowset = nullptr;
    for (auto& it : _rs_version_map) {
        if (it.second->empty() || it.second->zero_num_rows()) {
            continue;
        }
        if (largest_rowset == nullptr || it.second->rowset_meta()->index_disk_size() >
                                                 largest_rowset->rowset_meta()->index_disk_size()) {
            largest_rowset = it.second;
        }
    }

    return largest_rowset;
}

// add inc rowset should not persist tablet meta, because it will be persisted when publish txn.
Status Tablet::add_inc_rowset(const RowsetSharedPtr& rowset) {
    DCHECK(rowset != nullptr);
    std::lock_guard<std::shared_mutex> wrlock(_meta_lock);
    SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
    if (_contains_rowset(rowset->rowset_id())) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_contains_version(rowset->version()));

    RETURN_IF_ERROR(_tablet_meta->add_rs_meta(rowset->rowset_meta()));
    _rs_version_map[rowset->version()] = rowset;

    _timestamped_version_tracker.add_version(rowset->version());

    ++_newly_created_rowset_num;
    return Status::OK();
}

void Tablet::_delete_stale_rowset_by_version(const Version& version) {
    RowsetMetaSharedPtr rowset_meta = _tablet_meta->acquire_stale_rs_meta_by_version(version);
    if (rowset_meta == nullptr) {
        return;
    }
    _tablet_meta->delete_stale_rs_meta_by_version(version);
    VLOG_NOTICE << "delete stale rowset. tablet=" << tablet_id() << ", version=" << version;
}

void Tablet::delete_expired_stale_rowset() {
    int64_t now = UnixSeconds();
    // hold write lock while processing stable rowset
    {
        std::lock_guard<std::shared_mutex> wrlock(_meta_lock);
        SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
        // Compute the end time to delete rowsets, when a expired rowset createtime less then this time, it will be deleted.
        double expired_stale_sweep_endtime =
                ::difftime(now, config::tablet_rowset_stale_sweep_time_sec);
        if (config::tablet_rowset_stale_sweep_by_size) {
            expired_stale_sweep_endtime = now;
        }

        std::vector<int64_t> path_id_vec;
        // capture the path version to delete
        _timestamped_version_tracker.capture_expired_paths(
                static_cast<int64_t>(expired_stale_sweep_endtime), &path_id_vec);

        if (path_id_vec.empty()) {
            return;
        }

        const RowsetSharedPtr lastest_delta = get_rowset_with_max_version();
        if (lastest_delta == nullptr) {
            LOG(WARNING) << "lastest_delta is null " << tablet_id();
            return;
        }

        // fetch missing version before delete
        Versions missed_versions = get_missed_versions_unlocked(lastest_delta->end_version());
        if (!missed_versions.empty()) {
            LOG(WARNING) << "tablet:" << tablet_id()
                         << ", missed version for version:" << lastest_delta->end_version();
            _print_missed_versions(missed_versions);
            return;
        }

        // do check consistent operation
        auto path_id_iter = path_id_vec.begin();

        std::map<int64_t, PathVersionListSharedPtr> stale_version_path_map;
        while (path_id_iter != path_id_vec.end()) {
            PathVersionListSharedPtr version_path =
                    _timestamped_version_tracker.fetch_and_delete_path_by_id(*path_id_iter);

            Version test_version = Version(0, lastest_delta->end_version());
            stale_version_path_map[*path_id_iter] = version_path;

            Status status =
                    capture_consistent_versions_unlocked(test_version, nullptr, false, false);
            // 1. When there is no consistent versions, we must reconstruct the tracker.
            if (!status.ok()) {
                // 2. fetch missing version after delete
                Versions after_missed_versions =
                        get_missed_versions_unlocked(lastest_delta->end_version());

                // 2.1 check whether missed_versions and after_missed_versions are the same.
                // when they are the same, it means we can delete the path securely.
                bool is_missing = missed_versions.size() != after_missed_versions.size();

                if (!is_missing) {
                    for (int ver_index = 0; ver_index < missed_versions.size(); ver_index++) {
                        if (missed_versions[ver_index] != after_missed_versions[ver_index]) {
                            is_missing = true;
                            break;
                        }
                    }
                }

                if (is_missing) {
                    LOG(WARNING) << "The consistent version check fails, there are bugs. "
                                 << "Reconstruct the tracker to recover versions in tablet="
                                 << tablet_id();

                    // 3. try to recover
                    _timestamped_version_tracker.recover_versioned_tracker(stale_version_path_map);

                    // 4. double check the consistent versions
                    // fetch missing version after recover
                    Versions recover_missed_versions =
                            get_missed_versions_unlocked(lastest_delta->end_version());

                    // 4.1 check whether missed_versions and recover_missed_versions are the same.
                    // when they are the same, it means we recover successfully.
                    bool is_recover_missing =
                            missed_versions.size() != recover_missed_versions.size();

                    if (!is_recover_missing) {
                        for (int ver_index = 0; ver_index < missed_versions.size(); ver_index++) {
                            if (missed_versions[ver_index] != recover_missed_versions[ver_index]) {
                                is_recover_missing = true;
                                break;
                            }
                        }
                    }

                    // 5. check recover fail, version is mission
                    if (is_recover_missing) {
                        if (!config::ignore_rowset_stale_unconsistent_delete) {
                            LOG(FATAL)
                                    << "rowset stale unconsistent delete. tablet= " << tablet_id();
                        } else {
                            LOG(WARNING)
                                    << "rowset stale unconsistent delete. tablet= " << tablet_id();
                        }
                    }
                }
                return;
            }
            path_id_iter++;
        }

        auto old_size = _stale_rs_version_map.size();
        auto old_meta_size = _tablet_meta->all_stale_rs_metas().size();

        // do delete operation
        auto to_delete_iter = stale_version_path_map.begin();
        while (to_delete_iter != stale_version_path_map.end()) {
            std::vector<TimestampedVersionSharedPtr>& to_delete_version =
                    to_delete_iter->second->timestamped_versions();
            for (auto& timestampedVersion : to_delete_version) {
                auto it = _stale_rs_version_map.find(timestampedVersion->version());
                if (it != _stale_rs_version_map.end()) {
                    it->second->clear_cache();
                    // delete rowset
                    if (it->second->is_local()) {
                        _engine.add_unused_rowset(it->second);
                    }
                    _stale_rs_version_map.erase(it);
                    VLOG_NOTICE << "delete stale rowset tablet=" << tablet_id() << " version["
                                << timestampedVersion->version().first << ","
                                << timestampedVersion->version().second
                                << "] move to unused_rowset success " << std::fixed
                                << expired_stale_sweep_endtime;
                } else {
                    LOG(WARNING) << "delete stale rowset tablet=" << tablet_id() << " version["
                                 << timestampedVersion->version().first << ","
                                 << timestampedVersion->version().second
                                 << "] not find in stale rs version map";
                }
                _delete_stale_rowset_by_version(timestampedVersion->version());
            }
            to_delete_iter++;
        }

        bool reconstructed = _reconstruct_version_tracker_if_necessary();

        VLOG_NOTICE << "delete stale rowset _stale_rs_version_map tablet=" << tablet_id()
                    << " current_size=" << _stale_rs_version_map.size() << " old_size=" << old_size
                    << " current_meta_size=" << _tablet_meta->all_stale_rs_metas().size()
                    << " old_meta_size=" << old_meta_size << " sweep endtime " << std::fixed
                    << expired_stale_sweep_endtime << ", reconstructed=" << reconstructed;
    }
#ifndef BE_TEST
    {
        std::shared_lock<std::shared_mutex> rlock(_meta_lock);
        save_meta();
    }
#endif
}

Status Tablet::capture_consistent_versions_unlocked(const Version& spec_version,
                                                    Versions* version_path,
                                                    bool skip_missing_version, bool quiet) const {
    Status status =
            _timestamped_version_tracker.capture_consistent_versions(spec_version, version_path);
    if (!status.ok() && !quiet) {
        Versions missed_versions = get_missed_versions_unlocked(spec_version.second);
        if (missed_versions.empty()) {
            // if version_path is null, it may be a compaction check logic.
            // so to avoid print too many logs.
            if (version_path != nullptr) {
                LOG(WARNING) << "tablet:" << tablet_id()
                             << ", version already has been merged. spec_version: " << spec_version
                             << ", max_version: " << max_version_unlocked();
            }
            status = Status::Error<VERSION_ALREADY_MERGED>(
                    "missed_versions is empty, spec_version "
                    "{}, max_version {}, tablet_id {}",
                    spec_version.second, max_version_unlocked(), tablet_id());
        } else {
            if (version_path != nullptr) {
                LOG(WARNING) << "status:" << status << ", tablet:" << tablet_id()
                             << ", missed version for version:" << spec_version;
                _print_missed_versions(missed_versions);
                if (skip_missing_version) {
                    LOG(WARNING) << "force skipping missing version for tablet:" << tablet_id();
                    return Status::OK();
                }
            }
        }
    }

    DBUG_EXECUTE_IF("TTablet::capture_consistent_versions.inject_failure", {
        auto tablet_id = dp->param<int64>("tablet_id", -1);
        if (tablet_id != -1 && tablet_id == _tablet_meta->tablet_id()) {
            status = Status::Error<VERSION_ALREADY_MERGED>("version already merged");
        }
    });

    return status;
}

Status Tablet::check_version_integrity(const Version& version, bool quiet) {
    std::shared_lock rdlock(_meta_lock);
    return capture_consistent_versions_unlocked(version, nullptr, false, quiet);
}

bool Tablet::exceed_version_limit(int32_t limit) {
    if (_tablet_meta->version_count() > limit) {
        exceed_version_limit_counter << 1;
        return true;
    }
    return false;
}

// If any rowset contains the specific version, it means the version already exist
bool Tablet::check_version_exist(const Version& version) const {
    std::shared_lock rdlock(_meta_lock);
    for (auto& it : _rs_version_map) {
        if (it.first.contains(version)) {
            return true;
        }
    }
    return false;
}

// The meta read lock should be held before calling
void Tablet::acquire_version_and_rowsets(
        std::vector<std::pair<Version, RowsetSharedPtr>>* version_rowsets) const {
    for (const auto& it : _rs_version_map) {
        version_rowsets->emplace_back(it.first, it.second);
    }
}

Status Tablet::capture_consistent_rowsets_unlocked(const Version& spec_version,
                                                   std::vector<RowsetSharedPtr>* rowsets) const {
    std::vector<Version> version_path;
    RETURN_IF_ERROR(
            capture_consistent_versions_unlocked(spec_version, &version_path, false, false));
    RETURN_IF_ERROR(_capture_consistent_rowsets_unlocked(version_path, rowsets));
    return Status::OK();
}

Status Tablet::capture_rs_readers(const Version& spec_version, std::vector<RowSetSplits>* rs_splits,
                                  bool skip_missing_version) {
    std::shared_lock rlock(_meta_lock);
    std::vector<Version> version_path;
    RETURN_IF_ERROR(capture_consistent_versions_unlocked(spec_version, &version_path,
                                                         skip_missing_version, false));
    RETURN_IF_ERROR(capture_rs_readers_unlocked(version_path, rs_splits));
    return Status::OK();
}

Versions Tablet::calc_missed_versions(int64_t spec_version, Versions existing_versions) const {
    DCHECK(spec_version > 0) << "invalid spec_version: " << spec_version;

    // sort the existing versions in ascending order
    std::sort(existing_versions.begin(), existing_versions.end(),
              [](const Version& a, const Version& b) {
                  // simple because 2 versions are certainly not overlapping
                  return a.first < b.first;
              });

    // From the first version(=0),  find the missing version until spec_version
    int64_t last_version = -1;
    Versions missed_versions;
    for (const Version& version : existing_versions) {
        if (version.first > last_version + 1) {
            for (int64_t i = last_version + 1; i < version.first && i <= spec_version; ++i) {
                // Don't merge missed_versions because clone & snapshot use single version.
                // For example, if miss 4 ~ 6, clone need [4, 4], [5, 5], [6, 6], but not [4, 6].
                missed_versions.emplace_back(i, i);
            }
        }
        last_version = version.second;
        if (last_version >= spec_version) {
            break;
        }
    }
    for (int64_t i = last_version + 1; i <= spec_version; ++i) {
        missed_versions.emplace_back(i, i);
    }

    return missed_versions;
}

bool Tablet::can_do_compaction(size_t path_hash, CompactionType compaction_type) {
    if (compaction_type == CompactionType::BASE_COMPACTION && tablet_state() != TABLET_RUNNING) {
        // base compaction can only be done for tablet in TABLET_RUNNING state.
        // but cumulative compaction can be done for TABLET_NOTREADY, such as tablet under alter process.
        return false;
    }

    if (data_dir()->path_hash() != path_hash || !is_used() || !init_succeeded()) {
        return false;
    }

    // In TABLET_NOTREADY, we keep last 10 versions in new tablet so base tablet max_version
    // not merged in new tablet and then we can do compaction
    return tablet_state() == TABLET_RUNNING || tablet_state() == TABLET_NOTREADY;
}

uint32_t Tablet::calc_compaction_score(
        CompactionType compaction_type,
        std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy) {
    // Need meta lock, because it will iterator "all_rs_metas" of tablet meta.
    std::shared_lock rdlock(_meta_lock);
    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
        return _calc_cumulative_compaction_score(cumulative_compaction_policy);
    } else {
        DCHECK_EQ(compaction_type, CompactionType::BASE_COMPACTION);
        return _calc_base_compaction_score();
    }
}

uint32_t Tablet::calc_cold_data_compaction_score() const {
    uint32_t score = 0;
    std::vector<RowsetMetaSharedPtr> cooldowned_rowsets;
    int64_t max_delete_version = 0;
    {
        std::shared_lock rlock(_meta_lock);
        for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
            if (!rs_meta->is_local()) {
                cooldowned_rowsets.push_back(rs_meta);
                if (rs_meta->has_delete_predicate() &&
                    rs_meta->end_version() > max_delete_version) {
                    max_delete_version = rs_meta->end_version();
                }
            }
        }
    }
    for (auto& rs_meta : cooldowned_rowsets) {
        if (rs_meta->end_version() < max_delete_version) {
            score += rs_meta->num_segments();
        } else {
            score += rs_meta->get_compaction_score();
        }
    }
    return (keys_type() != KeysType::DUP_KEYS) ? score * 2 : score;
}

uint32_t Tablet::_calc_cumulative_compaction_score(
        std::shared_ptr<CumulativeCompactionPolicy> cumulative_compaction_policy) {
    if (cumulative_compaction_policy == nullptr) [[unlikely]] {
        return 0;
    }
#ifndef BE_TEST
    if (_cumulative_compaction_policy == nullptr ||
        _cumulative_compaction_policy->name() != cumulative_compaction_policy->name()) {
        _cumulative_compaction_policy = cumulative_compaction_policy;
    }
#endif
    return _cumulative_compaction_policy->calc_cumulative_compaction_score(this);
}

uint32_t Tablet::_calc_base_compaction_score() const {
    uint32_t score = 0;
    const int64_t point = cumulative_layer_point();
    bool base_rowset_exist = false;
    bool has_delete = false;
    for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
        if (rs_meta->start_version() == 0) {
            base_rowset_exist = true;
        }
        if (rs_meta->start_version() >= point || !rs_meta->is_local()) {
            // all_rs_metas() is not sorted, so we use _continue_ other than _break_ here.
            continue;
        }
        if (rs_meta->has_delete_predicate()) {
            has_delete = true;
        }
        score += rs_meta->get_compaction_score();
    }

    // In the time series compaction policy, we want the base compaction to be triggered
    // when there are delete versions present.
    if (_tablet_meta->compaction_policy() == CUMULATIVE_TIME_SERIES_POLICY) {
        return (base_rowset_exist && has_delete) ? score : 0;
    }

    // base不存在可能是tablet正在做alter table，先不选它，设score=0
    return base_rowset_exist ? score : 0;
}

void Tablet::max_continuous_version_from_beginning(Version* version, Version* max_version) {
    bool has_version_cross;
    std::shared_lock rdlock(_meta_lock);
    _max_continuous_version_from_beginning_unlocked(version, max_version, &has_version_cross);
}

void Tablet::_max_continuous_version_from_beginning_unlocked(Version* version, Version* max_version,
                                                             bool* has_version_cross) const {
    std::vector<Version> existing_versions;
    *has_version_cross = false;
    for (auto& rs : _tablet_meta->all_rs_metas()) {
        existing_versions.emplace_back(rs->version());
    }

    // sort the existing versions in ascending order
    std::sort(existing_versions.begin(), existing_versions.end(),
              [](const Version& left, const Version& right) {
                  // simple because 2 versions are certainly not overlapping
                  return left.first < right.first;
              });

    Version max_continuous_version = {-1, -1};
    for (int i = 0; i < existing_versions.size(); ++i) {
        if (existing_versions[i].first > max_continuous_version.second + 1) {
            break;
        } else if (existing_versions[i].first <= max_continuous_version.second) {
            *has_version_cross = true;
        }
        max_continuous_version = existing_versions[i];
    }
    *version = max_continuous_version;
    // tablet may not has rowset, eg, tablet has just been clear for restore.
    if (max_version != nullptr && !existing_versions.empty()) {
        *max_version = existing_versions.back();
    }
}

void Tablet::calculate_cumulative_point() {
    std::lock_guard<std::shared_mutex> wrlock(_meta_lock);
    SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
    int64_t ret_cumulative_point;
    _cumulative_compaction_policy->calculate_cumulative_point(
            this, _tablet_meta->all_rs_metas(), _cumulative_point, &ret_cumulative_point);

    if (ret_cumulative_point == K_INVALID_CUMULATIVE_POINT) {
        return;
    }
    set_cumulative_layer_point(ret_cumulative_point);
}

// NOTE: only used when create_table, so it is sure that there is no concurrent reader and writer.
void Tablet::delete_all_files() {
    // Release resources like memory and disk space.
    std::shared_lock rdlock(_meta_lock);
    for (auto it : _rs_version_map) {
        static_cast<void>(it.second->remove());
    }
    _rs_version_map.clear();

    for (auto it : _stale_rs_version_map) {
        static_cast<void>(it.second->remove());
    }
    _stale_rs_version_map.clear();
}

void Tablet::check_tablet_path_exists() {
    if (!tablet_path().empty()) {
        std::error_code ec;
        if (std::filesystem::is_directory(tablet_path(), ec)) {
            _is_tablet_path_exists.store(true, std::memory_order_relaxed);
        } else if (ec.value() == ENOENT || ec.value() == 0) {
            _is_tablet_path_exists.store(false, std::memory_order_relaxed);
        }
    }
}

Status Tablet::_contains_version(const Version& version) {
    // check if there exist a rowset contains the added rowset
    for (auto& it : _rs_version_map) {
        if (it.first.contains(version)) {
            // TODO(lingbin): Is this check unnecessary?
            // because the value type is std::shared_ptr, when will it be nullptr?
            // In addition, in this class, there are many places that do not make this judgment
            // when access _rs_version_map's value.
            CHECK(it.second != nullptr) << "there exist a version=" << it.first
                                        << " contains the input rs with version=" << version
                                        << ", but the related rs is null";
            return Status::Error<PUSH_VERSION_ALREADY_EXIST>("Tablet push duplicate version {}",
                                                             version.to_string());
        }
    }

    return Status::OK();
}

TabletInfo Tablet::get_tablet_info() const {
    return TabletInfo(tablet_id(), tablet_uid());
}

std::vector<RowsetSharedPtr> Tablet::pick_candidate_rowsets_to_cumulative_compaction() {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    if (_cumulative_point == K_INVALID_CUMULATIVE_POINT) {
        return candidate_rowsets;
    }
    return _pick_visible_rowsets_to_compaction(_cumulative_point,
                                               std::numeric_limits<int64_t>::max());
}

std::vector<RowsetSharedPtr> Tablet::pick_candidate_rowsets_to_base_compaction() {
    return _pick_visible_rowsets_to_compaction(std::numeric_limits<int64_t>::min(),
                                               _cumulative_point - 1);
}

std::vector<RowsetSharedPtr> Tablet::_pick_visible_rowsets_to_compaction(
        int64_t min_start_version, int64_t max_start_version) {
    auto [visible_version, update_ts] = get_visible_version_and_time();
    bool update_time_long = MonotonicMillis() - update_ts >
                            config::compaction_keep_invisible_version_timeout_sec * 1000L;
    int32_t keep_invisible_version_limit =
            update_time_long ? config::compaction_keep_invisible_version_min_count
                             : config::compaction_keep_invisible_version_max_count;

    std::vector<RowsetSharedPtr> candidate_rowsets;
    {
        std::shared_lock rlock(_meta_lock);
        for (const auto& [version, rs] : _rs_version_map) {
            int64_t version_start = version.first;
            // rowset is remote or rowset is not in given range
            if (!rs->is_local() || version_start < min_start_version ||
                version_start > max_start_version) {
                continue;
            }

            // can compact, met one of the conditions:
            // 1. had been visible;
            // 2. exceeds the limit of keep invisible versions.
            int64_t version_end = version.second;
            if (version_end <= visible_version ||
                version_end > visible_version + keep_invisible_version_limit) {
                candidate_rowsets.push_back(rs);
            }
        }
    }
    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);
    return candidate_rowsets;
}

std::vector<RowsetSharedPtr> Tablet::pick_candidate_rowsets_to_full_compaction() {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    traverse_rowsets([&candidate_rowsets](const auto& rs) {
        // Do full compaction on all local rowsets.
        if (rs->is_local()) {
            candidate_rowsets.emplace_back(rs);
        }
    });
    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);
    return candidate_rowsets;
}

std::vector<RowsetSharedPtr> Tablet::pick_candidate_rowsets_to_build_inverted_index(
        const std::set<int64_t>& alter_index_uids, bool is_drop_op) {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    {
        std::shared_lock rlock(_meta_lock);
        auto has_alter_inverted_index = [&](RowsetSharedPtr rowset) -> bool {
            for (const auto& index_id : alter_index_uids) {
                if (rowset->tablet_schema()->has_inverted_index_with_index_id(index_id, "")) {
                    return true;
                }
            }
            return false;
        };

        for (const auto& [version, rs] : _rs_version_map) {
            if (!has_alter_inverted_index(rs) && is_drop_op) {
                continue;
            }
            if (has_alter_inverted_index(rs) && !is_drop_op) {
                continue;
            }

            if (rs->is_local()) {
                candidate_rowsets.push_back(rs);
            }
        }
    }
    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);
    return candidate_rowsets;
}

std::tuple<int64_t, int64_t> Tablet::get_visible_version_and_time() const {
    // some old tablet has bug, its partition_id is 0, fe couldn't update its visible version.
    // so let this tablet's visible version become int64 max.
    auto version_info = std::atomic_load_explicit(&_visible_version, std::memory_order_relaxed);
    if (version_info != nullptr && partition_id() != 0) {
        return std::make_tuple(version_info->version.load(std::memory_order_relaxed),
                               version_info->update_ts);
    } else {
        return std::make_tuple(std::numeric_limits<int64_t>::max(),
                               std::numeric_limits<int64_t>::max());
    }
}

// For http compaction action
void Tablet::get_compaction_status(std::string* json_result) {
    rapidjson::Document root;
    root.SetObject();

    rapidjson::Document path_arr;
    path_arr.SetArray();

    std::vector<RowsetSharedPtr> rowsets;
    std::vector<RowsetSharedPtr> stale_rowsets;
    std::vector<bool> delete_flags;
    {
        std::shared_lock rdlock(_meta_lock);
        rowsets.reserve(_rs_version_map.size());
        for (auto& it : _rs_version_map) {
            rowsets.push_back(it.second);
        }
        std::sort(rowsets.begin(), rowsets.end(), Rowset::comparator);

        stale_rowsets.reserve(_stale_rs_version_map.size());
        for (auto& it : _stale_rs_version_map) {
            stale_rowsets.push_back(it.second);
        }
        std::sort(stale_rowsets.begin(), stale_rowsets.end(), Rowset::comparator);

        delete_flags.reserve(rowsets.size());
        for (auto& rs : rowsets) {
            delete_flags.push_back(rs->rowset_meta()->has_delete_predicate());
        }
        // get snapshot version path json_doc
        _timestamped_version_tracker.get_stale_version_path_json_doc(path_arr);
    }
    rapidjson::Value cumulative_policy_type;
    std::string policy_type_str = "cumulative compaction policy not initializied";
    if (_cumulative_compaction_policy != nullptr) {
        policy_type_str = _cumulative_compaction_policy->name();
    }
    cumulative_policy_type.SetString(policy_type_str.c_str(), policy_type_str.length(),
                                     root.GetAllocator());
    root.AddMember("cumulative policy type", cumulative_policy_type, root.GetAllocator());
    root.AddMember("cumulative point", _cumulative_point.load(), root.GetAllocator());
    rapidjson::Value cumu_value;
    std::string format_str = ToStringFromUnixMillis(_last_cumu_compaction_failure_millis.load());
    cumu_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last cumulative failure time", cumu_value, root.GetAllocator());
    rapidjson::Value base_value;
    format_str = ToStringFromUnixMillis(_last_base_compaction_failure_millis.load());
    base_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last base failure time", base_value, root.GetAllocator());
    rapidjson::Value full_value;
    format_str = ToStringFromUnixMillis(_last_full_compaction_failure_millis.load());
    full_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last full failure time", full_value, root.GetAllocator());
    rapidjson::Value cumu_success_value;
    format_str = ToStringFromUnixMillis(_last_cumu_compaction_success_millis.load());
    cumu_success_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last cumulative success time", cumu_success_value, root.GetAllocator());
    rapidjson::Value base_success_value;
    format_str = ToStringFromUnixMillis(_last_base_compaction_success_millis.load());
    base_success_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last base success time", base_success_value, root.GetAllocator());
    rapidjson::Value full_success_value;
    format_str = ToStringFromUnixMillis(_last_full_compaction_success_millis.load());
    full_success_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last full success time", full_success_value, root.GetAllocator());
    rapidjson::Value base_schedule_value;
    format_str = ToStringFromUnixMillis(_last_base_compaction_schedule_millis.load());
    base_schedule_value.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last base schedule time", base_schedule_value, root.GetAllocator());
    rapidjson::Value base_compaction_status_value;
    base_compaction_status_value.SetString(_last_base_compaction_status.c_str(),
                                           _last_base_compaction_status.length(),
                                           root.GetAllocator());
    root.AddMember("last base status", base_compaction_status_value, root.GetAllocator());

    // last single replica compaction status
    // "single replica compaction status": {
    //     "remote peer": "172.100.1.0:10875",
    //     "last failure status": "",
    //     "last fetched rowset": "[8-10]"
    // }
    rapidjson::Document status;
    status.SetObject();
    TReplicaInfo replica_info;
    std::string dummp_token;
    if (tablet_meta()->tablet_schema()->enable_single_replica_compaction() &&
        _engine.get_peer_replica_info(tablet_id(), &replica_info, &dummp_token)) {
        // remote peer
        rapidjson::Value peer_addr;
        std::string addr = replica_info.host + ":" + std::to_string(replica_info.brpc_port);
        peer_addr.SetString(addr.c_str(), addr.length(), status.GetAllocator());
        status.AddMember("remote peer", peer_addr, status.GetAllocator());
        // last failure status
        rapidjson::Value compaction_status;
        compaction_status.SetString(_last_single_compaction_failure_status.c_str(),
                                    _last_single_compaction_failure_status.length(),
                                    status.GetAllocator());
        status.AddMember("last failure status", compaction_status, status.GetAllocator());
        // last fetched rowset
        rapidjson::Value version;
        std::string fetched_version = _last_fetched_version.to_string();
        version.SetString(fetched_version.c_str(), fetched_version.length(), status.GetAllocator());
        status.AddMember("last fetched rowset", version, status.GetAllocator());
        root.AddMember("single replica compaction status", status, root.GetAllocator());
    }

    // print all rowsets' version as an array
    rapidjson::Document versions_arr;
    rapidjson::Document missing_versions_arr;
    versions_arr.SetArray();
    missing_versions_arr.SetArray();
    int64_t last_version = -1;
    for (auto& rowset : rowsets) {
        const Version& ver = rowset->version();
        if (ver.first != last_version + 1) {
            rapidjson::Value miss_value;
            miss_value.SetString(fmt::format("[{}-{}]", last_version + 1, ver.first - 1).c_str(),
                                 missing_versions_arr.GetAllocator());
            missing_versions_arr.PushBack(miss_value, missing_versions_arr.GetAllocator());
        }
        rapidjson::Value value;
        std::string version_str = rowset->get_rowset_info_str();
        value.SetString(version_str.c_str(), version_str.length(), versions_arr.GetAllocator());
        versions_arr.PushBack(value, versions_arr.GetAllocator());
        last_version = ver.second;
    }
    root.AddMember("rowsets", versions_arr, root.GetAllocator());
    root.AddMember("missing_rowsets", missing_versions_arr, root.GetAllocator());

    // print all stale rowsets' version as an array
    rapidjson::Document stale_versions_arr;
    stale_versions_arr.SetArray();
    for (auto& rowset : stale_rowsets) {
        rapidjson::Value value;
        std::string version_str = rowset->get_rowset_info_str();
        value.SetString(version_str.c_str(), version_str.length(),
                        stale_versions_arr.GetAllocator());
        stale_versions_arr.PushBack(value, stale_versions_arr.GetAllocator());
    }
    root.AddMember("stale_rowsets", stale_versions_arr, root.GetAllocator());

    // add stale version rowsets
    root.AddMember("stale version path", path_arr, root.GetAllocator());

    // to json string
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    *json_result = std::string(strbuf.GetString());
}

bool Tablet::do_tablet_meta_checkpoint() {
    std::lock_guard<std::shared_mutex> store_lock(_meta_store_lock);
    if (_newly_created_rowset_num == 0) {
        return false;
    }
    if (UnixMillis() - _last_checkpoint_time <
                config::tablet_meta_checkpoint_min_interval_secs * 1000 &&
        _newly_created_rowset_num < config::tablet_meta_checkpoint_min_new_rowsets_num) {
        return false;
    }

    // hold read-lock other than write-lock, because it will not modify meta structure
    std::shared_lock rdlock(_meta_lock);
    if (tablet_state() != TABLET_RUNNING) {
        LOG(INFO) << "tablet is under state=" << tablet_state()
                  << ", not running, skip do checkpoint"
                  << ", tablet=" << tablet_id();
        return false;
    }
    VLOG_NOTICE << "start to do tablet meta checkpoint, tablet=" << tablet_id();
    save_meta();
    // if save meta successfully, then should remove the rowset meta existing in tablet
    // meta from rowset meta store
    for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
        // If we delete it from rowset manager's meta explicitly in previous checkpoint, just skip.
        if (rs_meta->is_remove_from_rowset_meta()) {
            continue;
        }
        if (RowsetMetaManager::check_rowset_meta(_data_dir->get_meta(), tablet_uid(),
                                                 rs_meta->rowset_id())) {
            RETURN_FALSE_IF_ERROR(RowsetMetaManager::remove(_data_dir->get_meta(), tablet_uid(),
                                                            rs_meta->rowset_id()));
            VLOG_NOTICE << "remove rowset id from meta store because it is already persistent with "
                        << "tablet meta, rowset_id=" << rs_meta->rowset_id();
        }
        rs_meta->set_remove_from_rowset_meta();
    }

    // check _stale_rs_version_map to remove meta from rowset meta store
    for (auto& rs_meta : _tablet_meta->all_stale_rs_metas()) {
        // If we delete it from rowset manager's meta explicitly in previous checkpoint, just skip.
        if (rs_meta->is_remove_from_rowset_meta()) {
            continue;
        }
        if (RowsetMetaManager::check_rowset_meta(_data_dir->get_meta(), tablet_uid(),
                                                 rs_meta->rowset_id())) {
            RETURN_FALSE_IF_ERROR(RowsetMetaManager::remove(_data_dir->get_meta(), tablet_uid(),
                                                            rs_meta->rowset_id()));
            VLOG_NOTICE << "remove rowset id from meta store because it is already persistent with "
                        << "tablet meta, rowset_id=" << rs_meta->rowset_id();
        }
        rs_meta->set_remove_from_rowset_meta();
    }

    if (keys_type() == UNIQUE_KEYS && enable_unique_key_merge_on_write()) {
        RETURN_FALSE_IF_ERROR(TabletMetaManager::remove_old_version_delete_bitmap(
                _data_dir, tablet_id(), max_version_unlocked()));
    }

    _newly_created_rowset_num = 0;
    _last_checkpoint_time = UnixMillis();
    return true;
}

bool Tablet::rowset_meta_is_useful(RowsetMetaSharedPtr rowset_meta) {
    std::shared_lock rdlock(_meta_lock);
    bool find_version = false;
    for (auto& version_rowset : _rs_version_map) {
        if (version_rowset.second->rowset_id() == rowset_meta->rowset_id()) {
            return true;
        }
        if (version_rowset.second->contains_version(rowset_meta->version())) {
            find_version = true;
        }
    }
    for (auto& stale_version_rowset : _stale_rs_version_map) {
        if (stale_version_rowset.second->rowset_id() == rowset_meta->rowset_id()) {
            return true;
        }
        if (stale_version_rowset.second->contains_version(rowset_meta->version())) {
            find_version = true;
        }
    }
    return !find_version;
}

bool Tablet::_contains_rowset(const RowsetId rowset_id) {
    for (auto& version_rowset : _rs_version_map) {
        if (version_rowset.second->rowset_id() == rowset_id) {
            return true;
        }
    }
    for (auto& stale_version_rowset : _stale_rs_version_map) {
        if (stale_version_rowset.second->rowset_id() == rowset_id) {
            return true;
        }
    }
    return false;
}

// need check if consecutive version missing in full report
// alter tablet will ignore this check
void Tablet::build_tablet_report_info(TTabletInfo* tablet_info,
                                      bool enable_consecutive_missing_check,
                                      bool enable_path_check) {
    std::shared_lock rdlock(_meta_lock);
    tablet_info->__set_tablet_id(_tablet_meta->tablet_id());
    tablet_info->__set_schema_hash(_tablet_meta->schema_hash());
    tablet_info->__set_row_count(_tablet_meta->num_rows());
    tablet_info->__set_data_size(_tablet_meta->tablet_local_size());

    // Here we need to report to FE if there are any missing versions of tablet.
    // We start from the initial version and traverse backwards until we meet a discontinuous version.
    Version cversion;
    Version max_version;
    bool has_version_cross;
    _max_continuous_version_from_beginning_unlocked(&cversion, &max_version, &has_version_cross);
    // cause publish version task runs concurrently, version may be flying
    // so we add a consecutive miss check to solve this problem:
    // if publish version 5 arrives but version 4 flying, we may judge replica miss version
    // and set version miss in tablet_info, which makes fe treat this replica as unhealth
    // and lead to other problems
    if (enable_consecutive_missing_check) {
        if (cversion.second < max_version.second) {
            if (_last_missed_version == cversion.second + 1) {
                if (MonotonicSeconds() - _last_missed_time_s >= 60) {
                    // version missed for over 60 seconds
                    tablet_info->__set_version_miss(true);
                    _last_missed_version = -1;
                    _last_missed_time_s = 0;
                }
            } else {
                _last_missed_version = cversion.second + 1;
                _last_missed_time_s = MonotonicSeconds();
            }
        }
    } else {
        tablet_info->__set_version_miss(cversion.second < max_version.second);
    }

    DBUG_EXECUTE_IF("Tablet.build_tablet_report_info.version_miss", {
        auto tablet_id = dp->param<int64>("tablet_id", -1);
        if (tablet_id != -1 && tablet_id == _tablet_meta->tablet_id()) {
            auto miss = dp->param<bool>("version_miss", true);
            tablet_info->__set_version_miss(miss);
        }
    });

    // find rowset with max version
    auto iter = _rs_version_map.find(max_version);
    if (iter == _rs_version_map.end()) {
        // If the tablet is in running state, it must not be doing schema-change. so if we can not
        // access its rowsets, it means that the tablet is bad and needs to be reported to the FE
        // for subsequent repairs (through the cloning task)
        if (tablet_state() == TABLET_RUNNING) {
            tablet_info->__set_used(false);
        }
        // For other states, FE knows that the tablet is in a certain change process, so here
        // still sets the state to normal when reporting. Note that every task has an timeout,
        // so if the task corresponding to this change hangs, when the task timeout, FE will know
        // and perform state modification operations.
    }

    if (tablet_state() == TABLET_RUNNING) {
        if (has_version_cross || is_io_error_too_times() || !data_dir()->is_used()) {
            LOG(INFO) << "report " << tablet_id() << " as bad, version_cross=" << has_version_cross
                      << ", ioe times=" << get_io_error_times() << ", data_dir used "
                      << data_dir()->is_used();
            tablet_info->__set_used(false);
        }

        if (enable_path_check) {
            if (!_is_tablet_path_exists.exchange(true, std::memory_order_relaxed)) {
                LOG(INFO) << "report " << tablet_id() << " as bad, tablet directory not found";
                tablet_info->__set_used(false);
            }
        }
    }

    // There are two cases when tablet state is TABLET_NOTREADY
    // case 1: tablet is doing schema change. Fe knows it's state, doing nothing.
    // case 2: tablet has finished schema change, but failed. Fe will perform recovery.
    if (tablet_state() == TABLET_NOTREADY && is_alter_failed()) {
        tablet_info->__set_used(false);
    }

    if (tablet_state() == TABLET_SHUTDOWN) {
        tablet_info->__set_used(false);
    }

    DBUG_EXECUTE_IF("Tablet.build_tablet_report_info.used", {
        auto tablet_id = dp->param<int64>("tablet_id", -1);
        if (tablet_id != -1 && tablet_id == _tablet_meta->tablet_id()) {
            auto used = dp->param<bool>("used", true);
            LOG_WARNING("Tablet.build_tablet_report_info.used")
                    .tag("tablet id", tablet_id)
                    .tag("used", used);
            tablet_info->__set_used(used);
        } else {
            LOG_WARNING("Tablet.build_tablet_report_info.used").tag("tablet id", tablet_id);
        }
    });

    int64_t total_version_count = _tablet_meta->version_count();

    // For compatibility.
    // For old fe, it wouldn't send visible version request to be, then be's visible version is always 0.
    // Let visible_version_count set to total_version_count in be's report.
    int64_t visible_version_count = total_version_count;
    if (auto [visible_version, _] = get_visible_version_and_time(); visible_version > 0) {
        visible_version_count = _tablet_meta->version_count_cross_with_range({0, visible_version});
    }
    // the report version is the largest continuous version, same logic as in FE side
    tablet_info->__set_version(cversion.second);
    // Useless but it is a required filed in TTabletInfo
    tablet_info->__set_version_hash(0);
    tablet_info->__set_partition_id(_tablet_meta->partition_id());
    tablet_info->__set_storage_medium(_data_dir->storage_medium());
    tablet_info->__set_total_version_count(total_version_count);
    tablet_info->__set_visible_version_count(visible_version_count);
    tablet_info->__set_path_hash(_data_dir->path_hash());
    tablet_info->__set_is_in_memory(_tablet_meta->tablet_schema()->is_in_memory());
    tablet_info->__set_replica_id(replica_id());
    tablet_info->__set_remote_data_size(_tablet_meta->tablet_remote_size());
    if (_tablet_meta->cooldown_meta_id().initialized()) { // has cooldowned data
        tablet_info->__set_cooldown_term(_cooldown_conf.term);
        tablet_info->__set_cooldown_meta_id(_tablet_meta->cooldown_meta_id().to_thrift());
    }
    if (tablet_state() == TABLET_RUNNING && _tablet_meta->storage_policy_id() > 0) {
        // tablet may not have cooldowned data, but the storage policy is set
        tablet_info->__set_cooldown_term(_cooldown_conf.term);
    }
}

Status Tablet::prepare_compaction_and_calculate_permits(
        CompactionType compaction_type, const TabletSharedPtr& tablet,
        std::shared_ptr<CompactionMixin>& compaction, int64_t& permits) {
    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
        MonotonicStopWatch watch;
        watch.start();

        compaction = std::make_shared<CumulativeCompaction>(tablet->_engine, tablet);
        DorisMetrics::instance()->cumulative_compaction_request_total->increment(1);
        Status res = compaction->prepare_compact();
        if (!config::disable_compaction_trace_log &&
            watch.elapsed_time() / 1e9 > config::cumulative_compaction_trace_threshold) {
            std::stringstream ss;
            compaction->runtime_profile()->pretty_print(&ss);
            LOG(WARNING) << "prepare cumulative compaction cost " << watch.elapsed_time() / 1e9
                         << std::endl
                         << ss.str();
        }

        if (!res.ok()) {
            tablet->set_last_cumu_compaction_failure_time(UnixMillis());
            permits = 0;
            if (!res.is<CUMULATIVE_NO_SUITABLE_VERSION>()) {
                DorisMetrics::instance()->cumulative_compaction_request_failed->increment(1);
                return Status::InternalError("prepare cumulative compaction with err: {}", res);
            }
            // return OK if OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSION, so that we don't need to
            // print too much useless logs.
            // And because we set permits to 0, so even if we return OK here, nothing will be done.
            return Status::OK();
        }
    } else if (compaction_type == CompactionType::BASE_COMPACTION) {
        MonotonicStopWatch watch;
        watch.start();

        compaction = std::make_shared<BaseCompaction>(tablet->_engine, tablet);
        DorisMetrics::instance()->base_compaction_request_total->increment(1);
        Status res = compaction->prepare_compact();
        if (!config::disable_compaction_trace_log &&
            watch.elapsed_time() / 1e9 > config::base_compaction_trace_threshold) {
            std::stringstream ss;
            compaction->runtime_profile()->pretty_print(&ss);
            LOG(WARNING) << "prepare base compaction cost " << watch.elapsed_time() / 1e9
                         << std::endl
                         << ss.str();
        }

        tablet->set_last_base_compaction_status(res.to_string());
        if (!res.ok()) {
            tablet->set_last_base_compaction_failure_time(UnixMillis());
            permits = 0;
            if (!res.is<BE_NO_SUITABLE_VERSION>()) {
                DorisMetrics::instance()->base_compaction_request_failed->increment(1);
                return Status::InternalError("prepare base compaction with err: {}", res);
            }
            // return OK if OLAP_ERR_BE_NO_SUITABLE_VERSION, so that we don't need to
            // print too much useless logs.
            // And because we set permits to 0, so even if we return OK here, nothing will be done.
            return Status::OK();
        }
    } else {
        DCHECK_EQ(compaction_type, CompactionType::FULL_COMPACTION);

        compaction = std::make_shared<FullCompaction>(tablet->_engine, tablet);
        Status res = compaction->prepare_compact();
        if (!res.ok()) {
            tablet->set_last_full_compaction_failure_time(UnixMillis());
            permits = 0;
            if (!res.is<FULL_NO_SUITABLE_VERSION>()) {
                return Status::InternalError("prepare full compaction with err: {}", res);
            }
            // return OK if OLAP_ERR_BE_NO_SUITABLE_VERSION, so that we don't need to
            // print too much useless logs.
            // And because we set permits to 0, so even if we return OK here, nothing will be done.
            return Status::OK();
        }
    }

    // Time series policy does not rely on permits, it uses goal size to control memory
    if (tablet->tablet_meta()->compaction_policy() == CUMULATIVE_TIME_SERIES_POLICY) {
        // permits = 0 means that prepare_compaction failed
        permits = 1;
    } else {
        permits = compaction->get_compaction_permits();
    }
    return Status::OK();
}

void Tablet::execute_single_replica_compaction(SingleReplicaCompaction& compaction) {
    Status res = compaction.execute_compact();
    if (!res.ok()) {
        set_last_failure_time(this, compaction, UnixMillis());
        set_last_single_compaction_failure_status(res.to_string());
        if (res.is<CANCELLED>()) {
            DorisMetrics::instance()->single_compaction_request_cancelled->increment(1);
            // "CANCELLED" indicates that the peer has not performed compaction,
            // wait for the peer to perform compaction
            set_skip_compaction(true, compaction.real_compact_type(), UnixSeconds());
            VLOG_CRITICAL << "Cannel fetching from the remote peer. res=" << res
                          << ", tablet=" << tablet_id();
        } else {
            DorisMetrics::instance()->single_compaction_request_failed->increment(1);
            LOG(WARNING) << "failed to do single replica compaction. res=" << res
                         << ", tablet=" << tablet_id();
        }
        return;
    }
    set_last_failure_time(this, compaction, 0);
}

bool Tablet::should_fetch_from_peer() {
    return tablet_meta()->tablet_schema()->enable_single_replica_compaction() &&
           _engine.should_fetch_from_peer(tablet_id());
}

std::vector<Version> Tablet::get_all_local_versions() {
    std::vector<Version> local_versions;
    {
        std::shared_lock rlock(_meta_lock);
        for (const auto& [version, rs] : _rs_version_map) {
            if (rs->is_local()) {
                local_versions.emplace_back(version);
            }
        }
    }
    std::sort(local_versions.begin(), local_versions.end(),
              [](const Version& left, const Version& right) { return left.first < right.first; });
    return local_versions;
}

void Tablet::execute_compaction(CompactionMixin& compaction) {
    signal::tablet_id = tablet_id();

    MonotonicStopWatch watch;
    watch.start();

    Status res = compaction.execute_compact();

    if (!res.ok()) [[unlikely]] {
        set_last_failure_time(this, compaction, UnixMillis());
        LOG(WARNING) << "failed to do " << compaction.compaction_name()
                     << ", tablet=" << tablet_id() << " : " << res;
    } else {
        set_last_failure_time(this, compaction, 0);
    }

    if (!config::disable_compaction_trace_log) {
        auto need_trace = [&compaction, &watch] {
            return compaction.compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION
                           ? watch.elapsed_time() / 1e9 >
                                     config::cumulative_compaction_trace_threshold
                   : compaction.compaction_type() == ReaderType::READER_BASE_COMPACTION
                           ? watch.elapsed_time() / 1e9 > config::base_compaction_trace_threshold
                           : false;
        };
        if (need_trace()) {
            std::stringstream ss;
            compaction.runtime_profile()->pretty_print(&ss);
            LOG(WARNING) << "execute " << compaction.compaction_name() << " cost "
                         << watch.elapsed_time() / 1e9 << std::endl
                         << ss.str();
        }
    }
}

Status Tablet::create_initial_rowset(const int64_t req_version) {
    if (req_version < 1) {
        return Status::Error<CE_CMD_PARAMS_ERROR>(
                "init version of tablet should at least 1. req.ver={}", req_version);
    }
    Version version(0, req_version);
    RowsetSharedPtr new_rowset;
    // there is no data in init rowset, so overlapping info is unknown.
    RowsetWriterContext context;
    context.version = version;
    context.rowset_state = VISIBLE;
    context.segments_overlap = OVERLAP_UNKNOWN;
    context.tablet_schema = tablet_schema();
    context.newest_write_timestamp = UnixSeconds();
    auto rs_writer = DORIS_TRY(create_rowset_writer(context, false));
    RETURN_IF_ERROR(rs_writer->flush());
    RETURN_IF_ERROR(rs_writer->build(new_rowset));
    RETURN_IF_ERROR(add_rowset(std::move(new_rowset)));
    set_cumulative_layer_point(req_version + 1);
    return Status::OK();
}

Result<std::unique_ptr<RowsetWriter>> Tablet::create_rowset_writer(RowsetWriterContext& context,
                                                                   bool vertical) {
    context.rowset_id = _engine.next_rowset_id();
    _init_context_common_fields(context);
    return RowsetFactory::create_rowset_writer(_engine, context, vertical);
}

// create a rowset writer with rowset_id and seg_id
// after writer, merge this transient rowset with original rowset
Result<std::unique_ptr<RowsetWriter>> Tablet::create_transient_rowset_writer(
        const Rowset& rowset, std::shared_ptr<PartialUpdateInfo> partial_update_info,
        int64_t txn_expiration) {
    RowsetWriterContext context;
    context.rowset_state = PREPARED;
    context.segments_overlap = OVERLAPPING;
    context.tablet_schema = std::make_shared<TabletSchema>();
    // During a partial update, the extracted columns of a variant should not be included in the tablet schema.
    // This is because the partial update for a variant needs to ignore the extracted columns.
    // Otherwise, the schema types in different rowsets might be inconsistent. When performing a partial update,
    // the complete variant is constructed by reading all the sub-columns of the variant.
    context.tablet_schema = rowset.tablet_schema()->copy_without_variant_extracted_columns();
    context.newest_write_timestamp = UnixSeconds();
    context.tablet_id = table_id();
    context.enable_segcompaction = false;
    // ATTN: context.tablet is a shared_ptr, can't simply set it's value to `this`. We should
    // get the shared_ptr from tablet_manager.
    auto tablet = _engine.tablet_manager()->get_tablet(tablet_id());
    if (!tablet) {
        LOG(WARNING) << "cant find tablet by tablet_id=" << tablet_id();
        return ResultError(Status::NotFound("cant find tablet by tablet_id={}", tablet_id()));
    }
    context.tablet = tablet;
    context.write_type = DataWriteType::TYPE_DIRECT;
    context.partial_update_info = std::move(partial_update_info);
    context.is_transient_rowset_writer = true;
    return create_transient_rowset_writer(context, rowset.rowset_id())
            .transform([&](auto&& writer) {
                writer->set_segment_start_id(rowset.num_segments());
                return writer;
            });
}

Result<std::unique_ptr<RowsetWriter>> Tablet::create_transient_rowset_writer(
        RowsetWriterContext& context, const RowsetId& rowset_id) {
    context.rowset_id = rowset_id;
    _init_context_common_fields(context);
    return RowsetFactory::create_rowset_writer(_engine, context, false);
}

void Tablet::_init_context_common_fields(RowsetWriterContext& context) {
    context.tablet_uid = tablet_uid();
    context.tablet_id = tablet_id();
    context.partition_id = partition_id();
    context.tablet_schema_hash = schema_hash();
    context.rowset_type = tablet_meta()->preferred_rowset_type();
    // Alpha Rowset will be removed in the future, so that if the tablet's default rowset type is
    // alpha rowset, then set the newly created rowset to storage engine's default rowset.
    if (context.rowset_type == ALPHA_ROWSET) {
        context.rowset_type = _engine.default_rowset_type();
    }

    if (context.is_local_rowset()) {
        context.tablet_path = _tablet_path;
    }

    context.data_dir = data_dir();
    context.enable_unique_key_merge_on_write = enable_unique_key_merge_on_write();
}

Status Tablet::create_rowset(const RowsetMetaSharedPtr& rowset_meta, RowsetSharedPtr* rowset) {
    return RowsetFactory::create_rowset(_tablet_meta->tablet_schema(),
                                        rowset_meta->is_local() ? _tablet_path : "", rowset_meta,
                                        rowset);
}

Status Tablet::cooldown(RowsetSharedPtr rowset) {
    std::unique_lock schema_change_lock(_schema_change_lock, std::try_to_lock);
    if (!schema_change_lock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED>("try schema_change_lock failed");
    }
    // Check executing serially with compaction task.
    std::unique_lock base_compaction_lock(_base_compaction_lock, std::try_to_lock);
    if (!base_compaction_lock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED>("try base_compaction_lock failed");
    }
    std::unique_lock cumu_compaction_lock(_cumulative_compaction_lock, std::try_to_lock);
    if (!cumu_compaction_lock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED>("try cumu_compaction_lock failed");
    }
    std::shared_lock cooldown_conf_rlock(_cooldown_conf_lock);
    if (_cooldown_conf.cooldown_replica_id <= 0) { // wait for FE to push cooldown conf
        return Status::InternalError("invalid cooldown_replica_id");
    }

    if (_cooldown_conf.cooldown_replica_id == replica_id()) {
        // this replica is cooldown replica
        RETURN_IF_ERROR(_cooldown_data(std::move(rowset)));
    } else {
        Status st = _follow_cooldowned_data();
        if (UNLIKELY(!st.ok())) {
            _last_failed_follow_cooldown_time = time(nullptr);
            return st;
        }
        _last_failed_follow_cooldown_time = 0;
    }
    return Status::OK();
}

// hold SHARED `cooldown_conf_lock`
Status Tablet::_cooldown_data(RowsetSharedPtr rowset) {
    DCHECK(_cooldown_conf.cooldown_replica_id == replica_id());

    auto storage_resource = DORIS_TRY(get_resource_by_storage_policy_id(storage_policy_id()));
    RowsetSharedPtr old_rowset = nullptr;

    if (rowset) {
        const auto& rowset_id = rowset->rowset_id();
        const auto& rowset_version = rowset->version();
        std::shared_lock meta_rlock(_meta_lock);
        auto iter = _rs_version_map.find(rowset_version);
        if (iter != _rs_version_map.end() && iter->second->rowset_id() == rowset_id) {
            old_rowset = rowset;
        }
    }

    if (!old_rowset) {
        old_rowset = pick_cooldown_rowset();
    }

    if (!old_rowset) {
        LOG(INFO) << "cannot pick cooldown rowset in tablet " << tablet_id();
        return Status::OK();
    }

    RowsetId new_rowset_id = _engine.next_rowset_id();
    auto pending_rs_guard = _engine.pending_remote_rowsets().add(new_rowset_id);
    Status st;
    Defer defer {[&] {
        if (!st.ok()) {
            // reclaim the incomplete rowset data in remote storage
            record_unused_remote_rowset(new_rowset_id, storage_resource.fs->id(),
                                        old_rowset->num_segments());
        }
    }};
    auto start = std::chrono::steady_clock::now();
    if (st = old_rowset->upload_to(storage_resource, new_rowset_id); !st.ok()) {
        return st;
    }

    auto duration = std::chrono::duration<float>(std::chrono::steady_clock::now() - start);
    LOG(INFO) << "Upload rowset " << old_rowset->version() << " " << new_rowset_id.to_string()
              << " to " << storage_resource.fs->root_path().native()
              << ", tablet_id=" << tablet_id() << ", duration=" << duration.count()
              << ", capacity=" << old_rowset->data_disk_size()
              << ", tp=" << old_rowset->data_disk_size() / duration.count()
              << ", old rowset_id=" << old_rowset->rowset_id().to_string();

    // gen a new rowset
    auto new_rowset_meta = std::make_shared<RowsetMeta>();
    new_rowset_meta->init(old_rowset->rowset_meta().get());
    new_rowset_meta->set_rowset_id(new_rowset_id);
    new_rowset_meta->set_remote_storage_resource(std::move(storage_resource));
    new_rowset_meta->set_creation_time(time(nullptr));
    UniqueId cooldown_meta_id = UniqueId::gen_uid();
    RowsetSharedPtr new_rowset;
    RETURN_IF_ERROR(RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), "", new_rowset_meta,
                                                 &new_rowset));

    {
        std::unique_lock meta_wlock(_meta_lock);
        SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
        if (tablet_state() == TABLET_RUNNING) {
            delete_rowsets({std::move(old_rowset)}, false);
            add_rowsets({std::move(new_rowset)});
            // TODO(plat1ko): process primary key
            _tablet_meta->set_cooldown_meta_id(cooldown_meta_id);
        }
    }
    {
        std::shared_lock meta_rlock(_meta_lock);
        SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
        save_meta();
    }
    // Upload cooldowned rowset meta to remote fs
    // ATTN: Even if it is an empty rowset, in order for the followers to synchronize, the coolown meta must be
    // uploaded, otherwise followers may never completely cooldown.
    if (auto t = _engine.tablet_manager()->get_tablet(tablet_id());
        t != nullptr) { // `t` can be nullptr if it has been dropped
        async_write_cooldown_meta(std::move(t));
    }
    return Status::OK();
}

// hold SHARED `cooldown_conf_lock`
Status Tablet::_read_cooldown_meta(const StorageResource& storage_resource,
                                   TabletMetaPB* tablet_meta_pb) {
    std::string remote_meta_path = storage_resource.cooldown_tablet_meta_path(
            tablet_id(), _cooldown_conf.cooldown_replica_id, _cooldown_conf.term);
    io::FileReaderSPtr tablet_meta_reader;
    RETURN_IF_ERROR(storage_resource.fs->open_file(remote_meta_path, &tablet_meta_reader));
    auto file_size = tablet_meta_reader->size();
    size_t bytes_read;
    auto buf = std::unique_ptr<uint8_t[]>(new uint8_t[file_size]);
    RETURN_IF_ERROR(tablet_meta_reader->read_at(0, {buf.get(), file_size}, &bytes_read));
    RETURN_IF_ERROR(tablet_meta_reader->close());
    if (!tablet_meta_pb->ParseFromArray(buf.get(), file_size)) {
        return Status::InternalError("malformed tablet meta, path={}/{}",
                                     storage_resource.fs->root_path().native(), remote_meta_path);
    }
    return Status::OK();
}

// `rs_metas` MUST already be sorted by `RowsetMeta::comparator`
Status check_version_continuity(const std::vector<RowsetMetaSharedPtr>& rs_metas) {
    if (rs_metas.size() < 2) {
        return Status::OK();
    }
    auto prev = rs_metas.begin();
    for (auto it = rs_metas.begin() + 1; it != rs_metas.end(); ++it) {
        if ((*prev)->end_version() + 1 != (*it)->start_version()) {
            return Status::InternalError("versions are not continuity: prev={} cur={}",
                                         (*prev)->version().to_string(),
                                         (*it)->version().to_string());
        }
        prev = it;
    }
    return Status::OK();
}

// It's guaranteed the write cooldown meta task would be invoked at the end unless BE crashes
// one tablet would at most have one async task to be done
void Tablet::async_write_cooldown_meta(TabletSharedPtr tablet) {
    ExecEnv::GetInstance()->write_cooldown_meta_executors()->submit(std::move(tablet));
}

bool Tablet::update_cooldown_conf(int64_t cooldown_term, int64_t cooldown_replica_id) {
    std::unique_lock wlock(_cooldown_conf_lock, std::try_to_lock);
    if (!wlock.owns_lock()) {
        LOG(INFO) << "try cooldown_conf_lock failed, tablet_id=" << tablet_id();
        return false;
    }
    if (cooldown_term <= _cooldown_conf.term) {
        return false;
    }
    LOG(INFO) << "update cooldown conf. tablet_id=" << tablet_id()
              << " cooldown_replica_id: " << _cooldown_conf.cooldown_replica_id << " -> "
              << cooldown_replica_id << ", cooldown_term: " << _cooldown_conf.term << " -> "
              << cooldown_term;
    _cooldown_conf.cooldown_replica_id = cooldown_replica_id;
    _cooldown_conf.term = cooldown_term;
    return true;
}

Status Tablet::write_cooldown_meta() {
    std::shared_lock rlock(_cooldown_conf_lock);
    if (_cooldown_conf.cooldown_replica_id != _tablet_meta->replica_id()) {
        return Status::Aborted<false>("not cooldown replica({} vs {}) tablet_id={}",
                                      _tablet_meta->replica_id(),
                                      _cooldown_conf.cooldown_replica_id, tablet_id());
    }

    auto storage_resource = DORIS_TRY(get_resource_by_storage_policy_id(storage_policy_id()));

    std::vector<RowsetMetaSharedPtr> cooldowned_rs_metas;
    UniqueId cooldown_meta_id;
    {
        std::shared_lock meta_rlock(_meta_lock);
        for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
            if (!rs_meta->is_local()) {
                cooldowned_rs_metas.push_back(rs_meta);
            }
        }
        cooldown_meta_id = _tablet_meta->cooldown_meta_id();
    }
    if (cooldowned_rs_metas.empty()) {
        LOG(INFO) << "no cooldown meta to write, tablet_id=" << tablet_id();
        return Status::OK();
    }
    std::sort(cooldowned_rs_metas.begin(), cooldowned_rs_metas.end(), RowsetMeta::comparator);
    DCHECK(cooldowned_rs_metas.front()->start_version() == 0);
    // If version not continuous, it must be a bug
    if (auto st = check_version_continuity(cooldowned_rs_metas); !st.ok()) {
        DCHECK(st.ok()) << st << " tablet_id=" << tablet_id();
        st.set_code(ABORTED);
        return st;
    }

    TabletMetaPB tablet_meta_pb;
    auto* rs_metas = tablet_meta_pb.mutable_rs_metas();
    rs_metas->Reserve(cooldowned_rs_metas.size());
    for (auto& rs_meta : cooldowned_rs_metas) {
        rs_metas->Add(rs_meta->get_rowset_pb());
    }
    tablet_meta_pb.mutable_cooldown_meta_id()->set_hi(cooldown_meta_id.hi);
    tablet_meta_pb.mutable_cooldown_meta_id()->set_lo(cooldown_meta_id.lo);

    std::string remote_meta_path = storage_resource.cooldown_tablet_meta_path(
            tablet_id(), _cooldown_conf.cooldown_replica_id, _cooldown_conf.term);
    io::FileWriterPtr tablet_meta_writer;
    // FIXME(plat1ko): What if object store permanently unavailable?
    RETURN_IF_ERROR(storage_resource.fs->create_file(remote_meta_path, &tablet_meta_writer));
    auto val = tablet_meta_pb.SerializeAsString();
    RETURN_IF_ERROR(tablet_meta_writer->append({val.data(), val.size()}));
    return tablet_meta_writer->close();
}

// hold SHARED `cooldown_conf_lock`
Status Tablet::_follow_cooldowned_data() {
    DCHECK(_cooldown_conf.cooldown_replica_id != replica_id());
    LOG(INFO) << "try to follow cooldowned data. tablet_id=" << tablet_id()
              << " cooldown_replica_id=" << _cooldown_conf.cooldown_replica_id
              << " local replica=" << replica_id();

    auto storage_resource = DORIS_TRY(get_resource_by_storage_policy_id(storage_policy_id()));
    // MUST executing serially with cold data compaction, because compaction input rowsets may be deleted by this function
    std::unique_lock cold_compaction_lock(_cold_compaction_lock, std::try_to_lock);
    if (!cold_compaction_lock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED>("try cold_compaction_lock failed");
    }

    TabletMetaPB cooldown_meta_pb;
    auto st = _read_cooldown_meta(storage_resource, &cooldown_meta_pb);
    if (!st.ok()) {
        LOG(INFO) << "cannot read cooldown meta: " << st;
        return Status::InternalError<false>("cannot read cooldown meta");
    }
    DCHECK(cooldown_meta_pb.rs_metas_size() > 0);
    if (_tablet_meta->cooldown_meta_id() == cooldown_meta_pb.cooldown_meta_id()) {
        // cooldowned rowsets are same, no need to follow
        return Status::OK();
    }

    int64_t cooldowned_version = cooldown_meta_pb.rs_metas().rbegin()->end_version();

    std::vector<RowsetSharedPtr> overlap_rowsets;
    bool version_aligned = false;

    // Holding these to delete rowsets' shared ptr until save meta can avoid trash sweeping thread
    // deleting these rowsets' files before rowset meta has been removed from disk, which may cause
    // data loss when BE reboot before save meta to disk.
    std::vector<RowsetSharedPtr> to_delete;
    std::vector<RowsetSharedPtr> to_add;

    {
        std::lock_guard wlock(_meta_lock);
        SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
        if (tablet_state() != TABLET_RUNNING) {
            return Status::InternalError<false>("tablet not running");
        }

        for (auto& [v, rs] : _rs_version_map) {
            if (v.second <= cooldowned_version) {
                overlap_rowsets.push_back(rs);
                if (!version_aligned && v.second == cooldowned_version) {
                    version_aligned = true;
                }
            } else if (!rs->is_local()) {
                return Status::InternalError<false>(
                        "cooldowned version larger than that to follow with cooldown version {}",
                        cooldowned_version);
            }
        }

        if (!version_aligned) {
            return Status::InternalError<false>("cooldowned version is not aligned with version {}",
                                                cooldowned_version);
        }

        std::sort(overlap_rowsets.begin(), overlap_rowsets.end(), Rowset::comparator);

        // Find different rowset in `overlap_rowsets` and `cooldown_meta_pb.rs_metas`
        auto rs_pb_it = cooldown_meta_pb.rs_metas().begin();
        auto rs_it = overlap_rowsets.begin();
        for (; rs_pb_it != cooldown_meta_pb.rs_metas().end() && rs_it != overlap_rowsets.end();
             ++rs_pb_it, ++rs_it) {
            if (rs_pb_it->rowset_id_v2() != (*rs_it)->rowset_id().to_string()) {
                break;
            }
        }

        to_delete.assign(rs_it, overlap_rowsets.end());
        to_add.reserve(cooldown_meta_pb.rs_metas().end() - rs_pb_it);
        for (; rs_pb_it != cooldown_meta_pb.rs_metas().end(); ++rs_pb_it) {
            auto rs_meta = std::make_shared<RowsetMeta>();
            rs_meta->init_from_pb(*rs_pb_it);
            RowsetSharedPtr rs;
            RETURN_IF_ERROR(
                    RowsetFactory::create_rowset(_tablet_meta->tablet_schema(), "", rs_meta, &rs));
            to_add.push_back(std::move(rs));
        }
        // Note: We CANNOT call `modify_rowsets` here because `modify_rowsets` cannot process version graph correctly.
        delete_rowsets(to_delete, false);
        add_rowsets(to_add);
        // TODO(plat1ko): process primary key
        _tablet_meta->set_cooldown_meta_id(cooldown_meta_pb.cooldown_meta_id());
    }

    {
        std::lock_guard rlock(_meta_lock);
        SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
        save_meta();
    }

    if (!to_add.empty()) {
        LOG(INFO) << "modify rowsets when follow cooldowned data, tablet_id=" << tablet_id()
                  << [&]() {
                         std::stringstream ss;
                         ss << " delete rowsets:\n";
                         for (auto&& rs : to_delete) {
                             ss << rs->version() << ' ' << rs->rowset_id() << '\n';
                         }
                         ss << "add rowsets:\n";
                         for (auto&& rs : to_add) {
                             ss << rs->version() << ' ' << rs->rowset_id() << '\n';
                         }
                         return ss.str();
                     }();
    }

    return Status::OK();
}

bool Tablet::_has_data_to_cooldown() {
    int64_t min_local_version = std::numeric_limits<int64_t>::max();
    RowsetSharedPtr rowset;
    std::shared_lock meta_rlock(_meta_lock);
    // Ususally once the tablet has done cooldown successfully then the first
    // rowset would always be remote rowset
    bool has_cooldowned = false;
    for (const auto& [_, rs] : _rs_version_map) {
        if (!rs->is_local()) {
            has_cooldowned = true;
            break;
        }
    }
    for (auto& [v, rs] : _rs_version_map) {
        auto predicate = rs->is_local() && v.first < min_local_version;
        if (!has_cooldowned) {
            predicate = predicate && (rs->data_disk_size() > 0);
        }
        if (predicate) {
            // this is a local rowset and has data
            min_local_version = v.first;
            rowset = rs;
        }
    }

    int64_t newest_cooldown_time = 0;
    if (rowset != nullptr) {
        newest_cooldown_time = _get_newest_cooldown_time(rowset);
    }

    return (newest_cooldown_time != 0) && (newest_cooldown_time < UnixSeconds());
}

RowsetSharedPtr Tablet::pick_cooldown_rowset() {
    RowsetSharedPtr rowset;

    if (!_has_data_to_cooldown()) {
        return nullptr;
    }

    // TODO(plat1ko): should we maintain `cooldowned_version` in `Tablet`?
    int64_t cooldowned_version = -1;
    // We pick the rowset with smallest start version in local.
    int64_t min_local_version = std::numeric_limits<int64_t>::max();
    {
        std::shared_lock meta_rlock(_meta_lock);
        for (auto& [v, rs] : _rs_version_map) {
            if (!rs->is_local()) {
                cooldowned_version = std::max(cooldowned_version, v.second);
            } else if (v.first < min_local_version) { // this is a local rowset
                min_local_version = v.first;
                rowset = rs;
            }
        }
    }
    if (!rowset) {
        return nullptr;
    }
    if (tablet_footprint() == 0) {
        VLOG_DEBUG << "skip cooldown due to empty tablet_id = " << tablet_id();
        return nullptr;
    }
    if (min_local_version != cooldowned_version + 1) { // ensure version continuity
        if (UNLIKELY(cooldowned_version != -1)) {
            LOG(WARNING) << "version not continuous. tablet_id=" << tablet_id()
                         << " cooldowned_version=" << cooldowned_version
                         << " min_local_version=" << min_local_version;
        }
        return nullptr;
    }
    return rowset;
}

int64_t Tablet::_get_newest_cooldown_time(const RowsetSharedPtr& rowset) {
    int64_t id = storage_policy_id();
    if (id <= 0) {
        VLOG_DEBUG << "tablet does not need cooldown, tablet id: " << tablet_id();
        return 0;
    }
    auto storage_policy = get_storage_policy(id);
    if (!storage_policy) {
        LOG(WARNING) << "Cannot get storage policy: " << id;
        return 0;
    }
    auto cooldown_ttl_sec = storage_policy->cooldown_ttl;
    auto cooldown_datetime = storage_policy->cooldown_datetime;
    int64_t newest_cooldown_time = std::numeric_limits<int64_t>::max();

    if (cooldown_ttl_sec >= 0) {
        newest_cooldown_time = rowset->newest_write_timestamp() + cooldown_ttl_sec;
    }
    if (cooldown_datetime > 0) {
        newest_cooldown_time = std::min(newest_cooldown_time, cooldown_datetime);
    }

    return newest_cooldown_time;
}

RowsetSharedPtr Tablet::need_cooldown(int64_t* cooldown_timestamp, size_t* file_size) {
    RowsetSharedPtr rowset = pick_cooldown_rowset();
    if (!rowset) {
        VLOG_DEBUG << "pick cooldown rowset, get null, tablet id: " << tablet_id();
        return nullptr;
    }

    auto newest_cooldown_time = _get_newest_cooldown_time(rowset);

    // the rowset should do cooldown job only if it's cooldown ttl plus newest write time is less than
    // current time or it's datatime is less than current time
    if (newest_cooldown_time != 0 && newest_cooldown_time < UnixSeconds()) {
        *cooldown_timestamp = newest_cooldown_time;
        *file_size = rowset->data_disk_size();
        VLOG_DEBUG << "tablet need cooldown, tablet id: " << tablet_id()
                   << " file_size: " << *file_size;
        return rowset;
    }

    VLOG_DEBUG << "tablet does not need cooldown, tablet id: " << tablet_id()
               << " newest write time: " << rowset->newest_write_timestamp();
    return nullptr;
}

void Tablet::record_unused_remote_rowset(const RowsetId& rowset_id, const std::string& resource,
                                         int64_t num_segments) {
    auto gc_key = REMOTE_ROWSET_GC_PREFIX + rowset_id.to_string();
    RemoteRowsetGcPB gc_pb;
    gc_pb.set_resource_id(resource);
    gc_pb.set_tablet_id(tablet_id());
    gc_pb.set_num_segments(num_segments);
    auto st =
            _data_dir->get_meta()->put(META_COLUMN_FAMILY_INDEX, gc_key, gc_pb.SerializeAsString());
    if (!st.ok()) {
        LOG(WARNING) << "failed to record unused remote rowset. tablet_id=" << tablet_id()
                     << " rowset_id=" << rowset_id << " resource_id=" << resource;
    }
    unused_remote_rowset_num << 1;
}

Status Tablet::remove_all_remote_rowsets() {
    DCHECK(tablet_state() == TABLET_SHUTDOWN);
    std::set<std::string> resource_ids;
    for (auto& rs_meta : _tablet_meta->all_rs_metas()) {
        if (!rs_meta->is_local()) {
            resource_ids.insert(rs_meta->resource_id());
        }
    }
    if (resource_ids.empty()) {
        return Status::OK();
    }
    auto tablet_gc_key = REMOTE_TABLET_GC_PREFIX + std::to_string(tablet_id());
    RemoteTabletGcPB gc_pb;
    for (auto& resource_id : resource_ids) {
        gc_pb.add_resource_ids(resource_id);
    }
    return _data_dir->get_meta()->put(META_COLUMN_FAMILY_INDEX, tablet_gc_key,
                                      gc_pb.SerializeAsString());
}

void Tablet::update_max_version_schema(const TabletSchemaSPtr& tablet_schema) {
    std::lock_guard wrlock(_meta_lock);
    SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
    // Double Check for concurrent update
    if (!_max_version_schema ||
        tablet_schema->schema_version() > _max_version_schema->schema_version()) {
        _max_version_schema = tablet_schema;
    }
}

CalcDeleteBitmapExecutor* Tablet::calc_delete_bitmap_executor() {
    return _engine.calc_delete_bitmap_executor();
}

Status Tablet::save_delete_bitmap(const TabletTxnInfo* txn_info, int64_t txn_id,
                                  DeleteBitmapPtr delete_bitmap, RowsetWriter* rowset_writer,
                                  const RowsetIdUnorderedSet& cur_rowset_ids) {
    RowsetSharedPtr rowset = txn_info->rowset;
    int64_t cur_version = rowset->start_version();

    // update version without write lock, compaction and publish_txn
    // will update delete bitmap, handle compaction with _rowset_update_lock
    // and publish_txn runs sequential so no need to lock here
    for (auto& [key, bitmap] : delete_bitmap->delete_bitmap) {
        // skip sentinel mark, which is used for delete bitmap correctness check
        if (std::get<1>(key) != DeleteBitmap::INVALID_SEGMENT_ID) {
            _tablet_meta->delete_bitmap().merge({std::get<0>(key), std::get<1>(key), cur_version},
                                                bitmap);
        }
    }

    return Status::OK();
}

void Tablet::merge_delete_bitmap(const DeleteBitmap& delete_bitmap) {
    _tablet_meta->delete_bitmap().merge(delete_bitmap);
}

bool Tablet::check_all_rowset_segment() {
    std::shared_lock rdlock(_meta_lock);
    for (auto& version_rowset : _rs_version_map) {
        RowsetSharedPtr rowset = version_rowset.second;
        if (!rowset->check_rowset_segment()) {
            LOG(WARNING) << "Tablet Segment Check. find a bad tablet, tablet_id=" << tablet_id();
            return false;
        }
    }
    return true;
}

void Tablet::set_skip_compaction(bool skip, CompactionType compaction_type, int64_t start) {
    if (!skip) {
        _skip_cumu_compaction = false;
        _skip_base_compaction = false;
        return;
    }
    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
        _skip_cumu_compaction = true;
        _skip_cumu_compaction_ts = start;
    } else {
        DCHECK(compaction_type == CompactionType::BASE_COMPACTION);
        _skip_base_compaction = true;
        _skip_base_compaction_ts = start;
    }
}

bool Tablet::should_skip_compaction(CompactionType compaction_type, int64_t now) {
    if (compaction_type == CompactionType::CUMULATIVE_COMPACTION && _skip_cumu_compaction &&
        now < _skip_cumu_compaction_ts + 120) {
        return true;
    } else if (compaction_type == CompactionType::BASE_COMPACTION && _skip_base_compaction &&
               now < _skip_base_compaction_ts + 120) {
        return true;
    }
    return false;
}

std::pair<std::string, int64_t> Tablet::get_binlog_info(std::string_view binlog_version) const {
    return RowsetMetaManager::get_binlog_info(_data_dir->get_meta(), tablet_uid(), binlog_version);
}

std::string Tablet::get_rowset_binlog_meta(std::string_view binlog_version,
                                           std::string_view rowset_id) const {
    return RowsetMetaManager::get_rowset_binlog_meta(_data_dir->get_meta(), tablet_uid(),
                                                     binlog_version, rowset_id);
}

Status Tablet::get_rowset_binlog_metas(const std::vector<int64_t>& binlog_versions,
                                       RowsetBinlogMetasPB* metas_pb) {
    return RowsetMetaManager::get_rowset_binlog_metas(_data_dir->get_meta(), tablet_uid(),
                                                      binlog_versions, metas_pb);
}

std::string Tablet::get_segment_filepath(std::string_view rowset_id,
                                         std::string_view segment_index) const {
    return fmt::format("{}/_binlog/{}_{}.dat", _tablet_path, rowset_id, segment_index);
}

std::string Tablet::get_segment_filepath(std::string_view rowset_id, int64_t segment_index) const {
    return fmt::format("{}/_binlog/{}_{}.dat", _tablet_path, rowset_id, segment_index);
}

std::string Tablet::get_segment_index_filepath(std::string_view rowset_id,
                                               std::string_view segment_index,
                                               std::string_view index_id) const {
    auto format = _tablet_meta->tablet_schema()->get_inverted_index_storage_format();
    if (format == doris::InvertedIndexStorageFormatPB::V1) {
        return fmt::format("{}/_binlog/{}_{}_{}.idx", _tablet_path, rowset_id, segment_index,
                           index_id);
    } else {
        return fmt::format("{}/_binlog/{}_{}.idx", _tablet_path, rowset_id, segment_index);
    }
}

std::string Tablet::get_segment_index_filepath(std::string_view rowset_id, int64_t segment_index,
                                               int64_t index_id) const {
    auto format = _tablet_meta->tablet_schema()->get_inverted_index_storage_format();
    if (format == doris::InvertedIndexStorageFormatPB::V1) {
        return fmt::format("{}/_binlog/{}_{}_{}.idx", _tablet_path, rowset_id, segment_index,
                           index_id);
    } else {
        DCHECK(index_id == -1);
        return fmt::format("{}/_binlog/{}_{}.idx", _tablet_path, rowset_id, segment_index);
    }
}

std::vector<std::string> Tablet::get_binlog_filepath(std::string_view binlog_version) const {
    const auto& [rowset_id, num_segments] = get_binlog_info(binlog_version);
    std::vector<std::string> binlog_filepath;
    for (int i = 0; i < num_segments; ++i) {
        // TODO(Drogon): rewrite by filesystem path
        auto segment_file = fmt::format("{}_{}.dat", rowset_id, i);
        binlog_filepath.emplace_back(fmt::format("{}/_binlog/{}", _tablet_path, segment_file));
    }
    return binlog_filepath;
}

bool Tablet::can_add_binlog(uint64_t total_binlog_size) const {
    return !_data_dir->reach_capacity_limit(total_binlog_size);
}

bool Tablet::is_enable_binlog() {
    return config::enable_feature_binlog && tablet_meta()->binlog_config().is_enable();
}

void Tablet::set_binlog_config(BinlogConfig binlog_config) {
    tablet_meta()->set_binlog_config(binlog_config);
}

void Tablet::gc_binlogs(int64_t version) {
    auto meta = _data_dir->get_meta();
    DCHECK(meta != nullptr);

    const auto& tablet_uid = this->tablet_uid();
    const auto tablet_id = this->tablet_id();
    std::string begin_key = make_binlog_meta_key_prefix(tablet_uid);
    std::string end_key = make_binlog_meta_key_prefix(tablet_uid, version + 1);
    LOG(INFO) << fmt::format("gc binlog meta, tablet_id:{}, begin_key:{}, end_key:{}", tablet_id,
                             begin_key, end_key);

    std::vector<std::string> wait_for_deleted_binlog_keys;
    std::vector<std::string> wait_for_deleted_binlog_files;
    auto add_to_wait_for_deleted = [&](std::string_view key, std::string_view rowset_id,
                                       int64_t num_segments) {
        // add binlog meta key and binlog data key
        wait_for_deleted_binlog_keys.emplace_back(key);
        wait_for_deleted_binlog_keys.push_back(get_binlog_data_key_from_meta_key(key));

        // add binlog segment files and index files
        for (int64_t i = 0; i < num_segments; ++i) {
            wait_for_deleted_binlog_files.emplace_back(get_segment_filepath(rowset_id, i));
            for (const auto& index : this->tablet_schema()->indexes()) {
                if (index.index_type() != IndexType::INVERTED) {
                    continue;
                }
                wait_for_deleted_binlog_files.emplace_back(
                        get_segment_index_filepath(rowset_id, i, index.index_id()));
            }
        }
    };

    auto check_binlog_ttl = [&](std::string_view key, std::string_view value) mutable -> bool {
        if (key >= end_key) {
            return false;
        }

        BinlogMetaEntryPB binlog_meta_entry_pb;
        if (!binlog_meta_entry_pb.ParseFromArray(value.data(), value.size())) {
            LOG(WARNING) << "failed to parse binlog meta entry, key:" << key;
            return true;
        }

        auto num_segments = binlog_meta_entry_pb.num_segments();
        std::string rowset_id;
        if (binlog_meta_entry_pb.has_rowset_id_v2()) {
            rowset_id = binlog_meta_entry_pb.rowset_id_v2();
        } else {
            // key is 'binlog_meta_6943f1585fe834b5-e542c2b83a21d0b7_00000000000000000069_020000000000000135449d7cd7eadfe672aa0f928fa99593', extract last part '020000000000000135449d7cd7eadfe672aa0f928fa99593'
            auto pos = key.rfind('_');
            if (pos == std::string::npos) {
                LOG(WARNING) << fmt::format("invalid binlog meta key:{}", key);
                return false;
            }
            rowset_id = key.substr(pos + 1);
        }
        add_to_wait_for_deleted(key, rowset_id, num_segments);

        return true;
    };

    auto status = meta->iterate(META_COLUMN_FAMILY_INDEX, begin_key, check_binlog_ttl);
    if (!status.ok()) {
        LOG(WARNING) << "failed to iterate binlog meta, status:" << status;
        return;
    }

    // first remove binlog files, if failed, just break, then retry next time
    // this keep binlog meta in meta store, so that binlog can be removed next time
    bool remove_binlog_files_failed = false;
    for (auto& file : wait_for_deleted_binlog_files) {
        if (unlink(file.c_str()) != 0) {
            // file not exist, continue
            if (errno == ENOENT) {
                continue;
            }

            remove_binlog_files_failed = true;
            LOG(WARNING) << "failed to remove binlog file:" << file << ", errno:" << errno;
            break;
        }
    }
    if (!remove_binlog_files_failed) {
        static_cast<void>(meta->remove(META_COLUMN_FAMILY_INDEX, wait_for_deleted_binlog_keys));
    }
}

Status Tablet::ingest_binlog_metas(RowsetBinlogMetasPB* metas_pb) {
    return RowsetMetaManager::ingest_binlog_metas(_data_dir->get_meta(), tablet_uid(), metas_pb);
}

void Tablet::clear_cache() {
    std::vector<RowsetSharedPtr> rowsets;
    {
        std::shared_lock rlock(get_header_lock());
        SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);

        for (auto& [_, rowset] : rowset_map()) {
            rowsets.push_back(rowset);
        }
        for (auto& [_, rowset] : stale_rowset_map()) {
            rowsets.push_back(rowset);
        }
    }
    for (auto& rowset : rowsets) {
        rowset->clear_cache();
    }
}

} // namespace doris
