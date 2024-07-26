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

#include "cloud/cloud_tablet.h"

#include <gen_cpp/olap_file.pb.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>

#include <atomic>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet_mgr.h"
#include "io/cache/block_file_cache_downloader.h"
#include "io/cache/block_file_cache_factory.h"
#include "olap/cumulative_compaction_time_series_policy.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/storage_policy.h"
#include "olap/tablet_schema.h"
#include "olap/txn_manager.h"
#include "util/debug_points.h"
#include "vec/common/schema_util.h"

namespace doris {
using namespace ErrorCode;

static constexpr int COMPACTION_DELETE_BITMAP_LOCK_ID = -1;

CloudTablet::CloudTablet(CloudStorageEngine& engine, TabletMetaSharedPtr tablet_meta)
        : BaseTablet(std::move(tablet_meta)), _engine(engine) {}

CloudTablet::~CloudTablet() = default;

bool CloudTablet::exceed_version_limit(int32_t limit) {
    return _approximate_num_rowsets.load(std::memory_order_relaxed) > limit;
}

Status CloudTablet::capture_consistent_rowsets_unlocked(
        const Version& spec_version, std::vector<RowsetSharedPtr>* rowsets) const {
    Versions version_path;
    auto st = _timestamped_version_tracker.capture_consistent_versions(spec_version, &version_path);
    if (!st.ok()) {
        // Check no missed versions or req version is merged
        auto missed_versions = get_missed_versions(spec_version.second);
        if (missed_versions.empty()) {
            st.set_code(VERSION_ALREADY_MERGED); // Reset error code
        }
        st.append(" tablet_id=" + std::to_string(tablet_id()));
        return st;
    }
    VLOG_DEBUG << "capture consitent versions: " << version_path;
    return _capture_consistent_rowsets_unlocked(version_path, rowsets);
}

Status CloudTablet::capture_rs_readers(const Version& spec_version,
                                       std::vector<RowSetSplits>* rs_splits,
                                       bool skip_missing_version) {
    DBUG_EXECUTE_IF("CloudTablet.capture_rs_readers.return.e-230", {
        LOG_WARNING("CloudTablet.capture_rs_readers.return e-230").tag("tablet_id", tablet_id());
        return Status::Error<false>(-230, "injected error");
    });
    Versions version_path;
    std::shared_lock rlock(_meta_lock);
    auto st = _timestamped_version_tracker.capture_consistent_versions(spec_version, &version_path);
    if (!st.ok()) {
        rlock.unlock(); // avoid logging in lock range
        // Check no missed versions or req version is merged
        auto missed_versions = get_missed_versions(spec_version.second);
        if (missed_versions.empty()) {
            st.set_code(VERSION_ALREADY_MERGED); // Reset error code
        }
        st.append(" tablet_id=" + std::to_string(tablet_id()));
        // clang-format off
        LOG(WARNING) << st << '\n' << [this]() { std::string json; get_compaction_status(&json); return json; }();
        // clang-format on
        return st;
    }
    VLOG_DEBUG << "capture consitent versions: " << version_path;
    return capture_rs_readers_unlocked(version_path, rs_splits);
}

// There are only two tablet_states RUNNING and NOT_READY in cloud mode
// This function will erase the tablet from `CloudTabletMgr` when it can't find this tablet in MS.
Status CloudTablet::sync_rowsets(int64_t query_version, bool warmup_delta_data) {
    RETURN_IF_ERROR(sync_if_not_running());

    if (query_version > 0) {
        std::shared_lock rlock(_meta_lock);
        if (_max_version >= query_version) {
            return Status::OK();
        }
    }

    // serially execute sync to reduce unnecessary network overhead
    std::lock_guard lock(_sync_meta_lock);
    if (query_version > 0) {
        std::shared_lock rlock(_meta_lock);
        if (_max_version >= query_version) {
            return Status::OK();
        }
    }

    auto st = _engine.meta_mgr().sync_tablet_rowsets(this, warmup_delta_data);
    if (st.is<ErrorCode::NOT_FOUND>()) {
        clear_cache();
    }
    return st;
}

TabletSchemaSPtr CloudTablet::merged_tablet_schema() const {
    std::shared_lock rdlock(_meta_lock);
    TabletSchemaSPtr target_schema;
    std::vector<TabletSchemaSPtr> schemas;
    for (const auto& [_, rowset] : _rs_version_map) {
        schemas.push_back(rowset->tablet_schema());
    }
    // get the max version schema and merge all schema
    static_cast<void>(
            vectorized::schema_util::get_least_common_schema(schemas, nullptr, target_schema));
    return target_schema;
}

// Sync tablet meta and all rowset meta if not running.
// This could happen when BE didn't finish schema change job and another BE committed this schema change job.
// It should be a quite rare situation.
Status CloudTablet::sync_if_not_running() {
    if (tablet_state() == TABLET_RUNNING) {
        return Status::OK();
    }

    // Serially execute sync to reduce unnecessary network overhead
    std::lock_guard lock(_sync_meta_lock);

    {
        std::shared_lock rlock(_meta_lock);
        if (tablet_state() == TABLET_RUNNING) {
            return Status::OK();
        }
    }

    TabletMetaSharedPtr tablet_meta;
    auto st = _engine.meta_mgr().get_tablet_meta(tablet_id(), &tablet_meta);
    if (!st.ok()) {
        if (st.is<ErrorCode::NOT_FOUND>()) {
            clear_cache();
        }
        return st;
    }

    if (tablet_meta->tablet_state() != TABLET_RUNNING) [[unlikely]] {
        // MoW may go to here when load while schema change
        return Status::Error<INVALID_TABLET_STATE>("invalid tablet state {}. tablet_id={}",
                                                   tablet_meta->tablet_state(), tablet_id());
    }

    TimestampedVersionTracker empty_tracker;
    {
        std::lock_guard wlock(_meta_lock);
        RETURN_IF_ERROR(set_tablet_state(TABLET_RUNNING));
        _rs_version_map.clear();
        _stale_rs_version_map.clear();
        std::swap(_timestamped_version_tracker, empty_tracker);
        _tablet_meta->clear_rowsets();
        _tablet_meta->clear_stale_rowset();
        _max_version = -1;
    }

    st = _engine.meta_mgr().sync_tablet_rowsets(this);
    if (st.is<ErrorCode::NOT_FOUND>()) {
        clear_cache();
    }
    return st;
}

void CloudTablet::add_rowsets(std::vector<RowsetSharedPtr> to_add, bool version_overlap,
                              std::unique_lock<std::shared_mutex>& meta_lock,
                              bool warmup_delta_data) {
    if (to_add.empty()) {
        return;
    }

    auto add_rowsets_directly = [=, this](std::vector<RowsetSharedPtr>& rowsets) {
        for (auto& rs : rowsets) {
            if (version_overlap || warmup_delta_data) {
#ifndef BE_TEST
                // Warmup rowset data in background
                for (int seg_id = 0; seg_id < rs->num_segments(); ++seg_id) {
                    const auto& rowset_meta = rs->rowset_meta();
                    constexpr int64_t interval = 600; // 10 mins
                    // When BE restart and receive the `load_sync` rpc, it will sync all historical rowsets first time.
                    // So we need to filter out the old rowsets avoid to download the whole table.
                    if (warmup_delta_data &&
                        ::time(nullptr) - rowset_meta->newest_write_timestamp() >= interval) {
                        continue;
                    }

                    auto storage_resource = rowset_meta->remote_storage_resource();
                    if (!storage_resource) {
                        LOG(WARNING) << storage_resource.error();
                        continue;
                    }

                    int64_t expiration_time =
                            _tablet_meta->ttl_seconds() == 0 ||
                                            rowset_meta->newest_write_timestamp() <= 0
                                    ? 0
                                    : rowset_meta->newest_write_timestamp() +
                                              _tablet_meta->ttl_seconds();
                    _engine.file_cache_block_downloader().submit_download_task(
                            io::DownloadFileMeta {
                                    .path = storage_resource.value()->remote_segment_path(
                                            *rowset_meta, seg_id),
                                    .file_size = rs->rowset_meta()->segment_file_size(seg_id),
                                    .file_system = storage_resource.value()->fs,
                                    .ctx =
                                            {
                                                    .expiration_time = expiration_time,
                                            },
                                    .download_done {},
                            });
                }
#endif
            }
            _rs_version_map.emplace(rs->version(), rs);
            _timestamped_version_tracker.add_version(rs->version());
            _max_version = std::max(rs->end_version(), _max_version);
            update_base_size(*rs);
        }
        _tablet_meta->add_rowsets_unchecked(rowsets);
    };

    if (!version_overlap) {
        add_rowsets_directly(to_add);
        return;
    }

    // Filter out existed rowsets
    auto remove_it =
            std::remove_if(to_add.begin(), to_add.end(), [this](const RowsetSharedPtr& rs) {
                if (auto find_it = _rs_version_map.find(rs->version());
                    find_it == _rs_version_map.end()) {
                    return false;
                } else if (find_it->second->rowset_id() == rs->rowset_id()) {
                    return true; // Same rowset
                }

                // If version of rowset in `to_add` is equal to rowset in tablet but rowset_id is not equal,
                // replace existed rowset with `to_add` rowset. This may occur when:
                //  1. schema change converts rowsets which have been double written to new tablet
                //  2. cumu compaction picks single overlapping input rowset to perform compaction
                _tablet_meta->delete_rs_meta_by_version(rs->version(), nullptr);
                _rs_version_map[rs->version()] = rs;
                _tablet_meta->add_rowsets_unchecked({rs});
                update_base_size(*rs);
                return true;
            });

    to_add.erase(remove_it, to_add.end());

    // delete rowsets with overlapped version
    std::vector<RowsetSharedPtr> to_add_directly;
    for (auto& to_add_rs : to_add) {
        // delete rowsets with overlapped version
        std::vector<RowsetSharedPtr> to_delete;
        Version to_add_v = to_add_rs->version();
        // if start_version  > max_version, we can skip checking overlap here.
        if (to_add_v.first > _max_version) {
            // if start_version  > max_version, we can skip checking overlap here.
            to_add_directly.push_back(to_add_rs);
        } else {
            to_add_directly.push_back(to_add_rs);
            for (auto& [v, rs] : _rs_version_map) {
                if (to_add_v.contains(v)) {
                    to_delete.push_back(rs);
                }
            }
            delete_rowsets(to_delete, meta_lock);
        }
    }

    add_rowsets_directly(to_add_directly);
}

void CloudTablet::delete_rowsets(const std::vector<RowsetSharedPtr>& to_delete,
                                 std::unique_lock<std::shared_mutex>&) {
    if (to_delete.empty()) {
        return;
    }
    std::vector<RowsetMetaSharedPtr> rs_metas;
    rs_metas.reserve(to_delete.size());
    for (auto&& rs : to_delete) {
        rs_metas.push_back(rs->rowset_meta());
        _stale_rs_version_map[rs->version()] = rs;
    }
    _timestamped_version_tracker.add_stale_path_version(rs_metas);
    for (auto&& rs : to_delete) {
        _rs_version_map.erase(rs->version());
    }

    _tablet_meta->modify_rs_metas({}, rs_metas, false);
}

int CloudTablet::delete_expired_stale_rowsets() {
    std::vector<RowsetSharedPtr> expired_rowsets;
    int64_t expired_stale_sweep_endtime =
            ::time(nullptr) - config::tablet_rowset_stale_sweep_time_sec;
    {
        std::unique_lock wlock(_meta_lock);

        std::vector<int64_t> path_ids;
        // capture the path version to delete
        _timestamped_version_tracker.capture_expired_paths(expired_stale_sweep_endtime, &path_ids);

        if (path_ids.empty()) {
            return 0;
        }

        for (int64_t path_id : path_ids) {
            // delete stale versions in version graph
            auto version_path = _timestamped_version_tracker.fetch_and_delete_path_by_id(path_id);
            for (auto& v_ts : version_path->timestamped_versions()) {
                auto rs_it = _stale_rs_version_map.find(v_ts->version());
                if (rs_it != _stale_rs_version_map.end()) {
                    expired_rowsets.push_back(rs_it->second);
                    _stale_rs_version_map.erase(rs_it);
                } else {
                    LOG(WARNING) << "cannot find stale rowset " << v_ts->version() << " in tablet "
                                 << tablet_id();
                    // clang-format off
                    DCHECK(false) << [this, &wlock]() { wlock.unlock(); std::string json; get_compaction_status(&json); return json; }();
                    // clang-format on
                }
                _tablet_meta->delete_stale_rs_meta_by_version(v_ts->version());
                VLOG_DEBUG << "delete stale rowset " << v_ts->version();
            }
        }
        _reconstruct_version_tracker_if_necessary();
    }
    recycle_cached_data(expired_rowsets);
    return expired_rowsets.size();
}

void CloudTablet::update_base_size(const Rowset& rs) {
    // Define base rowset as the rowset of version [2-x]
    if (rs.start_version() == 2) {
        _base_size = rs.data_disk_size();
    }
}

void CloudTablet::clear_cache() {
    CloudTablet::recycle_cached_data(get_snapshot_rowset(true));
    _engine.tablet_mgr().erase_tablet(tablet_id());
}

void CloudTablet::recycle_cached_data(const std::vector<RowsetSharedPtr>& rowsets) {
    for (auto& rs : rowsets) {
        // Clear cached opened segments and inverted index cache in memory
        rs->clear_cache();
    }

    if (config::enable_file_cache) {
        for (const auto& rs : rowsets) {
            for (int seg_id = 0; seg_id < rs->num_segments(); ++seg_id) {
                // TODO: Segment::file_cache_key
                auto file_key = Segment::file_cache_key(rs->rowset_id().to_string(), seg_id);
                auto* file_cache = io::FileCacheFactory::instance()->get_by_path(file_key);
                file_cache->remove_if_cached(file_key);
            }
        }
    }
}

void CloudTablet::reset_approximate_stats(int64_t num_rowsets, int64_t num_segments,
                                          int64_t num_rows, int64_t data_size) {
    _approximate_num_rowsets.store(num_rowsets, std::memory_order_relaxed);
    _approximate_num_segments.store(num_segments, std::memory_order_relaxed);
    _approximate_num_rows.store(num_rows, std::memory_order_relaxed);
    _approximate_data_size.store(data_size, std::memory_order_relaxed);
    int64_t cumu_num_deltas = 0;
    int64_t cumu_num_rowsets = 0;
    auto cp = _cumulative_point.load(std::memory_order_relaxed);
    for (auto& [v, r] : _rs_version_map) {
        if (v.second < cp) {
            continue;
        }

        cumu_num_deltas += r->is_segments_overlapping() ? r->num_segments() : 1;
        ++cumu_num_rowsets;
    }
    _approximate_cumu_num_rowsets.store(cumu_num_rowsets, std::memory_order_relaxed);
    _approximate_cumu_num_deltas.store(cumu_num_deltas, std::memory_order_relaxed);
}

Result<std::unique_ptr<RowsetWriter>> CloudTablet::create_rowset_writer(
        RowsetWriterContext& context, bool vertical) {
    context.rowset_id = _engine.next_rowset_id();
    // FIXME(plat1ko): Seems `tablet_id` and `index_id` has been set repeatedly
    context.tablet_id = tablet_id();
    context.index_id = index_id();
    context.partition_id = partition_id();
    context.enable_unique_key_merge_on_write = enable_unique_key_merge_on_write();
    return RowsetFactory::create_rowset_writer(_engine, context, vertical);
}

// create a rowset writer with rowset_id and seg_id
// after writer, merge this transient rowset with original rowset
Result<std::unique_ptr<RowsetWriter>> CloudTablet::create_transient_rowset_writer(
        const Rowset& rowset, std::shared_ptr<PartialUpdateInfo> partial_update_info,
        int64_t txn_expiration) {
    if (rowset.rowset_meta()->rowset_state() != RowsetStatePB::BEGIN_PARTIAL_UPDATE) [[unlikely]] {
        // May cause the segment files generated by the transient rowset writer unable to be
        // recycled, see `CloudRowsetWriter::build` for detail.
        LOG(WARNING) << "Wrong rowset state: " << rowset.rowset_meta()->rowset_state();
        DCHECK(false) << rowset.rowset_meta()->rowset_state();
    }

    RowsetWriterContext context;
    context.rowset_state = PREPARED;
    context.segments_overlap = OVERLAPPING;
    // During a partial update, the extracted columns of a variant should not be included in the tablet schema.
    // This is because the partial update for a variant needs to ignore the extracted columns.
    // Otherwise, the schema types in different rowsets might be inconsistent. When performing a partial update,
    // the complete variant is constructed by reading all the sub-columns of the variant.
    context.tablet_schema = rowset.tablet_schema()->copy_without_variant_extracted_columns();
    context.newest_write_timestamp = UnixSeconds();
    context.tablet_id = table_id();
    context.enable_segcompaction = false;
    context.write_type = DataWriteType::TYPE_DIRECT;
    context.partial_update_info = std::move(partial_update_info);
    context.is_transient_rowset_writer = true;
    context.rowset_id = rowset.rowset_id();
    context.tablet_id = tablet_id();
    context.index_id = index_id();
    context.partition_id = partition_id();
    context.enable_unique_key_merge_on_write = enable_unique_key_merge_on_write();
    context.txn_expiration = txn_expiration;

    auto storage_resource = rowset.rowset_meta()->remote_storage_resource();
    if (!storage_resource) {
        return ResultError(std::move(storage_resource.error()));
    }

    context.storage_resource = *storage_resource.value();

    return RowsetFactory::create_rowset_writer(_engine, context, false)
            .transform([&](auto&& writer) {
                writer->set_segment_start_id(rowset.num_segments());
                return writer;
            });
}

int64_t CloudTablet::get_cloud_base_compaction_score() const {
    if (_tablet_meta->compaction_policy() == CUMULATIVE_TIME_SERIES_POLICY) {
        bool has_delete = false;
        int64_t point = cumulative_layer_point();
        std::shared_lock<std::shared_mutex> rlock(_meta_lock);
        for (const auto& rs_meta : _tablet_meta->all_rs_metas()) {
            if (rs_meta->start_version() >= point) {
                continue;
            }
            if (rs_meta->has_delete_predicate()) {
                has_delete = true;
                break;
            }
        }
        if (!has_delete) {
            return 0;
        }
    }

    return _approximate_num_rowsets.load(std::memory_order_relaxed) -
           _approximate_cumu_num_rowsets.load(std::memory_order_relaxed);
}

int64_t CloudTablet::get_cloud_cumu_compaction_score() const {
    // TODO(plat1ko): Propose an algorithm that considers tablet's key type, number of delete rowsets,
    //  number of tablet versions simultaneously.
    return _approximate_cumu_num_deltas.load(std::memory_order_relaxed);
}

// return a json string to show the compaction status of this tablet
void CloudTablet::get_compaction_status(std::string* json_result) {
    rapidjson::Document root;
    root.SetObject();

    rapidjson::Document path_arr;
    path_arr.SetArray();

    std::vector<RowsetSharedPtr> rowsets;
    std::vector<RowsetSharedPtr> stale_rowsets;
    {
        std::shared_lock rdlock(_meta_lock);
        rowsets.reserve(_rs_version_map.size());
        for (auto& it : _rs_version_map) {
            rowsets.push_back(it.second);
        }
        stale_rowsets.reserve(_stale_rs_version_map.size());
        for (auto& it : _stale_rs_version_map) {
            stale_rowsets.push_back(it.second);
        }
    }
    std::sort(rowsets.begin(), rowsets.end(), Rowset::comparator);
    std::sort(stale_rowsets.begin(), stale_rowsets.end(), Rowset::comparator);

    // get snapshot version path json_doc
    _timestamped_version_tracker.get_stale_version_path_json_doc(path_arr);
    root.AddMember("cumulative point", _cumulative_point.load(), root.GetAllocator());

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

void CloudTablet::set_cumulative_layer_point(int64_t new_point) {
    // cumulative point should only be reset to -1, or be increased
    CHECK(new_point == Tablet::K_INVALID_CUMULATIVE_POINT || new_point >= _cumulative_point)
            << "Unexpected cumulative point: " << new_point
            << ", origin: " << _cumulative_point.load();
    _cumulative_point = new_point;
}

std::vector<RowsetSharedPtr> CloudTablet::pick_candidate_rowsets_to_base_compaction() {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    {
        std::shared_lock rlock(_meta_lock);
        for (const auto& [version, rs] : _rs_version_map) {
            if (version.first != 0 && version.first < _cumulative_point) {
                candidate_rowsets.push_back(rs);
            }
        }
    }
    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);
    return candidate_rowsets;
}

std::vector<RowsetSharedPtr> CloudTablet::pick_candidate_rowsets_to_full_compaction() {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    {
        std::shared_lock rlock(_meta_lock);
        for (auto& [v, rs] : _rs_version_map) {
            // MUST NOT compact rowset [0-1] for some historical reasons (see cloud_schema_change)
            if (v.first != 0) {
                candidate_rowsets.push_back(rs);
            }
        }
    }
    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);
    return candidate_rowsets;
}

CalcDeleteBitmapExecutor* CloudTablet::calc_delete_bitmap_executor() {
    return _engine.calc_delete_bitmap_executor();
}

Status CloudTablet::save_delete_bitmap(const TabletTxnInfo* txn_info, int64_t txn_id,
                                       DeleteBitmapPtr delete_bitmap, RowsetWriter* rowset_writer,
                                       const RowsetIdUnorderedSet& cur_rowset_ids) {
    RowsetSharedPtr rowset = txn_info->rowset;
    int64_t cur_version = rowset->start_version();
    // update delete bitmap info, in order to avoid recalculation when trying again
    _engine.txn_delete_bitmap_cache().update_tablet_txn_info(
            txn_id, tablet_id(), delete_bitmap, cur_rowset_ids, PublishStatus::PREPARE);

    if (txn_info->partial_update_info && txn_info->partial_update_info->is_partial_update &&
        rowset_writer->num_rows() > 0) {
        const auto& rowset_meta = rowset->rowset_meta();
        RETURN_IF_ERROR(_engine.meta_mgr().update_tmp_rowset(*rowset_meta));
    }

    DeleteBitmapPtr new_delete_bitmap = std::make_shared<DeleteBitmap>(tablet_id());
    for (auto iter = delete_bitmap->delete_bitmap.begin();
         iter != delete_bitmap->delete_bitmap.end(); ++iter) {
        // skip sentinel mark, which is used for delete bitmap correctness check
        if (std::get<1>(iter->first) != DeleteBitmap::INVALID_SEGMENT_ID) {
            new_delete_bitmap->merge(
                    {std::get<0>(iter->first), std::get<1>(iter->first), cur_version},
                    iter->second);
        }
    }

    RETURN_IF_ERROR(_engine.meta_mgr().update_delete_bitmap(
            *this, txn_id, COMPACTION_DELETE_BITMAP_LOCK_ID, new_delete_bitmap.get()));
    _engine.txn_delete_bitmap_cache().update_tablet_txn_info(
            txn_id, tablet_id(), new_delete_bitmap, cur_rowset_ids, PublishStatus::SUCCEED);

    return Status::OK();
}

Versions CloudTablet::calc_missed_versions(int64_t spec_version, Versions existing_versions) const {
    DCHECK(spec_version > 0) << "invalid spec_version: " << spec_version;

    // sort the existing versions in ascending order
    std::sort(existing_versions.begin(), existing_versions.end(),
              [](const Version& a, const Version& b) {
                  // simple because 2 versions are certainly not overlapping
                  return a.first < b.first;
              });

    // From the first version(=0), find the missing version until spec_version
    int64_t last_version = -1;
    Versions missed_versions;
    for (const Version& version : existing_versions) {
        if (version.first > last_version + 1) {
            // there is a hole between versions
            missed_versions.emplace_back(last_version + 1, std::min(version.first, spec_version));
        }
        last_version = version.second;
        if (last_version >= spec_version) {
            break;
        }
    }
    if (last_version < spec_version) {
        // there is a hole between the last version and the specificed version.
        missed_versions.emplace_back(last_version + 1, spec_version);
    }
    return missed_versions;
}

Status CloudTablet::calc_delete_bitmap_for_compaction(
        const std::vector<RowsetSharedPtr>& input_rowsets, const RowsetSharedPtr& output_rowset,
        const RowIdConversion& rowid_conversion, ReaderType compaction_type, int64_t merged_rows,
        int64_t initiator, DeleteBitmapPtr& output_rowset_delete_bitmap,
        bool allow_delete_in_cumu_compaction) {
    output_rowset_delete_bitmap = std::make_shared<DeleteBitmap>(tablet_id());
    std::set<RowLocation> missed_rows;
    std::map<RowsetSharedPtr, std::list<std::pair<RowLocation, RowLocation>>> location_map;

    // 1. calc delete bitmap for historical data
    RETURN_IF_ERROR(_engine.meta_mgr().sync_tablet_rowsets(this));
    Version version = max_version();
    calc_compaction_output_rowset_delete_bitmap(
            input_rowsets, rowid_conversion, 0, version.second + 1, &missed_rows, &location_map,
            tablet_meta()->delete_bitmap(), output_rowset_delete_bitmap.get());
    std::size_t missed_rows_size = missed_rows.size();
    if (!allow_delete_in_cumu_compaction) {
        if (compaction_type == ReaderType::READER_CUMULATIVE_COMPACTION &&
            tablet_state() == TABLET_RUNNING) {
            if (merged_rows >= 0 && merged_rows != missed_rows_size) {
                std::string err_msg = fmt::format(
                        "cumulative compaction: the merged rows({}) is not equal to missed "
                        "rows({}) in rowid conversion, tablet_id: {}, table_id:{}",
                        merged_rows, missed_rows_size, tablet_id(), table_id());
                if (config::enable_mow_compaction_correctness_check_core) {
                    CHECK(false) << err_msg;
                } else {
                    DCHECK(false) << err_msg;
                }
                LOG(WARNING) << err_msg;
            }
        }
    }
    if (config::enable_rowid_conversion_correctness_check) {
        RETURN_IF_ERROR(check_rowid_conversion(output_rowset, location_map));
    }
    location_map.clear();

    // 2. calc delete bitmap for incremental data
    RETURN_IF_ERROR(_engine.meta_mgr().get_delete_bitmap_update_lock(
            *this, COMPACTION_DELETE_BITMAP_LOCK_ID, initiator));
    RETURN_IF_ERROR(_engine.meta_mgr().sync_tablet_rowsets(this));

    calc_compaction_output_rowset_delete_bitmap(
            input_rowsets, rowid_conversion, version.second, UINT64_MAX, &missed_rows,
            &location_map, tablet_meta()->delete_bitmap(), output_rowset_delete_bitmap.get());
    if (config::enable_rowid_conversion_correctness_check) {
        RETURN_IF_ERROR(check_rowid_conversion(output_rowset, location_map));
    }
    if (compaction_type == ReaderType::READER_CUMULATIVE_COMPACTION) {
        DCHECK_EQ(missed_rows.size(), missed_rows_size);
        if (missed_rows.size() != missed_rows_size) {
            LOG(WARNING) << "missed rows don't match, before: " << missed_rows_size
                         << " after: " << missed_rows.size();
        }
    }

    // 3. store delete bitmap
    RETURN_IF_ERROR(_engine.meta_mgr().update_delete_bitmap(*this, -1, initiator,
                                                            output_rowset_delete_bitmap.get()));
    return Status::OK();
}

Status CloudTablet::sync_meta() {
    if (!config::enable_file_cache) {
        return Status::OK();
    }

    TabletMetaSharedPtr tablet_meta;
    auto st = _engine.meta_mgr().get_tablet_meta(tablet_id(), &tablet_meta);
    if (!st.ok()) {
        if (st.is<ErrorCode::NOT_FOUND>()) {
            // TODO(Lchangliang): recycle_resources_by_self();
        }
        return st;
    }
    if (tablet_meta->tablet_state() != TABLET_RUNNING) { // impossible
        return Status::InternalError("invalid tablet state. tablet_id={}", tablet_id());
    }

    auto new_ttl_seconds = tablet_meta->ttl_seconds();
    if (_tablet_meta->ttl_seconds() != new_ttl_seconds) {
        _tablet_meta->set_ttl_seconds(new_ttl_seconds);
        int64_t cur_time = UnixSeconds();
        std::shared_lock rlock(_meta_lock);
        for (auto& [_, rs] : _rs_version_map) {
            for (int seg_id = 0; seg_id < rs->num_segments(); ++seg_id) {
                int64_t new_expiration_time =
                        new_ttl_seconds + rs->rowset_meta()->newest_write_timestamp();
                new_expiration_time = new_expiration_time > cur_time ? new_expiration_time : 0;
                auto file_key = Segment::file_cache_key(rs->rowset_id().to_string(), seg_id);
                auto* file_cache = io::FileCacheFactory::instance()->get_by_path(file_key);
                file_cache->modify_expiration_time(file_key, new_expiration_time);
            }
        }
    }

    auto new_compaction_policy = tablet_meta->compaction_policy();
    if (_tablet_meta->compaction_policy() != new_compaction_policy) {
        _tablet_meta->set_compaction_policy(new_compaction_policy);
    }
    auto new_time_series_compaction_goal_size_mbytes =
            tablet_meta->time_series_compaction_goal_size_mbytes();
    if (_tablet_meta->time_series_compaction_goal_size_mbytes() !=
        new_time_series_compaction_goal_size_mbytes) {
        _tablet_meta->set_time_series_compaction_goal_size_mbytes(
                new_time_series_compaction_goal_size_mbytes);
    }
    auto new_time_series_compaction_file_count_threshold =
            tablet_meta->time_series_compaction_file_count_threshold();
    if (_tablet_meta->time_series_compaction_file_count_threshold() !=
        new_time_series_compaction_file_count_threshold) {
        _tablet_meta->set_time_series_compaction_file_count_threshold(
                new_time_series_compaction_file_count_threshold);
    }
    auto new_time_series_compaction_time_threshold_seconds =
            tablet_meta->time_series_compaction_time_threshold_seconds();
    if (_tablet_meta->time_series_compaction_time_threshold_seconds() !=
        new_time_series_compaction_time_threshold_seconds) {
        _tablet_meta->set_time_series_compaction_time_threshold_seconds(
                new_time_series_compaction_time_threshold_seconds);
    }
    auto new_time_series_compaction_empty_rowsets_threshold =
            tablet_meta->time_series_compaction_empty_rowsets_threshold();
    if (_tablet_meta->time_series_compaction_empty_rowsets_threshold() !=
        new_time_series_compaction_empty_rowsets_threshold) {
        _tablet_meta->set_time_series_compaction_empty_rowsets_threshold(
                new_time_series_compaction_empty_rowsets_threshold);
    }
    auto new_time_series_compaction_level_threshold =
            tablet_meta->time_series_compaction_level_threshold();
    if (_tablet_meta->time_series_compaction_level_threshold() !=
        new_time_series_compaction_level_threshold) {
        _tablet_meta->set_time_series_compaction_level_threshold(
                new_time_series_compaction_level_threshold);
    }

    return Status::OK();
}

} // namespace doris
