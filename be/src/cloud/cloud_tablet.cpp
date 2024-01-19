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

#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/stringbuffer.h>

#include <atomic>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "io/cache/block/block_file_cache_factory.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"

namespace doris {
using namespace ErrorCode;

CloudTablet::CloudTablet(CloudStorageEngine& engine, TabletMetaSharedPtr tablet_meta)
        : BaseTablet(std::move(tablet_meta)), _engine(engine) {}

CloudTablet::~CloudTablet() = default;

bool CloudTablet::exceed_version_limit(int32_t limit) {
    return _approximate_num_rowsets.load(std::memory_order_relaxed) > limit;
}

Status CloudTablet::capture_rs_readers(const Version& spec_version,
                                       std::vector<RowSetSplits>* rs_splits,
                                       bool skip_missing_version) {
    Versions version_path;
    std::shared_lock rlock(_meta_lock);
    auto st = _timestamped_version_tracker.capture_consistent_versions(spec_version, &version_path);
    if (!st.ok()) {
        rlock.unlock(); // avoid logging in lock range
        // Check no missed versions or req version is merged
        auto missed_versions = calc_missed_versions(spec_version.second);
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

// for example:
//     [0-4][5-5][8-8][9-9][13-13]
// if spec_version = 12, it will return [6-7],[10-12]
Versions CloudTablet::calc_missed_versions(int64_t spec_version) {
    DCHECK(spec_version > 0) << "invalid spec_version: " << spec_version;

    Versions missed_versions;
    Versions existing_versions;
    {
        std::shared_lock rdlock(_meta_lock);
        for (const auto& rs : _tablet_meta->all_rs_metas()) {
            existing_versions.emplace_back(rs->version());
        }
    }

    // sort the existing versions in ascending order
    std::sort(existing_versions.begin(), existing_versions.end(),
              [](const Version& a, const Version& b) {
                  // simple because 2 versions are certainly not overlapping
                  return a.first < b.first;
              });

    auto min_version = existing_versions.front().first;
    if (min_version > 0) {
        missed_versions.emplace_back(0, std::min(spec_version, min_version - 1));
    }
    for (auto it = existing_versions.begin(); it != existing_versions.end() - 1; ++it) {
        auto prev_v = it->second;
        if (prev_v >= spec_version) {
            return missed_versions;
        }
        auto next_v = (it + 1)->first;
        if (next_v > prev_v + 1) {
            // there is a hole between versions
            missed_versions.emplace_back(prev_v + 1, std::min(spec_version, next_v - 1));
        }
    }
    auto max_version = existing_versions.back().second;
    if (max_version < spec_version) {
        missed_versions.emplace_back(max_version + 1, spec_version);
    }
    return missed_versions;
}

Status CloudTablet::sync_meta() {
    // TODO(lightman): FileCache
    return Status::NotSupported("CloudTablet::sync_meta is not implemented");
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
        recycle_cached_data();
    }
    return st;
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
            recycle_cached_data();
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
        recycle_cached_data();
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
            _rs_version_map.emplace(rs->version(), rs);
            _timestamped_version_tracker.add_version(rs->version());
            _max_version = std::max(rs->end_version(), _max_version);
            update_base_size(*rs);
        }
        _tablet_meta->add_rowsets_unchecked(rowsets);
        // TODO(plat1ko): Warmup delta rowset data in background
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

void CloudTablet::recycle_cached_data() {
    // TODO(plat1ko)
}

void CloudTablet::recycle_cached_data(const std::vector<RowsetSharedPtr>& rowsets) {
    // TODO(plat1ko)
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
    return ResultError(
            Status::NotSupported("CloudTablet::create_rowset_writer is not implemented"));
}

int64_t CloudTablet::get_cloud_base_compaction_score() const {
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

} // namespace doris
