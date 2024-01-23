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

#include "olap/base_tablet.h"

#include <fmt/format.h>

#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/tablet_fwd.h"
#include "util/doris_metrics.h"
#include "vec/common/schema_util.h"

namespace doris {
using namespace ErrorCode;

extern MetricPrototype METRIC_query_scan_bytes;
extern MetricPrototype METRIC_query_scan_rows;
extern MetricPrototype METRIC_query_scan_count;
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(flush_bytes, MetricUnit::BYTES);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(flush_finish_count, MetricUnit::OPERATIONS);

BaseTablet::BaseTablet(TabletMetaSharedPtr tablet_meta) : _tablet_meta(std::move(tablet_meta)) {
    _metric_entity = DorisMetrics::instance()->metric_registry()->register_entity(
            fmt::format("Tablet.{}", tablet_id()), {{"tablet_id", std::to_string(tablet_id())}},
            MetricEntityType::kTablet);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, query_scan_bytes);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, query_scan_rows);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, query_scan_count);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, flush_bytes);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, flush_finish_count);
}

BaseTablet::~BaseTablet() {
    DorisMetrics::instance()->metric_registry()->deregister_entity(_metric_entity);
}

Status BaseTablet::set_tablet_state(TabletState state) {
    if (_tablet_meta->tablet_state() == TABLET_SHUTDOWN && state != TABLET_SHUTDOWN) {
        return Status::Error<META_INVALID_ARGUMENT>(
                "could not change tablet state from shutdown to {}", state);
    }
    _tablet_meta->set_tablet_state(state);
    return Status::OK();
}

void BaseTablet::update_max_version_schema(const TabletSchemaSPtr& tablet_schema) {
    std::lock_guard wrlock(_meta_lock);
    // Double Check for concurrent update
    if (!_max_version_schema ||
        tablet_schema->schema_version() > _max_version_schema->schema_version()) {
        _max_version_schema = tablet_schema;
    }
}

Status BaseTablet::update_by_least_common_schema(const TabletSchemaSPtr& update_schema) {
    std::lock_guard wrlock(_meta_lock);
    CHECK(_max_version_schema->schema_version() >= update_schema->schema_version());
    TabletSchemaSPtr final_schema;
    bool check_column_size = true;
    RETURN_IF_ERROR(vectorized::schema_util::get_least_common_schema(
            {_max_version_schema, update_schema}, _max_version_schema, final_schema,
            check_column_size));
    _max_version_schema = final_schema;
    VLOG_DEBUG << "dump updated tablet schema: " << final_schema->dump_structure();
    return Status::OK();
}

Status BaseTablet::capture_rs_readers_unlocked(const Versions& version_path,
                                               std::vector<RowSetSplits>* rs_splits) const {
    DCHECK(rs_splits != nullptr && rs_splits->empty());
    for (auto version : version_path) {
        auto it = _rs_version_map.find(version);
        if (it == _rs_version_map.end()) {
            VLOG_NOTICE << "fail to find Rowset in rs_version for version. tablet=" << tablet_id()
                        << ", version='" << version.first << "-" << version.second;

            it = _stale_rs_version_map.find(version);
            if (it == _stale_rs_version_map.end()) {
                return Status::Error<CAPTURE_ROWSET_READER_ERROR>(
                        "fail to find Rowset in stale_rs_version for version. tablet={}, "
                        "version={}-{}",
                        tablet_id(), version.first, version.second);
            }
        }
        RowsetReaderSharedPtr rs_reader;
        auto res = it->second->create_reader(&rs_reader);
        if (!res.ok()) {
            return Status::Error<CAPTURE_ROWSET_READER_ERROR>(
                    "failed to create reader for rowset:{}", it->second->rowset_id().to_string());
        }
        rs_splits->emplace_back(std::move(rs_reader));
    }
    return Status::OK();
}

// snapshot manager may call this api to check if version exists, so that
// the version maybe not exist
RowsetSharedPtr BaseTablet::get_rowset_by_version(const Version& version,
                                                  bool find_in_stale) const {
    auto iter = _rs_version_map.find(version);
    if (iter == _rs_version_map.end()) {
        if (find_in_stale) {
            return get_stale_rowset_by_version(version);
        }
        return nullptr;
    }
    return iter->second;
}

RowsetSharedPtr BaseTablet::get_stale_rowset_by_version(const Version& version) const {
    auto iter = _stale_rs_version_map.find(version);
    if (iter == _stale_rs_version_map.end()) {
        VLOG_NOTICE << "no rowset for version:" << version << ", tablet: " << tablet_id();
        return nullptr;
    }
    return iter->second;
}

// Already under _meta_lock
RowsetSharedPtr BaseTablet::get_rowset_with_max_version() const {
    Version max_version = _tablet_meta->max_version();
    if (max_version.first == -1) {
        return nullptr;
    }

    auto iter = _rs_version_map.find(max_version);
    if (iter == _rs_version_map.end()) {
        DCHECK(false) << "invalid version:" << max_version;
        return nullptr;
    }
    return iter->second;
}

Status BaseTablet::get_all_rs_id(int64_t max_version, RowsetIdUnorderedSet* rowset_ids) const {
    std::shared_lock rlock(_meta_lock);
    return get_all_rs_id_unlocked(max_version, rowset_ids);
}

Status BaseTablet::get_all_rs_id_unlocked(int64_t max_version,
                                          RowsetIdUnorderedSet* rowset_ids) const {
    //  Ensure that the obtained versions of rowsets are continuous
    Version spec_version(0, max_version);
    Versions version_path;
    auto st = _timestamped_version_tracker.capture_consistent_versions(spec_version, &version_path);
    if (!st.ok()) [[unlikely]] {
        return st;
    }

    for (auto& ver : version_path) {
        if (ver.second == 1) {
            // [0-1] rowset is empty for each tablet, skip it
            continue;
        }
        auto it = _rs_version_map.find(ver);
        if (it == _rs_version_map.end()) {
            return Status::Error<CAPTURE_ROWSET_ERROR, false>(
                    "fail to find Rowset for version. tablet={}, version={}", tablet_id(),
                    ver.to_string());
        }
        rowset_ids->emplace(it->second->rowset_id());
    }
    return Status::OK();
}

Versions BaseTablet::get_missed_versions(int64_t spec_version) const {
    DCHECK(spec_version > 0) << "invalid spec_version: " << spec_version;

    Versions existing_versions;
    {
        std::shared_lock rdlock(_meta_lock);
        for (const auto& rs : _tablet_meta->all_rs_metas()) {
            existing_versions.emplace_back(rs->version());
        }
    }
    return calc_missed_versions(spec_version, existing_versions);
}

Versions BaseTablet::get_missed_versions_unlocked(int64_t spec_version) const {
    DCHECK(spec_version > 0) << "invalid spec_version: " << spec_version;

    Versions existing_versions;
    for (const auto& rs : _tablet_meta->all_rs_metas()) {
        existing_versions.emplace_back(rs->version());
    }
    return calc_missed_versions(spec_version, existing_versions);
}

Versions BaseTablet::calc_missed_versions(int64_t spec_version, Versions existing_versions) {
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

void BaseTablet::_print_missed_versions(const Versions& missed_versions) const {
    std::stringstream ss;
    ss << tablet_id() << " has " << missed_versions.size() << " missed version:";
    // print at most 10 version
    for (int i = 0; i < 10 && i < missed_versions.size(); ++i) {
        ss << missed_versions[i] << ",";
    }
    LOG(WARNING) << ss.str();
}

bool BaseTablet::_reconstruct_version_tracker_if_necessary() {
    double orphan_vertex_ratio = _timestamped_version_tracker.get_orphan_vertex_ratio();
    if (orphan_vertex_ratio >= config::tablet_version_graph_orphan_vertex_ratio) {
        _timestamped_version_tracker.construct_versioned_tracker(
                _tablet_meta->all_rs_metas(), _tablet_meta->all_stale_rs_metas());
        return true;
    }
    return false;
}

} // namespace doris
