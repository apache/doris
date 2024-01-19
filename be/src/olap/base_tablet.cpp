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
#include "olap/tablet_schema_cache.h"
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

Status BaseTablet::capture_rs_readers_unlocked(const std::vector<Version>& version_path,
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
        rs_splits->push_back(RowSetSplits(std::move(rs_reader)));
    }
    return Status::OK();
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

} /* namespace doris */
