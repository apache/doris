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

#pragma once

#include <memory>
#include <shared_mutex>
#include <string>

#include "common/status.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_meta.h"
#include "olap/version_graph.h"
#include "util/metrics.h"

namespace doris {
struct RowSetSplits;
struct RowsetWriterContext;
class RowsetWriter;

// Base class for all tablet classes
class BaseTablet {
public:
    explicit BaseTablet(TabletMetaSharedPtr tablet_meta);
    virtual ~BaseTablet();
    BaseTablet(const BaseTablet&) = delete;
    BaseTablet& operator=(const BaseTablet&) = delete;

    const std::string& tablet_path() const { return _tablet_path; }
    TabletState tablet_state() const { return _tablet_meta->tablet_state(); }
    Status set_tablet_state(TabletState state);
    int64_t table_id() const { return _tablet_meta->table_id(); }
    int64_t partition_id() const { return _tablet_meta->partition_id(); }
    int64_t tablet_id() const { return _tablet_meta->tablet_id(); }
    int32_t schema_hash() const { return _tablet_meta->schema_hash(); }
    KeysType keys_type() const { return _tablet_meta->tablet_schema()->keys_type(); }
    size_t num_key_columns() const { return _tablet_meta->tablet_schema()->num_key_columns(); }
    bool enable_unique_key_merge_on_write() const {
#ifdef BE_TEST
        if (_tablet_meta == nullptr) {
            return false;
        }
#endif
        return _tablet_meta->enable_unique_key_merge_on_write();
    }

    // Property encapsulated in TabletMeta
    const TabletMetaSharedPtr& tablet_meta() { return _tablet_meta; }

    // FIXME(plat1ko): It is not appropriate to expose this lock
    std::shared_mutex& get_header_lock() { return _meta_lock; }

    void update_max_version_schema(const TabletSchemaSPtr& tablet_schema);

    Status update_by_least_common_schema(const TabletSchemaSPtr& update_schema);

    TabletSchemaSPtr tablet_schema() const {
        std::shared_lock rlock(_meta_lock);
        return _max_version_schema;
    }

    virtual bool exceed_version_limit(int32_t limit) = 0;

    virtual Result<std::unique_ptr<RowsetWriter>> create_rowset_writer(RowsetWriterContext& context,
                                                                       bool vertical) = 0;

    virtual Status capture_rs_readers(const Version& spec_version,
                                      std::vector<RowSetSplits>* rs_splits,
                                      bool skip_missing_version) = 0;

    virtual size_t tablet_footprint() = 0;

    // MUST hold shared meta lock
    Status capture_rs_readers_unlocked(const std::vector<Version>& version_path,
                                       std::vector<RowSetSplits>* rs_splits) const;

protected:
    bool _reconstruct_version_tracker_if_necessary();

    mutable std::shared_mutex _meta_lock;
    TimestampedVersionTracker _timestamped_version_tracker;
    // After version 0.13, all newly created rowsets are saved in _rs_version_map.
    // And if rowset being compacted, the old rowsetis will be saved in _stale_rs_version_map;
    std::unordered_map<Version, RowsetSharedPtr, HashOfVersion> _rs_version_map;
    // This variable _stale_rs_version_map is used to record these rowsets which are be compacted.
    // These _stale rowsets are been removed when rowsets' pathVersion is expired,
    // this policy is judged and computed by TimestampedVersionTracker.
    std::unordered_map<Version, RowsetSharedPtr, HashOfVersion> _stale_rs_version_map;
    const TabletMetaSharedPtr _tablet_meta;
    TabletSchemaSPtr _max_version_schema;

    std::string _tablet_path;

    // metrics of this tablet
    std::shared_ptr<MetricEntity> _metric_entity;

public:
    IntCounter* query_scan_bytes = nullptr;
    IntCounter* query_scan_rows = nullptr;
    IntCounter* query_scan_count = nullptr;
    IntCounter* flush_bytes = nullptr;
    IntCounter* flush_finish_count = nullptr;
    std::atomic<int64_t> published_count = 0;
};

} /* namespace doris */
