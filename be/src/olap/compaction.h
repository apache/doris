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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "cloud/cloud_tablet.h"
#include "common/status.h"
#include "io/io_common.h"
#include "olap/merger.h"
#include "olap/olap_common.h"
#include "olap/rowid_conversion.h"
#include "olap/rowset/pending_rowset_helper.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/tablet_fwd.h"
#include "util/runtime_profile.h"

namespace doris {

class MemTrackerLimiter;
class RowsetWriter;
struct RowsetWriterContext;
class StorageEngine;
class CloudStorageEngine;

// This class is a base class for compaction.
// The entrance of this class is compact()
// Any compaction should go through four procedures.
//  1. pick rowsets satisfied to compact
//  2. do compaction
//  3. modify rowsets
//  4. gc output rowset if failed
class Compaction {
public:
    Compaction(BaseTabletSPtr tablet, const std::string& label);
    virtual ~Compaction();

    // Pick input rowsets satisfied to compact
    virtual Status prepare_compact() = 0;

    // Merge input rowsets to output rowset and modify tablet meta
    virtual Status execute_compact() = 0;

    RuntimeProfile* runtime_profile() const { return _profile.get(); }

    virtual ReaderType compaction_type() const = 0;
    virtual std::string_view compaction_name() const = 0;

protected:
    Status merge_input_rowsets();

    Status do_inverted_index_compaction();

    void construct_skip_inverted_index(RowsetWriterContext& ctx);

    virtual Status construct_output_rowset_writer(RowsetWriterContext& ctx) = 0;

    Status check_correctness();

    int64_t get_avg_segment_rows();

    void init_profile(const std::string& label);

    void _load_segment_to_cache();

    int64_t merge_way_num();

    // the root tracker for this compaction
    std::shared_ptr<MemTrackerLimiter> _mem_tracker;

    BaseTabletSPtr _tablet;

    std::vector<RowsetSharedPtr> _input_rowsets;
    int64_t _input_rowsets_size {0};
    int64_t _input_row_num {0};
    int64_t _input_num_segments {0};
    int64_t _input_index_size {0};

    Merger::Statistics _stats;

    RowsetSharedPtr _output_rowset;
    std::unique_ptr<RowsetWriter> _output_rs_writer;

    enum CompactionState : uint8_t { INITED = 0, SUCCESS = 1 };
    CompactionState _state {CompactionState::INITED};

    bool _is_vertical;
    bool _allow_delete_in_cumu_compaction;

    Version _output_version;

    int64_t _newest_write_timestamp {-1};
    RowIdConversion _rowid_conversion;
    TabletSchemaSPtr _cur_tablet_schema;

    std::unique_ptr<RuntimeProfile> _profile;

    RuntimeProfile::Counter* _input_rowsets_data_size_counter = nullptr;
    RuntimeProfile::Counter* _input_rowsets_counter = nullptr;
    RuntimeProfile::Counter* _input_row_num_counter = nullptr;
    RuntimeProfile::Counter* _input_segments_num_counter = nullptr;
    RuntimeProfile::Counter* _merged_rows_counter = nullptr;
    RuntimeProfile::Counter* _filtered_rows_counter = nullptr;
    RuntimeProfile::Counter* _output_rowset_data_size_counter = nullptr;
    RuntimeProfile::Counter* _output_row_num_counter = nullptr;
    RuntimeProfile::Counter* _output_segments_num_counter = nullptr;
    RuntimeProfile::Counter* _merge_rowsets_latency_timer = nullptr;
};

// `StorageEngine` mixin for `Compaction`
class CompactionMixin : public Compaction {
public:
    CompactionMixin(StorageEngine& engine, TabletSharedPtr tablet, const std::string& label);

    ~CompactionMixin() override;

    Status execute_compact() override;

    int64_t get_compaction_permits();

protected:
    // Convert `_tablet` from `BaseTablet` to `Tablet`
    Tablet* tablet();

    Status construct_output_rowset_writer(RowsetWriterContext& ctx) override;

    virtual Status modify_rowsets();

    StorageEngine& _engine;

private:
    Status execute_compact_impl(int64_t permits);

    void build_basic_info();

    // Return true if do ordered data compaction successfully
    bool handle_ordered_data_compaction();

    Status do_compact_ordered_rowsets();

    bool _check_if_includes_input_rowsets(const RowsetIdUnorderedSet& commit_rowset_ids_set) const;

    PendingRowsetGuard _pending_rs_guard;
};

class CloudCompactionMixin : public Compaction {
public:
    CloudCompactionMixin(CloudStorageEngine& engine, CloudTabletSPtr tablet,
                         const std::string& label);

    ~CloudCompactionMixin() override = default;

    Status execute_compact() override;

protected:
    CloudTablet* cloud_tablet() { return static_cast<CloudTablet*>(_tablet.get()); }

    virtual void garbage_collection();

    CloudStorageEngine& _engine;

    int64_t _expiration = 0;

private:
    Status construct_output_rowset_writer(RowsetWriterContext& ctx) override;

    Status execute_compact_impl(int64_t permits);

    void build_basic_info();

    virtual Status modify_rowsets();

    int64_t get_compaction_permits();
};

} // namespace doris
