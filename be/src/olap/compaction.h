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
// clang20 + -O1 causes warnings about pass-failed
// error: loop not unrolled: the optimizer was unable to perform the requested transformation; the transformation might be disabled or specified as part of an unsupported transformation ordering [-Werror,-Wpass-failed=transform-warning]
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wpass-failed"
#endif
#include <memory>
#if defined(__clang__)
#pragma clang diagnostic pop
#endif
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

static constexpr int COMPACTION_DELETE_BITMAP_LOCK_ID = -1;
static constexpr int64_t INVALID_COMPACTION_INITIATOR_ID = -100;
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

    // merge inverted index files
    Status do_inverted_index_compaction();

    // mark all columns in columns_to_do_index_compaction to skip index compaction next time.
    void mark_skip_index_compaction(const RowsetWriterContext& context,
                                    const std::function<void(int64_t, int64_t)>& error_handler);

    void construct_index_compaction_columns(RowsetWriterContext& ctx);

    virtual Status construct_output_rowset_writer(RowsetWriterContext& ctx) = 0;

    Status check_correctness();

    int64_t get_avg_segment_rows();

    void init_profile(const std::string& label);

    void _load_segment_to_cache();

    int64_t merge_way_num();

    virtual Status update_delete_bitmap() = 0;

    // the root tracker for this compaction
    std::shared_ptr<MemTrackerLimiter> _mem_tracker;

    BaseTabletSPtr _tablet;

    std::vector<RowsetSharedPtr> _input_rowsets;
    int64_t _input_rowsets_data_size {0};
    int64_t _input_rowsets_index_size {0};
    int64_t _input_rowsets_total_size {0};
    int64_t _input_row_num {0};
    int64_t _input_num_segments {0};

    int64_t _local_read_bytes_total {};
    int64_t _remote_read_bytes_total {};

    Merger::Statistics _stats;

    RowsetSharedPtr _output_rowset;
    std::unique_ptr<RowsetWriter> _output_rs_writer;

    enum CompactionState : uint8_t { INITED = 0, SUCCESS = 1 };
    CompactionState _state {CompactionState::INITED};

    bool _is_vertical;
    bool _allow_delete_in_cumu_compaction;
    bool _enable_vertical_compact_variant_subcolumns;

    Version _output_version;

    int64_t _newest_write_timestamp {-1};
    std::unique_ptr<RowIdConversion> _rowid_conversion = nullptr;
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

    int64_t initiator() const { return INVALID_COMPACTION_INITIATOR_ID; }

    int64_t calc_input_rowsets_total_size() const;

    int64_t calc_input_rowsets_row_num() const;

protected:
    // Convert `_tablet` from `BaseTablet` to `Tablet`
    Tablet* tablet();

    Status construct_output_rowset_writer(RowsetWriterContext& ctx) override;

    virtual Status modify_rowsets();

    Status update_delete_bitmap() override;

    StorageEngine& _engine;

private:
    Status execute_compact_impl(int64_t permits);

    Status build_basic_info(bool is_ordered_compaction = false);

    // Return true if do ordered data compaction successfully
    bool handle_ordered_data_compaction();

    Status do_compact_ordered_rowsets();

    bool _check_if_includes_input_rowsets(const RowsetIdUnorderedSet& commit_rowset_ids_set) const;

    void update_compaction_level();

    PendingRowsetGuard _pending_rs_guard;
};

class CloudCompactionMixin : public Compaction {
public:
    CloudCompactionMixin(CloudStorageEngine& engine, CloudTabletSPtr tablet,
                         const std::string& label);

    ~CloudCompactionMixin() override = default;

    Status execute_compact() override;

    int64_t initiator() const;

protected:
    CloudTablet* cloud_tablet() { return static_cast<CloudTablet*>(_tablet.get()); }

    Status update_delete_bitmap() override;

    virtual Status garbage_collection();

    CloudStorageEngine& _engine;

    std::string _uuid;

    int64_t _expiration = 0;

private:
    Status construct_output_rowset_writer(RowsetWriterContext& ctx) override;

    Status execute_compact_impl(int64_t permits);

    Status build_basic_info();

    virtual Status modify_rowsets();

    int64_t get_compaction_permits();

    void update_compaction_level();
};

} // namespace doris
