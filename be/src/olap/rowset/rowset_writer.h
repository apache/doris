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

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/types.pb.h>

#include <functional>
#include <optional>

#include "common/factory_creator.h"
#include "gen_cpp/olap_file.pb.h"
#include "gutil/macros.h"
#include "olap/column_mapping.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/tablet_schema.h"
#include "vec/core/block.h"

namespace doris {

struct SegmentStatistics {
    int64_t row_num;
    int64_t data_size;
    int64_t index_size;
    KeyBoundsPB key_bounds;

    SegmentStatistics() = default;

    SegmentStatistics(SegmentStatisticsPB pb)
            : row_num(pb.row_num()),
              data_size(pb.data_size()),
              index_size(pb.index_size()),
              key_bounds(pb.key_bounds()) {}

    void to_pb(SegmentStatisticsPB* segstat_pb) {
        segstat_pb->set_row_num(row_num);
        segstat_pb->set_data_size(data_size);
        segstat_pb->set_index_size(index_size);
        segstat_pb->mutable_key_bounds()->CopyFrom(key_bounds);
    }

    std::string to_string() {
        std::stringstream ss;
        ss << "row_num: " << row_num << ", data_size: " << data_size
           << ", index_size: " << index_size << ", key_bounds: " << key_bounds.ShortDebugString();
        return ss.str();
    }
};
using SegmentStatisticsSharedPtr = std::shared_ptr<SegmentStatistics>;

class RowsetWriter {
public:
    RowsetWriter() = default;
    virtual ~RowsetWriter() = default;

    virtual Status init(const RowsetWriterContext& rowset_writer_context) = 0;

    virtual Status add_block(const vectorized::Block* block) {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "RowsetWriter not support add_block");
    }
    virtual Status add_columns(const vectorized::Block* block, const std::vector<uint32_t>& col_ids,
                               bool is_key, uint32_t max_rows_per_segment) {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "RowsetWriter not support add_columns");
    }

    // Precondition: the input `rowset` should have the same type of the rowset we're building
    virtual Status add_rowset(RowsetSharedPtr rowset) = 0;

    // Precondition: the input `rowset` should have the same type of the rowset we're building
    virtual Status add_rowset_for_linked_schema_change(RowsetSharedPtr rowset) = 0;

    virtual Status create_file_writer(uint32_t segment_id, io::FileWriterPtr& writer) {
        return Status::NotSupported("RowsetWriter does not support create_file_writer");
    }

    // explicit flush all buffered rows into segment file.
    // note that `add_row` could also trigger flush when certain conditions are met
    virtual Status flush() = 0;
    virtual Status flush_columns(bool is_key) {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "RowsetWriter not support flush_columns");
    }
    virtual Status final_flush() {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "RowsetWriter not support final_flush");
    }

    virtual Status flush_memtable(vectorized::Block* block, int32_t segment_id,
                                  int64_t* flush_size) {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "RowsetWriter not support flush_memtable");
    }

    virtual Status flush_single_block(const vectorized::Block* block) {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "RowsetWriter not support flush_single_block");
    }

    virtual Status add_segment(uint32_t segment_id, SegmentStatistics& segstat) {
        return Status::NotSupported("RowsetWriter does not support add_segment");
    }

    // finish building and set rowset pointer to the built rowset (guaranteed to be inited).
    // rowset is invalid if returned Status is not OK
    virtual Status build(RowsetSharedPtr& rowset) = 0;

    // For ordered rowset compaction, manual build rowset
    virtual RowsetSharedPtr manual_build(const RowsetMetaSharedPtr& rowset_meta) = 0;

    virtual Version version() = 0;

    virtual int64_t num_rows() const = 0;

    virtual int64_t num_rows_filtered() const = 0;

    virtual std::shared_ptr<IndicatorMaps> get_indicator_maps() const = 0;

    virtual RowsetId rowset_id() = 0;

    virtual RowsetTypePB type() const = 0;

    virtual Status get_segment_num_rows(std::vector<uint32_t>* segment_num_rows) const {
        return Status::NotSupported("to be implemented");
    }

    virtual int32_t allocate_segment_id() = 0;

    virtual bool is_doing_segcompaction() const = 0;

    virtual Status wait_flying_segcompaction() = 0;

    virtual void set_segment_start_id(int num_segment) { LOG(FATAL) << "not supported!"; }

    virtual int64_t delete_bitmap_ns() { return 0; }

    virtual int64_t segment_writer_ns() { return 0; }

    virtual std::shared_ptr<PartialUpdateInfo> get_partial_update_info() = 0;

    virtual bool is_partial_update() = 0;

private:
    DISALLOW_COPY_AND_ASSIGN(RowsetWriter);
};

} // namespace doris
