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
#include "gutil/macros.h"
#include "olap/column_mapping.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/tablet_schema.h"
#include "vec/core/block.h"

namespace doris {

class MemTable;

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

    // finish building and return pointer to the built rowset (guaranteed to be inited).
    // return nullptr when failed
    virtual RowsetSharedPtr build() = 0;

    // For ordered rowset compaction, manual build rowset
    virtual RowsetSharedPtr manual_build(const RowsetMetaSharedPtr& rowset_meta) = 0;

    virtual Version version() = 0;

    virtual int64_t num_rows() const = 0;

    virtual int64_t num_rows_filtered() const = 0;

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

private:
    DISALLOW_COPY_AND_ASSIGN(RowsetWriter);
};

} // namespace doris
