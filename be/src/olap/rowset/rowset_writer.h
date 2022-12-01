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

#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/types.pb.h"
#include "gutil/macros.h"
#include "olap/column_mapping.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_writer_context.h"
#include "vec/core/block.h"

namespace doris {

struct ContiguousRow;
class MemTable;

class RowsetWriter {
public:
    RowsetWriter() = default;
    virtual ~RowsetWriter() = default;

    virtual Status init(const RowsetWriterContext& rowset_writer_context) = 0;

    // Memory note: input `row` is guaranteed to be copied into writer's internal buffer, including all slice data
    // referenced by `row`. That means callers are free to de-allocate memory for `row` after this method returns.
    virtual Status add_row(const RowCursor& row) = 0;
    virtual Status add_row(const ContiguousRow& row) = 0;

    virtual Status add_block(const vectorized::Block* block) {
        return Status::OLAPInternalError(OLAP_ERR_FUNC_NOT_IMPLEMENTED);
    }
    virtual Status add_columns(const vectorized::Block* block, const std::vector<uint32_t>& col_ids,
                               bool is_key, uint32_t max_rows_per_segment) {
        return Status::OLAPInternalError(OLAP_ERR_FUNC_NOT_IMPLEMENTED);
    }

    // Precondition: the input `rowset` should have the same type of the rowset we're building
    virtual Status add_rowset(RowsetSharedPtr rowset) = 0;

    // Precondition: the input `rowset` should have the same type of the rowset we're building
    virtual Status add_rowset_for_linked_schema_change(RowsetSharedPtr rowset) = 0;

    // explicit flush all buffered rows into segment file.
    // note that `add_row` could also trigger flush when certain conditions are met
    virtual Status flush() = 0;
    virtual Status flush_columns() {
        return Status::OLAPInternalError(OLAP_ERR_FUNC_NOT_IMPLEMENTED);
    }
    virtual Status final_flush() {
        return Status::OLAPInternalError(OLAP_ERR_FUNC_NOT_IMPLEMENTED);
    }

    virtual Status flush_single_memtable(MemTable* memtable, int64_t* flush_size) {
        return Status::OLAPInternalError(OLAP_ERR_FUNC_NOT_IMPLEMENTED);
    }
    virtual Status flush_single_memtable(const vectorized::Block* block) {
        return Status::OLAPInternalError(OLAP_ERR_FUNC_NOT_IMPLEMENTED);
    }

    // finish building and return pointer to the built rowset (guaranteed to be inited).
    // return nullptr when failed
    virtual RowsetSharedPtr build() = 0;

    // we have to load segment data to build delete_bitmap for current segment,
    // so we  build a tmp rowset ptr to load segment data.
    // real build will be called in DeltaWriter close_wait.
    virtual RowsetSharedPtr build_tmp() = 0;

    // For ordered rowset compaction, manual build rowset
    virtual RowsetSharedPtr manual_build(const RowsetMetaSharedPtr& rowset_meta) = 0;

    virtual Version version() = 0;

    virtual int64_t num_rows() const = 0;

    virtual RowsetId rowset_id() = 0;

    virtual RowsetTypePB type() const = 0;

    virtual Status get_segment_num_rows(std::vector<uint32_t>* segment_num_rows) const {
        return Status::NotSupported("to be implemented");
    }

private:
    DISALLOW_COPY_AND_ASSIGN(RowsetWriter);
};

} // namespace doris
