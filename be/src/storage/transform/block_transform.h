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

#include <map>
#include <memory>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/olap_common.h"
#include "storage/partial_update_info.h"
#include "storage/tablet/tablet_fwd.h"

namespace doris {
class Block;
class IColumn;
struct RowsetWriterContext;

namespace segment_v2 {

// Computes a derived column (e.g. the hidden row-store column) in bounded batches.
class DerivedColumnGenerator {
public:
    virtual ~DerivedColumnGenerator() = default;
    // Appends a bounded batch (<= max_rows, stops near max_bytes, but always >= 1
    // row); returns the rows produced.
    virtual size_t generate(const Block& block, size_t row_pos, size_t max_rows, size_t max_bytes,
                            IColumn* dst) const = 0;
};

using DerivedColumn = std::pair<uint32_t, std::shared_ptr<const DerivedColumnGenerator>>;

// For the horizontal writer, which can't feed derived columns in bounded batches.
Status materialize_derived_columns(const DerivedColumn& derived_column, Block* block);

// State for each flush, kept out of the chain so the chain stays immutable and
// shareable across concurrent flushes. Filled in for each flush; the tablet
// schema in particular differs between flushes for variant tables.
struct TransformExecContext {
    TabletSchemaSPtr tablet_schema;
    DataWriteType write_type = DataWriteType::TYPE_DEFAULT;

    // --- partial update inputs (filled by the call sites from RowsetWriterContext) ---
    BaseTabletSPtr tablet;
    std::shared_ptr<MowContext> mow_context;
    std::shared_ptr<PartialUpdateInfo> partial_update_info;
    RowsetWriterContext* rowset_ctx = nullptr;
    // identifies the segment this block lands in (for the self-marks)
    RowsetId rowset_id;
    // -1 until flush_single_block sets it; partial update needs it set.
    int32_t segment_id = -1;

    // --- outputs ---
    // the derived column for the writer to generate in batches; null generator = none
    DerivedColumn derived_column;
    PartialUpdateStats partial_update_stats;
};

// One Block -> Block step run before the segment writers. apply() may mutate
// the block in place or swap its columns, but must not keep references to it.
class BlockTransform {
public:
    virtual ~BlockTransform() = default;
    virtual Status apply(TransformExecContext& ctx, Block* block) const = 0;
    // Stable name used for debugging and to check how the chain is built.
    virtual std::string_view name() const = 0;
};

// An ordered list of transforms, built once per rowset writer and applied to
// each flushed block.
class BlockTransformChain {
public:
    BlockTransformChain() = default;
    explicit BlockTransformChain(std::vector<std::shared_ptr<const BlockTransform>> stages)
            : _stages(std::move(stages)) {}

    Status apply(TransformExecContext& ctx, Block* block) const {
        for (const auto& stage : _stages) {
            RETURN_IF_ERROR(stage->apply(ctx, block));
        }
        return Status::OK();
    }

    bool empty() const { return _stages.empty(); }

    std::vector<std::string_view> stage_names() const {
        std::vector<std::string_view> names;
        names.reserve(_stages.size());
        for (const auto& stage : _stages) {
            names.push_back(stage->name());
        }
        return names;
    }

private:
    std::vector<std::shared_ptr<const BlockTransform>> _stages;
};

// The single place that decides which transforms a write path gets:
//   - compaction: empty (rows are already final)
//   - direct binlog sub-writer: [RowBinlogDerive]
//   - non-direct binlog sub-writer: empty (rows are already in binlog schema)
//   - fixed partial update: [Validate, FixedFill, VariantParse, RowStoreFill]
//   - flexible partial update: [Validate, FlexibleFill, RowStoreFill, VariantParse]
//   - direct / schema change / transient flush: [Validate, RowStoreFill, VariantParse]
// RowStoreFill is omitted when the write type does not rebuild the row-store column.
BlockTransformChain build_transform_chain(const RowsetWriterContext& context);

} // namespace segment_v2
} // namespace doris
