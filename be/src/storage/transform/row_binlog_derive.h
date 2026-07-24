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

#include "storage/transform/block_transform.h"

namespace doris {
struct RowsetWriterContext;

namespace segment_v2 {

// The binlog<Row> derive stages rebuild the load block into a full-width block
// over the binlog schema -- key + AFTER values, optional __BEFORE__* values, and
// the TSO / LSN / op columns -- so the ordinary segment writers can write
// it like any DUP_KEYS block. build_transform_chain picks Plain (no historical
// probe) or Mow (with probe) via binlog_needs_historical_lookup().

// Whether the flush needs the historical key probe: a direct partial update
// (flexible is rejected later) or a requested BEFORE image. Decided per flush.
bool binlog_needs_historical_lookup(const RowsetWriterContext& context);

// Context for each flush that the base stage works out once and passes to
// derive(): the binlog/source schemas, the consumed LSN range, row count, and
// column layout [keys..., AFTER..., (BEFORE...), TSO, LSN, OP].
struct BinlogDeriveContext {
    TabletSchemaSPtr binlog_schema;
    TabletSchemaSPtr source_schema;
    std::shared_ptr<const std::vector<int64_t>> lsn_ids;
    size_t num_rows = 0;
    uint32_t binlog_tso_cid = 0;
    uint32_t binlog_lsn_cid = 0;
    uint32_t binlog_op_cid = 0;
    uint32_t normal_col_start = 0;
    uint32_t before_col_start = 0;
    std::vector<uint32_t> normal_source_cids;
    std::vector<uint32_t> value_source_cids;
    bool write_before = false;
};

// Base for the derive stages: apply() runs the cloud-mode guard and the setup
// steps (schema layout + LSN hand-off), then calls derive().
class RowBinlogDeriveStage : public BlockTransform {
public:
    Status apply(TransformExecContext& ctx, Block* block) const final;

protected:
    virtual Status derive(TransformExecContext& ctx, Block* block,
                          const BinlogDeriveContext& c) const = 0;
};

// No probe: DUP loads and full-row upserts without a BEFORE image. AFTER is the
// source columns as-is; op is APPEND, or DELETE from the row's own delete sign.
class PlainRowBinlogDeriveStage : public RowBinlogDeriveStage {
public:
    std::string_view name() const override { return "PlainRowBinlogDerive"; }

protected:
    Status derive(TransformExecContext& ctx, Block* block,
                  const BinlogDeriveContext& c) const override;
};

// With a probe: fixed partial update (rebuild AFTER from history) and/or the
// BEFORE image. It repeats the data chain's key probe on the same source block,
// but never marks the delete bitmap.
class MowRowBinlogDeriveStage : public RowBinlogDeriveStage {
public:
    std::string_view name() const override { return "MowRowBinlogDerive"; }

protected:
    Status derive(TransformExecContext& ctx, Block* block,
                  const BinlogDeriveContext& c) const override;
};

} // namespace segment_v2
} // namespace doris
