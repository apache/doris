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

// IWYU pragma: no_include <bits/chrono.h>
#include <fmt/format.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/PaloInternalService_types.h>

#include <cstdint>
#include <string>
#include <vector>

#include "common/status.h"
#include "exec/tablet_info.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/sink/vtablet_block_convertor.h"
#include "vec/sink/vtablet_finder.h"

namespace doris::vectorized {

class IndexChannel;
class VNodeChannel;

// <row_idx, partition_id, tablet_id>
class RowPartTabletIds {
public:
    std::vector<int64_t> row_ids;
    std::vector<int64_t> partition_ids;
    std::vector<int64_t> tablet_ids;

    std::string debug_string() const {
        std::string value;
        value.reserve(row_ids.size() * 15);
        for (int i = 0; i < row_ids.size(); i++) {
            value.append(fmt::format("[{}, {}, {}]", row_ids[i], partition_ids[i], tablet_ids[i]));
        }
        return value;
    }
};

// void* for caller
using CreatePartitionCallback = Status (*)(void*, TCreatePartitionResult*);

class VRowDistribution {
public:
    // only used to pass parameters for VRowDistribution
    struct VRowDistributionContext {
        RuntimeState* state = nullptr;
        OlapTableBlockConvertor* block_convertor = nullptr;
        OlapTabletFinder* tablet_finder = nullptr;
        VOlapTablePartitionParam* vpartition = nullptr;
        RuntimeProfile::Counter* add_partition_request_timer = nullptr;
        int64_t txn_id = -1;
        ObjectPool* pool = nullptr;
        OlapTableLocationParam* location = nullptr;
        const VExprContextSPtrs* vec_output_expr_ctxs = nullptr;
        std::shared_ptr<OlapTableSchemaParam> schema;
        void* caller = nullptr;
        CreatePartitionCallback create_partition_callback;
    };
    friend class VTabletWriter;
    friend class VTabletWriterV2;

    VRowDistribution() = default;
    virtual ~VRowDistribution() = default;

    void init(VRowDistributionContext ctx) {
        _state = ctx.state;
        _batch_size = std::max(_state->batch_size(), 8192);
        _block_convertor = ctx.block_convertor;
        _tablet_finder = ctx.tablet_finder;
        _vpartition = ctx.vpartition;
        _add_partition_request_timer = ctx.add_partition_request_timer;
        _txn_id = ctx.txn_id;
        _pool = ctx.pool;
        _location = ctx.location;
        _vec_output_expr_ctxs = ctx.vec_output_expr_ctxs;
        _schema = ctx.schema;
        _caller = ctx.caller;
        _create_partition_callback = ctx.create_partition_callback;
    }

    Status open(RowDescriptor* output_row_desc) {
        if (_vpartition->is_auto_partition()) {
            auto [part_ctxs, part_funcs] = _get_partition_function();
            for (auto part_ctx : part_ctxs) {
                RETURN_IF_ERROR(part_ctx->prepare(_state, *output_row_desc));
                RETURN_IF_ERROR(part_ctx->open(_state));
            }
        }
        for (const auto& index : _schema->indexes()) {
            auto& where_clause = index->where_clause;
            if (where_clause != nullptr) {
                RETURN_IF_ERROR(where_clause->prepare(_state, *output_row_desc));
                RETURN_IF_ERROR(where_clause->open(_state));
            }
        }
        return Status::OK();
    }

    // auto partition
    // mv where clause
    // v1 needs index->node->row_ids - tabletids
    // v2 needs index,tablet->rowids
    Status generate_rows_distribution(vectorized::Block& input_block,
                                      std::shared_ptr<vectorized::Block>& block,
                                      int64_t& filtered_rows, bool& has_filtered_rows,
                                      std::vector<RowPartTabletIds>& row_part_tablet_ids,
                                      int64_t& rows_stat_val);
    bool need_deal_batching() const { return _deal_batched && _batching_rows > 0; }
    // create partitions when need for auto-partition table using #_partitions_need_create.
    Status automatic_create_partition();
    void clear_batching_stats();

private:
    std::pair<vectorized::VExprContextSPtrs, vectorized::VExprSPtrs> _get_partition_function();

    Status _save_missing_values(std::vector<std::vector<std::string>>& col_strs, int col_size,
                                Block* block, std::vector<int64_t> filter,
                                const std::vector<const NullMap*>& col_null_maps);

    void _get_tablet_ids(vectorized::Block* block, int32_t index_idx,
                         std::vector<int64_t>& tablet_ids);

    void _filter_block_by_skip(vectorized::Block* block, RowPartTabletIds& row_part_tablet_id);

    Status _filter_block_by_skip_and_where_clause(vectorized::Block* block,
                                                  const vectorized::VExprContextSPtr& where_clause,
                                                  RowPartTabletIds& row_part_tablet_id);

    Status _filter_block(vectorized::Block* block,
                         std::vector<RowPartTabletIds>& row_part_tablet_ids);

    Status _generate_rows_distribution_for_auto_partition(
            vectorized::Block* block, const std::vector<uint16_t>& partition_col_idx,
            bool has_filtered_rows, std::vector<RowPartTabletIds>& row_part_tablet_ids,
            int64_t& rows_stat_val);

    Status _generate_rows_distribution_for_non_auto_partition(
            vectorized::Block* block, bool has_filtered_rows,
            std::vector<RowPartTabletIds>& row_part_tablet_ids);

    Status _generate_rows_distribution_for_auto_overwrite(
            vectorized::Block* block, bool has_filtered_rows,
            std::vector<RowPartTabletIds>& row_part_tablet_ids);
    Status _replace_overwriting_partition();

    void _reset_row_part_tablet_ids(std::vector<RowPartTabletIds>& row_part_tablet_ids,
                                    int64_t rows);
    void _reset_find_tablets(int64_t rows);

    RuntimeState* _state = nullptr;
    int _batch_size = 0;

    // for auto partitions
    std::vector<std::vector<TNullableStringLiteral>> _partitions_need_create;

public:
    std::unique_ptr<MutableBlock> _batching_block;
    bool _deal_batched = false; // If true, send batched block before any block's append.
private:
    size_t _batching_rows = 0, _batching_bytes = 0;

    OlapTableBlockConvertor* _block_convertor = nullptr;
    OlapTabletFinder* _tablet_finder = nullptr;
    VOlapTablePartitionParam* _vpartition = nullptr;
    RuntimeProfile::Counter* _add_partition_request_timer = nullptr;
    int64_t _txn_id = -1;
    ObjectPool* _pool = nullptr;
    OlapTableLocationParam* _location = nullptr;
    // int64_t _number_output_rows = 0;
    const VExprContextSPtrs* _vec_output_expr_ctxs = nullptr;
    // generally it's writer's on_partitions_created
    CreatePartitionCallback _create_partition_callback = nullptr;
    void* _caller = nullptr;
    std::shared_ptr<OlapTableSchemaParam> _schema;

    // reuse for find_tablet. save partitions found by find_tablets
    std::vector<VOlapTablePartition*> _partitions;
    std::vector<bool> _skip;
    std::vector<uint32_t> _tablet_indexes;
    std::vector<int64_t> _tablet_ids;
    std::vector<int64_t> _missing_map; // indice of missing values in partition_col
    // for auto detect overwrite partition
    std::set<int64_t> _new_partition_ids; // if contains, not to replace it again.
};

} // namespace doris::vectorized
