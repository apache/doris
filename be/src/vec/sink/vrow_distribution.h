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
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/PaloInternalService_types.h>

#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "exec/tablet_info.h"
#include "runtime/types.h"
#include "util/runtime_profile.h"
#include "util/stopwatch.hpp"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
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
};

typedef Status (*OnPartitionsCreated)(void*, TCreatePartitionResult*);

class VRowDistributionContext {
public:
    RuntimeState* state = nullptr;
    OlapTableBlockConvertor* block_convertor = nullptr;
    OlapTabletFinder* tablet_finder = nullptr;
    VOlapTablePartitionParam* vpartition = nullptr;
    RuntimeProfile::Counter* add_partition_request_timer = nullptr;
    int64_t txn_id = -1;
    ObjectPool* pool;
    OlapTableLocationParam* location;
    const VExprContextSPtrs* vec_output_expr_ctxs;
    OnPartitionsCreated on_partitions_created;
    void* caller;
    std::shared_ptr<OlapTableSchemaParam> schema;
};

class VRowDistribution {
public:
    VRowDistribution() {}
    virtual ~VRowDistribution() {}

    void init(VRowDistributionContext* ctx) {
        _state = ctx->state;
        _block_convertor = ctx->block_convertor;
        _tablet_finder = ctx->tablet_finder;
        _vpartition = ctx->vpartition;
        _add_partition_request_timer = ctx->add_partition_request_timer;
        _txn_id = ctx->txn_id;
        _pool = ctx->pool;
        _location = ctx->location;
        _vec_output_expr_ctxs = ctx->vec_output_expr_ctxs;
        _on_partitions_created = ctx->on_partitions_created;
        _caller = ctx->caller;
        _schema = ctx->schema;
    }

    // auto partition
    // mv where clause
    // v1 needs index->node->row_ids - tabletids
    // v2 needs index,tablet->rowids
    Status generate_rows_distribution(vectorized::Block& input_block,
                                      std::shared_ptr<vectorized::Block>& block,
                                      int64_t& filtered_rows, bool& has_filtered_rows,
                                      std::vector<RowPartTabletIds>& row_part_tablet_ids);

private:
    std::pair<vectorized::VExprContextSPtr, vectorized::VExprSPtr> _get_partition_function();

    void _save_missing_values(vectorized::ColumnPtr col, vectorized::DataTypePtr value_type);

    // create partitions when need for auto-partition table using #_partitions_need_create.
    Status _automatic_create_partition();

    void _get_tablet_ids(vectorized::Block* block, int32_t index_idx,
                         std::vector<int64_t>& tablet_ids);

    void _filter_block_by_skip(vectorized::Block* block, RowPartTabletIds& row_part_tablet_id);

    Status _filter_block_by_skip_and_where_clause(vectorized::Block* block,
                                                  const vectorized::VExprContextSPtr& where_clause,
                                                  RowPartTabletIds& row_part_tablet_id);

    Status _filter_block(vectorized::Block* block,
                         std::vector<RowPartTabletIds>& row_part_tablet_ids);

    Status _generate_rows_distribution_for_auto_parititon(
            vectorized::Block* block, int partition_col_idx, bool has_filtered_rows,
            std::vector<RowPartTabletIds>& row_part_tablet_ids);

    Status _generate_rows_distribution_for_non_auto_parititon(
            vectorized::Block* block, bool has_filtered_rows,
            std::vector<RowPartTabletIds>& row_part_tablet_ids);

    void _reset_row_part_tablet_ids(std::vector<RowPartTabletIds>& row_part_tablet_ids,
                                    int64_t rows);

private:
    RuntimeState* _state = nullptr;

    // support only one partition column now
    std::vector<std::vector<TStringLiteral>> _partitions_need_create;

    MonotonicStopWatch _row_distribution_watch;
    OlapTableBlockConvertor* _block_convertor = nullptr;
    OlapTabletFinder* _tablet_finder = nullptr;
    VOlapTablePartitionParam* _vpartition = nullptr;
    RuntimeProfile::Counter* _add_partition_request_timer = nullptr;
    int64_t _txn_id = -1;
    ObjectPool* _pool;
    OlapTableLocationParam* _location = nullptr;
    // std::function _on_partition_created;
    // int64_t _number_output_rows = 0;
    const VExprContextSPtrs* _vec_output_expr_ctxs;
    OnPartitionsCreated _on_partitions_created = nullptr;
    void* _caller;
    std::shared_ptr<OlapTableSchemaParam> _schema;

    // reuse for find_tablet.
    std::vector<VOlapTablePartition*> _partitions;
    std::vector<bool> _skip;
    std::vector<uint32_t> _tablet_indexes;
    std::vector<int64_t> _tablet_ids;
    std::vector<int64_t> _missing_map; // indice of missing values in partition_col
};

} // namespace doris::vectorized
