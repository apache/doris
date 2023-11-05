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

using Payload = std::pair<std::unique_ptr<vectorized::IColumn::Selector>, std::vector<int64_t>>;

typedef Status (*OnPartitionsCreated)(void*, TCreatePartitionResult*);

class VRowDistributionContext {
public:
    RuntimeState* state = nullptr;     // not owned, set when open
    std::vector<std::shared_ptr<IndexChannel>>* channels;
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
};

class VRowDistribution {
public:
    VRowDistribution() {
    }

    void init(VRowDistributionContext *ctx) {
        _state = ctx->state;
        _channels = ctx->channels;
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
    }

    using ChannelDistributionPayload = std::vector<std::unordered_map<VNodeChannel*, Payload>>;

    // auto partition
    // mv where clause
    // v1 needs index->node->row_ids - tabletids
    // v2 needs index,tablet->rowids
    Status generate_rows_distribution(vectorized::Block& input_block,
                                      std::shared_ptr<vectorized::Block>& block,
                                      int64_t& filtered_rows, bool& has_filtered_rows,
                                      ChannelDistributionPayload& channel_to_payload);
 
private:
    std::pair<vectorized::VExprContextSPtr, vectorized::VExprSPtr> _get_partition_function();
    void _save_missing_values(vectorized::ColumnPtr col, vectorized::DataTypePtr value_type,
                              std::vector<int64_t> filter);

    // create partitions when need for auto-partition table using #_partitions_need_create.
    Status _automatic_create_partition();

    Status _single_partition_generate(vectorized::Block* block,
                                      ChannelDistributionPayload& channel_to_payload,
                                      size_t num_rows, bool has_filtered_rows);

    void _generate_rows_distribution_payload(
        ChannelDistributionPayload& channel_to_payload,
        const std::vector<VOlapTablePartition*>& partitions,
        const std::vector<uint32_t>& tablet_indexes, const std::vector<bool>& skip,
        size_t row_cnt);

private:
    RuntimeState* _state = nullptr;     // not owned, set when open

    // support only one partition column now
    std::vector<std::vector<TStringLiteral>> _partitions_need_create;
    std::vector<std::shared_ptr<IndexChannel>>* _channels;

    MonotonicStopWatch _row_distribution_watch;
    OlapTableBlockConvertor* _block_convertor = nullptr;
    OlapTabletFinder* _tablet_finder = nullptr;
    VOlapTablePartitionParam* _vpartition = nullptr;
    RuntimeProfile::Counter* _add_partition_request_timer = nullptr;
    int64_t _txn_id = -1;
    ObjectPool* _pool;
    OlapTableLocationParam* _location = nullptr;
    // std::function _on_partition_created;
    int64_t _number_output_rows = 0;
    const VExprContextSPtrs* _vec_output_expr_ctxs;
    OnPartitionsCreated _on_partitions_created = nullptr;
    void *_caller;
};

} // namespace doris::vectorized
