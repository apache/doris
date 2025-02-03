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

#include "exec/tablet_info.h"
#include "pipeline/exec/exchange_sink_operator.h"
#include "runtime/runtime_state.h"
#include "vec/core/block.h"
#include "vec/runtime/partitioner.h"
#include "vec/sink/vrow_distribution.h"
#include "vec/sink/vtablet_block_convertor.h"
#include "vec/sink/vtablet_finder.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class TabletSinkHashPartitioner final : public PartitionerBase {
public:
    using HashValType = int64_t;
    TabletSinkHashPartitioner(size_t partition_count, int64_t txn_id,
                              const TOlapTableSchemaParam& tablet_sink_schema,
                              const TOlapTablePartitionParam& tablet_sink_partition,
                              const TOlapTableLocationParam& tablet_sink_location,
                              const TTupleId& tablet_sink_tuple_id,
                              pipeline::ExchangeSinkLocalState* local_state);

    ~TabletSinkHashPartitioner() override = default;

    Status init(const std::vector<TExpr>& texprs) override;

    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override;

    Status open(RuntimeState* state) override;

    Status do_partitioning(RuntimeState* state, Block* block) const override;

    ChannelField get_channel_ids() const override;
    Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) override;

    Status close(RuntimeState* state) override;

private:
    static Status empty_callback_function(void* sender, TCreatePartitionResult* result) {
        return Status::OK();
    }

    Status _send_new_partition_batch(RuntimeState* state, vectorized::Block* input_block) const;

    const int64_t _txn_id = -1;
    const TOlapTableSchemaParam _tablet_sink_schema;
    const TOlapTablePartitionParam _tablet_sink_partition;
    const TOlapTableLocationParam _tablet_sink_location;
    const TTupleId _tablet_sink_tuple_id;
    mutable pipeline::ExchangeSinkLocalState* _local_state;
    mutable OlapTableLocationParam* _location = nullptr;
    mutable vectorized::VRowDistribution _row_distribution;
    mutable vectorized::VExprContextSPtrs _tablet_sink_expr_ctxs;
    mutable std::unique_ptr<VOlapTablePartitionParam> _vpartition = nullptr;
    mutable std::unique_ptr<vectorized::OlapTabletFinder> _tablet_finder = nullptr;
    mutable std::shared_ptr<OlapTableSchemaParam> _schema = nullptr;
    mutable std::unique_ptr<vectorized::OlapTableBlockConvertor> _block_convertor = nullptr;
    mutable TupleDescriptor* _tablet_sink_tuple_desc = nullptr;
    mutable RowDescriptor* _tablet_sink_row_desc = nullptr;
    mutable std::vector<vectorized::RowPartTabletIds> _row_part_tablet_ids;
    mutable std::vector<HashValType> _hash_vals;
};
#include "common/compile_check_end.h"

} // namespace doris::vectorized