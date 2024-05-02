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

#include "operator.h"
#include "vec/sink/group_commit_block_sink.h"

namespace doris::pipeline {

class GroupCommitBlockSinkOperatorX;
class GroupCommitBlockSinkLocalState final : public PipelineXSinkLocalState<BasicSharedState> {
    ENABLE_FACTORY_CREATOR(GroupCommitBlockSinkLocalState);
    using Base = PipelineXSinkLocalState<BasicSharedState>;

public:
    GroupCommitBlockSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state)
            : Base(parent, state), _filter_bitmap(1024) {}

    ~GroupCommitBlockSinkLocalState() override;

    Status open(RuntimeState* state) override;

    Status close(RuntimeState* state, Status exec_status) override;

private:
    friend class GroupCommitBlockSinkOperatorX;
    Status _add_block(RuntimeState* state, std::shared_ptr<vectorized::Block> block);
    Status _add_blocks(RuntimeState* state, bool is_blocks_contain_all_load_data);
    size_t _calculate_estimated_wal_bytes(bool is_blocks_contain_all_load_data);
    void _remove_estimated_wal_bytes();

    vectorized::VExprContextSPtrs _output_vexpr_ctxs;

    std::unique_ptr<vectorized::OlapTableBlockConvertor> _block_convertor;

    std::shared_ptr<LoadBlockQueue> _load_block_queue;
    // used to calculate if meet the max filter ratio
    std::vector<std::shared_ptr<vectorized::Block>> _blocks;
    bool _is_block_appended = false;
    // used for find_partition
    std::unique_ptr<VOlapTablePartitionParam> _vpartition = nullptr;
    // reuse for find_tablet.
    std::vector<VOlapTablePartition*> _partitions;
    bool _has_filtered_rows = false;
    size_t _estimated_wal_bytes = 0;
    TGroupCommitMode::type _group_commit_mode;
    Bitmap _filter_bitmap;
};

class GroupCommitBlockSinkOperatorX final
        : public DataSinkOperatorX<GroupCommitBlockSinkLocalState> {
    using Base = DataSinkOperatorX<GroupCommitBlockSinkLocalState>;

public:
    GroupCommitBlockSinkOperatorX(int operator_id, const RowDescriptor& row_desc)
            : Base(operator_id, 0), _row_desc(row_desc) {}

    ~GroupCommitBlockSinkOperatorX() override = default;

    Status init(const TDataSink& sink) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* block, bool eos) override;

private:
    friend class GroupCommitBlockSinkLocalState;

    const RowDescriptor& _row_desc;
    vectorized::VExprContextSPtrs _output_vexpr_ctxs;

    int _tuple_desc_id = -1;
    std::shared_ptr<OlapTableSchemaParam> _schema;

    TupleDescriptor* _output_tuple_desc = nullptr;

    int64_t _db_id;
    int64_t _table_id;
    int64_t _base_schema_version = 0;
    UniqueId _load_id;
    double _max_filter_ratio = 0.0;

    TOlapTablePartitionParam _partition;
    TGroupCommitMode::type _group_commit_mode;
};

} // namespace doris::pipeline
