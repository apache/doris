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
#include "exec/data_sink.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/sink/volap_table_sink.h"
#include "vec/sink/writer/async_result_writer.h"

namespace doris {

class OlapTableSchemaParam;
class MemTracker;
class LoadBlockQueue;

namespace vectorized {

class VGroupCommitBlockWriter : public AsyncResultWriter {
public:
    VGroupCommitBlockWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs);

    ~VGroupCommitBlockWriter() override;

    Status write(Block& block) override;

    Status open(RuntimeState* state, RuntimeProfile* profile) override;

    Status close(Status close_status) override;

private:
    Status _init(RuntimeState* state, RuntimeProfile* profile);
    Status _add_block(RuntimeState* state, std::shared_ptr<vectorized::Block> block);
    Status _add_blocks(RuntimeState* state, bool is_blocks_contain_all_load_data);
    size_t _calculate_estimated_wal_bytes(bool is_blocks_contain_all_load_data);
    void _remove_estimated_wal_bytes();

    vectorized::VExprContextSPtrs _output_vexpr_ctxs;

    int _tuple_desc_id = -1;
    std::shared_ptr<OlapTableSchemaParam> _schema;

    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;
    std::shared_ptr<MemTracker> _mem_tracker;
    // this is tuple descriptor of destination OLAP table
    TupleDescriptor* _output_tuple_desc = nullptr;
    std::unique_ptr<vectorized::OlapTableBlockConvertor> _block_convertor;

    int64_t _db_id;
    int64_t _table_id;
    int64_t _base_schema_version = 0;
    TGroupCommitMode::type _group_commit_mode;
    UniqueId _load_id;
    std::shared_ptr<LoadBlockQueue> _load_block_queue;
    // used to calculate if meet the max filter ratio
    std::vector<std::shared_ptr<vectorized::Block>> _blocks;
    bool _is_block_appended = false;
    double _max_filter_ratio = 0.0;

    // used for find_partition
    std::unique_ptr<VOlapTablePartitionParam> _vpartition = nullptr;
    // reuse for find_tablet.
    std::vector<VOlapTablePartition*> _partitions;
    Bitmap _filter_bitmap;
    bool _has_filtered_rows = false;
    size_t _estimated_wal_bytes = 0;
    TDataSink _t_sink;
};

} // namespace vectorized
} // namespace doris
