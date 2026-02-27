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

#include <gen_cpp/DataSinks_types.h>

#include <atomic>
#include <set>
#include <unordered_map>

#include "util/runtime_profile.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/sink/writer/async_result_writer.h"

namespace doris {

class ObjectPool;
class RuntimeState;

namespace vectorized {

class VMCPartitionWriter;

class VMCTableWriter final : public AsyncResultWriter {
public:
    VMCTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs,
                   std::shared_ptr<pipeline::Dependency> dep,
                   std::shared_ptr<pipeline::Dependency> fin_dep);

    ~VMCTableWriter() = default;

    Status init_properties(ObjectPool* pool);

    Status open(RuntimeState* state, RuntimeProfile* profile) override;

    Status write(RuntimeState* state, vectorized::Block& block) override;

    Status close(Status) override;

private:
    std::shared_ptr<VMCPartitionWriter> _create_partition_writer(const std::string& partition_spec);

    std::map<std::string, std::string> _build_base_writer_params();

    TDataSink _t_sink;
    const TMaxComputeTableSink& _mc_sink;
    RuntimeState* _state = nullptr;

    // partition_spec -> writer mapping
    std::unordered_map<std::string, std::shared_ptr<VMCPartitionWriter>> _partitions_to_writers;

    // Partition column names
    std::vector<std::string> _partition_column_names;

    // Whether static partition is specified
    bool _has_static_partition = false;
    std::string _static_partition_spec; // "key1=val1/key2=val2"

    // Indices of partition columns to erase before writing data columns
    std::set<size_t> _non_write_columns_indices;

    // Write output expr contexts (after removing partition columns)
    VExprContextSPtrs _write_output_vexpr_ctxs;

    // Atomic block_id counter: each partition writer gets a unique block_id
    // Initialized with offset based on per_fragment_instance_idx to avoid collisions
    // across pipeline instances sharing the same write session.
    std::atomic<int64_t> _next_block_id {0};
    static constexpr int64_t BLOCK_ID_STRIDE = 100;

    size_t _row_count = 0;
    int64_t _send_data_ns = 0;
    int64_t _close_ns = 0;

    // Profile counters
    RuntimeProfile::Counter* _written_rows_counter = nullptr;
    RuntimeProfile::Counter* _send_data_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _partition_writers_count = nullptr;
};

} // namespace vectorized
} // namespace doris
