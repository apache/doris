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

#include "util/runtime_profile.h"
#include "vec/columns/column.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/sink/writer/async_result_writer.h"

namespace doris {

class ObjectPool;
class RuntimeState;
class RuntimeProfile;
struct TypeDescriptor;

namespace vectorized {

class Block;
class VHivePartitionWriter;
struct ColumnWithTypeAndName;

class VHiveTableWriter final : public AsyncResultWriter {
public:
    VHiveTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs,
                     std::shared_ptr<pipeline::Dependency> dep,
                     std::shared_ptr<pipeline::Dependency> fin_dep);

    ~VHiveTableWriter() override = default;

    Status init_properties(ObjectPool* pool);

    Status open(RuntimeState* state, RuntimeProfile* profile) override;

    Status write(RuntimeState* state, vectorized::Block& block) override;

    Status close(Status) override;

private:
    std::shared_ptr<VHivePartitionWriter> _create_partition_writer(
            vectorized::Block& block, int position, const std::string* file_name = nullptr,
            int file_name_index = 0);

    std::vector<std::string> _create_partition_values(vectorized::Block& block, int position);

    std::string _to_partition_value(const TypeDescriptor& type_desc,
                                    const ColumnWithTypeAndName& partition_column, int position);

    std::string _compute_file_name();

    Status _filter_block(doris::vectorized::Block& block, const vectorized::IColumn::Filter* filter,
                         doris::vectorized::Block* output_block);

    // Currently it is a copy, maybe it is better to use move semantics to eliminate it.
    TDataSink _t_sink;
    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;
    std::vector<int> _partition_columns_input_index;
    std::set<size_t> _non_write_columns_indices;
    std::unordered_map<std::string, std::shared_ptr<VHivePartitionWriter>> _partitions_to_writers;

    VExprContextSPtrs _write_output_vexpr_ctxs;

    size_t _row_count = 0;

    // profile counters
    int64_t _send_data_ns = 0;
    int64_t _partition_writers_dispatch_ns = 0;
    int64_t _partition_writers_write_ns = 0;
    int64_t _close_ns = 0;
    int64_t _write_file_count = 0;

    RuntimeProfile::Counter* _written_rows_counter = nullptr;
    RuntimeProfile::Counter* _send_data_timer = nullptr;
    RuntimeProfile::Counter* _partition_writers_dispatch_timer = nullptr;
    RuntimeProfile::Counter* _partition_writers_write_timer = nullptr;
    RuntimeProfile::Counter* _partition_writers_count = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _write_file_counter = nullptr;
};
} // namespace vectorized
} // namespace doris
