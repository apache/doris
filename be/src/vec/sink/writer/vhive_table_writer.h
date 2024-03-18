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
    VHiveTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs);

    ~VHiveTableWriter() = default;

    Status init_properties(ObjectPool* pool);

    Status open(RuntimeState* state, RuntimeProfile* profile) override;

    Status write(vectorized::Block& block) override;

    Status close(Status) override;

private:
    std::shared_ptr<VHivePartitionWriter> _create_partition_writer(vectorized::Block& block,
                                                                   int position);

    std::vector<std::string> _create_partition_values(vectorized::Block& block, int position);

    std::string _to_partition_value(const TypeDescriptor& type_desc,
                                    const ColumnWithTypeAndName& partition_column, int position);

    std::string _get_file_extension(TFileFormatType::type file_format_type,
                                    TFileCompressType::type write_compress_type);

    std::string _compute_file_name();

    // Currently it is a copy, maybe it is better to use move semantics to eliminate it.
    TDataSink _t_sink;
    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;
    std::vector<int> _partition_columns_input_index;
    std::unordered_map<std::string, std::shared_ptr<VHivePartitionWriter>> _partitions_to_writers;
};
} // namespace vectorized
} // namespace doris
