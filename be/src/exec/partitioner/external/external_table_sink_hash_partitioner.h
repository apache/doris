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

#include <gen_cpp/Partitions_types.h>

#include <memory>
#include <vector>

#include "exec/partitioner/partitioner.h"
#include "exec/partitioner/writer_assigner.h"

namespace doris {

// Computes external sink logical partitions and maps them to Doris exchange channels.
// Optional partition transforms are evaluated transiently and never appended to the sink row.
class ExternalTableSinkHashPartitioner final : public PartitionerBase {
public:
    ExternalTableSinkHashPartitioner(HashValType partition_count, ShuffleHashMethod hash_method,
                                     TExternalTableSinkHashPartitionInfo partition_info);

    Status init(const std::vector<TExpr>& texprs) override;
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    Status do_partitioning(RuntimeState* state, Block* block) const override;
    const std::vector<HashValType>& get_channel_ids() const override;
    Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) override;

private:
    ShuffleHashMethod _hash_method;
    TExternalTableSinkHashPartitionInfo _partition_info;
    HashValType _logical_partition_count;
    std::unique_ptr<PartitionFunction> _partition_function;
    mutable std::unique_ptr<WriterAssigner> _writer_assigner;
    mutable std::vector<HashValType> _logical_partition_ids;
    mutable std::vector<HashValType> _channel_ids;
};

} // namespace doris
