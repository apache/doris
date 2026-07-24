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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "exec/partitioner/partitioner.h"

#ifdef WITH_PAIMON_CPP
namespace paimon {
class BucketIdCalculator;
class MemoryPool;
} // namespace paimon
#endif

namespace doris {

struct PaimonBucketShuffleParams {
    int32_t bucket_num = 0;
    std::vector<std::string> bucket_keys;
    std::vector<std::string> column_names;
};

class PaimonBucketPartitioner final : public PartitionerBase {
public:
    explicit PaimonBucketPartitioner(HashValType partition_count, PaimonBucketShuffleParams params);
    ~PaimonBucketPartitioner() override = default;

    Status init(const std::vector<TExpr>& texprs) override;
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override;
    Status open(RuntimeState* state) override;
    Status do_partitioning(RuntimeState* state, Block* block) const override;

    Status close(RuntimeState* state) override { return Status::OK(); }

    const std::vector<HashValType>& get_channel_ids() const override { return _channel_ids; }

    Status clone(RuntimeState* state, std::unique_ptr<PartitionerBase>& partitioner) override;

private:
    Status _compute_bucket_ids(RuntimeState* state, const Block& block, int32_t* bucket_ids) const;

    PaimonBucketShuffleParams _params;
    mutable std::vector<HashValType> _channel_ids;

#ifdef WITH_PAIMON_CPP
    std::shared_ptr<::paimon::MemoryPool> _pool;
    std::unique_ptr<::paimon::BucketIdCalculator> _calculator;
#endif
};

} // namespace doris
