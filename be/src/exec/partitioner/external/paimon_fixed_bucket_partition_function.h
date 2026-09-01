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

#include "exec/partitioner/external/paimon_row_hash_partition_function.h"

namespace doris {

// Stateless native implementation of Paimon FixedBucketWriteSelector for the
// explicitly supported primitive routing types.
class PaimonFixedBucketPartitionFunction final : public PaimonRowHashPartitionFunction {
public:
    PaimonFixedBucketPartitionFunction(HashValType partition_count,
                                       TPaimonFixedBucketInfo fixed_bucket_info);

    Status init(const std::vector<TExpr>& texprs) override;
    Status get_partitions(RuntimeState* state, Block* block, size_t partition_count,
                          std::vector<HashValType>& partitions) const override;
    Status clone(RuntimeState* state, std::unique_ptr<PartitionFunction>& function) const override;

private:
    TPaimonFixedBucketInfo _fixed_bucket_info;
};

} // namespace doris
