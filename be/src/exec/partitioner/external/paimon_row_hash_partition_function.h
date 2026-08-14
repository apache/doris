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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include "exec/partitioner/partitioner.h"

namespace doris {
#include "common/compile_check_begin.h"

// Shared expression lifecycle and BinaryRow hashing for Paimon routing functions.
class PaimonRowHashPartitionFunction : public PartitionFunction {
public:
    explicit PaimonRowHashPartitionFunction(HashValType partition_count);

    Status init(const std::vector<TExpr>& texprs) override;
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override { return Status::OK(); }
    HashValType partition_count() const override { return _partition_count; }

protected:
    Status _validate_field_indexes(const std::vector<int32_t>& indexes,
                                   bool require_non_empty) const;
    Status _evaluate_fields(Block* block, std::vector<ColumnWithTypeAndName>& fields) const;
    Status _hash_fields(const std::vector<int32_t>& indexes,
                        const std::vector<ColumnWithTypeAndName>& fields,
                        std::vector<int32_t>& hashes) const;
    Status _clone_expr_ctxs(RuntimeState* state, VExprContextSPtrs& destination) const;

    const HashValType _partition_count;
    VExprContextSPtrs _field_expr_ctxs;
};

#include "common/compile_check_end.h"
} // namespace doris
