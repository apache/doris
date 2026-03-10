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
#include <string>
#include <vector>

#include "exec/partitioner/partitioner.h"
#include "exec/sink/writer/iceberg/partition_transformers.h"

namespace doris {
#include "common/compile_check_begin.h"

class IcebergInsertPartitionFunction final : public PartitionFunction {
public:
    IcebergInsertPartitionFunction(HashValType partition_count, ShuffleHashMethod hash_method,
                                   std::vector<TExpr> partition_exprs,
                                   std::vector<TIcebergPartitionField> partition_fields);

    Status init(const std::vector<TExpr>& texprs) override;
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override { return Status::OK(); }
    Status get_partitions(RuntimeState* state, Block* block, size_t partition_count,
                          std::vector<HashValType>& partitions) const override;
    HashValType partition_count() const override { return _partition_count; }
    Status clone(RuntimeState* state, std::unique_ptr<PartitionFunction>& function) const override;

    bool fallback_to_random() const { return _fallback_to_random; }

private:
    struct InsertPartitionField {
        std::string transform;
        VExprContextSPtr expr_ctx;
        std::unique_ptr<PartitionColumnTransform> transformer;
        int32_t source_id = 0;
        std::string name;
    };

    Status _compute_hashes_with_transform(Block* block, std::vector<HashValType>& partitions) const;
    Status _compute_hashes_with_exprs(Block* block, std::vector<HashValType>& partitions) const;
    Status _clone_expr_ctxs(RuntimeState* state, const VExprContextSPtrs& src,
                            VExprContextSPtrs& dst) const;

    const HashValType _partition_count;
    const ShuffleHashMethod _hash_method;
    std::vector<TExpr> _partition_exprs;
    std::vector<TIcebergPartitionField> _partition_fields_spec;
    VExprContextSPtrs _partition_expr_ctxs;
    std::vector<InsertPartitionField> _partition_fields;
    bool _fallback_to_random = false;
};

class IcebergDeletePartitionFunction final : public PartitionFunction {
public:
    IcebergDeletePartitionFunction(HashValType partition_count, ShuffleHashMethod hash_method,
                                   std::vector<TExpr> delete_exprs);

    Status init(const std::vector<TExpr>& texprs) override;
    Status prepare(RuntimeState* state, const RowDescriptor& row_desc) override;
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override { return Status::OK(); }
    Status get_partitions(RuntimeState* state, Block* block, size_t partition_count,
                          std::vector<HashValType>& partitions) const override;
    HashValType partition_count() const override { return _partition_count; }
    Status clone(RuntimeState* state, std::unique_ptr<PartitionFunction>& function) const override;

private:
    Status _compute_hashes(Block* block, std::vector<HashValType>& partitions) const;
    Status _get_delete_hash_column(const ColumnWithTypeAndName& column, ColumnPtr* out_column,
                                   DataTypePtr* out_type) const;
    int _find_file_path_index(const DataTypeStruct& struct_type) const;
    Status _clone_expr_ctxs(RuntimeState* state, const VExprContextSPtrs& src,
                            VExprContextSPtrs& dst) const;

    const HashValType _partition_count;
    const ShuffleHashMethod _hash_method;
    std::vector<TExpr> _delete_exprs;
    VExprContextSPtrs _delete_partition_expr_ctxs;
};

#include "common/compile_check_end.h"
} // namespace doris
