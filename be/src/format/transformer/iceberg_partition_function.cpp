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

#include "format/transformer/iceberg_partition_function.h"

#include "common/cast_set.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/data_type/data_type_struct.h"
#include "exec/sink/writer/iceberg/partition_transformers.h"
#include "format/table/iceberg/partition_spec.h"
#include "util/string_util.h"

namespace doris {
#include "common/compile_check_begin.h"

using HashValType = PartitionerBase::HashValType;

static void initialize_shuffle_hashes(std::vector<HashValType>& hashes, size_t rows,
                                      ShuffleHashMethod method) {
    hashes.resize(rows);
    if (method == ShuffleHashMethod::CRC32C) {
        constexpr HashValType CRC32C_SHUFFLE_SEED = 0x9E3779B9U;
        std::fill(hashes.begin(), hashes.end(), CRC32C_SHUFFLE_SEED);
    } else {
        std::fill(hashes.begin(), hashes.end(), 0);
    }
}

static void update_shuffle_hashes(const ColumnPtr& column, const DataTypePtr& type,
                                  HashValType* __restrict result, ShuffleHashMethod method) {
    if (method == ShuffleHashMethod::CRC32C) {
        column->update_crc32c_batch(result, nullptr);
    } else {
        column->update_crcs_with_value(result, type->get_primitive_type(),
                                       cast_set<HashValType>(column->size()));
    }
}

static void apply_shuffle_channel_ids(std::vector<HashValType>& hashes, size_t partition_count,
                                      ShuffleHashMethod method) {
    for (auto& h : hashes) {
        if (method == ShuffleHashMethod::CRC32C) {
            h = crc32c_shuffle_mix(h) % partition_count;
        } else {
            h = h % partition_count;
        }
    }
}

IcebergInsertPartitionFunction::IcebergInsertPartitionFunction(
        HashValType partition_count, ShuffleHashMethod hash_method,
        std::vector<TExpr> partition_exprs, std::vector<TIcebergPartitionField> partition_fields)
        : _partition_count(partition_count),
          _hash_method(hash_method),
          _partition_exprs(std::move(partition_exprs)),
          _partition_fields_spec(std::move(partition_fields)) {}

Status IcebergInsertPartitionFunction::init(const std::vector<TExpr>& texprs) {
    const auto& exprs = _partition_exprs.empty() ? texprs : _partition_exprs;
    if (!exprs.empty()) {
        RETURN_IF_ERROR(VExpr::create_expr_trees(exprs, _partition_expr_ctxs));
    }
    if (!_partition_fields_spec.empty()) {
        _partition_fields.reserve(_partition_fields_spec.size());
        for (const auto& field : _partition_fields_spec) {
            VExprContextSPtr ctx;
            RETURN_IF_ERROR(VExpr::create_expr_tree(field.source_expr, ctx));
            InsertPartitionField insert_field;
            insert_field.transform = field.transform;
            insert_field.expr_ctx = std::move(ctx);
            insert_field.source_id = field.__isset.source_id ? field.source_id : 0;
            insert_field.name = field.__isset.name ? field.name : "";
            _partition_fields.emplace_back(std::move(insert_field));
        }
    }
    return Status::OK();
}

Status IcebergInsertPartitionFunction::prepare(RuntimeState* state, const RowDescriptor& row_desc) {
    RETURN_IF_ERROR(VExpr::prepare(_partition_expr_ctxs, state, row_desc));
    if (!_partition_fields.empty()) {
        VExprContextSPtrs field_ctxs;
        field_ctxs.reserve(_partition_fields.size());
        for (const auto& field : _partition_fields) {
            field_ctxs.emplace_back(field.expr_ctx);
        }
        RETURN_IF_ERROR(VExpr::prepare(field_ctxs, state, row_desc));
    }
    return Status::OK();
}

Status IcebergInsertPartitionFunction::open(RuntimeState* state) {
    RETURN_IF_ERROR(VExpr::open(_partition_expr_ctxs, state));
    if (!_partition_fields.empty()) {
        VExprContextSPtrs field_ctxs;
        field_ctxs.reserve(_partition_fields.size());
        for (const auto& field : _partition_fields) {
            field_ctxs.emplace_back(field.expr_ctx);
        }
        RETURN_IF_ERROR(VExpr::open(field_ctxs, state));
        for (auto& field : _partition_fields) {
            try {
                doris::iceberg::PartitionField partition_field(field.source_id, 0, field.name,
                                                               field.transform);
                field.transformer = PartitionColumnTransforms::create(
                        partition_field, field.expr_ctx->root()->data_type());
            } catch (const doris::Exception& e) {
                LOG(WARNING) << "Merge partitioning fallback to RR: " << e.what();
                _fallback_to_random = true;
                _partition_fields.clear();
                break;
            }
        }
    }
    return Status::OK();
}

Status IcebergInsertPartitionFunction::get_partitions(RuntimeState* /*state*/, Block* block,
                                                      size_t partition_count,
                                                      std::vector<HashValType>& partitions) const {
    if (_fallback_to_random) {
        return Status::InternalError("Merge partitioning fallback to random");
    }
    if (partition_count == 0) {
        return Status::InternalError("Partition count is zero");
    }
    if (!_partition_fields.empty()) {
        RETURN_IF_ERROR(_compute_hashes_with_transform(block, partitions));
    } else {
        RETURN_IF_ERROR(_compute_hashes_with_exprs(block, partitions));
    }
    apply_shuffle_channel_ids(partitions, partition_count, _hash_method);
    return Status::OK();
}

Status IcebergInsertPartitionFunction::clone(RuntimeState* state,
                                             std::unique_ptr<PartitionFunction>& function) const {
    auto* new_function = new IcebergInsertPartitionFunction(
            _partition_count, _hash_method, _partition_exprs, _partition_fields_spec);
    function.reset(new_function);
    RETURN_IF_ERROR(
            _clone_expr_ctxs(state, _partition_expr_ctxs, new_function->_partition_expr_ctxs));
    if (!_partition_fields.empty()) {
        VExprContextSPtrs src_field_ctxs;
        src_field_ctxs.reserve(_partition_fields.size());
        for (const auto& field : _partition_fields) {
            src_field_ctxs.emplace_back(field.expr_ctx);
        }
        VExprContextSPtrs dst_field_ctxs;
        RETURN_IF_ERROR(_clone_expr_ctxs(state, src_field_ctxs, dst_field_ctxs));
        new_function->_partition_fields.reserve(dst_field_ctxs.size());
        for (size_t i = 0; i < dst_field_ctxs.size(); ++i) {
            InsertPartitionField field;
            field.transform = _partition_fields[i].transform;
            field.expr_ctx = dst_field_ctxs[i];
            field.source_id = _partition_fields[i].source_id;
            field.name = _partition_fields[i].name;
            new_function->_partition_fields.emplace_back(std::move(field));
        }
    }
    new_function->_fallback_to_random = _fallback_to_random;
    return Status::OK();
}

Status IcebergInsertPartitionFunction::_compute_hashes_with_transform(
        Block* block, std::vector<HashValType>& partitions) const {
    const size_t rows = block->rows();
    if (rows == 0) {
        partitions.clear();
        return Status::OK();
    }
    if (_partition_fields.empty()) {
        return Status::InternalError("Merge partitioning insert fields are empty");
    }

    std::vector<int> results(_partition_fields.size());
    for (size_t i = 0; i < _partition_fields.size(); ++i) {
        RETURN_IF_ERROR(_partition_fields[i].expr_ctx->execute(block, &results[i]));
    }

    initialize_shuffle_hashes(partitions, rows, _hash_method);
    auto* __restrict hash_values = partitions.data();
    for (size_t i = 0; i < _partition_fields.size(); ++i) {
        if (_partition_fields[i].transformer == nullptr) {
            return Status::InternalError("Merge partitioning transform is not initialized");
        }
        ColumnWithTypeAndName transformed =
                _partition_fields[i].transformer->apply(*block, results[i]);
        const auto& [column, is_const] = unpack_if_const(transformed.column);
        if (is_const) {
            // A const column has the same value for all rows in this block,
            // so it contributes an identical hash delta to every row and does
            // not affect relative partition assignment. Actual partition
            // placement is determined by downstream IcebergPartitionWriter.
            continue;
        }
        update_shuffle_hashes(column, transformed.type, hash_values, _hash_method);
    }
    return Status::OK();
}

Status IcebergInsertPartitionFunction::_compute_hashes_with_exprs(
        Block* block, std::vector<HashValType>& partitions) const {
    const size_t rows = block->rows();
    if (rows == 0) {
        partitions.clear();
        return Status::OK();
    }
    if (_partition_expr_ctxs.empty()) {
        return Status::InternalError("Merge partitioning insert exprs are empty");
    }

    std::vector<int> results(_partition_expr_ctxs.size());
    for (size_t i = 0; i < _partition_expr_ctxs.size(); ++i) {
        RETURN_IF_ERROR(_partition_expr_ctxs[i]->execute(block, &results[i]));
    }

    initialize_shuffle_hashes(partitions, rows, _hash_method);
    auto* __restrict hash_values = partitions.data();
    for (size_t i = 0; i < results.size(); ++i) {
        const auto& col_info = block->get_by_position(results[i]);
        const auto& [column, is_const] = unpack_if_const(col_info.column);
        if (is_const) {
            // Same value for all rows — no effect on inter-row partitioning.
            continue;
        }
        update_shuffle_hashes(column, col_info.type, hash_values, _hash_method);
    }
    return Status::OK();
}

Status IcebergInsertPartitionFunction::_clone_expr_ctxs(RuntimeState* state,
                                                        const VExprContextSPtrs& src,
                                                        VExprContextSPtrs& dst) const {
    dst.resize(src.size());
    for (size_t i = 0; i < src.size(); ++i) {
        RETURN_IF_ERROR(src[i]->clone(state, dst[i]));
    }
    return Status::OK();
}

IcebergDeletePartitionFunction::IcebergDeletePartitionFunction(HashValType partition_count,
                                                               ShuffleHashMethod hash_method,
                                                               std::vector<TExpr> delete_exprs)
        : _partition_count(partition_count),
          _hash_method(hash_method),
          _delete_exprs(std::move(delete_exprs)) {}

Status IcebergDeletePartitionFunction::init(const std::vector<TExpr>& texprs) {
    const auto& exprs = _delete_exprs.empty() ? texprs : _delete_exprs;
    if (!exprs.empty()) {
        RETURN_IF_ERROR(VExpr::create_expr_trees(exprs, _delete_partition_expr_ctxs));
    }
    return Status::OK();
}

Status IcebergDeletePartitionFunction::prepare(RuntimeState* state, const RowDescriptor& row_desc) {
    return VExpr::prepare(_delete_partition_expr_ctxs, state, row_desc);
}

Status IcebergDeletePartitionFunction::open(RuntimeState* state) {
    return VExpr::open(_delete_partition_expr_ctxs, state);
}

Status IcebergDeletePartitionFunction::get_partitions(RuntimeState* /*state*/, Block* block,
                                                      size_t partition_count,
                                                      std::vector<HashValType>& partitions) const {
    if (partition_count == 0) {
        return Status::InternalError("Partition count is zero");
    }
    RETURN_IF_ERROR(_compute_hashes(block, partitions));
    apply_shuffle_channel_ids(partitions, partition_count, _hash_method);
    return Status::OK();
}

Status IcebergDeletePartitionFunction::clone(RuntimeState* state,
                                             std::unique_ptr<PartitionFunction>& function) const {
    auto* new_function =
            new IcebergDeletePartitionFunction(_partition_count, _hash_method, _delete_exprs);
    function.reset(new_function);
    return _clone_expr_ctxs(state, _delete_partition_expr_ctxs,
                            new_function->_delete_partition_expr_ctxs);
}

Status IcebergDeletePartitionFunction::_compute_hashes(Block* block,
                                                       std::vector<HashValType>& partitions) const {
    const size_t rows = block->rows();
    if (rows == 0) {
        partitions.clear();
        return Status::OK();
    }
    if (_delete_partition_expr_ctxs.empty()) {
        return Status::InternalError("Merge partitioning delete exprs are empty");
    }

    std::vector<int> results(_delete_partition_expr_ctxs.size());
    for (size_t i = 0; i < _delete_partition_expr_ctxs.size(); ++i) {
        RETURN_IF_ERROR(_delete_partition_expr_ctxs[i]->execute(block, &results[i]));
    }

    initialize_shuffle_hashes(partitions, rows, _hash_method);
    auto* __restrict hash_values = partitions.data();
    for (size_t i = 0; i < results.size(); ++i) {
        const auto& col_info = block->get_by_position(results[i]);
        const auto& [column, is_const] = unpack_if_const(col_info.column);
        if (is_const) {
            // Same value for all rows — no effect on inter-row partitioning.
            continue;
        }
        ColumnPtr hash_col = column;
        DataTypePtr hash_type = col_info.type;
        RETURN_IF_ERROR(_get_delete_hash_column(col_info, &hash_col, &hash_type));
        update_shuffle_hashes(hash_col, hash_type, hash_values, _hash_method);
    }
    return Status::OK();
}

Status IcebergDeletePartitionFunction::_get_delete_hash_column(const ColumnWithTypeAndName& column,
                                                               ColumnPtr* out_column,
                                                               DataTypePtr* out_type) const {
    ColumnPtr hash_col = column.column;
    DataTypePtr hash_type = column.type;
    if (auto* nullable_col = check_and_get_column<ColumnNullable>(hash_col.get())) {
        hash_col = nullable_col->get_nested_column_ptr();
        hash_type = remove_nullable(hash_type);
    }
    const auto* struct_col = check_and_get_column<ColumnStruct>(hash_col.get());
    const auto* struct_type = check_and_get_data_type<DataTypeStruct>(hash_type.get());
    if (!struct_col || !struct_type) {
        *out_column = column.column;
        *out_type = column.type;
        return Status::OK();
    }

    int file_path_idx = _find_file_path_index(*struct_type);
    if (file_path_idx < 0 || file_path_idx >= struct_col->tuple_size()) {
        return Status::InternalError("Row id struct missing file_path field");
    }
    *out_column = struct_col->get_column_ptr(file_path_idx);
    *out_type = struct_type->get_element(file_path_idx);
    return Status::OK();
}

int IcebergDeletePartitionFunction::_find_file_path_index(const DataTypeStruct& struct_type) const {
    auto normalize = [](const std::string& name) { return doris::to_lower(name); };
    auto match_any = [](const std::string& name, std::initializer_list<const char*> candidates) {
        for (const char* candidate : candidates) {
            if (name == candidate) {
                return true;
            }
        }
        return false;
    };

    int file_path_idx = -1;
    const auto& field_names = struct_type.get_element_names();
    for (size_t i = 0; i < field_names.size(); ++i) {
        std::string name = normalize(field_names[i]);
        if (file_path_idx < 0 && match_any(name, {"file_path", "data_file_path", "path"})) {
            file_path_idx = static_cast<int>(i);
            break;
        }
    }

    if (file_path_idx < 0 && !struct_type.get_elements().empty()) {
        file_path_idx = 0;
    }
    return file_path_idx;
}

Status IcebergDeletePartitionFunction::_clone_expr_ctxs(RuntimeState* state,
                                                        const VExprContextSPtrs& src,
                                                        VExprContextSPtrs& dst) const {
    dst.resize(src.size());
    for (size_t i = 0; i < src.size(); ++i) {
        RETURN_IF_ERROR(src[i]->clone(state, dst[i]));
    }
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
