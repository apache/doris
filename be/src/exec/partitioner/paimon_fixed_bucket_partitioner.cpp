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

#include "exec/partitioner/paimon_fixed_bucket_partitioner.h"

#include <cstdlib>
#include <cstring>
#include <string>
#include <utility>

#include "common/cast_set.h"
#include "core/assert_cast.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "exec/exchange/local_exchange_sink_operator.h"
#include "runtime/thread_context.h"
#include "util/hash_util.hpp"

namespace doris {
namespace {

constexpr uint32_t PAIMON_MURMUR_HASH_SEED = 42;
constexpr size_t PAIMON_BINARY_ROW_HEADER_SIZE_IN_BITS = 8;

size_t paimon_binary_row_null_bits_size_in_bytes(size_t arity) {
    return ((arity + 63 + PAIMON_BINARY_ROW_HEADER_SIZE_IN_BITS) / 64) * 8;
}

void paimon_binary_row_set_null_bit(std::string& row, size_t pos) {
    const size_t bit_index = pos + PAIMON_BINARY_ROW_HEADER_SIZE_IN_BITS;
    row[bit_index / 8] |= static_cast<char>(1U << (bit_index % 8));
}

template <typename T>
void paimon_binary_row_write_primitive(std::string& row, size_t offset, T value) {
    std::memcpy(row.data() + offset, &value, sizeof(T));
}

int32_t paimon_bucket_from_hash(int32_t hash, int32_t bucket_num) {
    return std::abs(hash % bucket_num);
}

int32_t paimon_floor_mod(int64_t value, int32_t bucket_num) {
    return static_cast<int32_t>(((value % bucket_num) + bucket_num) % bucket_num);
}

ColumnPtr paimon_get_nested_column(ColumnPtr column, const ColumnUInt8::Container** null_map) {
    column = column->convert_to_full_column_if_const();
    if (const auto* nullable = check_and_get_column<const ColumnNullable>(*column)) {
        *null_map = &nullable->get_null_map_data();
        return nullable->get_nested_column_ptr();
    }
    *null_map = nullptr;
    return column;
}

template <PrimitiveType Type>
bool paimon_try_write_fixed_width_key(const IColumn& column, size_t row_id, std::string& binary_row,
                                      size_t field_offset) {
    const auto* vector_column = check_and_get_column<ColumnVector<Type>>(&column);
    if (vector_column == nullptr) {
        return false;
    }
    using ValueType = typename PrimitiveTypeTraits<Type>::CppType;
    paimon_binary_row_write_primitive(binary_row, field_offset,
                                      static_cast<ValueType>(vector_column->get_data()[row_id]));
    return true;
}

Status paimon_write_fixed_width_key(const IColumn& column, size_t row_id, std::string& binary_row,
                                    size_t field_offset) {
    if (paimon_try_write_fixed_width_key<TYPE_TINYINT>(column, row_id, binary_row, field_offset) ||
        paimon_try_write_fixed_width_key<TYPE_SMALLINT>(column, row_id, binary_row, field_offset) ||
        paimon_try_write_fixed_width_key<TYPE_INT>(column, row_id, binary_row, field_offset) ||
        paimon_try_write_fixed_width_key<TYPE_BIGINT>(column, row_id, binary_row, field_offset)) {
        return Status::OK();
    }
    return Status::NotSupported("Unsupported Paimon default bucket key column {}",
                                column.get_name());
}

template <PrimitiveType Type>
bool paimon_try_get_int_value(const IColumn& column, size_t row_id, int64_t* value) {
    const auto* vector_column = check_and_get_column<ColumnVector<Type>>(&column);
    if (vector_column == nullptr) {
        return false;
    }
    *value = static_cast<int64_t>(vector_column->get_data()[row_id]);
    return true;
}

Status paimon_get_int_value(const IColumn& column, size_t row_id, int64_t* value) {
    if (paimon_try_get_int_value<TYPE_TINYINT>(column, row_id, value) ||
        paimon_try_get_int_value<TYPE_SMALLINT>(column, row_id, value) ||
        paimon_try_get_int_value<TYPE_INT>(column, row_id, value) ||
        paimon_try_get_int_value<TYPE_BIGINT>(column, row_id, value)) {
        return Status::OK();
    }
    return Status::NotSupported("Unsupported Paimon mod bucket key column {}", column.get_name());
}

Status clone_expr_ctxs(RuntimeState* state, const VExprContextSPtrs& src, VExprContextSPtrs& dst) {
    dst.resize(src.size());
    for (size_t i = 0; i < src.size(); ++i) {
        RETURN_IF_ERROR(src[i]->clone(state, dst[i]));
    }
    return Status::OK();
}

} // namespace

PaimonFixedBucketPartitioner::PaimonFixedBucketPartitioner(int partition_count,
                                                           TPaimonRouteBucketInfo route_bucket_info)
        : Crc32HashPartitioner<ShuffleChannelIds>(partition_count),
          _route_bucket_info(std::move(route_bucket_info)) {}

Status PaimonFixedBucketPartitioner::init(const std::vector<TExpr>& texprs) {
    RETURN_IF_ERROR(Crc32HashPartitioner<ShuffleChannelIds>::init(texprs));
    return VExpr::create_expr_trees(_route_bucket_info.bucket_key_exprs, _bucket_key_expr_ctxs);
}

Status PaimonFixedBucketPartitioner::prepare(RuntimeState* state, const RowDescriptor& row_desc) {
    RETURN_IF_ERROR(Crc32HashPartitioner<ShuffleChannelIds>::prepare(state, row_desc));
    return VExpr::prepare(_bucket_key_expr_ctxs, state, row_desc);
}

Status PaimonFixedBucketPartitioner::open(RuntimeState* state) {
    RETURN_IF_ERROR(Crc32HashPartitioner<ShuffleChannelIds>::open(state));
    return VExpr::open(_bucket_key_expr_ctxs, state);
}

Status PaimonFixedBucketPartitioner::close(RuntimeState* state) {
    return Crc32HashPartitioner<ShuffleChannelIds>::close(state);
}

Status PaimonFixedBucketPartitioner::do_partitioning(RuntimeState* state, Block* block) const {
    const size_t rows = block->rows();
    if (rows == 0) {
        _hash_vals.clear();
        return Status::OK();
    }

    auto column_to_keep = block->columns();
    _initialize_hash_vals(rows);
    auto* __restrict hashes = _hash_vals.data();
    if (!_partition_expr_ctxs.empty()) {
        std::vector<int> result(cast_set<int>(_partition_expr_ctxs.size()));
        RETURN_IF_ERROR(_get_partition_column_result(block, result));
        for (int i = 0; i < result.size(); ++i) {
            const auto& [col, is_const] = unpack_if_const(block->get_by_position(result[i]).column);
            if (is_const) {
                continue;
            }
            _do_hash(col, hashes, i);
        }
    }

    std::vector<int32_t> bucket_ids;
    RETURN_IF_ERROR(_compute_bucket_ids(block, rows, bucket_ids));
    auto bucket_column = ColumnInt32::create(rows);
    auto& bucket_data = bucket_column->get_data();
    for (size_t row = 0; row < rows; ++row) {
        bucket_data[row] = bucket_ids[row];
    }
    bucket_column->update_crcs_with_value(hashes, TYPE_INT, cast_set<uint32_t>(rows), 0, nullptr);

    for (size_t i = 0; i < rows; i++) {
        hashes[i] = ShuffleChannelIds()(hashes[i], _partition_count);
    }

    Block::erase_useless_column(block, column_to_keep);
    return Status::OK();
}

Status PaimonFixedBucketPartitioner::clone(RuntimeState* state,
                                           std::unique_ptr<PartitionerBase>& partitioner) {
    auto* new_partitioner = new PaimonFixedBucketPartitioner(_partition_count, _route_bucket_info);
    partitioner.reset(new_partitioner);
    RETURN_IF_ERROR(_clone_expr_ctxs(state, new_partitioner->_partition_expr_ctxs));
    return clone_expr_ctxs(state, _bucket_key_expr_ctxs, new_partitioner->_bucket_key_expr_ctxs);
}

Status PaimonFixedBucketPartitioner::_compute_bucket_ids(Block* block, size_t rows,
                                                         std::vector<int32_t>& bucket_ids) const {
    if (_route_bucket_info.bucket_function_type == TPaimonBucketFunctionType::MOD) {
        return _compute_mod_bucket_ids(block, rows, bucket_ids);
    }
    if (_route_bucket_info.bucket_function_type == TPaimonBucketFunctionType::DEFAULT) {
        return _compute_default_bucket_ids(block, rows, bucket_ids);
    }
    return Status::NotSupported("Unsupported Paimon bucket function type {}",
                                _route_bucket_info.bucket_function_type);
}

Status PaimonFixedBucketPartitioner::_compute_default_bucket_ids(
        Block* block, size_t rows, std::vector<int32_t>& bucket_ids) const {
    const int32_t bucket_num = _route_bucket_info.bucket_num;
    DCHECK_GT(bucket_num, 0);
    const size_t arity = _bucket_key_expr_ctxs.size();
    const size_t null_bits_size = paimon_binary_row_null_bits_size_in_bytes(arity);
    const size_t fixed_part_size = null_bits_size + 8 * arity;

    std::vector<int> result(cast_set<int>(arity));
    for (size_t i = 0; i < arity; ++i) {
        RETURN_IF_ERROR(_bucket_key_expr_ctxs[i]->execute(block, &result[i]));
    }

    std::vector<ColumnPtr> key_columns(arity);
    std::vector<const ColumnUInt8::Container*> null_maps(arity);
    for (size_t i = 0; i < arity; ++i) {
        key_columns[i] =
                paimon_get_nested_column(block->get_by_position(result[i]).column, &null_maps[i]);
    }

    bucket_ids.resize(rows);
    for (size_t row = 0; row < rows; ++row) {
        std::string binary_row(fixed_part_size, '\0');
        for (size_t field = 0; field < arity; ++field) {
            const size_t field_offset = null_bits_size + 8 * field;
            if (null_maps[field] != nullptr && (*null_maps[field])[row]) {
                paimon_binary_row_set_null_bit(binary_row, field);
                continue;
            }
            RETURN_IF_ERROR(paimon_write_fixed_width_key(*key_columns[field], row, binary_row,
                                                         field_offset));
        }
        const int32_t hash = static_cast<int32_t>(HashUtil::murmur_hash3_32(
                binary_row.data(), binary_row.size(), PAIMON_MURMUR_HASH_SEED));
        bucket_ids[row] = paimon_bucket_from_hash(hash, bucket_num);
    }
    return Status::OK();
}

Status PaimonFixedBucketPartitioner::_compute_mod_bucket_ids(
        Block* block, size_t rows, std::vector<int32_t>& bucket_ids) const {
    if (_bucket_key_expr_ctxs.size() != 1) {
        return Status::InternalError("Paimon mod bucket requires exactly one bucket key");
    }
    int result = 0;
    RETURN_IF_ERROR(_bucket_key_expr_ctxs[0]->execute(block, &result));

    const ColumnUInt8::Container* null_map = nullptr;
    ColumnPtr key_column =
            paimon_get_nested_column(block->get_by_position(result).column, &null_map);
    const int32_t bucket_num = _route_bucket_info.bucket_num;
    DCHECK_GT(bucket_num, 0);

    bucket_ids.resize(rows);
    for (size_t row = 0; row < rows; ++row) {
        int64_t value = 0;
        if (null_map == nullptr || !(*null_map)[row]) {
            RETURN_IF_ERROR(paimon_get_int_value(*key_column, row, &value));
        }
        bucket_ids[row] = paimon_floor_mod(value, bucket_num);
    }
    return Status::OK();
}

} // namespace doris
