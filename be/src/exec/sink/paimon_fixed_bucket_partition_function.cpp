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

#include "exec/sink/paimon_fixed_bucket_partition_function.h"

#include <cstring>
#include <limits>
#include <string_view>

#include "common/status.h"
#include "core/data_type/data_type_nullable.h"
#include "exec/sink/paimon_native_row_hash.h"

namespace doris {
#include "common/compile_check_begin.h"

namespace {

bool is_supported_type(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
    case TYPE_BINARY:
    case TYPE_VARBINARY:
        return true;
    default:
        return false;
    }
}

template <typename T>
bool read_fixed_value(const IColumn& column, size_t row, T* value) {
    StringRef data = column.get_data_at(row);
    if (data.size != sizeof(T)) {
        return false;
    }
    std::memcpy(value, data.data, sizeof(T));
    return true;
}

Status encode_field(paimon_native::BinaryRowEncoder* encoder, size_t target_position,
                    const ColumnWithTypeAndName& field, size_t row) {
    const IColumn& column = *field.column;
    if (column.is_null_at(row)) {
        if (!encoder->set_null(target_position)) {
            return Status::InternalError("Failed to encode null Paimon routing field {}",
                                         target_position);
        }
        return Status::OK();
    }

    bool encoded = false;
    switch (remove_nullable(field.type)->get_primitive_type()) {
    case TYPE_BOOLEAN: {
        uint8_t value = 0;
        encoded = read_fixed_value(column, row, &value) &&
                  encoder->write_boolean(target_position, value != 0);
        break;
    }
    case TYPE_TINYINT: {
        int8_t value = 0;
        encoded = read_fixed_value(column, row, &value) &&
                  encoder->write_tinyint(target_position, value);
        break;
    }
    case TYPE_SMALLINT: {
        int16_t value = 0;
        encoded = read_fixed_value(column, row, &value) &&
                  encoder->write_smallint(target_position, value);
        break;
    }
    case TYPE_INT: {
        int32_t value = 0;
        encoded =
                read_fixed_value(column, row, &value) && encoder->write_int(target_position, value);
        break;
    }
    case TYPE_BIGINT: {
        int64_t value = 0;
        encoded = read_fixed_value(column, row, &value) &&
                  encoder->write_bigint(target_position, value);
        break;
    }
    case TYPE_FLOAT: {
        float value = 0;
        encoded = read_fixed_value(column, row, &value) &&
                  encoder->write_float(target_position, value);
        break;
    }
    case TYPE_DOUBLE: {
        double value = 0;
        encoded = read_fixed_value(column, row, &value) &&
                  encoder->write_double(target_position, value);
        break;
    }
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        StringRef value = column.get_data_at(row);
        encoded = encoder->write_string(target_position, std::string_view(value.data, value.size));
        break;
    }
    case TYPE_BINARY:
    case TYPE_VARBINARY: {
        StringRef value = column.get_data_at(row);
        encoded = encoder->write_binary(target_position, std::string_view(value.data, value.size));
        break;
    }
    default:
        return Status::InvalidArgument("Unsupported Doris type {} for Paimon native routing",
                                       field.type->get_name());
    }
    if (!encoded) {
        return Status::InvalidArgument("Doris column {} cannot be encoded for Paimon routing",
                                       field.name);
    }
    return Status::OK();
}

Status encode_fields(paimon_native::BinaryRowEncoder* encoder,
                     const std::vector<int32_t>& field_indexes,
                     const std::vector<ColumnWithTypeAndName>& fields, size_t row) {
    encoder->reset();
    for (size_t position = 0; position < field_indexes.size(); ++position) {
        RETURN_IF_ERROR(encode_field(encoder, position, fields[field_indexes[position]], row));
    }
    return Status::OK();
}

} // namespace

PaimonRowHashPartitionFunction::PaimonRowHashPartitionFunction(HashValType partition_count)
        : _partition_count(partition_count) {}

Status PaimonRowHashPartitionFunction::init(const std::vector<TExpr>& texprs) {
    if (_partition_count == 0) {
        return Status::InvalidArgument("Paimon writer count must be positive");
    }
    RETURN_IF_ERROR(VExpr::create_expr_trees(texprs, _field_expr_ctxs));
    for (const auto& context : _field_expr_ctxs) {
        PrimitiveType type = remove_nullable(context->root()->data_type())->get_primitive_type();
        if (!is_supported_type(type)) {
            return Status::InvalidArgument("Unsupported Paimon native routing type {}",
                                           context->root()->data_type()->get_name());
        }
    }
    return Status::OK();
}

Status PaimonRowHashPartitionFunction::_validate_field_indexes(const std::vector<int32_t>& indexes,
                                                               bool require_non_empty) const {
    if (require_non_empty && indexes.empty()) {
        return Status::InvalidArgument("Paimon routing fields are missing");
    }
    for (int32_t index : indexes) {
        if (index < 0 || index >= _field_expr_ctxs.size()) {
            return Status::InvalidArgument("Invalid Paimon routing field index {}", index);
        }
    }
    return Status::OK();
}

Status PaimonRowHashPartitionFunction::prepare(RuntimeState* state, const RowDescriptor& row_desc) {
    return VExpr::prepare(_field_expr_ctxs, state, row_desc);
}

Status PaimonRowHashPartitionFunction::open(RuntimeState* state) {
    return VExpr::open(_field_expr_ctxs, state);
}

Status PaimonRowHashPartitionFunction::_evaluate_fields(
        Block* block, std::vector<ColumnWithTypeAndName>& fields) const {
    fields.resize(_field_expr_ctxs.size());
    for (size_t index = 0; index < _field_expr_ctxs.size(); ++index) {
        RETURN_IF_ERROR(_field_expr_ctxs[index]->execute(block, fields[index]));
    }
    return Status::OK();
}

Status PaimonRowHashPartitionFunction::_hash_fields(
        const std::vector<int32_t>& indexes, const std::vector<ColumnWithTypeAndName>& fields,
        std::vector<int32_t>& hashes) const {
    paimon_native::BinaryRowEncoder encoder(indexes.size());
    hashes.resize(fields.empty() ? 0 : fields.front().column->size());
    for (size_t row = 0; row < hashes.size(); ++row) {
        RETURN_IF_ERROR(encode_fields(&encoder, indexes, fields, row));
        hashes[row] = encoder.hash();
    }
    return Status::OK();
}

Status PaimonRowHashPartitionFunction::_clone_expr_ctxs(RuntimeState* state,
                                                        VExprContextSPtrs& destination) const {
    destination.resize(_field_expr_ctxs.size());
    for (size_t index = 0; index < _field_expr_ctxs.size(); ++index) {
        RETURN_IF_ERROR(_field_expr_ctxs[index]->clone(state, destination[index]));
    }
    return Status::OK();
}

PaimonFixedBucketPartitionFunction::PaimonFixedBucketPartitionFunction(
        HashValType partition_count, TPaimonFixedBucketInfo fixed_bucket_info)
        : PaimonRowHashPartitionFunction(partition_count),
          _fixed_bucket_info(std::move(fixed_bucket_info)) {}

Status PaimonFixedBucketPartitionFunction::init(const std::vector<TExpr>& texprs) {
    RETURN_IF_ERROR(PaimonRowHashPartitionFunction::init(texprs));
    if (_fixed_bucket_info.num_buckets <= 0) {
        return Status::InvalidArgument("Paimon fixed-bucket count must be positive");
    }
    RETURN_IF_ERROR(_validate_field_indexes(_fixed_bucket_info.partition_field_indexes, false));
    return _validate_field_indexes(_fixed_bucket_info.bucket_field_indexes, true);
}

Status PaimonFixedBucketPartitionFunction::get_partitions(
        RuntimeState* /*state*/, Block* block, size_t partition_count,
        std::vector<HashValType>& partitions) const {
    if (partition_count != _partition_count) {
        return Status::InvalidArgument("Paimon writer count {} does not match planned count {}",
                                       partition_count, _partition_count);
    }
    const size_t rows = block->rows();
    if (rows == 0) {
        partitions.clear();
        return Status::OK();
    }

    std::vector<ColumnWithTypeAndName> fields;
    RETURN_IF_ERROR(_evaluate_fields(block, fields));
    std::vector<int32_t> partition_hashes;
    std::vector<int32_t> bucket_hashes;
    RETURN_IF_ERROR(
            _hash_fields(_fixed_bucket_info.partition_field_indexes, fields, partition_hashes));
    RETURN_IF_ERROR(_hash_fields(_fixed_bucket_info.bucket_field_indexes, fields, bucket_hashes));
    partitions.resize(rows);
    for (size_t row = 0; row < rows; ++row) {
        auto bucket =
                paimon_native::default_bucket(bucket_hashes[row], _fixed_bucket_info.num_buckets);
        if (!bucket.has_value()) {
            return Status::InternalError("Failed to compute Paimon fixed bucket");
        }
        auto channel = paimon_native::fixed_bucket_channel(partition_hashes[row], *bucket,
                                                           _partition_count);
        if (!channel.has_value()) {
            return Status::InternalError("Failed to compute Paimon fixed-bucket writer");
        }
        partitions[row] = *channel;
    }
    return Status::OK();
}

Status PaimonFixedBucketPartitionFunction::clone(
        RuntimeState* state, std::unique_ptr<PartitionFunction>& function) const {
    auto cloned = std::make_unique<PaimonFixedBucketPartitionFunction>(_partition_count,
                                                                       _fixed_bucket_info);
    RETURN_IF_ERROR(_clone_expr_ctxs(state, cloned->_field_expr_ctxs));
    function = std::move(cloned);
    return Status::OK();
}

#include "common/compile_check_end.h"
} // namespace doris
