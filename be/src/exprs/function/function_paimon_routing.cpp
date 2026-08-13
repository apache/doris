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

#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>

#include "core/block/block.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exec/sink/paimon_native_row_hash.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
namespace {

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

enum class PaimonRoutingResult { BINARY_ROW_HASH, FIXED_BUCKET };

template <PaimonRoutingResult result_kind>
class FunctionPaimonRouting final : public IFunction {
public:
    static constexpr auto name = result_kind == PaimonRoutingResult::BINARY_ROW_HASH
                                         ? "__paimon_binary_row_hash_v1"
                                         : "__paimon_fixed_bucket_v1";

    static FunctionPtr create() { return std::make_shared<FunctionPaimonRouting<result_kind>>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    size_t get_number_of_arguments() const override { return 0; }

    bool use_default_implementation_for_nulls() const override { return false; }

    // Fixed-bucket routing has one always-constant argument (the bucket count) followed by
    // ordinary row arguments. The generic constant wrapper keeps the former as an N-row
    // ColumnConst while unwrapping constant row arguments to one-row nested columns, which makes
    // the temporary block cardinalities inconsistent. This implementation already reads
    // ColumnConst correctly, so execute it directly for all-constant rows as well.
    bool use_default_implementation_for_constants() const override { return false; }

    ColumnNumbers get_arguments_that_are_always_constant() const override {
        if constexpr (result_kind == PaimonRoutingResult::FIXED_BUCKET) {
            return {0};
        }
        return {};
    }

    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return std::make_shared<DataTypeInt32>();
    }

    Status execute_impl(FunctionContext*, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        constexpr size_t first_field = result_kind == PaimonRoutingResult::FIXED_BUCKET ? 1 : 0;
        if (arguments.size() <= first_field) {
            return Status::InvalidArgument("Paimon routing function {} has no fields", name);
        }

        int32_t num_buckets = 0;
        if constexpr (result_kind == PaimonRoutingResult::FIXED_BUCKET) {
            int64_t value = block.get_by_position(arguments[0]).column->get_int(0);
            if (value <= 0 || value > std::numeric_limits<int32_t>::max()) {
                return Status::InvalidArgument("Invalid Paimon bucket count {}", value);
            }
            num_buckets = static_cast<int32_t>(value);
        }

        auto output = ColumnInt32::create(input_rows_count);
        auto& output_data = output->get_data();
        paimon_native::BinaryRowEncoder encoder(arguments.size() - first_field);
        for (size_t row = 0; row < input_rows_count; ++row) {
            encoder.reset();
            for (size_t index = first_field; index < arguments.size(); ++index) {
                RETURN_IF_ERROR(encode_field(&encoder, index - first_field,
                                             block.get_by_position(arguments[index]), row));
            }
            int32_t hash = encoder.hash();
            if constexpr (result_kind == PaimonRoutingResult::FIXED_BUCKET) {
                std::optional<uint32_t> bucket = paimon_native::default_bucket(hash, num_buckets);
                if (!bucket.has_value()) {
                    return Status::InternalError("Failed to compute Paimon fixed bucket");
                }
                output_data[row] = static_cast<int32_t>(*bucket);
            } else {
                output_data[row] = hash;
            }
        }
        block.replace_by_position(result, std::move(output));
        return Status::OK();
    }
};

} // namespace

void register_function_paimon_routing(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionPaimonRouting<PaimonRoutingResult::BINARY_ROW_HASH>>();
    factory.register_function<FunctionPaimonRouting<PaimonRoutingResult::FIXED_BUCKET>>();
}

} // namespace doris
