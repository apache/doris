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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "olap/hll.h"
#include "util/hash_util.hpp"
#include "util/url_coding.h"
#include "vec/columns/column.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_hll.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_always_not_nullable.h"
#include "vec/functions/function_const.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct HLLCardinality {
    static constexpr auto name = "hll_cardinality";

    using ReturnType = DataTypeNumber<Int64>;

    static void vector(const std::vector<HyperLogLog>& data, MutableColumnPtr& col_res) {
        typename ColumnVector<Int64>::Container& res =
                reinterpret_cast<ColumnVector<Int64>*>(col_res.get())->get_data();

        auto size = res.size();
        for (int i = 0; i < size; ++i) {
            res[i] = data[i].estimate_cardinality();
        }
    }

    static void vector_nullable(const std::vector<HyperLogLog>& data, const NullMap& nullmap,
                                MutableColumnPtr& col_res) {
        typename ColumnVector<Int64>::Container& res =
                reinterpret_cast<ColumnVector<Int64>*>(col_res.get())->get_data();

        auto size = res.size();
        for (int i = 0; i < size; ++i) {
            if (nullmap[i]) {
                res[i] = 0;
            } else {
                res[i] = data[i].estimate_cardinality();
            }
        }
    }
};

template <typename Function>
class FunctionHLL : public IFunction {
public:
    static constexpr auto name = Function::name;

    static FunctionPtr create() { return std::make_shared<FunctionHLL>(); }

    String get_name() const override { return Function::name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<typename Function::ReturnType>();
    }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto column = block.get_by_position(arguments[0]).column;

        MutableColumnPtr column_result = get_return_type_impl({})->create_column();
        column_result->resize(input_rows_count);
        if (const ColumnNullable* col_nullable =
                    check_and_get_column<ColumnNullable>(column.get())) {
            const ColumnHLL* col =
                    check_and_get_column<ColumnHLL>(col_nullable->get_nested_column_ptr().get());
            const ColumnUInt8* col_nullmap = check_and_get_column<ColumnUInt8>(
                    col_nullable->get_null_map_column_ptr().get());

            if (col != nullptr && col_nullmap != nullptr) {
                Function::vector_nullable(col->get_data(), col_nullmap->get_data(), column_result);
                block.replace_by_position(result, std::move(column_result));
                return Status::OK();
            }
        } else if (const ColumnHLL* col = check_and_get_column<ColumnHLL>(column.get())) {
            Function::vector(col->get_data(), column_result);
            block.replace_by_position(result, std::move(column_result));
            return Status::OK();
        } else {
            return Status::RuntimeError("Illegal column {} of argument of function {}",
                                        block.get_by_position(arguments[0]).column->get_name(),
                                        get_name());
        }

        block.replace_by_position(result, std::move(column_result));
        return Status::OK();
    }
};

struct HLLEmptyImpl {
    static constexpr auto name = "hll_empty";
    using ReturnColVec = ColumnHLL;
    static auto get_return_type() { return std::make_shared<DataTypeHLL>(); }
    static HyperLogLog init_value() { return HyperLogLog {}; }
};

class FunctionHllFromBase64 : public IFunction {
public:
    static constexpr auto name = "hll_from_base64";

    String get_name() const override { return name; }

    static FunctionPtr create() { return std::make_shared<FunctionHllFromBase64>(); }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeHLL>());
    }

    size_t get_number_of_arguments() const override { return 1; }

    bool use_default_implementation_for_nulls() const override { return true; }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        auto res_null_map = ColumnUInt8::create(input_rows_count, 0);
        auto res_data_column = ColumnHLL::create();
        auto& null_map = res_null_map->get_data();
        auto& res = res_data_column->get_data();

        auto& argument_column = block.get_by_position(arguments[0]).column;
        const auto& str_column = static_cast<const ColumnString&>(*argument_column);
        const ColumnString::Chars& data = str_column.get_chars();
        const ColumnString::Offsets& offsets = str_column.get_offsets();

        res.reserve(input_rows_count);

        std::string decode_buff;
        int last_decode_buff_len = 0;
        int curr_decode_buff_len = 0;
        for (size_t i = 0; i < input_rows_count; ++i) {
            const char* src_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            int64_t src_size = offsets[i] - offsets[i - 1];

            // Base64 encoding has a characteristic where every 4 characters represent 3 bytes of data.
            // Here, we check if the length of the input string is a multiple of 4 to ensure it's a valid base64 encoded string.
            if (0 != src_size % 4) {
                res.emplace_back();
                null_map[i] = 1;
                continue;
            }

            // Allocate sufficient space for the decoded data.
            // The number 3 here represents the number of bytes in the decoded data for each group of 4 base64 characters.
            // We set the size of the decoding buffer to be 'src_size + 3' to ensure there is enough space to store the decoded data.
            curr_decode_buff_len = src_size + 3;
            if (curr_decode_buff_len > last_decode_buff_len) {
                decode_buff.resize(curr_decode_buff_len);
                last_decode_buff_len = curr_decode_buff_len;
            }
            auto outlen = base64_decode(src_str, src_size, decode_buff.data());
            if (outlen < 0) {
                res.emplace_back();
                null_map[i] = 1;
            } else {
                doris::Slice decoded_slice(decode_buff.data(), outlen);
                doris::HyperLogLog hll;
                if (!hll.deserialize(decoded_slice)) {
                    return Status::RuntimeError(
                            fmt::format("hll_from_base64 decode failed: base64: {}", src_str));
                } else {
                    res.emplace_back(std::move(hll));
                }
            }
        }

        block.get_by_position(result).column =
                ColumnNullable::create(std::move(res_data_column), std::move(res_null_map));
        return Status::OK();
    }
};

struct HLLHash {
    static constexpr auto name = "hll_hash";

    using ReturnType = DataTypeHLL;
    template <typename ColumnType>
    static void vector(const ColumnType* col, MutableColumnPtr& col_res) {
        if constexpr (std::is_same_v<ColumnType, ColumnString>) {
            const ColumnString::Chars& data = col->get_chars();
            const ColumnString::Offsets& offsets = col->get_offsets();
            auto* res_column = reinterpret_cast<ColumnHLL*>(col_res.get());
            auto& res_data = res_column->get_data();
            size_t size = offsets.size();

            for (size_t i = 0; i < size; ++i) {
                const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
                size_t str_size = offsets[i] - offsets[i - 1];
                uint64_t hash_value =
                        HashUtil::murmur_hash64A(raw_str, str_size, HashUtil::MURMUR_SEED);
                res_data[i].update(hash_value);
            }
        }
    }

    template <typename ColumnType>
    static void vector_nullable(const ColumnType* col, const NullMap& nullmap,
                                MutableColumnPtr& col_res) {
        if constexpr (std::is_same_v<ColumnType, ColumnString>) {
            const ColumnString::Chars& data = col->get_chars();
            const ColumnString::Offsets& offsets = col->get_offsets();
            auto* res_column = reinterpret_cast<ColumnHLL*>(col_res.get());
            auto& res_data = res_column->get_data();
            size_t size = offsets.size();

            for (size_t i = 0; i < size; ++i) {
                if (nullmap[i]) {
                    continue;
                } else {
                    const char* raw_str = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
                    size_t str_size = offsets[i] - offsets[i - 1];
                    uint64_t hash_value =
                            HashUtil::murmur_hash64A(raw_str, str_size, HashUtil::MURMUR_SEED);
                    res_data[i].update(hash_value);
                }
            }
        }
    }
};

struct NameHllToBase64 {
    static constexpr auto name = "hll_to_base64";
};

struct HllToBase64 {
    using ReturnType = DataTypeString;
    static constexpr auto TYPE_INDEX = TypeIndex::HLL;
    using Type = DataTypeHLL::FieldType;
    using ReturnColumnType = ColumnString;
    using Chars = ColumnString::Chars;
    using Offsets = ColumnString::Offsets;

    static Status vector(const std::vector<HyperLogLog>& data, Chars& chars, Offsets& offsets) {
        size_t size = data.size();
        offsets.resize(size);
        size_t output_char_size = 0;
        for (size_t i = 0; i < size; ++i) {
            auto& hll_val = const_cast<HyperLogLog&>(data[i]);
            auto ser_size = hll_val.max_serialized_size();
            output_char_size += (int)(4.0 * ceil((double)ser_size / 3.0));
        }
        ColumnString::check_chars_length(output_char_size, size);
        chars.resize(output_char_size);
        auto* chars_data = chars.data();

        size_t cur_ser_size = 0;
        size_t last_ser_size = 0;
        std::string ser_buff;
        size_t encoded_offset = 0;
        for (size_t i = 0; i < size; ++i) {
            auto& hll_val = const_cast<HyperLogLog&>(data[i]);

            cur_ser_size = hll_val.max_serialized_size();
            if (cur_ser_size > last_ser_size) {
                last_ser_size = cur_ser_size;
                ser_buff.resize(cur_ser_size);
            }
            hll_val.serialize(reinterpret_cast<uint8_t*>(ser_buff.data()));
            auto outlen = base64_encode((const unsigned char*)ser_buff.data(), cur_ser_size,
                                        chars_data + encoded_offset);
            DCHECK(outlen > 0);

            encoded_offset += outlen;
            offsets[i] = encoded_offset;
        }
        return Status::OK();
    }
};

using FunctionHLLCardinality = FunctionHLL<HLLCardinality>;
using FunctionHLLEmpty = FunctionConst<HLLEmptyImpl, false>;
using FunctionHLLHash = FunctionAlwaysNotNullable<HLLHash>;
using FunctionHllToBase64 = FunctionUnaryToType<HllToBase64, NameHllToBase64>;

void register_function_hll(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionHLLCardinality>();
    factory.register_function<FunctionHLLEmpty>();
    factory.register_function<FunctionHllFromBase64>();
    factory.register_function<FunctionHLLHash>();
    factory.register_function<FunctionHllToBase64>();
}

} // namespace doris::vectorized
