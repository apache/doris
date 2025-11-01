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

#include <glog/logging.h>
#include <stddef.h>

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <memory>
#include <string>
#include <utility>

#include "common/status.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column_string.h"
#include "vec/common/string_utils/string_utils.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function.h"
#include "vec/functions/function_totype.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {
class Uuid : public IFunction {
public:
    static constexpr auto name = "uuid";
    static constexpr size_t uuid_length = 36; //uuid fixed length

    static FunctionPtr create() { return std::make_shared<Uuid>(); }

    String get_name() const override { return name; }

    bool use_default_implementation_for_constants() const override { return false; }

    size_t get_number_of_arguments() const override { return 0; }

    bool is_variadic() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeString>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        auto col_res = ColumnString::create();
        col_res->get_offsets().reserve(input_rows_count);
        col_res->get_chars().reserve(input_rows_count * uuid_length);

        boost::uuids::random_generator generator;
        for (int i = 0; i < input_rows_count; i++) {
            std::string uuid = boost::uuids::to_string(generator());
            DCHECK(uuid.length() == uuid_length);
            col_res->insert_data_without_reserve(uuid.c_str(), uuid.length());
        }

        block.replace_by_position(result, std::move(col_res));
        return Status::OK();
    }
};

struct NameIsUuid {
    static constexpr auto name = "is_uuid";
};

struct IsUuidImpl {
    using ReturnType = DataTypeBool;
    using ReturnColumnType = ColumnUInt8;
    static constexpr auto PrimitiveTypeImpl = PrimitiveType::TYPE_STRING;
    static constexpr size_t uuid_without_dash_length = 32;
    static constexpr size_t uuid_with_dash_length = 36;
    static constexpr size_t uuid_with_braces_and_dash_length = 38;
    static constexpr size_t dash_positions[4] = {8, 13, 18, 23};

    static bool is_uuid_with_dash(const char* src, const char* end) {
        size_t str_size = end - src;
        for (int i = 0; i < str_size; ++i) {
            if (!is_hex_ascii(src[i])) {
                if (i == dash_positions[0] || i == dash_positions[1] || i == dash_positions[2] ||
                    i == dash_positions[3]) {
                    if (src[i] != '-') {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    static Status vector(const ColumnString::Chars& data, const ColumnString::Offsets& offsets,
                         PaddedPODArray<UInt8>& res) {
        size_t rows_count = offsets.size();
        res.resize(rows_count);
        for (size_t i = 0; i < rows_count; ++i) {
            const char* source = reinterpret_cast<const char*>(&data[offsets[i - 1]]);
            int str_size = offsets[i] - offsets[i - 1];
            if (str_size == uuid_without_dash_length) {
                bool is_valid = true;
                for (int j = 0; j < str_size; ++j) {
                    if (!is_hex_ascii(source[j])) {
                        is_valid = false;
                        break;
                    }
                }
                res[i] = is_valid;
            } else if (str_size == uuid_with_dash_length) {
                res[i] = is_uuid_with_dash(source, source + str_size);
            } else if (str_size == uuid_with_braces_and_dash_length) {
                if (source[0] != '{' || source[str_size - 1] != '}') {
                    res[i] = 0;
                    continue;
                }
                res[i] = is_uuid_with_dash(source + 1, source + str_size - 1);
            } else {
                res[i] = 0;
            }
        }
        return Status::OK();
    }
};

using FunctionIsUuid = FunctionUnaryToType<IsUuidImpl, NameIsUuid>;

void register_function_uuid(SimpleFunctionFactory& factory) {
    factory.register_function<Uuid>();
    factory.register_function<FunctionIsUuid>();
}

} // namespace doris::vectorized
