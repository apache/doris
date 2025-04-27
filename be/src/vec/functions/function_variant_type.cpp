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

#include "vec/columns/column_object.h"
#include "vec/common/schema_util.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

// get data type of variant column
class FunctionVariantType : public IFunction {
public:
    static constexpr auto name = "variant_type";
    static FunctionPtr create() { return std::make_shared<FunctionVariantType>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 1; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return make_nullable(std::make_shared<DataTypeString>());
    }

    std::map<std::string, std::string> get_type_info(const ColumnObject& column, size_t row) const {
        std::map<std::string, std::string> result;
        Field field = column[row];
        const auto& variant_map = field.get<const VariantMap&>();
        for (const auto& [key, value] : variant_map) {
            if (key.empty() && value.get_type() == Field::Types::JSONB &&
                value.get<const JsonbField&>().get_size() == 0) {
                // ignore empty jsonb root, it's tricky here
                continue;
            }
            FieldInfo info;
            schema_util::get_field_info(value, &info);
            result[key.get_path()] = getTypeName(info.scalar_type_id);
        }
        return result;
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        const auto& arg_column =
                assert_cast<const ColumnObject&>(*block.get_by_position(arguments[0]).column);
        auto result_column = ColumnString::create();
        auto arg_real_type = arg_column.get_root_type();

        for (size_t i = 0; i < input_rows_count; ++i) {
            const Field& variant_map = arg_column[i];
            auto type_info = get_type_info(arg_column, i);

            // Use ColumnString as buffer for JSON serialization
            VectorBufferWriter writer(*result_column.get());

            // Write JSON object
            writeChar('{', writer);

            bool first = true;
            for (const auto& [key, value] : type_info) {
                if (!first) {
                    writeChar(',', writer);
                }
                first = false;

                // Write key
                writeJSONString(key, writer);
                writeCString(":", writer);

                // Write value
                writeJSONString(value, writer);
            }

            writeChar('}', writer);
            writer.commit();
        }

        block.replace_by_position(result, std::move(result_column));
        return Status::OK();
    }
};

void register_function_variant_type(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionVariantType>();
}

} // namespace doris::vectorized
