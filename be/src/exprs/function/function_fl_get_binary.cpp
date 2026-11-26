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

#include <cstring>

#include "core/column/column_const.h"
#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_varbinary.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "runtime/lance_blob_reader.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"

namespace doris {

class FunctionFlGetBinary : public IFunction {
public:
    static constexpr auto name = "fl_get_binary";
    static FunctionPtr create() { return std::make_shared<FunctionFlGetBinary>(); }
    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }
    bool use_default_implementation_for_constants() const override { return false; }
    bool use_default_implementation_for_nulls() const override { return false; }
    bool is_blockable() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes&) const override {
        return make_nullable(std::make_shared<DataTypeVarbinary>());
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const auto& argument = block.get_by_position(arguments[0]).column;
        if (argument->only_null()) {
            block.replace_by_position(result,
                                      block.get_by_position(result).type->create_column_const(
                                              input_rows_count, Field()));
            return Status::OK();
        }
        const auto expanded = argument->convert_to_full_column_if_const();
        const IColumn* data = expanded.get();
        const NullMap* parent_nulls = nullptr;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(data)) {
            parent_nulls = &nullable->get_null_map_data();
            data = &nullable->get_nested_column();
        }
        const auto& file = assert_cast<const ColumnFile&>(*data);
        const auto& inline_column = assert_cast<const ColumnNullable&>(
                file.get_column(DataTypeFile::INLINE_FIELD_INDEX));
        auto values = ColumnVarbinary::create(input_rows_count);
        auto nulls = ColumnUInt8::create(input_rows_count, 0);
        std::vector<LanceBlobReader::BlobRequest> requests;
        for (size_t row = 0; row < input_rows_count; ++row) {
            if (parent_nulls && (*parent_nulls)[row]) {
                nulls->get_data()[row] = 1;
                values->get_data()[row] = StringView();
            } else if (!inline_column.is_null_at(row)) {
                const auto bytes = inline_column.get_nested_column().get_data_at(row);
                char* owned = bytes.size == 0 ? nullptr : values->alloc(bytes.size);
                if (bytes.size != 0) {
                    std::memcpy(owned, bytes.data, bytes.size);
                }
                values->get_data()[row] = StringView(owned, cast_set<uint32_t>(bytes.size));
            } else {
                const auto source_id = file.blob_source_id_at(row);
                if (source_id == 0) {
                    return Status::InvalidArgument(
                            "FL_GET_BINARY cannot read a descriptor FILE without a bound read "
                            "source");
                }
                requests.push_back({source_id, file.blob_row_id_at(row), row});
            }
        }
        if (!requests.empty()) {
            if (context == nullptr || context->state() == nullptr ||
                context->state()->get_query_ctx() == nullptr) {
                return Status::InvalidArgument("External FILE content requires a query context");
            }
            auto* reader = context->state()->get_query_ctx()->get_lance_blob_reader();
            if (reader == nullptr) {
                return Status::InvalidArgument(
                        "External FILE content requires Lance BLOB resources from FE");
            }
            RETURN_IF_ERROR(reader->read_blob(context->state(), requests, values.get()));
        }
        block.replace_by_position(result,
                                  ColumnNullable::create(std::move(values), std::move(nulls)));
        return Status::OK();
    }
};

void register_function_fl_get_binary(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionFlGetBinary>();
}

} // namespace doris
