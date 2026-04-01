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
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "common/cast_set.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_file.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/file_schema_descriptor.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"
#include "util/jsonb_writer.h"

namespace doris {

class FunctionToFile : public IFunction {
public:
    static constexpr auto name = "to_file";

    static FunctionPtr create() { return std::make_shared<FunctionToFile>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 3; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return std::make_shared<DataTypeFile>();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DCHECK_EQ(arguments.size(), 3);

        ColumnPtr uri_holder, endpoint_holder, role_arn_holder;
        const ColumnString* uri_col =
                _unwrap_string_column(block.get_by_position(arguments[0]), uri_holder);
        const ColumnString* endpoint_col =
                _unwrap_string_column(block.get_by_position(arguments[1]), endpoint_holder);
        const ColumnString* role_arn_col =
                _unwrap_string_column(block.get_by_position(arguments[2]), role_arn_holder);

        using S = FileSchemaDescriptor;
        const auto& schema = S::instance();
        auto result_col = ColumnFile::create(schema);
        auto& jsonb_col = assert_cast<ColumnString&>(result_col->get_jsonb_column());
        jsonb_col.reserve(input_rows_count);
        JsonbWriter writer;

        for (size_t row = 0; row < input_rows_count; ++row) {
            std::string uri = uri_col->get_data_at(row).to_string();
            std::string endpoint = endpoint_col->get_data_at(row).to_string();
            std::string role_arn = role_arn_col->get_data_at(row).to_string();
            std::string file_name = S::extract_file_name(uri);
            std::string content_type =
                    S::extension_to_content_type(S::extract_file_extension(file_name));

            writer.reset();
            _write_file_jsonb(writer, schema, uri, file_name, content_type, endpoint, role_arn);
            jsonb_col.insert_data(writer.getOutput()->getBuffer(), writer.getOutput()->getSize());
        }
        block.replace_by_position(result, std::move(result_col));
        return Status::OK();
    }

private:
    static const ColumnString* _unwrap_string_column(const ColumnWithTypeAndName& col_with_type,
                                                     ColumnPtr& holder) {
        holder = col_with_type.column->convert_to_full_column_if_const();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(holder.get())) {
            return &assert_cast<const ColumnString&>(nullable->get_nested_column());
        }
        return &assert_cast<const ColumnString&>(*holder);
    }

    static void _write_file_jsonb(JsonbWriter& writer, const FileSchemaDescriptor& schema,
                                  const std::string& uri, const std::string& file_name,
                                  const std::string& content_type,
                                  const std::string& endpoint, const std::string& role_arn) {
        using S = FileSchemaDescriptor;
        auto write_nullable_str = [&](S::Field field, const std::string& s) {
            S::write_jsonb_key(writer, schema.field_name(field));
            if (s.empty()) {
                writer.writeNull();
            } else {
                S::write_jsonb_string(writer, s);
            }
        };

        writer.writeStartObject();
        S::write_jsonb_key(writer, schema.field_name(S::Field::OBJECT_URI));
        S::write_jsonb_string(writer, uri);
        S::write_jsonb_key(writer, schema.field_name(S::Field::FILE_NAME));
        S::write_jsonb_string(writer, file_name);
        S::write_jsonb_key(writer, schema.field_name(S::Field::CONTENT_TYPE));
        S::write_jsonb_string(writer, content_type);
        S::write_jsonb_key(writer, schema.field_name(S::Field::SIZE));
        writer.writeInt64(-1);
        S::write_jsonb_key(writer, schema.field_name(S::Field::ETAG));
        writer.writeNull();
        S::write_jsonb_key(writer, schema.field_name(S::Field::LAST_MODIFIED_AT));
        S::write_jsonb_string(writer, std::string(S::LAST_MODIFIED_AT_FALLBACK));
        write_nullable_str(S::Field::REGION, "");
        write_nullable_str(S::Field::ENDPOINT, endpoint);
        write_nullable_str(S::Field::ROLE_ARN, role_arn);
        writer.writeEndObject();
    }
};

void register_function_file(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionToFile>();
}

} // namespace doris
