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

#include "common/config.h"
#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_variant.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_variant.h"
#include "core/data_type/data_type_variant_v2.h"
#include "exec/common/variant_util.h"
#include "exprs/function/parse/variant_string_parse.h"
#include "exprs/function/simple_function_factory.h"
#include "util/json/json_parser.h"

namespace doris {
namespace {

template <bool ERROR_TO_NULL>
class FunctionVariantParse final : public IFunction {
public:
    static constexpr auto name =
            ERROR_TO_NULL ? "parse_to_variant_error_to_null" : "parse_to_variant";

    static FunctionPtr create() { return std::make_shared<FunctionVariantParse>(); }

    String get_name() const override { return name; }
    size_t get_number_of_arguments() const override { return 1; }
    bool use_default_implementation_for_nulls() const override { return false; }
    bool skip_return_type_check() const override { return true; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        auto result = std::make_shared<DataTypeVariant>();
        return ERROR_TO_NULL || arguments[0]->is_nullable() ? make_nullable(std::move(result))
                                                            : std::move(result);
    }

    // Keep strict/error-to-null, SQL-null, and legacy/V2 parse transitions in one auditable state machine.
    // NOLINTNEXTLINE(readability-function-size,readability-function-cognitive-complexity)
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        const ColumnPtr source =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const ColumnString* strings = nullptr;
        const NullMap* input_nulls = nullptr;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(source.get())) {
            strings = &assert_cast<const ColumnString&>(nullable->get_nested_column());
            input_nulls = &nullable->get_null_map_data();
        } else {
            strings = &assert_cast<const ColumnString&>(*source);
        }
        DORIS_CHECK_EQ(strings->size(), input_rows_count);

        const bool result_is_nullable = block.get_by_position(result).type->is_nullable();
        DORIS_CHECK_EQ(result_is_nullable, ERROR_TO_NULL || input_nulls != nullptr);
        auto result_nulls = ColumnUInt8::create(input_rows_count, uint8_t {0});

        const IDataType* result_type = remove_nullable(block.get_by_position(result).type).get();
        if (const auto* variant_type = dynamic_cast<const DataTypeVariant*>(result_type)) {
            const int32_t max_subcolumns_count = variant_type->variant_max_subcolumns_count();
            const bool enable_doc_mode = variant_type->enable_doc_mode();
            auto values = ColumnVariant::create(max_subcolumns_count, enable_doc_mode);
            ParseConfig parse_config;
            parse_config.check_duplicate_json_path =
                    config::variant_enable_duplicate_json_path_check;
            JsonParser parser;
            const StringRef empty_json("", 0);
            if constexpr (!ERROR_TO_NULL) {
                for (size_t row = 0; row < input_rows_count; ++row) {
                    const bool input_is_null = input_nulls != nullptr && (*input_nulls)[row] != 0;
                    try {
                        variant_util::parse_json_to_variant(
                                *values, input_is_null ? empty_json : strings->get_data_at(row),
                                &parser, parse_config);
                    } catch (const Exception& exception) {
                        const Status status = exception.to_status();
                        if (!status.is<ErrorCode::INVALID_ARGUMENT>()) {
                            return status;
                        }
                        return Status::InvalidArgument(
                                "Parse json document failed at row {}, error: {}", row,
                                status.to_string());
                    }
                    if (input_is_null) {
                        result_nulls->get_data()[row] = 1;
                    }
                }
            } else {
                for (size_t row = 0; row < input_rows_count; ++row) {
                    const bool input_is_null = input_nulls != nullptr && (*input_nulls)[row] != 0;
                    auto one_row = ColumnVariant::create(max_subcolumns_count, enable_doc_mode);
                    try {
                        variant_util::parse_json_to_variant(
                                *one_row, input_is_null ? empty_json : strings->get_data_at(row),
                                &parser, parse_config);
                        one_row->finalize();
                    } catch (const Exception& exception) {
                        const Status status = exception.to_status();
                        if (!status.is<ErrorCode::INVALID_ARGUMENT>()) {
                            return status;
                        }
                        one_row = ColumnVariant::create(max_subcolumns_count, enable_doc_mode);
                        variant_util::parse_json_to_variant(*one_row, empty_json, &parser,
                                                            parse_config);
                        one_row->finalize();
                        result_nulls->get_data()[row] = 1;
                    }
                    if (input_is_null) {
                        result_nulls->get_data()[row] = 1;
                    }
                    values->insert_range_from(*one_row, 0, 1);
                }
            }
            values->finalize();

            ColumnPtr output;
            if (result_is_nullable) {
                output = ColumnNullable::create(std::move(values), std::move(result_nulls));
            } else {
                output = std::move(values);
            }
            DORIS_CHECK_EQ(output->size(), input_rows_count);
            block.replace_by_position(result, std::move(output));
            return Status::OK();
        }
        DORIS_CHECK(dynamic_cast<const DataTypeVariantV2*>(result_type) != nullptr);

        JsonStringToVariantEncoder encoder(JsonToVariantOptions::current_config());
        const StringRef null_json("null", 4);
        for (size_t row = 0; row < input_rows_count; ++row) {
            if (input_nulls != nullptr && (*input_nulls)[row] != 0) {
                encoder.add_json(null_json);
                result_nulls->get_data()[row] = 1;
                continue;
            }

            const Status status = encoder.try_add_json(strings->get_data_at(row));
            if (status.ok()) {
                continue;
            }
            if constexpr (!ERROR_TO_NULL) {
                return Status::InvalidArgument("Parse json document failed at row {}, error: {}",
                                               row, status.to_string());
            }
            encoder.add_json(null_json);
            result_nulls->get_data()[row] = 1;
        }

        VariantBatchBuilder encoded = encoder.finish_batch();
        auto values = ColumnVariantV2::create();
        values->insert_encoded_batch(encoded);

        ColumnPtr output;
        if (result_is_nullable) {
            output = ColumnNullable::create(std::move(values), std::move(result_nulls));
        } else {
            output = std::move(values);
        }
        DORIS_CHECK_EQ(output->size(), input_rows_count);
        block.replace_by_position(result, std::move(output));
        return Status::OK();
    }
};

using FunctionParseToVariant = FunctionVariantParse<false>;
using FunctionParseToVariantErrorToNull = FunctionVariantParse<true>;

} // namespace

void register_function_variant_parse(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionParseToVariant>();
    factory.register_function<FunctionParseToVariantErrorToNull>();
}

} // namespace doris
