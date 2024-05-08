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

#include <fmt/format.h>
#include <glog/logging.h>
#include <stddef.h>

#include <memory>
#include <ostream>
#include <string>
#include <utility>

#include "common/status.h"
#include "runtime/thread_context.h"
#include "vec/columns/column.h"
#include "vec/functions/array/function_array_index.h"
//#include "vec/columns/column_array.h"
//#include "vec/columns/column_const.h"
//#include "vec/columns/column_nullable.h"
//#include "vec/columns/column_vector.h"
//#include "vec/columns/columns_number.h"
//#include "vec/common/assert_cast.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
//#include "vec/core/column_with_type_and_name.h"
#include "vec/columns/column_map.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"
#include "vec/functions/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris::vectorized {

// array_match_any(columnArray, "a,b") -> true
class FunctionArrayMatchAny : public IFunction {
public:
    static constexpr auto name = "array_match_any";

    static FunctionPtr create() { return std::make_shared<FunctionArrayMatchAny>(); }

    String get_name() const override { return name; }

    bool is_variadic() const override { return true; }

    size_t get_number_of_arguments() const override { return 3; }

    // match functions only need to set 1/true
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DCHECK(arguments.size() >= 2);
        DCHECK(is_array(remove_nullable(arguments[0])))
                << "First argument for function: " << name
                << " should be DataTypeArray but it has type " << arguments[0]->get_name() << ".";
        DCHECK(is_string(remove_nullable(arguments[1])))
                << "Second argument for function: " << name << " should be String but it has type "
                << arguments[1]->get_name() << ".";
        const auto* array_type =
                check_and_get_data_type<DataTypeArray>(remove_nullable(arguments[0]).get());
        DCHECK(is_string(remove_nullable(array_type->get_nested_type())))
                << "Element type of array in function: " << name
                << " should be String but it has type " << array_type->get_nested_type()->get_name()
                << ".";
        return std::make_shared<DataTypeUInt8>();
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }

        DCHECK(context->get_num_args() >= 2);
        DCHECK(context->get_arg_type(0)->is_array_type());
        DCHECK(context->get_arg_type(1)->is_string_type());
        std::shared_ptr<ParamValue> state = std::make_shared<ParamValue>();
        Field field;
        if (context->get_constant_col(1)) {
            context->get_constant_col(1)->column_ptr->get(0, field);
            state->value = field;
            state->type = context->get_arg_type(1)->type;
            context->set_function_state(scope, state);
        }
        return Status::OK();
    }

    Status eval_inverted_index(FunctionContext* context,
                               const vectorized::NameAndTypePair& data_type_with_name,
                               segment_v2::InvertedIndexIterator* iter, uint32_t num_rows,
                               roaring::Roaring* bitmap) const override {
        DBUG_EXECUTE_IF("match.invert_index_not_support_eval_match", {
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                    "FunctionMatchAny not support eval_match");
        })
        std::shared_ptr<roaring::Roaring> roaring = std::make_shared<roaring::Roaring>();
        auto* param_value = reinterpret_cast<ParamValue*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (param_value == nullptr) {
            return Status::Error<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>(
                    "Inverted index evaluate skipped, param_value is nullptr");
        }
        std::unique_ptr<InvertedIndexQueryParamFactory> query_param = nullptr;
        RETURN_IF_ERROR(InvertedIndexQueryParamFactory::create_query_value(
                param_value->type, &param_value->value, query_param));
        // here should use MATCH_ANY_QUERY
        RETURN_IF_ERROR(iter->read_from_inverted_index(
                data_type_with_name.first, query_param->get_value(),
                segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY, num_rows, roaring));

        segment_v2::InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
        RETURN_IF_ERROR(iter->read_null_bitmap(&null_bitmap_cache_handle));
        std::shared_ptr<roaring::Roaring> null_bitmap = null_bitmap_cache_handle.get_bitmap();
        if (null_bitmap) {
            *bitmap -= *null_bitmap;
        }
        *bitmap = *roaring;
        param_value->is_evaled_inverted_idx = true;
        return Status::OK();
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        size_t result, size_t input_rows_count) const override {
        DBUG_EXECUTE_IF("match.invert_index_not_support_execute_match", {
            return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED>(
                    "FunctionMatchAny not support execute_match");
        })
        bool is_eval_inverted_index = false;
        auto* param_value = reinterpret_cast<ParamValue*>(
                context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
        if (param_value != nullptr) {
            is_eval_inverted_index = param_value->is_evaled_inverted_idx;
        }

        // the array column in block should prepared
        auto left_column =
                block.get_by_position(arguments[0]).column->convert_to_full_column_if_const();
        const ColumnArray* array_column = nullptr;
        const UInt8* array_null_map = nullptr;
        if (left_column->is_nullable()) {
            auto nullable_array = reinterpret_cast<const ColumnNullable*>(left_column.get());
            array_column =
                    reinterpret_cast<const ColumnArray*>(&nullable_array->get_nested_column());
            array_null_map = nullable_array->get_null_map_column().get_data().data();
        } else {
            array_column = reinterpret_cast<const ColumnArray*>(left_column.get());
        }
        const auto& offsets = array_column->get_offsets();
        // prepare dst column
        auto dst = ColumnVector<UInt8>::create(offsets.size());
        auto& dst_data = dst->get_data();
        auto dst_null_column = ColumnUInt8::create(offsets.size());
        auto& dst_null_data = dst_null_column->get_data();

        if (is_eval_inverted_index) {
            // here is no need to eval array column again,just put array column to res
            for (size_t row = 0; row < offsets.size(); ++row) {
                if (array_null_map && array_null_map[row]) {
                    dst_null_data[row] = true;
                } else {
                    dst_null_data[row] = false;
                    dst_data[row] = true;
                }
            }
            auto return_column = ColumnNullable::create(std::move(dst), std::move(dst_null_column));
            block.replace_by_position(result, std::move(return_column));
        } else {
            // here we not eval inverted index, so we need to eval array column
            InvertedIndexCtxSPtr inverted_index_ctx = std::make_shared<InvertedIndexCtx>();
            if (arguments.size() == 3) {
                DCHECK(is_map(block.get_by_position(arguments[2]).type));
                auto config_col = block.get_by_position(arguments[2])
                                          .column->convert_to_full_column_if_const();
                auto map_col = reinterpret_cast<const ColumnMap*>(config_col.get());
                // make config map
                const auto& keys = check_and_get_column<ColumnString>(
                        remove_nullable(map_col->get_keys_ptr()));
                const auto& vals = check_and_get_column<ColumnString>(
                        remove_nullable(map_col->get_values_ptr()));
                std::map<std::string, std::string> _index_meta;
                for (int i = 0; i < map_col->offset_at(0); ++i) {
                    _index_meta[keys->get_data_at(i).to_string()] =
                            vals->get_data_at(i).to_string();
                }
                inverted_index_ctx->parser_type = get_inverted_index_parser_type_from_string(
                        get_parser_string_from_properties(_index_meta));
                inverted_index_ctx->parser_mode =
                        get_parser_mode_string_from_properties(_index_meta);
                inverted_index_ctx->char_filter_map =
                        get_parser_char_filter_map_from_properties(_index_meta);
                auto analyzer = InvertedIndexReader::create_analyzer(inverted_index_ctx.get());
                analyzer->set_lowercase(true);
                inverted_index_ctx->analyzer = analyzer.release();
            } else {
                inverted_index_ctx->parser_type = InvertedIndexParserType::PARSER_ENGLISH;
                inverted_index_ctx->parser_mode = INVERTED_INDEX_PARSER_FINE_GRANULARITY;
                auto analyzer = InvertedIndexReader::create_analyzer(inverted_index_ctx.get());
                analyzer->set_lowercase(true);
                inverted_index_ctx->analyzer = analyzer.release();
            }
            executeWithExtractTokens(block.get_by_position(arguments[0]).name,
                                     reinterpret_cast<StringRef*>(&param_value->value),
                                     input_rows_count,
                                     check_and_get_column<ColumnString>(
                                             remove_nullable(array_column->get_data_ptr())),
                                     &offsets, inverted_index_ctx, dst_data, dst_null_data);
            auto return_column = ColumnNullable::create(std::move(dst), std::move(dst_null_column));
            block.replace_by_position(result, std::move(return_column));
        }
        return Status::OK();
    }

    inline std::vector<std::string> analyse_data_token(const std::string& column_name,
                                                       InvertedIndexCtxSPtr inverted_index_ctx,
                                                       const ColumnString* string_col,
                                                       const ColumnArray::Offsets64* array_offsets,
                                                       const size_t cur_row_id) const {
        std::vector<std::string> data_tokens;
        auto query_type = inverted_index_ctx->parser_type == InvertedIndexParserType::PARSER_NONE
                                  ? InvertedIndexQueryType::EQUAL_QUERY
                                  : InvertedIndexQueryType::MATCH_ANY_QUERY;
        for (auto current_src_array_offset = (*array_offsets)[cur_row_id - 1];
             current_src_array_offset < (*array_offsets)[cur_row_id]; ++current_src_array_offset) {
            const auto& str_ref = string_col->get_data_at(current_src_array_offset);
            auto reader = doris::segment_v2::InvertedIndexReader::create_reader(
                    inverted_index_ctx.get(), str_ref.to_string());

            std::vector<std::string> element_tokens;
            doris::segment_v2::InvertedIndexReader::get_analyse_result(
                    element_tokens, reader.get(), inverted_index_ctx->analyzer, column_name,
                    query_type, false);
            data_tokens.insert(data_tokens.end(), element_tokens.begin(), element_tokens.end());
        }
        return data_tokens;
    }

    void executeWithExtractTokens(const std::string& column_name, StringRef* search_str,
                                  size_t input_rows_count, const ColumnString* string_col,
                                  const ColumnArray::Offsets64* array_offsets,
                                  InvertedIndexCtxSPtr inverted_index_ctx,
                                  ColumnUInt8::Container& result,
                                  ColumnUInt8::Container& null_map) const {
        VLOG_DEBUG << "begin to run FunctionArrayMatchAny::execute, parser_type: "
                   << inverted_index_parser_type_to_string(inverted_index_ctx->parser_type);
        auto reader = doris::segment_v2::InvertedIndexReader::create_reader(
                inverted_index_ctx.get(), search_str->to_string());
        std::vector<std::string> query_tokens;
        doris::segment_v2::InvertedIndexReader::get_analyse_result(
                query_tokens, reader.get(), inverted_index_ctx->analyzer, column_name,
                doris::segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY);
        if (query_tokens.empty()) {
            LOG(WARNING) << "token parser result is empty for query, "
                            "please check your query: '{}' and index parser: '{}'"
                         << search_str,
                    inverted_index_parser_type_to_string(inverted_index_ctx->parser_type);
            return;
        }

        for (int i = 0; i < input_rows_count; i++) {
            std::vector<std::string> data_tokens = analyse_data_token(
                    column_name, inverted_index_ctx, string_col, array_offsets, i);

            for (auto& token : query_tokens) {
                auto it = std::find(data_tokens.begin(), data_tokens.end(), token);
                if (it != data_tokens.end()) {
                    null_map[i] = false;
                    result[i] = true;
                    break;
                }
            }
        }
    }
};

void register_function_array_match_any(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayMatchAny>();
}

} // namespace doris::vectorized