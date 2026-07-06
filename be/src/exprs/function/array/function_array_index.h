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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/array/arrayIndex.h
// and modified by Doris
#pragma once

#include <stddef.h>

#include <memory>
#include <utility>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/call_on_type_index.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_array_view.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h" // IWYU pragma: keep
#include "core/data_type/define_primitive_type.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "exprs/function/function.h"
#include "storage/index/index_reader_helper.h"
#include "storage/index/inverted/inverted_index_query_type.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/predicate/column_predicate.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {

struct ArrayContainsAction {
    static constexpr auto ResultType = PrimitiveType::TYPE_BOOLEAN;
    static constexpr auto name = "array_contains";
    static constexpr const bool resume_execution = false;
    static constexpr void apply(typename PrimitiveTypeTraits<ResultType>::CppType& current,
                                size_t) noexcept {
        current = 1;
    }
};

struct ArrayPositionAction {
    static constexpr auto ResultType = PrimitiveType::TYPE_BIGINT;
    static constexpr auto name = "array_position";
    static constexpr const bool resume_execution = false;
    static constexpr void apply(typename PrimitiveTypeTraits<ResultType>::CppType& current,
                                size_t j) noexcept {
        current = j + 1;
    }
};

struct ArrayCountEqual {
    static constexpr auto ResultType = PrimitiveType::TYPE_BIGINT;
    static constexpr auto name = "countequal";
    static constexpr const bool resume_execution = true;
    static constexpr void apply(typename PrimitiveTypeTraits<ResultType>::CppType& current,
                                size_t j) noexcept {
        ++current;
    }
};

struct ParamValue {
    PrimitiveType type;
    Field value;
};

template <typename ConcreteAction>
class FunctionArrayIndex : public IFunction {
public:
    static constexpr auto ResultType = ConcreteAction::ResultType;

    static constexpr auto name = ConcreteAction::name;
    static FunctionPtr create() { return std::make_shared<FunctionArrayIndex>(); }

    /// Get function name.
    String get_name() const override { return name; }

    bool is_variadic() const override { return false; }

    size_t get_number_of_arguments() const override { return 2; }

    bool use_default_implementation_for_nulls() const override { return false; }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        if (scope == FunctionContext::THREAD_LOCAL) {
            return Status::OK();
        }

        DCHECK(context->get_num_args() >= 1);
        DCHECK_EQ(context->get_arg_type(0)->get_primitive_type(), PrimitiveType::TYPE_ARRAY);
        // now we only support same
        std::shared_ptr<ParamValue> state = std::make_shared<ParamValue>();
        Field field;
        if (context->get_constant_col(1)) {
            context->get_constant_col(1)->column_ptr->get(0, field);
            state->value = field;
            state->type = context->get_arg_type(1)->get_primitive_type();
            context->set_function_state(scope, state);
        }
        return Status::OK();
    }

    Status evaluate_inverted_index(
            const ColumnsWithTypeAndName& arguments,
            const std::vector<IndexFieldNameAndTypePair>& data_type_with_names,
            std::vector<segment_v2::IndexIterator*> iterators, uint32_t num_rows,
            const InvertedIndexAnalyzerCtx* analyzer_ctx,
            segment_v2::InvertedIndexResultBitmap& bitmap_result) const override {
        DCHECK(arguments.size() == 1);
        DCHECK(data_type_with_names.size() == 1);
        DCHECK(iterators.size() == 1);
        auto* iter = iterators[0];
        auto data_type_with_name = data_type_with_names[0];
        if (iter == nullptr) {
            return Status::OK();
        }
        if (!segment_v2::IndexReaderHelper::has_string_or_bkd_index(iter)) {
            // parser is not none we can not make sure the result is correct in expr combination
            // for example, filter: !array_index(array, 'tall:120cm, weight: 35kg')
            // here we have rows [tall:120cm, weight: 35kg, hobbies: reading book] which be tokenized
            // but query is also tokenized, and FULLTEXT reader will catch this row as matched,
            // so array_index(array, 'tall:120cm, weight: 35kg') return this rowid,
            // but we expect it to be filtered, because we want row is equal to 'tall:120cm, weight: 35kg'
            return Status::OK();
        }
        Field param_value;
        arguments[0].column->get(0, param_value);
        // The current implementation for the inverted index of arrays cannot handle cases where the array contains null values,
        // meaning an item in the array is null.
        if (param_value.is_null()) {
            return Status::OK();
        }

        std::shared_ptr<roaring::Roaring> null_bitmap = std::make_shared<roaring::Roaring>();
        if (iter->has_null()) {
            segment_v2::InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
            RETURN_IF_ERROR(iter->read_null_bitmap(&null_bitmap_cache_handle));
            null_bitmap = null_bitmap_cache_handle.get_bitmap();
        }
        InvertedIndexParam param;
        param.column_name = data_type_with_name.first;
        param.column_type = data_type_with_name.second;
        param.query_value = param_value;
        param.query_type = segment_v2::InvertedIndexQueryType::EQUAL_QUERY;
        param.num_rows = num_rows;
        param.roaring = std::make_shared<roaring::Roaring>();
        param.analyzer_ctx = analyzer_ctx;
        RETURN_IF_ERROR(iter->read_from_index(segment_v2::IndexParam {&param}));
        // here debug for check array_contains function really filter rows by inverted index correctly
        DBUG_EXECUTE_IF("array_func.array_contains", {
            auto result_bitmap = DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                    "array_func.array_contains", "result_bitmap", 0);
            if (result_bitmap < 0) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>(
                        "result_bitmap count cannot be negative");
            }
            if (param.roaring->cardinality() != result_bitmap) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>(
                        "array_contains really filtered {} by inverted index not equal to expected "
                        "{}",
                        param.roaring->cardinality(), result_bitmap);
            }
        })
        segment_v2::InvertedIndexResultBitmap result(param.roaring, null_bitmap);
        bitmap_result = result;
        bitmap_result.mask_out_null();

        return Status::OK();
    }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        if (arguments[0]->is_nullable()) {
            return make_nullable(
                    std::make_shared<typename PrimitiveTypeTraits<ResultType>::DataType>());
        } else {
            return std::make_shared<typename PrimitiveTypeTraits<ResultType>::DataType>();
        }
    }

    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        DBUG_EXECUTE_IF("array_func.array_contains", {
            auto req_id = DebugPoints::instance()->get_debug_param_or_default<int32_t>(
                    "array_func.array_contains", "req_id", 0);
            return Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "{} has already execute inverted index req_id {} , should not execute expr "
                    "with rows: {}",
                    get_name(), req_id, input_rows_count);
        });
        return _execute_dispatch(block, arguments, result, input_rows_count);
    }

private:
    template <PrimitiveType PType>
    ColumnPtr _execute_view(const ColumnArrayView<PType>& array_view,
                            const ColumnView<PType>& right_view) const {
        // prepare return data
        auto dst = PrimitiveTypeTraits<ResultType>::ColumnType::create(array_view.size(), 0);
        auto& dst_data = dst->get_data_mutable();
        auto dst_null_column = ColumnUInt8::create(array_view.size(), 0);
        auto& dst_null_data = dst_null_column->get_data_mutable();

        // process
        for (size_t row = 0; row < array_view.size(); ++row) {
            if (array_view.is_null_at(row)) {
                dst_null_data[row] = true;
                continue;
            }
            dst_null_data[row] = false;
            typename PrimitiveTypeTraits<ResultType>::CppType res = 0;
            const auto array_data = array_view[row];
            for (size_t pos = 0; pos < array_data.size(); ++pos) {
                // match null value
                if (right_view.is_null_at(row) && array_data.is_null_at(pos)) {
                    ConcreteAction::apply(res, pos);
                    if constexpr (!ConcreteAction::resume_execution) {
                        break;
                    }
                }
                // some is null while another is not
                if (right_view.is_null_at(row) != array_data.is_null_at(pos)) {
                    continue;
                }
                if (array_data.is_null_at(pos)) {
                    continue;
                }
                if (array_data.value_at(pos) == right_view.value_at(row)) {
                    ConcreteAction::apply(res, pos);
                    if constexpr (!ConcreteAction::resume_execution) {
                        break;
                    }
                }
            }
            dst_data[row] = res;
        }

        if (!array_view.is_nullable()) {
            return dst;
        }
        return ColumnNullable::create(std::move(dst), std::move(dst_null_column));
    }

    Status _execute_dispatch(Block& block, const ColumnNumbers& arguments, uint32_t result,
                             size_t input_rows_count) const {
        if (block.get_by_position(arguments[0]).type->get_primitive_type() != TYPE_ARRAY) {
            return Status::InvalidArgument(get_name() + " first argument must be array, but got " +
                                           block.get_by_position(arguments[0]).type->get_name());
        }
        // execute
        auto array_type = remove_nullable(block.get_by_position(arguments[0]).type);
        auto left_element_type = remove_nullable(
                assert_cast<const DataTypeArray*>(array_type.get())->get_nested_type());
        auto right_type = remove_nullable(block.get_by_position(arguments[1]).type);

        auto left_element_primitive_type = left_element_type->get_primitive_type();
        auto right_primitive_type = right_type->get_primitive_type();
        ColumnPtr return_column = nullptr;
        if (right_primitive_type == left_element_primitive_type ||
            (is_string_type(right_primitive_type) && is_string_type(left_element_primitive_type))) {
            auto call = [&](const auto& type) -> bool {
                using DispatchType = std::decay_t<decltype(type)>;
                constexpr PrimitiveType PType = DispatchType::PType;
                auto array_view =
                        ColumnArrayView<PType>::create(block.get_by_position(arguments[0]).column);
                auto right_view =
                        ColumnView<PType>::create(block.get_by_position(arguments[1]).column);
                return_column = _execute_view(array_view, right_view);
                return true;
            };
            dispatch_switch_all(left_element_primitive_type, call);
        }

        if (return_column) {
            block.replace_by_position(result, std::move(return_column));
            return Status::OK();
        }
        return Status::RuntimeError("execute failed or unsupported types for function {}({}, {})",
                                    get_name(),
                                    block.get_by_position(arguments[0]).type->get_name(),
                                    block.get_by_position(arguments[1]).type->get_name());
    }
};

} // namespace doris
