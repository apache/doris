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

#pragma once

#include <string>

#include "common/exception.h"
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "udf/udf.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_variant.h"
#include "vec/exprs/vexpr.h"
#include "vec/functions/function.h"

namespace doris {
class RowDescriptor;
class RuntimeState;
class TExprNode;

namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class VCaseExpr final : public VExpr {
    ENABLE_FACTORY_CREATOR(VCaseExpr);

public:
    VCaseExpr(const TExprNode& node);
    ~VCaseExpr() override = default;
    Status execute(VExprContext* context, Block* block, int* result_column_id) override;
    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override;
    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override;
    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) override;
    const std::string& expr_name() const override;
    std::string debug_string() const override;

private:
    std::string get_function_name() const {
        std::string res = FUNCTION_NAME;
        if (_has_else_expr) {
            res += "_has_else";
        }
        return res;
    }

    template <typename IndexType, typename ColumnType, bool then_null>
    ColumnPtr _execute_update_result(const IndexType* then_idx,
                                     std::vector<ColumnPtr>& then_columns) const {
        auto result_column_ptr = data_type()->create_column();
        result_column_ptr->reserve(then_columns.back()->size());
        if constexpr (std::is_same_v<ColumnType, ColumnString> ||
                      std::is_same_v<ColumnType, ColumnBitmap> ||
                      std::is_same_v<ColumnType, ColumnArray> ||
                      std::is_same_v<ColumnType, ColumnMap> ||
                      std::is_same_v<ColumnType, ColumnStruct> ||
                      std::is_same_v<ColumnType, ColumnVariant> ||
                      std::is_same_v<ColumnType, ColumnHLL> ||
                      std::is_same_v<ColumnType, ColumnQuantileState> ||
                      std::is_same_v<ColumnType, ColumnIPv4> ||
                      std::is_same_v<ColumnType, ColumnIPv6>) {
            // result_column and all then_column is not nullable.
            // can't simd when type is string.
            update_result_normal<IndexType, ColumnType, then_null>(result_column_ptr, then_idx,
                                                                   then_columns);
        } else if constexpr (then_null || !std::is_same_v<IndexType, uint8_t>) {
            // result_column and all then_column is nullable.
            update_result_normal<IndexType, ColumnType, then_null>(result_column_ptr, then_idx,
                                                                   then_columns);
        } else {
            update_result_auto_simd<IndexType, ColumnType>(result_column_ptr, then_idx,
                                                           then_columns);
        }
        return result_column_ptr;
    }

    template <typename IndexType, typename ColumnType, bool then_null>
    void update_result_normal(MutableColumnPtr& result_column_ptr,
                              const IndexType* __restrict then_idx,
                              std::vector<ColumnPtr>& then_columns) const {
        std::vector<ColumnPtr> raw_then_columns(then_columns.size());
        std::vector<uint8_t> is_consts(then_columns.size());
        std::vector<uint8_t> is_nullable(then_columns.size());
        for (size_t i = 0; i < then_columns.size(); i++) {
            if (!then_columns[i]) {
                continue;
            }
            std::tie(raw_then_columns[i], is_consts[i]) = unpack_if_const(then_columns[i]);
            is_nullable[i] = raw_then_columns[i]->is_nullable();
        }

        auto* raw_result_column = result_column_ptr.get();
        size_t rows_count = then_columns.back()->size();
        for (int row_idx = 0; row_idx < rows_count; row_idx++) {
            if (!_has_else_expr && !then_idx[row_idx]) {
                assert_cast<ColumnNullable*, TypeCheckOnRelease::DISABLE>(raw_result_column)
                        ->insert_default();
                continue;
            }
            size_t target = is_consts[then_idx[row_idx]] ? 0 : row_idx;
            if constexpr (then_null) {
                auto* nullable = assert_cast<ColumnNullable*, TypeCheckOnRelease::DISABLE>(
                        result_column_ptr.get());
                if (is_nullable[row_idx]) {
                    nullable->insert_from(*raw_then_columns[then_idx[row_idx]], target);
                } else {
                    auto* nested = assert_cast<ColumnType*, TypeCheckOnRelease::DISABLE>(
                            nullable->get_nested_column_ptr().get());
                    nested->insert_from(*raw_then_columns[then_idx[row_idx]], target);
                    nullable->push_false_to_nullmap(1);
                }
            } else {
                assert_cast<ColumnType*, TypeCheckOnRelease::DISABLE>(result_column_ptr.get())
                        ->insert_from(*raw_then_columns[then_idx[row_idx]], target);
            }
        }
    }

    template <typename IndexType, typename ColumnType>
    void update_result_auto_simd(MutableColumnPtr& result_column_ptr,
                                 const uint8_t* __restrict then_idx,
                                 std::vector<ColumnPtr>& then_columns) const {
        for (auto& then_ptr : then_columns) {
            then_ptr = then_ptr->convert_to_full_column_if_const();
        }

        size_t rows_count = then_columns.back()->size();
        result_column_ptr->resize(rows_count);
        auto* __restrict result_raw_data =
                assert_cast<ColumnType*, TypeCheckOnRelease::DISABLE>(result_column_ptr.get())
                        ->get_data()
                        .data();

        // set default value
        for (int i = 0; i < rows_count; i++) {
            result_raw_data[i] = {};
        }

        // some types had simd automatically, but some not.
        for (IndexType i = 0; i < then_columns.size(); i++) {
            if (!then_columns[i]) {
                continue;
            }
            auto* __restrict column_raw_data =
                    assert_cast<ColumnType*, TypeCheckOnRelease::DISABLE>(
                            then_columns[i]->assume_mutable().get())
                            ->get_data()
                            .data();

            for (int row_idx = 0; row_idx < rows_count; row_idx++) {
                result_raw_data[row_idx] +=
                        typename ColumnType::value_type(then_idx[row_idx] == i) *
                        column_raw_data[row_idx];
            }
        }
    }

    template <typename IndexType, typename ColumnType>
    ColumnPtr _execute_impl(const std::vector<ColumnPtr>& when_columns,
                            std::vector<ColumnPtr>& then_columns) {
        size_t row_count = then_columns.back()->size();
        std::vector<IndexType> then_idx(row_count, 0);
        IndexType* __restrict then_idx_ptr = then_idx.data();
        for (IndexType i = 0; i < when_columns.size(); i++) {
            IndexType column_idx = i + 1;
            if (when_columns[i]->is_nullable()) {
                const auto* column_nullable_ptr =
                        assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(
                                when_columns[i].get());
                const auto* __restrict cond_raw_data =
                        assert_cast<const ColumnUInt8*, TypeCheckOnRelease::DISABLE>(
                                column_nullable_ptr->get_nested_column_ptr().get())
                                ->get_data()
                                .data();
                if (!column_nullable_ptr->has_null()) {
                    for (int row_idx = 0; row_idx < row_count; row_idx++) {
                        then_idx_ptr[row_idx] |=
                                (!then_idx_ptr[row_idx]) * cond_raw_data[row_idx] * column_idx;
                    }
                    continue;
                }
                const auto* __restrict cond_raw_nullmap =
                        assert_cast<const ColumnUInt8*, TypeCheckOnRelease::DISABLE>(
                                column_nullable_ptr->get_null_map_column_ptr().get())
                                ->get_data()
                                .data();
                for (int row_idx = 0; row_idx < row_count; row_idx++) {
                    then_idx_ptr[row_idx] |= (!then_idx_ptr[row_idx] * cond_raw_data[row_idx] *
                                              !cond_raw_nullmap[row_idx]) *
                                             column_idx;
                }
            } else {
                const auto* __restrict cond_raw_data =
                        assert_cast<const ColumnUInt8*, TypeCheckOnRelease::DISABLE>(
                                when_columns[i].get())
                                ->get_data()
                                .data();
                for (int row_idx = 0; row_idx < row_count; row_idx++) {
                    then_idx_ptr[row_idx] |=
                            (!then_idx_ptr[row_idx]) * cond_raw_data[row_idx] * column_idx;
                }
            }
        }

        if (data_type()->is_nullable()) {
            return _execute_update_result<IndexType, ColumnType, true>(then_idx_ptr, then_columns);
        } else {
            return _execute_update_result<IndexType, ColumnType, false>(then_idx_ptr, then_columns);
        }
    }

    template <typename IndexType>
    ColumnPtr _execute_impl_with_type(const std::vector<ColumnPtr>& when_columns,
                                      std::vector<ColumnPtr>& then_columns) {
        switch (data_type()->get_primitive_type()) {
        case PrimitiveType::TYPE_BOOLEAN:
            return _execute_impl<IndexType, ColumnUInt8>(when_columns, then_columns);
        case PrimitiveType::TYPE_TINYINT:
            return _execute_impl<IndexType, ColumnInt8>(when_columns, then_columns);
        case PrimitiveType::TYPE_SMALLINT:
            return _execute_impl<IndexType, ColumnInt16>(when_columns, then_columns);
        case PrimitiveType::TYPE_INT:
            return _execute_impl<IndexType, ColumnInt32>(when_columns, then_columns);
        case PrimitiveType::TYPE_BIGINT:
            return _execute_impl<IndexType, ColumnInt64>(when_columns, then_columns);
        case PrimitiveType::TYPE_LARGEINT:
            return _execute_impl<IndexType, ColumnInt128>(when_columns, then_columns);
        case PrimitiveType::TYPE_FLOAT:
            return _execute_impl<IndexType, ColumnFloat32>(when_columns, then_columns);
        case PrimitiveType::TYPE_DOUBLE:
            return _execute_impl<IndexType, ColumnFloat64>(when_columns, then_columns);
        case PrimitiveType::TYPE_DECIMAL32:
            return _execute_impl<IndexType, ColumnDecimal32>(when_columns, then_columns);
        case PrimitiveType::TYPE_DECIMAL64:
            return _execute_impl<IndexType, ColumnDecimal64>(when_columns, then_columns);
        case PrimitiveType::TYPE_DECIMAL256:
            return _execute_impl<IndexType, ColumnDecimal256>(when_columns, then_columns);
        case PrimitiveType::TYPE_DECIMAL128I:
            return _execute_impl<IndexType, ColumnDecimal128V3>(when_columns, then_columns);
        case PrimitiveType::TYPE_DECIMALV2:
            return _execute_impl<IndexType, ColumnDecimal128V2>(when_columns, then_columns);
        case PrimitiveType::TYPE_STRING:
        case PrimitiveType::TYPE_CHAR:
        case PrimitiveType::TYPE_VARCHAR:
        case PrimitiveType::TYPE_JSONB:
            return _execute_impl<IndexType, ColumnString>(when_columns, then_columns);
        case PrimitiveType::TYPE_DATE:
            return _execute_impl<IndexType, ColumnDate>(when_columns, then_columns);
        case PrimitiveType::TYPE_DATETIME:
            return _execute_impl<IndexType, ColumnDateTime>(when_columns, then_columns);
        case PrimitiveType::TYPE_DATEV2:
            return _execute_impl<IndexType, ColumnDateV2>(when_columns, then_columns);
        case PrimitiveType::TYPE_DATETIMEV2:
            return _execute_impl<IndexType, ColumnDateTimeV2>(when_columns, then_columns);
        case PrimitiveType::TYPE_IPV6:
            return _execute_impl<IndexType, ColumnIPv6>(when_columns, then_columns);
        case PrimitiveType::TYPE_IPV4:
            return _execute_impl<IndexType, ColumnIPv4>(when_columns, then_columns);
        case PrimitiveType::TYPE_ARRAY:
            return _execute_impl<IndexType, ColumnArray>(when_columns, then_columns);
        case PrimitiveType::TYPE_MAP:
            return _execute_impl<IndexType, ColumnMap>(when_columns, then_columns);
        case PrimitiveType::TYPE_STRUCT:
            return _execute_impl<IndexType, ColumnStruct>(when_columns, then_columns);
        case PrimitiveType::TYPE_VARIANT:
            return _execute_impl<IndexType, ColumnVariant>(when_columns, then_columns);
        case PrimitiveType::TYPE_BITMAP:
            return _execute_impl<IndexType, ColumnBitmap>(when_columns, then_columns);
        case PrimitiveType::TYPE_HLL:
            return _execute_impl<IndexType, ColumnHLL>(when_columns, then_columns);
        case PrimitiveType::TYPE_QUANTILE_STATE:
            return _execute_impl<IndexType, ColumnQuantileState>(when_columns, then_columns);
        default:
            throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "argument_type {} not supported",
                            data_type()->get_name());
        }
    }

    bool _has_else_expr;

    inline static const std::string FUNCTION_NAME = "case";
    inline static const std::string EXPR_NAME = "vcase expr";
};
} // namespace doris::vectorized
