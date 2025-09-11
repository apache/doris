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
    template <typename IndexType, typename ColumnType>
    ColumnPtr _execute_update_result_impl(const IndexType* then_idx,
                                          std::vector<ColumnPtr>& then_columns,
                                          size_t rows_count) const {
        auto result_column_ptr = data_type()->create_column();
        result_column_ptr->reserve(rows_count);
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
            if (data_type()->is_nullable()) {
                update_result_normal<IndexType, ColumnType, true>(result_column_ptr, then_idx,
                                                                  then_columns, rows_count);
            } else {
                update_result_normal<IndexType, ColumnType, false>(result_column_ptr, then_idx,
                                                                   then_columns, rows_count);
            }
        } else if (data_type()->is_nullable()) {
            // result_column and all then_column is nullable.
            update_result_normal<IndexType, ColumnType, true>(result_column_ptr, then_idx,
                                                              then_columns, rows_count);
        } else {
            update_result_auto_simd<IndexType, ColumnType>(result_column_ptr, then_idx,
                                                           then_columns, rows_count);
        }
        return result_column_ptr;
    }

    template <typename IndexType>
    ColumnPtr _execute_update_result(const IndexType* then_idx,
                                     std::vector<ColumnPtr>& then_columns,
                                     size_t rows_count) const {
#define CASE_TYPE(ptype, coltype) \
    case PrimitiveType::ptype:    \
        return _execute_update_result_impl<IndexType, coltype>(then_idx, then_columns, rows_count);

        switch (data_type()->get_primitive_type()) {
            CASE_TYPE(TYPE_BOOLEAN, ColumnUInt8)
            CASE_TYPE(TYPE_TINYINT, ColumnInt8)
            CASE_TYPE(TYPE_SMALLINT, ColumnInt16)
            CASE_TYPE(TYPE_INT, ColumnInt32)
            CASE_TYPE(TYPE_BIGINT, ColumnInt64)
            CASE_TYPE(TYPE_LARGEINT, ColumnInt128)
            CASE_TYPE(TYPE_FLOAT, ColumnFloat32)
            CASE_TYPE(TYPE_DOUBLE, ColumnFloat64)
            CASE_TYPE(TYPE_DECIMAL32, ColumnDecimal32)
            CASE_TYPE(TYPE_DECIMAL64, ColumnDecimal64)
            CASE_TYPE(TYPE_DECIMAL256, ColumnDecimal256)
            CASE_TYPE(TYPE_DECIMAL128I, ColumnDecimal128V3)
            CASE_TYPE(TYPE_DECIMALV2, ColumnDecimal128V2)
            CASE_TYPE(TYPE_STRING, ColumnString)
            CASE_TYPE(TYPE_CHAR, ColumnString)
            CASE_TYPE(TYPE_VARCHAR, ColumnString)
            CASE_TYPE(TYPE_JSONB, ColumnString)
            CASE_TYPE(TYPE_DATE, ColumnDate)
            CASE_TYPE(TYPE_DATETIME, ColumnDateTime)
            CASE_TYPE(TYPE_DATEV2, ColumnDateV2)
            CASE_TYPE(TYPE_DATETIMEV2, ColumnDateTimeV2)
            CASE_TYPE(TYPE_IPV6, ColumnIPv6)
            CASE_TYPE(TYPE_IPV4, ColumnIPv4)
            CASE_TYPE(TYPE_ARRAY, ColumnArray)
            CASE_TYPE(TYPE_MAP, ColumnMap)
            CASE_TYPE(TYPE_STRUCT, ColumnStruct)
            CASE_TYPE(TYPE_VARIANT, ColumnVariant)
            CASE_TYPE(TYPE_BITMAP, ColumnBitmap)
            CASE_TYPE(TYPE_HLL, ColumnHLL)
            CASE_TYPE(TYPE_QUANTILE_STATE, ColumnQuantileState)
        default:
            throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "argument_type {} not supported",
                            data_type()->get_name());
        }
#undef CASE_TYPE
    }

    template <typename IndexType, typename ColumnType, bool then_null>
    void update_result_normal(MutableColumnPtr& result_column_ptr,
                              const IndexType* __restrict then_idx,
                              std::vector<ColumnPtr>& then_columns, size_t rows_count) const {
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
                if (is_nullable[then_idx[row_idx]]) {
                    nullable->insert_from_with_type<ColumnType>(
                            *raw_then_columns[then_idx[row_idx]], target);
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
                                 const IndexType* __restrict then_idx,
                                 std::vector<ColumnPtr>& then_columns, size_t rows_count) const {
        for (auto& then_ptr : then_columns) {
            then_ptr = then_ptr->convert_to_full_column_if_const();
        }

        result_column_ptr->resize(rows_count);
        auto* __restrict result_raw_data =
                assert_cast<ColumnType*, TypeCheckOnRelease::DISABLE>(result_column_ptr.get())
                        ->get_data()
                        .data();

        // set default value
        for (int i = 0; i < rows_count; i++) {
            result_raw_data[i] = {};
        }

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

    template <typename IndexType>
    ColumnPtr _execute_impl(const std::vector<ColumnPtr>& when_columns,
                            std::vector<ColumnPtr>& then_columns, size_t rows_count) {
        std::vector<IndexType> then_idx(rows_count, 0);
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
                    for (int row_idx = 0; row_idx < rows_count; row_idx++) {
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
                for (int row_idx = 0; row_idx < rows_count; row_idx++) {
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
                for (int row_idx = 0; row_idx < rows_count; row_idx++) {
                    then_idx_ptr[row_idx] |=
                            (!then_idx_ptr[row_idx]) * cond_raw_data[row_idx] * column_idx;
                }
            }
        }

        return _execute_update_result<IndexType>(then_idx_ptr, then_columns, rows_count);
    }

    bool _has_else_expr;

    inline static const std::string FUNCTION_NAME = "case";
    inline static const std::string EXPR_NAME = "vcase expr";
};
} // namespace doris::vectorized
