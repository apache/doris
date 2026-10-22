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

#include <cstddef>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/check.h"
#include "common/compare.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_numbers.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_nullable.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/data_type/primitive_type.h"
#include "core/types.h"
#include "exprs/function/function.h"
#include "exprs/function/simple_function_factory.h"

namespace doris {
class FunctionContext;
} // namespace doris

namespace doris {

namespace {

/// Upper bound of k. Keep in sync with the aggregate's MAX_TOPK_K
/// (aggregate_function_topk_count.h) and FE TopKCountFunction.MAX_TOPK_K.
constexpr int64_t MAX_TOPK_K = 65536;

/// One row of an ARRAY<STRUCT<value, count>> argument. A NULL array is an empty view.
struct TopKArrayView {
    const IColumn* values = nullptr;
    const ColumnInt64* counts = nullptr;
    /// NULL bitmap of the array element (the pair struct).
    const NullMap* null_map = nullptr;
    /// NULL bitmap of the `value` field; NULL values are not part of the contract.
    const NullMap* value_null_map = nullptr;
    /// NULL bitmap of the `count` field; NULL counts are not part of the contract.
    const NullMap* count_null_map = nullptr;
    size_t begin = 0;
    size_t end = 0;
};

TopKArrayView make_array_view(const IColumn& column, size_t row) {
    TopKArrayView view;
    const IColumn* array_column = &column;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(&column)) {
        if (nullable->is_null_at(row)) {
            return view;
        }
        array_column = &nullable->get_nested_column();
    }
    const auto& array = assert_cast<const ColumnArray&>(*array_column);
    // offsets[i] is the cumulative size after row i, so row's range is [offsets[row-1], offsets[row]).
    // offset_at(0) reads offsets[-1], which PaddedPODArray keeps as 0.
    view.begin = array.offset_at(static_cast<ssize_t>(row));
    view.end = array.offset_at(static_cast<ssize_t>(row) + 1);

    const IColumn* pair_column = &array.get_data();
    if (const auto* element_nullable = check_and_get_column<ColumnNullable>(pair_column)) {
        view.null_map = &element_nullable->get_null_map_data();
        pair_column = &element_nullable->get_nested_column();
    }
    const auto& pairs = assert_cast<const ColumnStruct&>(*pair_column);
    view.values = &pairs.get_column(0);
    // A cast may widen the `value` field to a nullable column; the entries themselves are never
    // NULL, so only the nested column is used.
    if (const auto* value_nullable = check_and_get_column<ColumnNullable>(view.values)) {
        view.value_null_map = &value_nullable->get_null_map_data();
        view.values = &value_nullable->get_nested_column();
    }
    // The same holds for `count`: a nested cast may have made the field nullable.
    const auto* count_column = &pairs.get_column(1);
    if (const auto* count_nullable = check_and_get_column<ColumnNullable>(count_column)) {
        view.count_null_map = &count_nullable->get_null_map_data();
        count_column = &count_nullable->get_nested_column();
    }
    view.counts = &assert_cast<const ColumnInt64&>(*count_column);
    return view;
}

/// One scalar bound argument: NULL means the corresponding side was not truncated. `row` is the
/// index of the current input row inside `value`: bounds are ordinary per-row columns, so a block
/// can carry a different bound in every row.
struct TopKBoundView {
    const IColumn* value = nullptr;
    size_t row = 0;
    bool is_null = true;
};

TopKBoundView make_bound_view(const IColumn& column, size_t row) {
    TopKBoundView view;
    const IColumn* value_column = &column;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(&column)) {
        if (nullable->is_null_at(row)) {
            return view;
        }
        value_column = &nullable->get_nested_column();
    }
    view.value = value_column;
    view.row = row;
    view.is_null = false;
    return view;
}

Status read_int64(const IColumn& column, size_t row, const char* function_name,
                  const char* argument_name, int64_t* out) {
    const IColumn* nested = &column;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(&column)) {
        // The signature accepts a nullable BIGINT, so a NULL here is invalid SQL input and must
        // be a query error; a checked invariant would terminate a debugging BE.
        if (UNLIKELY(nullable->is_null_at(row))) {
            return Status::InvalidArgument("{}: {} must not be NULL", function_name, argument_name);
        }
        nested = &nullable->get_nested_column();
    }
    *out = assert_cast<const ColumnInt64&, TypeCheckOnRelease::DISABLE>(*nested).get_element(row);
    return Status::OK();
}

int64_t checked_add(int64_t lhs, int64_t rhs, const char* function_name) {
    int64_t result = 0;
    if (UNLIKELY(__builtin_add_overflow(lhs, rhs, &result))) {
        throw Exception(ErrorCode::INVALID_ARGUMENT, "{}: value count overflow", function_name);
    }
    return result;
}

/// FLOAT/DOUBLE use CompareFloat so NaN sorts greatest and is distinct from every number; other
/// types use the column comparison, i.e. exactly the MIN/MAX comparison.
int compare_values(PrimitiveType value_type, const IColumn& lhs, size_t lhs_row, const IColumn& rhs,
                   size_t rhs_row) {
    if (value_type == TYPE_FLOAT) {
        return CompareFloat(
                assert_cast<const ColumnFloat32&, TypeCheckOnRelease::DISABLE>(lhs).get_element(
                        lhs_row),
                assert_cast<const ColumnFloat32&, TypeCheckOnRelease::DISABLE>(rhs).get_element(
                        rhs_row));
    }
    if (value_type == TYPE_DOUBLE) {
        return CompareFloat(
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(lhs).get_element(
                        lhs_row),
                assert_cast<const ColumnFloat64&, TypeCheckOnRelease::DISABLE>(rhs).get_element(
                        rhs_row));
    }
    return lhs.compare_at(lhs_row, rhs_row, rhs, 1);
}

DataTypePtr find_value_type(const DataTypes& arguments) {
    // Prefer the per-value type of S/I/D; it is also present when all bound arguments are NULL.
    for (size_t index : {0, 2, 4}) {
        const auto& type = remove_nullable(arguments[index]);
        if (type->get_primitive_type() != TYPE_ARRAY) {
            continue;
        }
        const auto& nested =
                remove_nullable(assert_cast<const DataTypeArray&>(*type).get_nested_type());
        if (nested->get_primitive_type() != TYPE_STRUCT) {
            continue;
        }
        const auto& fields = assert_cast<const DataTypeStruct&>(*nested).get_elements();
        if (fields.size() != 2) {
            continue;
        }
        // The result struct always declares a non-nullable `value` field, while an argument cast
        // may have produced a nullable one; drop it so both sides agree on the return type.
        return remove_nullable(fields[0]);
    }
    for (size_t index : {1, 3, 5}) {
        const auto& type = remove_nullable(arguments[index]);
        if (!type->is_null_literal()) {
            return type;
        }
    }
    return nullptr;
}

/**
 * topk_merge_min / topk_merge_max: one apply step of the IVM MIN/MAX top-K state.
 *
 * Arguments: S, b, I, b_i, D, b_d, n, dn, k. S is the pre-image state, I/D are the insert/delete
 * top-K lists of this delta batch and b/i/d bounds are their truncation points (NULL = not
 * truncated). Only values in [<= bE] are trusted, where bE = min of all non-NULL bounds (max for
 * MAX): outside it at least one side may have dropped values. The post count of a value is
 * S + I - D; non-positive counts are removed (net-zero cancellations), then the first k values are
 * kept.
 *
 * The result is the state itself, i.e. the same type topk_min_count/topk_max_count return:
 * STRUCT<values ARRAY<STRUCT<value T, count BIGINT>>, bound T NULL>. n' = n + dn and
 * exhausted = (size(values) == 0 && n' > 0) are derived by the caller, which writes n' to the
 * COUNT state column and uses exhausted to trigger the fallback.
 */
template <bool IsMin>
class FunctionTopKMerge : public IFunction {
public:
    static constexpr auto name = IsMin ? "topk_merge_min" : "topk_merge_max";

    static FunctionPtr create() { return std::make_shared<FunctionTopKMerge>(); }

    String get_name() const override { return name; }

    size_t get_number_of_arguments() const override { return 9; }

    /// A NULL list is an empty list and a NULL bound is a valid value: the default implementation
    /// would turn the whole row into NULL and break the contract.
    bool use_default_implementation_for_nulls() const override { return false; }

    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        DataTypePtr value_type = find_value_type(arguments);
        if (value_type == nullptr) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "{} can not infer the value type from arguments", name);
        }
        // The result is the state type, identical to the aggregate's return type, so that the
        // apply expression can write it straight back to the hidden MIN_TOPK/MAX_TOPK and BOUND
        // columns.
        auto pair_type = std::make_shared<DataTypeStruct>(
                DataTypes {value_type, std::make_shared<DataTypeInt64>()},
                Strings {"value", "count"});
        auto values_type = std::make_shared<DataTypeArray>(make_nullable(pair_type));
        return std::make_shared<DataTypeStruct>(DataTypes {values_type, make_nullable(value_type)},
                                                Strings {"values", "bound"});
    }

    Status execute_impl(FunctionContext* /*context*/, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override {
        std::vector<ColumnPtr> columns(arguments.size());
        DataTypes argument_types(arguments.size());
        for (size_t i = 0; i != arguments.size(); ++i) {
            columns[i] =
                    block.get_by_position(arguments[i]).column->convert_to_full_column_if_const();
            argument_types[i] = block.get_by_position(arguments[i]).type;
        }
        const DataTypePtr value_type = find_value_type(argument_types);
        // An untyped call is rejected while planning get_return_type(); the plan always carries T.
        DCHECK(value_type != nullptr);
        const PrimitiveType primitive_type = value_type->get_primitive_type();

        auto result_column = block.get_by_position(result).type->create_column();
        auto& result_struct = assert_cast<ColumnStruct&>(*result_column);
        auto& out_values = assert_cast<ColumnArray&>(result_struct.get_column(0));
        auto& out_pair_nullable = assert_cast<ColumnNullable&>(out_values.get_data());
        auto& out_pair = assert_cast<ColumnStruct&>(out_pair_nullable.get_nested_column());
        auto& out_value_column = out_pair.get_column(0);
        auto& out_count_column = assert_cast<ColumnInt64&>(out_pair.get_column(1));
        auto& out_bound_column = assert_cast<ColumnNullable&>(result_struct.get_column(1));

        for (size_t row = 0; row != input_rows_count; ++row) {
            int64_t k = 0;
            RETURN_IF_ERROR(read_int64(*columns[8], row, name, "k", &k));
            if (UNLIKELY(k < 1 || k > MAX_TOPK_K)) {
                return Status::InvalidArgument("{} requires 1 <= k <= {}, but got {}", name,
                                               MAX_TOPK_K, k);
            }
            int64_t n = 0;
            RETURN_IF_ERROR(read_int64(*columns[6], row, name, "n", &n));
            int64_t dn = 0;
            RETURN_IF_ERROR(read_int64(*columns[7], row, name, "dn", &dn));
            const int64_t new_n = checked_add(n, dn, name);

            const TopKArrayView s_view = make_array_view(*columns[0], row);
            const TopKArrayView i_view = make_array_view(*columns[2], row);
            const TopKArrayView d_view = make_array_view(*columns[4], row);
            const TopKBoundView b_view = make_bound_view(*columns[1], row);
            const TopKBoundView bi_view = make_bound_view(*columns[3], row);
            const TopKBoundView bd_view = make_bound_view(*columns[5], row);
            const TopKBoundView effective_bound =
                    effective_bound_of(b_view, bi_view, bd_view, primitive_type);

            size_t positive = 0;
            RETURN_IF_ERROR(append_trusted_values(s_view, i_view, d_view, effective_bound, k,
                                                  primitive_type, out_value_column,
                                                  out_count_column, out_pair_nullable, &positive));
            out_values.get_offsets().push_back(out_pair.size());

            // bound: NULL while the state is complete, the k-th kept value only when values were
            // actually dropped (pos > k), or the effective bound otherwise.
            // b IS NULL means the pre-image covers every distinct value; a NULL b_i/b_d means
            // that side of the delta was not truncated.
            // pos == k is NOT truncation: every known value fit, so keep bE instead of shrinking
            // the trusted range to max(S').
            const bool sides_complete = b_view.is_null && bi_view.is_null && bd_view.is_null;
            if (new_n == 0 || (sides_complete && std::cmp_less_equal(positive, k))) {
                out_bound_column.insert_default();
            } else if (std::cmp_greater(positive, k)) {
                out_bound_column.get_nested_column().insert_from(out_value_column,
                                                                 out_pair.size() - 1);
                out_bound_column.get_null_map_data().push_back(0);
            } else {
                DCHECK(!effective_bound.is_null);
                out_bound_column.get_nested_column().insert_from(*effective_bound.value,
                                                                 effective_bound.row);
                out_bound_column.get_null_map_data().push_back(0);
            }
        }

        block.replace_by_position(result, std::move(result_column));
        return Status::OK();
    }

private:
    /// bE: the best (smallest for MIN, largest for MAX) of the non-NULL bounds, or nullptr when
    /// every side is complete/untruncated.
    static TopKBoundView effective_bound_of(const TopKBoundView& b_view,
                                            const TopKBoundView& bi_view,
                                            const TopKBoundView& bd_view, PrimitiveType type) {
        TopKBoundView bound;
        for (const TopKBoundView* view : {&b_view, &bi_view, &bd_view}) {
            if (view->is_null) {
                continue;
            }
            if (bound.is_null) {
                bound = *view;
                continue;
            }
            int cmp = compare_values(type, *view->value, view->row, *bound.value, bound.row);
            if (IsMin ? cmp < 0 : cmp > 0) {
                bound = *view;
            }
        }
        return bound;
    }

    /// A list may have a nullable element type, but its entries, values and counts must not be
    /// NULL: the lists are top-K states, where NULL is neither counted nor representable. Reading
    /// a NULL payload would emit the column's default value as a real entry, so reject it.
    static Status check_entry(const TopKArrayView& view, size_t index) {
        if ((view.null_map != nullptr && (*view.null_map)[index] != 0) ||
            (view.value_null_map != nullptr && (*view.value_null_map)[index] != 0) ||
            (view.count_null_map != nullptr && (*view.count_null_map)[index] != 0)) {
            return Status::InvalidArgument(
                    "{}: the state lists must not contain NULL entries, values or counts", name);
        }
        return Status::OK();
    }

    /// Finds the best head (smallest for MIN, largest for MAX) among the three lists; `head` is
    /// set to nullptr when all of them are exhausted.
    static Status best_head(const TopKArrayView& s_view, size_t s_index,
                            const TopKArrayView& i_view, size_t i_index,
                            const TopKArrayView& d_view, size_t d_index, PrimitiveType type,
                            const IColumn** head, size_t* head_row) {
        *head = nullptr;
        const TopKArrayView* views[] = {&s_view, &i_view, &d_view};
        const size_t indexes[] = {s_index, i_index, d_index};
        for (size_t v = 0; v != 3; ++v) {
            if (indexes[v] == views[v]->end) {
                continue;
            }
            RETURN_IF_ERROR(check_entry(*views[v], indexes[v]));
            if (*head == nullptr) {
                *head = views[v]->values;
                *head_row = indexes[v];
                continue;
            }
            int cmp = compare_values(type, *views[v]->values, indexes[v], **head, *head_row);
            if (IsMin ? cmp < 0 : cmp > 0) {
                *head = views[v]->values;
                *head_row = indexes[v];
            }
        }
        return Status::OK();
    }

    /// Consumes one entry equal to the current head value, returning its count.
    static Status consume_equal(const TopKArrayView& view, size_t& index, const IColumn& head,
                                size_t head_row, PrimitiveType type, int64_t* count) {
        if (index == view.end) {
            return Status::OK();
        }
        RETURN_IF_ERROR(check_entry(view, index));
        if (compare_values(type, *view.values, index, head, head_row) != 0) {
            return Status::OK();
        }
        *count = view.counts->get_element(index);
        ++index;
        return Status::OK();
    }

    /// Merges the trusted part (v <= bE for MIN) of S, I and D into the output list, keeping the
    /// first k values with a positive post count S + I - D. `positive` receives the number of
    /// positive candidates, capped at k + 1: once more than k are found the scan stops, which is
    /// enough to decide the bound.
    Status append_trusted_values(const TopKArrayView& s_view, const TopKArrayView& i_view,
                                 const TopKArrayView& d_view, const TopKBoundView& effective_bound,
                                 int64_t k, PrimitiveType type, IColumn& out_value_column,
                                 ColumnInt64& out_count_column, ColumnNullable& out_pair_nullable,
                                 size_t* positive) const {
        size_t s_index = s_view.begin;
        size_t i_index = i_view.begin;
        size_t d_index = d_view.begin;
        size_t kept = 0;
        while (true) {
            const IColumn* head = nullptr;
            size_t head_row = 0;
            RETURN_IF_ERROR(best_head(s_view, s_index, i_view, i_index, d_view, d_index, type,
                                      &head, &head_row));
            if (head == nullptr) {
                break;
            }
            if (!effective_bound.is_null) {
                int cmp = compare_values(type, *head, head_row, *effective_bound.value,
                                         effective_bound.row);
                if (IsMin ? cmp > 0 : cmp < 0) {
                    // Everything left is outside the range whose counts both sides know exactly.
                    break;
                }
            }

            int64_t count_s = 0;
            int64_t count_i = 0;
            int64_t count_d = 0;
            RETURN_IF_ERROR(consume_equal(s_view, s_index, *head, head_row, type, &count_s));
            RETURN_IF_ERROR(consume_equal(i_view, i_index, *head, head_row, type, &count_i));
            RETURN_IF_ERROR(consume_equal(d_view, d_index, *head, head_row, type, &count_d));

            // A cancel pair can make an intermediate exceed int64 even though the final count
            // fits (for example S = INT64_MAX and I = D = 1), so evaluate the whole expression
            // in a wider type and range-check the result.
            const __int128 post_wide = static_cast<__int128>(count_s) + count_i - count_d;
            if (UNLIKELY(post_wide < 0)) {
                // Deleting a row that the pre-image did not have breaks the top-K invariant.
                return Status::InternalError(
                        "{}: negative post count {} for a value in the trusted range", name,
                        static_cast<int64_t>(post_wide));
            }
            if (UNLIKELY(post_wide > std::numeric_limits<int64_t>::max())) {
                return Status::InvalidArgument("{}: value count overflow", name);
            }
            const int64_t post = static_cast<int64_t>(post_wide);
            if (post > 0) {
                ++*positive;
                if (std::cmp_less(kept, k)) {
                    out_value_column.insert_from(*head, head_row);
                    out_count_column.get_data().push_back(post);
                    out_pair_nullable.get_null_map_data().push_back(0);
                    ++kept;
                }
                if (std::cmp_greater(*positive, k)) {
                    // pos > k is decided (a value was dropped); the rest only affects counts
                    // outside the kept prefix, so stop scanning.
                    break;
                }
            }
        }
        return Status::OK();
    }
};

} // namespace

void register_function_topk_merge(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionTopKMerge<true>>();
    factory.register_function<FunctionTopKMerge<false>>();
}

} // namespace doris
