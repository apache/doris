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
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/exprs/vexpr.h"

namespace doris::vectorized {

// Used to store a column along with its null_map and whether it is a const column.
// If the input column is not nullable, then null_map will be nullptr.
struct ColumnNullConstView {
    const IColumn& column;
    const NullMap* null_map;
    const bool is_const;

    static ColumnNullConstView create(const ColumnPtr& column_ptr) {
        const auto& [from_data_column, is_const] = unpack_if_const(column_ptr);

        if (const auto* nullable_column =
                    check_and_get_column<ColumnNullable>(from_data_column.get())) {
            return ColumnNullConstView {.column = nullable_column->get_nested_column(),
                                        .null_map = &nullable_column->get_null_map_data(),
                                        .is_const = is_const};
        } else {
            return ColumnNullConstView {
                    .column = *from_data_column, .null_map = nullptr, .is_const = is_const};
        }
    }
};

// Scalar version stores a reference to the actual data type array for convenient subsequent operations.
template <PrimitiveType PType>
struct ColumnNullConstViewScalar {
    using ColumnType = typename PrimitiveTypeTraits<PType>::ColumnType;
    using ArrayType = typename ColumnType::Container;

    const ArrayType& data;
    const NullMap* null_map;
    const bool is_const;

    static ColumnNullConstViewScalar create(const ColumnPtr& column_ptr) {
        const auto& [from_data_column, is_const] = unpack_if_const(column_ptr);
        const NullMap* null_map = nullptr;
        const ArrayType* data = nullptr;
        if (const auto* nullable_column =
                    check_and_get_column<ColumnNullable>(from_data_column.get())) {
            null_map = &nullable_column->get_null_map_data();
            const auto& nested_from_column = nullable_column->get_nested_column();
            data = &(assert_cast<const ColumnType&>(nested_from_column).get_data());
        } else {
            data = &(assert_cast<const ColumnType&>(*from_data_column).get_data());
        }

        return ColumnNullConstViewScalar {
                .data = *data, .null_map = null_map, .is_const = is_const};
    }
};

// Used to store a mutable column along with its null_map.
// If the input column is not nullable, then null_map will be nullptr.
struct MutableColumnNullView {
    IColumn& column;
    NullMap* null_map;
    static MutableColumnNullView create(const MutableColumnPtr& column_ptr) {
        if (auto* nullable_column = check_and_get_column<ColumnNullable>(column_ptr.get())) {
            return MutableColumnNullView {.column = nullable_column->get_nested_column(),
                                          .null_map = &nullable_column->get_null_map_data()};
        } else {
            return MutableColumnNullView {.column = *column_ptr, .null_map = nullptr};
        }
    }

    void insert_from(const ColumnNullConstView& from, size_t i) {
        auto index = index_check_const(i, from.is_const);
        column.insert_from(from.column, index);
        if (null_map != nullptr && from.null_map != nullptr) {
            null_map->push_back((*from.null_map)[index]);
        } else if (null_map != nullptr) {
            // from is not nullable, so insert 0
            null_map->push_back(0);
        }
    }

    void insert_null() {
        if (null_map == nullptr) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "Cannot insert null value into non-nullable column in "
                                   "ShortCircuitCoalesceExpr.");
        }
        column.insert_default();
        null_map->push_back(1);
    }
};

template <PrimitiveType PType>
struct MutableColumnNullViewScalar {
    using ColumnType = typename PrimitiveTypeTraits<PType>::ColumnType;
    using ArrayType = typename ColumnType::Container;

    ArrayType& data;
    NullMap* null_map;

    static MutableColumnNullViewScalar create(const MutableColumnPtr& column_ptr) {
        ColumnType* data_column = nullptr;
        NullMap* null_map = nullptr;
        if (auto* nullable_column = check_and_get_column<ColumnNullable>(column_ptr.get())) {
            null_map = &nullable_column->get_null_map_data();
            auto& nested_column = nullable_column->get_nested_column();
            data_column = &(assert_cast<ColumnType&>(nested_column));
        } else {
            data_column = assert_cast<ColumnType*>(column_ptr.get());
        }

        return MutableColumnNullViewScalar {.data = data_column->get_data(), .null_map = null_map};
    }

    void insert_from(const ColumnNullConstViewScalar<PType>& from, const Selector& selector) {
        auto& result_data = data;
        const auto& from_data = from.data;

        auto* result_null_map_data = null_map;
        const auto* from_null_map_data = from.null_map;

        if (from.is_const) {
            insert_into_result<true>(result_data, from_data, result_null_map_data,
                                     from_null_map_data, selector);
        } else {
            insert_into_result<false>(result_data, from_data, result_null_map_data,
                                      from_null_map_data, selector);
        }
    }

    template <bool is_const>
    static void insert_into_result(ArrayType& result_data, const ArrayType& from_data,
                                   NullMap* result_null_map_data, const NullMap* from_null_map_data,
                                   const Selector& selector) {
        insert_with_selector<is_const>(result_data, from_data, selector);
        if (result_null_map_data != nullptr && from_null_map_data != nullptr) {
            insert_with_selector<is_const>(*result_null_map_data, *from_null_map_data, selector);
        }
        // Note: When from_null_map_data is nullptr (non-nullable source),
        // no action needed since result null_map is already initialized to 0 (non-null)
        // by init_result_column's resize_fill(count, 0)
    }

    template <bool is_const>
    static void insert_with_selector(auto& result_data, const auto& data,
                                     const Selector& selector) {
        for (size_t i = 0; i < selector.size(); ++i) {
            auto index = selector[i];
            result_data[index] = data[index_check_const<is_const>(i)];
        }
    }

    void insert_null(const Selector& selector) {
        if (selector.empty()) {
            return;
        }
        DCHECK(null_map != nullptr)
                << "Cannot insert null value into non-nullable column in ShortCircuitCoalesceExpr.";

        auto& null_map_data = *null_map;
        for (size_t i = 0; i < selector.size(); ++i) {
            null_map_data[selector[i]] = 1;
        }
    }
};

// Used to store a column and its corresponding selector.
// It can be understood as: the positions selected by the selector in the result column correspond to the column inside.
// column = filter_column_with_selector(result_column, selector)
// What we need to do is fill these positions back into the result column (in a sense, it can be seen as restoring them).
// If the column is empty, it means these positions are all null, e.g., the last parameter of coalesce is null, or the else branch of case when is not provided.
struct ColumnAndSelector {
    ColumnPtr column = nullptr;
    Selector selector; // positions in result column

    bool output_nulls() const { return column.get() == nullptr; }

    std::string debug_string() const {
        std::stringstream ss;
        ss << "ColumnAndSelector(selector_size=" << selector.size()
           << ", output_nulls=" << output_nulls()
           << ", column size=" << (column ? std::to_string(column->size()) : "null") << ")";

        ss << "\n selector data: [";
        for (size_t i = 0; i < selector.size(); ++i) {
            if (i != 0) {
                ss << ", ";
            }
            ss << selector[i];
        }
        ss << "]";
        return ss.str();
    }
};

// Scalar version of fill.
// Initializes the result column at the beginning.
// Subsequently fills data from each column into the result column using selectors.
template <PrimitiveType PType>
struct ScalarFillWithSelector {
    using ColumnType = typename PrimitiveTypeTraits<PType>::ColumnType;
    using ArrayType = typename ColumnType::Container;

public:
    static ColumnPtr fill(const DataTypePtr& result_type, const ColumnPtr& true_column,
                          const Selector& true_selector, const ColumnPtr& false_column,
                          const Selector& false_selector, size_t count) {
        DCHECK_EQ(false_selector.size() + true_selector.size(), count);
        DCHECK_EQ(true_column->size(), true_selector.size());
        DCHECK_EQ(false_column->size(), false_selector.size());
        DCHECK_EQ(PType, result_type->get_primitive_type());

        auto result_column = result_type->create_column();

        MutableColumnNullViewScalar<PType> result_column_view =
                MutableColumnNullViewScalar<PType>::create(result_column);
        init_result_column(result_column_view, count);

        ColumnNullConstViewScalar<PType> true_column_view =
                ColumnNullConstViewScalar<PType>::create(true_column);
        ColumnNullConstViewScalar<PType> false_column_view =
                ColumnNullConstViewScalar<PType>::create(false_column);

        result_column_view.insert_from(true_column_view, true_selector);
        result_column_view.insert_from(false_column_view, false_selector);
        DCHECK_EQ(result_column->size(), count);
        return result_column;
    }

    static ColumnPtr fill(const DataTypePtr& result_type,
                          const std::vector<ColumnAndSelector>& columns_and_selectors,
                          size_t count) {
        DCHECK_EQ(count, std::accumulate(columns_and_selectors.begin(), columns_and_selectors.end(),
                                         0ULL, [](size_t sum, const ColumnAndSelector& cs) {
                                             return sum + cs.selector.size();
                                         }));
        DCHECK_EQ(PType, result_type->get_primitive_type());
        auto result_column = result_type->create_column();

        MutableColumnNullViewScalar<PType> result_column_view =
                MutableColumnNullViewScalar<PType>::create(result_column);
        init_result_column(result_column_view, count);

        for (const auto& columns_and_selector : columns_and_selectors) {
            if (columns_and_selector.output_nulls()) {
                result_column_view.insert_null(columns_and_selector.selector);
            } else {
                ColumnNullConstViewScalar<PType> from_column_view =
                        ColumnNullConstViewScalar<PType>::create(columns_and_selector.column);
                result_column_view.insert_from(from_column_view, columns_and_selector.selector);
            }
        }
        DCHECK_EQ(result_column->size(), count);
        return result_column;
    }

private:
    // if result_column is nullable,nullmap will all init to false
    static void init_result_column(MutableColumnNullViewScalar<PType>& result_column_view,
                                   size_t count) {
        result_column_view.data.resize(count);
        if (result_column_view.null_map != nullptr) {
            result_column_view.null_map->resize_fill(count, 0);
        }
    }
};

// Non-scalar version of fill.
// For types that do not support random access, such as string, array, map, etc.
struct NonScalarFillWithSelector {
    static ColumnPtr fill(const DataTypePtr& result_type, const ColumnPtr& true_column,
                          const Selector& true_selector, const ColumnPtr& false_column,
                          const Selector& false_selector, size_t count) {
        DCHECK_EQ(false_selector.size() + true_selector.size(), count)
                << "Mismatched selector sizes."
                << " false selector size: " << false_selector.size()
                << ", true selector size: " << true_selector.size() << ", count: " << count;
        DCHECK_EQ(true_column->size(), true_selector.size());
        DCHECK_EQ(false_column->size(), false_selector.size());

        auto result_column = result_type->create_column();

        MutableColumnNullView result_column_view = MutableColumnNullView::create(result_column);
        ColumnNullConstView true_column_view = ColumnNullConstView::create(true_column);
        ColumnNullConstView false_column_view = ColumnNullConstView::create(false_column);

        size_t true_index = 0;
        size_t false_index = 0;
        for (size_t i = 0; i < count; ++i) {
            if (true_index < true_selector.size() && i == true_selector[true_index]) {
                result_column_view.insert_from(true_column_view, true_index++);
            } else {
                result_column_view.insert_from(false_column_view, false_index++);
            }
        }

        DCHECK_EQ(result_column->size(), count);
        return result_column;
    }

    static ColumnPtr fill(const DataTypePtr& result_type,
                          const std::vector<ColumnAndSelector>& columns_and_selectors,
                          size_t count) {
        DCHECK_EQ(count, std::accumulate(columns_and_selectors.begin(), columns_and_selectors.end(),
                                         0ULL, [](size_t sum, const ColumnAndSelector& cs) {
                                             return sum + cs.selector.size();
                                         }));
        struct FillColumnWithPos {
            std::optional<ColumnNullConstView> source_column;
            size_t pos_in_source; // position in the column

            void insert_to_column(MutableColumnNullView& result_column) const {
                if (!source_column) {
                    result_column.insert_null();
                } else {
                    result_column.insert_from(*source_column, pos_in_source);
                }
            }
        };

        auto mutable_result_column = result_type->create_column();
        mutable_result_column->reserve(count);

        MutableColumnNullView mutable_result_column_view =
                MutableColumnNullView::create(mutable_result_column);

        std::vector<FillColumnWithPos> fill_positions(count);

        for (const ColumnAndSelector& column_with_selector : columns_and_selectors) {
            if (column_with_selector.selector.empty()) {
                continue;
            }
            for (size_t i = 0; i < column_with_selector.selector.size(); ++i) {
                size_t result_index = column_with_selector.selector[i];
                DCHECK(fill_positions[result_index].source_column.has_value() == false)
                        << "Position " << result_index << " has been filled already.";
                if (column_with_selector.column) {
                    ColumnNullConstView column_view =
                            ColumnNullConstView::create(column_with_selector.column);
                    fill_positions[result_index].source_column.emplace(column_view);
                    fill_positions[result_index].pos_in_source = i;
                } else {
                    fill_positions[result_index].source_column = std::nullopt;
                }
            }
        }

        for (const FillColumnWithPos& fill_pos : fill_positions) {
            fill_pos.insert_to_column(mutable_result_column_view);
        }

        DCHECK_EQ(mutable_result_column->size(), count);
        return mutable_result_column;
    }
};

struct ConditionColumnViewHelper {
    ConditionColumnViewHelper(const Selector* selector, size_t count)
            : _selector(selector), _count(count) {}

    // Iterate over the condition column and generate true_selector and false_selector
    // based on null_map and data.

    template <bool is_const, typename Func>
    void for_each_with_selector(Func& f) const {
        if (_selector != nullptr) {
            const auto& selector_data = *_selector;
            for (size_t i = 0; i < _count; ++i) {
                f(index_check_const<is_const>(i), i, selector_data[i]);
            }
        } else {
            for (size_t i = 0; i < _count; ++i) {
                f(index_check_const<is_const>(i), i, i);
            }
        }
    }

private:
    const Selector* _selector;
    const size_t _count;
};

// Utility class for columns that return boolean values.
// We care about whether the value is null, as well as true/false values.
struct ConditionColumnView : ColumnNullConstViewScalar<TYPE_BOOLEAN>, ConditionColumnViewHelper {
    ConditionColumnView(ColumnNullConstViewScalar<TYPE_BOOLEAN> base, const Selector* selector,
                        size_t count)
            : ColumnNullConstViewScalar<TYPE_BOOLEAN>(base),
              ConditionColumnViewHelper(selector, count) {}

    static ConditionColumnView create(const ColumnPtr& column_ptr, const Selector* selector,
                                      size_t count) {
        DCHECK_EQ(selector == nullptr ? count : selector->size(), count);
        return {ColumnNullConstViewScalar<TYPE_BOOLEAN>::create(column_ptr), selector, count};
    }

    template <typename NullFunc, typename TrueFunc, typename FalseFunc>
    void for_each(NullFunc& null_func, TrueFunc& true_func, FalseFunc& false_func) const {
        if (this->null_map != nullptr) {
            const auto& null_map_data = *(this->null_map);
            auto update = [&](size_t i, size_t self_index, size_t executor_index) {
                if (null_map_data[i]) {
                    null_func(self_index, executor_index);
                } else {
                    if (this->data[i]) {
                        true_func(self_index, executor_index);
                    } else {
                        false_func(self_index, executor_index);
                    }
                }
            };
            if (is_const) {
                for_each_with_selector<true>(update);
            } else {
                for_each_with_selector<false>(update);
            }
        } else {
            // non-nullable condition column
            auto update = [&](size_t i, size_t self_index, size_t executor_index) {
                if (this->data[i]) {
                    true_func(self_index, executor_index);
                } else {
                    false_func(self_index, executor_index);
                }
            };
            if (is_const) {
                for_each_with_selector<true>(update);
            } else {
                for_each_with_selector<false>(update);
            }
        }
    }
};

// Utility class for columns that return nullable values.
// We only care about whether the value is null, not the actual value.
struct ConditionColumnNullView : ColumnNullConstView, ConditionColumnViewHelper {
    ConditionColumnNullView(ColumnNullConstView base, const Selector* selector, size_t count)
            : ColumnNullConstView(base), ConditionColumnViewHelper(selector, count) {}

    static ConditionColumnNullView create(const ColumnPtr& column_ptr, const Selector* selector,
                                          size_t count) {
        DCHECK_EQ(selector == nullptr ? count : selector->size(), count);
        return {ColumnNullConstView::create(column_ptr), selector, count};
    }

    template <typename NullFunc, typename NotNullFunc>
    void for_each(NullFunc& null_func, NotNullFunc& not_null_func) const {
        if (this->null_map != nullptr) {
            const auto& null_map_data = *(this->null_map);
            auto update = [&](size_t i, size_t self_index, size_t executor_index) {
                if (null_map_data[i]) {
                    null_func(self_index, executor_index);
                } else {
                    not_null_func(self_index, executor_index);
                }
            };
            if (is_const) {
                for_each_with_selector<true>(update);
            } else {
                for_each_with_selector<false>(update);
            }
        } else {
            // non-nullable condition column
            auto update = [&](size_t i, size_t self_index, size_t executor_index) {
                not_null_func(self_index, executor_index);
            };
            if (is_const) {
                for_each_with_selector<true>(update);
            } else {
                for_each_with_selector<false>(update);
            }
        }
    }
};

} // namespace doris::vectorized