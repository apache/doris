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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Interpreters/sortBlock.h
// and modified by Doris

#pragma once
#include <glog/logging.h>
#include <pdqsort.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "util/simd/bits.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/memcmp_small.h"
#include "vec/common/string_ref.h"
#include "vec/core/block.h"
#include "vec/core/sort_description.h"
#include "vec/core/types.h"

namespace doris {
namespace vectorized {
template <typename T>
class ColumnDecimal;
template <typename>
class ColumnVector;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

/// Sort one block by `description`. If limit != 0, then the partial sort of the first `limit` rows is produced.
void sort_block(Block& src_block, Block& dest_block, const SortDescription& description,
                UInt64 limit = 0);

using ColumnWithSortDescription = std::pair<const IColumn*, SortColumnDescription>;

using ColumnsWithSortDescriptions = std::vector<ColumnWithSortDescription>;

ColumnsWithSortDescriptions get_columns_with_sort_description(const Block& block,
                                                              const SortDescription& description);

struct EqualRangeIterator {
    int range_begin;
    int range_end;

    EqualRangeIterator(const EqualFlags& flags) : EqualRangeIterator(flags, 0, flags.size()) {}

    EqualRangeIterator(const EqualFlags& flags, int begin, int end) : _flags(flags), _end(end) {
        range_begin = begin;
        range_end = end;
        _cur_range_begin = begin;
        _cur_range_end = end;
    }

    bool next() {
        if (_cur_range_begin >= _end) {
            return false;
        }

        // `_flags[i]=1` indicates that the i-th row is equal to the previous row, which means we
        // should continue to sort this row according to current column. Using the first non-zero
        // value and first zero value after first non-zero value as two bounds, we can get an equal range here
        if (!(_cur_range_begin == 0) || !(_flags[_cur_range_begin] == 1)) {
            _cur_range_begin = simd::find_one(_flags, _cur_range_begin + 1);
            if (_cur_range_begin >= _end) {
                return false;
            }
            _cur_range_begin--;
        }

        _cur_range_end = simd::find_zero(_flags, _cur_range_begin + 1);
        DCHECK(_cur_range_end <= _end);

        if (_cur_range_begin >= _cur_range_end) {
            return false;
        }

        range_begin = _cur_range_begin;
        range_end = _cur_range_end;
        _cur_range_begin = _cur_range_end;
        return true;
    }

private:
    int _cur_range_begin;
    int _cur_range_end;

    const EqualFlags& _flags;
    const int _end;
};

struct ColumnPartialSortingLess {
    const ColumnWithSortDescription& _column_with_sort_desc;

    explicit ColumnPartialSortingLess(const ColumnWithSortDescription& column)
            : _column_with_sort_desc(column) {}

    bool operator()(size_t a, size_t b) const {
        int res = _column_with_sort_desc.second.direction *
                  _column_with_sort_desc.first->compare_at(
                          a, b, *_column_with_sort_desc.first,
                          _column_with_sort_desc.second.nulls_direction);
        if (res < 0) {
            return true;
        } else if (res > 0) {
            return false;
        }
        return false;
    }
};

template <typename T>
struct PermutationWithInlineValue {
    T inline_value;
    uint32_t row_id;
};

template <typename T>
using PermutationForColumn = std::vector<PermutationWithInlineValue<T>>;

class ColumnSorter {
public:
    explicit ColumnSorter(const ColumnWithSortDescription& column, const int limit)
            : _column_with_sort_desc(column),
              _limit(limit),
              _nulls_direction(column.second.nulls_direction),
              _direction(column.second.direction) {}

    void operator()(EqualFlags& flags, IColumn::Permutation& perms, EqualRange& range,
                    bool last_column) const {
        _column_with_sort_desc.first->sort_column(this, flags, perms, range, last_column);
    }

    void sort_column(const IColumn& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        int new_limit = _limit;
        auto comparator = [&](const size_t a, const size_t b) {
            return column.compare_at(a, b, *_column_with_sort_desc.first, _nulls_direction);
        };
        ColumnPartialSortingLess less(_column_with_sort_desc);
        auto do_sort = [&](size_t first_iter, size_t last_iter) {
            auto begin = perms.begin() + first_iter;
            auto end = perms.begin() + last_iter;

            if (UNLIKELY(_limit > 0 && first_iter < _limit && _limit <= last_iter)) {
                int n = _limit - first_iter;
                std::partial_sort(begin, begin + n, end, less);

                auto nth = perms[_limit - 1];
                size_t equal_count = 0;
                for (auto iter = begin + n; iter < end; iter++) {
                    if (comparator(*iter, nth) == 0) {
                        std::iter_swap(iter, begin + n + equal_count);
                        equal_count++;
                    }
                }
                new_limit = _limit + equal_count;
            } else {
                pdqsort(begin, end, less);
            }
        };

        EqualRangeIterator iterator(flags, range.first, range.second);
        while (iterator.next()) {
            int range_begin = iterator.range_begin;
            int range_end = iterator.range_end;

            if (UNLIKELY(_limit > 0 && range_begin > _limit)) {
                break;
            }
            if (LIKELY(range_end - range_begin > 1)) {
                do_sort(range_begin, range_end);
                if (!last_column) {
                    flags[range_begin] = 0;
                    for (int i = range_begin + 1; i < range_end; i++) {
                        flags[i] &= comparator(perms[i - 1], perms[i]) == 0;
                    }
                }
            }
        }
        _shrink_to_fit(perms, flags, new_limit);
    }

    template <template <typename type> typename ColumnType, typename T>
    void sort_column(const ColumnType<T>& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        if (!_should_inline_value(perms)) {
            _sort_by_default(column, flags, perms, range, last_column);
        } else {
            _sort_by_inlined_permutation<T>(column, flags, perms, range, last_column);
        }
    }

    void sort_column(const ColumnString& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        if (!_should_inline_value(perms)) {
            _sort_by_default(column, flags, perms, range, last_column);
        } else {
            _sort_by_inlined_permutation<StringRef>(column, flags, perms, range, last_column);
        }
    }

    void sort_column(const ColumnString64& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        if (!_should_inline_value(perms)) {
            _sort_by_default(column, flags, perms, range, last_column);
        } else {
            _sort_by_inlined_permutation<StringRef>(column, flags, perms, range, last_column);
        }
    }

    void sort_column(const ColumnNullable& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        if (!column.has_null()) {
            column.get_nested_column().sort_column(this, flags, perms, range, last_column);
        } else {
            const auto& null_map = column.get_null_map_data();
            int limit = _limit;
            std::vector<std::pair<size_t, size_t>> is_null_ranges;
            EqualRangeIterator iterator(flags, range.first, range.second);
            while (iterator.next()) {
                int range_begin = iterator.range_begin;
                int range_end = iterator.range_end;

                if (UNLIKELY(_limit > 0 && range_begin > _limit)) {
                    break;
                }
                bool null_first = _nulls_direction * _direction < 0;
                if (LIKELY(range_end - range_begin > 1)) {
                    int range_split = 0;
                    if (null_first) {
                        range_split = std::partition(perms.begin() + range_begin,
                                                     perms.begin() + range_end,
                                                     [&](size_t row_id) -> bool {
                                                         return null_map[row_id] != 0;
                                                     }) -
                                      perms.begin();
                    } else {
                        range_split = std::partition(perms.begin() + range_begin,
                                                     perms.begin() + range_end,
                                                     [&](size_t row_id) -> bool {
                                                         return null_map[row_id] == 0;
                                                     }) -
                                      perms.begin();
                    }
                    std::pair<size_t, size_t> is_null_range = {range_begin, range_split};
                    std::pair<size_t, size_t> not_null_range = {range_split, range_end};
                    if (!null_first) {
                        std::swap(is_null_range, not_null_range);
                    }

                    if (not_null_range.first < not_null_range.second) {
                        flags[not_null_range.first] = 0;
                    }
                    if (is_null_range.first < is_null_range.second) {
                        // do not sort null values
                        std::fill(flags.begin() + is_null_range.first,
                                  flags.begin() + is_null_range.second, 0);

                        if (UNLIKELY(_limit > is_null_range.first &&
                                     _limit <= is_null_range.second)) {
                            _limit = is_null_range.second;
                        }
                        is_null_ranges.push_back(std::move(is_null_range));
                    }
                }
            }

            column.get_nested_column().sort_column(this, flags, perms, range, last_column);
            _limit = limit;
            if (!last_column) {
                for (const auto& nr : is_null_ranges) {
                    std::fill(flags.begin() + nr.first + 1, flags.begin() + nr.second, 1);
                }
            }
        }
    }

private:
    bool _should_inline_value(const IColumn::Permutation& perms) const {
        return _limit == 0 || _limit > (perms.size() / 5);
    }

    template <typename T>
    void _shrink_to_fit(PermutationForColumn<T>& permutation_for_column,
                        IColumn::Permutation& perms, EqualFlags& flags, int limit) const {
        if (limit < perms.size() && limit != 0) {
            permutation_for_column.resize(limit);
            perms.resize(limit);
            flags.resize(limit);
        }
    }

    void _shrink_to_fit(IColumn::Permutation& perms, EqualFlags& flags, int limit) const {
        if (_limit < perms.size() && limit != 0) {
            perms.resize(limit);
            flags.resize(limit);
        }
    }

    template <typename ColumnType>
    static constexpr bool always_false_v = false;

    template <typename ColumnType, typename T>
    void _create_permutation(const ColumnType& column,
                             PermutationWithInlineValue<T>* __restrict permutation_for_column,
                             const IColumn::Permutation& perms) const {
        for (size_t i = 0; i < perms.size(); i++) {
            size_t row_id = perms[i];
            if constexpr (std::is_same_v<ColumnType, ColumnVector<T>> ||
                          std::is_same_v<ColumnType, ColumnDecimal<T>>) {
                permutation_for_column[i].inline_value = column.get_data()[row_id];
            } else if constexpr (std::is_same_v<ColumnType, ColumnString> ||
                                 std::is_same_v<ColumnType, ColumnString64>) {
                permutation_for_column[i].inline_value = column.get_data_at(row_id);
            } else {
                static_assert(always_false_v<ColumnType>);
            }
            permutation_for_column[i].row_id = row_id;
        }
    }

    template <typename ColumnType>
    void _sort_by_default(const ColumnType& column, EqualFlags& flags, IColumn::Permutation& perms,
                          EqualRange& range, bool last_column) const {
        int new_limit = _limit;
        auto comparator = [&](const size_t a, const size_t b) {
            if constexpr (!std::is_same_v<ColumnType, ColumnString> &&
                          !std::is_same_v<ColumnType, ColumnString64>) {
                auto value_a = column.get_data()[a];
                auto value_b = column.get_data()[b];
                return value_a > value_b ? 1 : (value_a < value_b ? -1 : 0);
            } else {
                return column.compare_at(a, b, column, _nulls_direction);
            }
        };

        auto sort_comparator = [&](const size_t a, const size_t b) {
            return comparator(a, b) * _direction < 0;
        };
        auto do_sort = [&](size_t first_iter, size_t last_iter) {
            auto begin = perms.begin() + first_iter;
            auto end = perms.begin() + last_iter;

            if (UNLIKELY(_limit > 0 && first_iter < _limit && _limit <= last_iter)) {
                int n = _limit - first_iter;
                std::partial_sort(begin, begin + n, end, sort_comparator);

                auto nth = perms[_limit - 1];
                size_t equal_count = 0;
                for (auto iter = begin + n; iter < end; iter++) {
                    if (comparator(*iter, nth) == 0) {
                        std::iter_swap(iter, begin + n + equal_count);
                        equal_count++;
                    }
                }
                new_limit = _limit + equal_count;
            } else {
                pdqsort(begin, end, sort_comparator);
            }
        };

        EqualRangeIterator iterator(flags, range.first, range.second);
        while (iterator.next()) {
            int range_begin = iterator.range_begin;
            int range_end = iterator.range_end;

            if (UNLIKELY(_limit > 0 && range_begin > _limit)) {
                break;
            }
            if (LIKELY(range_end - range_begin > 1)) {
                do_sort(range_begin, range_end);
                if (!last_column) {
                    flags[range_begin] = 0;
                    for (int i = range_begin + 1; i < range_end; i++) {
                        flags[i] &= comparator(perms[i - 1], perms[i]) == 0;
                    }
                }
            }
        }
        _shrink_to_fit(perms, flags, new_limit);
    }

    template <typename InlineType, typename ColumnType>
    void _sort_by_inlined_permutation(const ColumnType& column, EqualFlags& flags,
                                      IColumn::Permutation& perms, EqualRange& range,
                                      bool last_column) const {
        int new_limit = _limit;
        // create inlined permutation
        PermutationForColumn<InlineType> permutation_for_column(perms.size());
        _create_permutation(column, permutation_for_column.data(), perms);
        auto comparator = [&](const PermutationWithInlineValue<InlineType>& a,
                              const PermutationWithInlineValue<InlineType>& b) {
            if constexpr (!std::is_same_v<ColumnType, ColumnString>) {
                return a.inline_value > b.inline_value ? 1
                                                       : (a.inline_value < b.inline_value ? -1 : 0);
            } else {
                return memcmp_small_allow_overflow15(
                        reinterpret_cast<const UInt8*>(a.inline_value.data), a.inline_value.size,
                        reinterpret_cast<const UInt8*>(b.inline_value.data), b.inline_value.size);
            }
        };

        auto sort_comparator = [&](const PermutationWithInlineValue<InlineType>& a,
                                   const PermutationWithInlineValue<InlineType>& b) {
            return comparator(a, b) * _direction < 0;
        };
        auto do_sort = [&](size_t first_iter, size_t last_iter) {
            auto begin = permutation_for_column.begin() + first_iter;
            auto end = permutation_for_column.begin() + last_iter;

            if (UNLIKELY(_limit > 0 && first_iter < _limit && _limit <= last_iter)) {
                int n = _limit - first_iter;
                std::partial_sort(begin, begin + n, end, sort_comparator);

                auto nth = permutation_for_column[_limit - 1];
                size_t equal_count = 0;
                for (auto iter = begin + n; iter < end; iter++) {
                    if (comparator(*iter, nth) == 0) {
                        std::iter_swap(iter, begin + n + equal_count);
                        equal_count++;
                    }
                }
                new_limit = _limit + equal_count;
            } else {
                pdqsort(begin, end, sort_comparator);
            }
        };

        EqualRangeIterator iterator(flags, range.first, range.second);
        while (iterator.next()) {
            int range_begin = iterator.range_begin;
            int range_end = iterator.range_end;

            if (UNLIKELY(_limit > 0 && range_begin > _limit)) {
                break;
            }
            if (LIKELY(range_end - range_begin > 1)) {
                do_sort(range_begin, range_end);
                if (!last_column) {
                    flags[range_begin] = 0;
                    for (int i = range_begin + 1; i < range_end; i++) {
                        flags[i] &= comparator(permutation_for_column[i - 1],
                                               permutation_for_column[i]) == 0;
                    }
                }
            }
        }
        _shrink_to_fit(permutation_for_column, perms, flags, new_limit);
        // restore `perms` from `permutation_for_column`
        _restore_permutation(permutation_for_column, perms.data());
    }

    template <typename T>
    void _restore_permutation(const PermutationForColumn<T>& permutation_for_column,
                              size_t* __restrict perms) const {
        for (size_t i = 0; i < permutation_for_column.size(); i++) {
            perms[i] = permutation_for_column[i].row_id;
        }
    }

    const ColumnWithSortDescription& _column_with_sort_desc;
    mutable int _limit;
    const int _nulls_direction;
    const int _direction;
};

} // namespace doris::vectorized
