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
#include <pdqsort.h>

#include "util/simd/bits.h"
#include "vec/core/block.h"
#include "vec/core/sort_description.h"

namespace doris::vectorized {

/// Sort one block by `description`. If limit != 0, then the partial sort of the first `limit` rows is produced.
void sort_block(Block& block, const SortDescription& description, UInt64 limit = 0);

/** Used only in StorageMergeTree to sort the data with INSERT.
  * Sorting is stable. This is important for keeping the order of rows in the CollapsingMergeTree engine
  *  - because based on the order of rows it is determined whether to delete or leave groups of rows when collapsing.
  * Collations are not supported. Partial sorting is not supported.
  */
void stable_sort_block(Block& block, const SortDescription& description);

/** Same as stable_sort_block, but do not sort the block, but only calculate the permutation of the values,
  *  so that you can rearrange the column values yourself.
  */
void stable_get_permutation(const Block& block, const SortDescription& description,
                            IColumn::Permutation& out_permutation);

/** Quickly check whether the block is already sorted. If the block is not sorted - returns false as fast as possible.
  * Collations are not supported.
  */
bool is_already_sorted(const Block& block, const SortDescription& description);

using ColumnWithSortDescription = std::pair<const IColumn*, SortColumnDescription>;

using ColumnsWithSortDescriptions = std::vector<ColumnWithSortDescription>;

ColumnsWithSortDescriptions get_columns_with_sort_description(const Block& block,
                                                              const SortDescription& description);

struct EqualRangeIterator {
    int range_begin;
    int range_end;

    EqualRangeIterator(const EqualFlags& flags) : EqualRangeIterator(flags, 0, flags.size()) {}

    EqualRangeIterator(const EqualFlags& flags, int begin, int end) : flags_(flags), end_(end) {
        range_begin = begin;
        range_end = end;
        cur_range_begin_ = begin;
        cur_range_end_ = end;
    }

    bool next() {
        if (cur_range_begin_ >= end_) {
            return false;
        }

        // `flags_[i]=1` indicates that the i-th row is equal to the previous row, which means we
        // should continue to sort this row according to current column. Using the first non-zero
        // value and first zero value after first non-zero value as two bounds, we can get an equal range here
        if (!(cur_range_begin_ == 0) || !(flags_[cur_range_begin_] == 1)) {
            cur_range_begin_ = simd::find_nonzero(flags_, cur_range_begin_ + 1);
            if (cur_range_begin_ >= end_) {
                return false;
            }
            cur_range_begin_--;
        }

        cur_range_end_ = simd::find_zero(flags_, cur_range_begin_ + 1);
        cur_range_end_ = std::min(cur_range_end_, end_);

        if (cur_range_begin_ >= cur_range_end_) {
            return false;
        }

        range_begin = cur_range_begin_;
        range_end = cur_range_end_;
        cur_range_begin_ = cur_range_end_;
        return true;
    }

private:
    int cur_range_begin_;
    int cur_range_end_;

    const EqualFlags& flags_;
    const int end_;
};

struct ColumnPartialSortingLess {
    const ColumnWithSortDescription& column_;

    explicit ColumnPartialSortingLess(const ColumnWithSortDescription& column) : column_(column) {}

    bool operator()(size_t a, size_t b) const {
        int res = column_.second.direction *
                  column_.first->compare_at(a, b, *column_.first, column_.second.nulls_direction);
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
    T inline_value_;
    uint32_t row_id_;

    PermutationWithInlineValue(T inline_value, uint32_t row_id)
            : inline_value_(inline_value), row_id_(row_id) {}

    PermutationWithInlineValue() : row_id_(-1) {}
};

template <typename T>
using PermutationForColumn = std::vector<PermutationWithInlineValue<T>>;

class ColumnSorter {
public:
    explicit ColumnSorter(const ColumnWithSortDescription& column, const int limit)
            : column_(column),
              limit_(limit),
              nulls_direction_(column.second.nulls_direction),
              direction_(column.second.direction) {}

    void operator()(EqualFlags& flags, IColumn::Permutation& perms, EqualRange& range,
                    bool last_column) const {
        column_.first->sort_column(this, flags, perms, range, last_column);
    }

    void sort_column(const IColumn& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        int new_limit = limit_;
        auto comparator = [&](const size_t a, const size_t b) {
            return column.compare_at(a, b, *column_.first, nulls_direction_);
        };
        ColumnPartialSortingLess less(column_);
        auto do_sort = [&](size_t first_iter, size_t last_iter) {
            auto begin = perms.begin() + first_iter;
            auto end = perms.begin() + last_iter;

            if (UNLIKELY(limit_ > 0 && first_iter < limit_ && limit_ <= last_iter)) {
                int n = limit_ - first_iter;
                std::partial_sort(begin, begin + n, end, less);

                auto nth = perms[limit_ - 1];
                size_t equal_count = 0;
                for (auto iter = begin + n; iter < end; iter++) {
                    if (comparator(*iter, nth) == 0) {
                        std::iter_swap(iter, begin + n + equal_count);
                        equal_count++;
                    }
                }
                new_limit = limit_ + equal_count;
            } else {
                pdqsort(begin, end, less);
            }
        };

        EqualRangeIterator iterator(flags, range.first, range.second);
        while (iterator.next()) {
            int range_begin = iterator.range_begin;
            int range_end = iterator.range_end;

            if (UNLIKELY(limit_ > 0 && range_begin > limit_)) {
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

    template <typename T>
    void sort_column(const ColumnVector<T>& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        int new_limit = limit_;
        if (!_should_inline_value(perms)) {
            auto comparator = [&](const size_t a, const size_t b) {
                return CompareHelper<T>::compare(column.get_data()[a], column.get_data()[b],
                                                 nulls_direction_);
            };

            auto sort_comparator = [&](const size_t a, const size_t b) {
                return CompareHelper<T>::compare(column.get_data()[a], column.get_data()[b],
                                                 nulls_direction_) *
                               direction_ <
                       0;
            };
            auto do_sort = [&](size_t first_iter, size_t last_iter) {
                auto begin = perms.begin() + first_iter;
                auto end = perms.begin() + last_iter;

                if (UNLIKELY(limit_ > 0 && first_iter < limit_ && limit_ <= last_iter)) {
                    int n = limit_ - first_iter;
                    std::partial_sort(begin, begin + n, end, sort_comparator);

                    auto nth = perms[limit_ - 1];
                    size_t equal_count = 0;
                    for (auto iter = begin + n; iter < end; iter++) {
                        if (comparator(*iter, nth) == 0) {
                            std::iter_swap(iter, begin + n + equal_count);
                            equal_count++;
                        }
                    }
                    new_limit = limit_ + equal_count;
                } else {
                    pdqsort(begin, end, sort_comparator);
                }
            };

            EqualRangeIterator iterator(flags, range.first, range.second);
            while (iterator.next()) {
                int range_begin = iterator.range_begin;
                int range_end = iterator.range_end;

                if (UNLIKELY(limit_ > 0 && range_begin > limit_)) {
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
        } else {
            // create inlined permutation
            PermutationForColumn<T> permutation_for_column(perms.size());
            _create_permutation(column, permutation_for_column.data(), perms);
            auto comparator = [&](const PermutationWithInlineValue<T>& a,
                                  const PermutationWithInlineValue<T>& b) {
                return CompareHelper<T>::compare(a.inline_value_, b.inline_value_,
                                                 nulls_direction_);
            };

            auto sort_comparator = [&](const PermutationWithInlineValue<T>& a,
                                       const PermutationWithInlineValue<T>& b) {
                return CompareHelper<T>::compare(a.inline_value_, b.inline_value_,
                                                 nulls_direction_) *
                               direction_ <
                       0;
            };
            auto do_sort = [&](size_t first_iter, size_t last_iter) {
                auto begin = permutation_for_column.begin() + first_iter;
                auto end = permutation_for_column.begin() + last_iter;

                if (UNLIKELY(limit_ > 0 && first_iter < limit_ && limit_ <= last_iter)) {
                    int n = limit_ - first_iter;
                    std::partial_sort(begin, begin + n, end, sort_comparator);

                    auto nth = permutation_for_column[limit_ - 1];
                    size_t equal_count = 0;
                    for (auto iter = begin + n; iter < end; iter++) {
                        if (comparator(*iter, nth) == 0) {
                            std::iter_swap(iter, begin + n + equal_count);
                            equal_count++;
                        }
                    }
                    new_limit = limit_ + equal_count;
                } else {
                    pdqsort(begin, end, sort_comparator);
                }
            };

            EqualRangeIterator iterator(flags, range.first, range.second);
            while (iterator.next()) {
                int range_begin = iterator.range_begin;
                int range_end = iterator.range_end;

                if (UNLIKELY(limit_ > 0 && range_begin > limit_)) {
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
    }

    template <typename T>
    void sort_column(const ColumnDecimal<T>& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        int new_limit = limit_;
        if (!_should_inline_value(perms)) {
            auto comparator = [&](const size_t a, const size_t b) {
                return column.get_data()[a] > column.get_data()[b]
                               ? 1
                               : (column.get_data()[a] < column.get_data()[b] ? -1 : 0);
            };

            auto sort_comparator = [&](const size_t a, const size_t b) {
                return comparator(a, b) * direction_ < 0;
            };
            auto do_sort = [&](size_t first_iter, size_t last_iter) {
                auto begin = perms.begin() + first_iter;
                auto end = perms.begin() + last_iter;

                if (UNLIKELY(limit_ > 0 && first_iter < limit_ && limit_ <= last_iter)) {
                    int n = limit_ - first_iter;
                    std::partial_sort(begin, begin + n, end, sort_comparator);

                    auto nth = perms[limit_ - 1];
                    size_t equal_count = 0;
                    for (auto iter = begin + n; iter < end; iter++) {
                        if (comparator(*iter, nth) == 0) {
                            std::iter_swap(iter, begin + n + equal_count);
                            equal_count++;
                        }
                    }
                    new_limit = limit_ + equal_count;
                } else {
                    pdqsort(begin, end, sort_comparator);
                }
            };

            EqualRangeIterator iterator(flags, range.first, range.second);
            while (iterator.next()) {
                int range_begin = iterator.range_begin;
                int range_end = iterator.range_end;

                if (UNLIKELY(limit_ > 0 && range_begin > limit_)) {
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
        } else {
            // create inlined permutation
            PermutationForColumn<T> permutation_for_column(perms.size());
            _create_permutation(column, permutation_for_column.data(), perms);
            auto comparator = [&](const PermutationWithInlineValue<T>& a,
                                  const PermutationWithInlineValue<T>& b) {
                return a.inline_value_ > b.inline_value_
                               ? 1
                               : (a.inline_value_ < b.inline_value_ ? -1 : 0);
            };

            auto sort_comparator = [&](const PermutationWithInlineValue<T>& a,
                                       const PermutationWithInlineValue<T>& b) {
                return comparator(a, b) * direction_ < 0;
            };
            auto do_sort = [&](size_t first_iter, size_t last_iter) {
                auto begin = permutation_for_column.begin() + first_iter;
                auto end = permutation_for_column.begin() + last_iter;

                if (UNLIKELY(limit_ > 0 && first_iter < limit_ && limit_ <= last_iter)) {
                    int n = limit_ - first_iter;
                    std::partial_sort(begin, begin + n, end, sort_comparator);

                    auto nth = permutation_for_column[limit_ - 1];
                    size_t equal_count = 0;
                    for (auto iter = begin + n; iter < end; iter++) {
                        if (comparator(*iter, nth) == 0) {
                            std::iter_swap(iter, begin + n + equal_count);
                            equal_count++;
                        }
                    }
                    new_limit = limit_ + equal_count;
                } else {
                    pdqsort(begin, end, sort_comparator);
                }
            };

            EqualRangeIterator iterator(flags, range.first, range.second);
            while (iterator.next()) {
                int range_begin = iterator.range_begin;
                int range_end = iterator.range_end;

                if (UNLIKELY(limit_ > 0 && range_begin > limit_)) {
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
    }

    void sort_column(const ColumnString& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        int new_limit = limit_;
        if (!_should_inline_value(perms)) {
            auto comparator = [&](const size_t a, const size_t b) {
                return column.compare_at(a, b, column, nulls_direction_);
            };

            auto sort_comparator = [&](const size_t a, const size_t b) {
                return column.compare_at(a, b, column, nulls_direction_) * direction_ < 0;
            };
            auto do_sort = [&](size_t first_iter, size_t last_iter) {
                auto begin = perms.begin() + first_iter;
                auto end = perms.begin() + last_iter;

                if (UNLIKELY(limit_ > 0 && first_iter < limit_ && limit_ <= last_iter)) {
                    int n = limit_ - first_iter;
                    std::partial_sort(begin, begin + n, end, sort_comparator);

                    auto nth = perms[limit_ - 1];
                    size_t equal_count = 0;
                    for (auto iter = begin + n; iter < end; iter++) {
                        if (comparator(*iter, nth) == 0) {
                            std::iter_swap(iter, begin + n + equal_count);
                            equal_count++;
                        }
                    }
                    new_limit = limit_ + equal_count;
                } else {
                    pdqsort(begin, end, sort_comparator);
                }
            };

            EqualRangeIterator iterator(flags, range.first, range.second);
            while (iterator.next()) {
                int range_begin = iterator.range_begin;
                int range_end = iterator.range_end;

                if (UNLIKELY(limit_ > 0 && range_begin > limit_)) {
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
        } else {
            // create inlined permutation
            PermutationForColumn<StringRef> permutation_for_column(perms.size());
            _create_permutation(column, permutation_for_column.data(), perms);
            auto comparator = [&](const PermutationWithInlineValue<StringRef>& a,
                                  const PermutationWithInlineValue<StringRef>& b) {
                return memcmp_small_allow_overflow15(a.inline_value_.data, a.inline_value_.size,
                                                     b.inline_value_.data, b.inline_value_.size);
            };

            auto sort_comparator = [&](const PermutationWithInlineValue<StringRef>& a,
                                       const PermutationWithInlineValue<StringRef>& b) {
                return memcmp_small_allow_overflow15(a.inline_value_.data, a.inline_value_.size,
                                                     b.inline_value_.data, b.inline_value_.size) *
                               direction_ <
                       0;
            };
            auto do_sort = [&](size_t first_iter, size_t last_iter) {
                auto begin = permutation_for_column.begin() + first_iter;
                auto end = permutation_for_column.begin() + last_iter;

                if (UNLIKELY(limit_ > 0 && first_iter < limit_ && limit_ <= last_iter)) {
                    int n = limit_ - first_iter;
                    std::partial_sort(begin, begin + n, end, sort_comparator);

                    auto nth = permutation_for_column[limit_ - 1];
                    size_t equal_count = 0;
                    for (auto iter = begin + n; iter < end; iter++) {
                        if (comparator(*iter, nth) == 0) {
                            std::iter_swap(iter, begin + n + equal_count);
                            equal_count++;
                        }
                    }
                    new_limit = limit_ + equal_count;
                } else {
                    pdqsort(begin, end, sort_comparator);
                }
            };

            EqualRangeIterator iterator(flags, range.first, range.second);
            while (iterator.next()) {
                int range_begin = iterator.range_begin;
                int range_end = iterator.range_end;

                if (UNLIKELY(limit_ > 0 && range_begin > limit_)) {
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
    }

    void sort_column(const ColumnNullable& column, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const {
        if (!column.has_null()) {
            column.get_nested_column().sort_column(this, flags, perms, range, last_column);
        } else {
            const auto& null_map = column.get_null_map_data();
            EqualRangeIterator iterator(flags, range.first, range.second);
            while (iterator.next()) {
                int range_begin = iterator.range_begin;
                int range_end = iterator.range_end;

                if (UNLIKELY(limit_ > 0 && range_begin > limit_)) {
                    break;
                }
                if (LIKELY(range_end - range_begin > 1)) {
                    int range_split = -1;
                    if (nulls_direction_) {
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
                    if (!nulls_direction_) {
                        std::swap(is_null_range, not_null_range);
                    }

                    if (not_null_range.first < not_null_range.second) {
                        flags[not_null_range.first] = 0;
                    }
                    if (range_begin <= is_null_range.first && is_null_range.first < range_end) {
                        std::fill(flags.begin() + is_null_range.first,
                                  flags.begin() + is_null_range.second, 1);

                        flags[is_null_range.first] = 0;
                    }
                }
            }

            column.get_nested_column().sort_column(this, flags, perms, range, last_column);
        }
    }

private:
    bool _should_inline_value(const IColumn::Permutation& perms) const {
        return limit_ == 0 || limit_ > (perms.size() / 5);
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
        if (limit_ < perms.size() && limit != 0) {
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
                permutation_for_column[i].inline_value_ = column.get_data()[row_id];
            } else if constexpr (std::is_same_v<ColumnType, ColumnString>) {
                permutation_for_column[i].inline_value_ = column.get_data_at(row_id);
            } else {
                static_assert(always_false_v<ColumnType>);
            }
            permutation_for_column[i].row_id_ = row_id;
        }
    }

    template <typename T>
    void _restore_permutation(const PermutationForColumn<T>& permutation_for_column,
                              size_t* __restrict perms) const {
        for (size_t i = 0; i < permutation_for_column.size(); i++) {
            perms[i] = permutation_for_column[i].row_id_;
        }
    }

    const ColumnWithSortDescription& column_;
    const int limit_;
    const int nulls_direction_;
    const int direction_;
};

} // namespace doris::vectorized
