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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnStruct.cpp
// and modified by Doris

#include "vec/columns/column_struct.h"

namespace doris::vectorized {

namespace ErrorCodes {
extern const int ILLEGAL_COLUMN;
extern const int NOT_IMPLEMENTED;
extern const int CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE;
extern const int LOGICAL_ERROR;
} // namespace ErrorCodes

std::string ColumnStruct::get_name() const {
    std::stringstream res;
    res << "Struct(";
    bool is_first = true;
    for (const auto& column : columns) {
        if (!is_first) {
            res << ", ";
        }
        is_first = false;
        res << column->get_name();
    }
    res << ")";
    return res.str();
}

ColumnStruct::ColumnStruct(MutableColumns&& mutable_columns) {
    columns.reserve(mutable_columns.size());
    for (auto& column : mutable_columns) {
        if (is_column_const(*column)) {
            throw Exception {"ColumnStruct cannot have ColumnConst as its element",
                             ErrorCodes::ILLEGAL_COLUMN};
        }
        columns.push_back(std::move(column));
    }
}

ColumnStruct::ColumnStruct(Columns&& columns) {
    columns.reserve(columns.size());
    for (auto& column : columns) {
        if (is_column_const(*column)) {
            throw Exception {"ColumnStruct cannot have ColumnConst as its element",
                             ErrorCodes::ILLEGAL_COLUMN};
        }
        columns.push_back(std::move(column));
    }
}

ColumnStruct::ColumnStruct(TupleColumns&& tuple_columns) {
    columns.reserve(tuple_columns.size());
    for (auto& column : tuple_columns) {
        if (is_column_const(*column)) {
            throw Exception {"ColumnStruct cannot have ColumnConst as its element",
                             ErrorCodes::ILLEGAL_COLUMN};
        }
        columns.push_back(std::move(column));
    }
}

ColumnStruct::Ptr ColumnStruct::create(Columns& columns) {
    for (const auto& column : columns) {
        if (is_column_const(*column))
            throw Exception {"ColumnStruct cannot have ColumnConst as its element",
                             ErrorCodes::ILLEGAL_COLUMN};
    }
    auto column_struct = ColumnStruct::create(columns);
    return column_struct;
}

ColumnStruct::Ptr ColumnStruct::create(TupleColumns& tuple_columns) {
    for (const auto& column : tuple_columns) {
        if (is_column_const(*column)) {
            throw Exception {"ColumnStruct cannot have ColumnConst as its element",
                             ErrorCodes::ILLEGAL_COLUMN};
        }
    }
    auto column_struct = ColumnStruct::create(tuple_columns);
    return column_struct;
}

MutableColumnPtr ColumnStruct::clone_empty() const {
    const size_t tuple_size = columns.size();
    MutableColumns new_columns(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i) {
        new_columns[i] = columns[i]->clone_empty();
    }
    return ColumnStruct::create(std::move(new_columns));
}

MutableColumnPtr ColumnStruct::clone_resized(size_t new_size) const {
    const size_t tuple_size = columns.size();
    MutableColumns new_columns(tuple_size);
    for (size_t i = 0; i < tuple_size; ++i) {
        new_columns[i] = columns[i]->clone_resized(new_size);
    }
    return ColumnStruct::create(std::move(new_columns));
}

Field ColumnStruct::operator[](size_t n) const {
    Field res;
    get(n, res);
    return res;
}

void ColumnStruct::get(size_t n, Field& res) const {
    const size_t tuple_size = columns.size();

    res = Tuple();
    Tuple& res_tuple = res.get<Tuple&>();
    res_tuple.reserve(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i) {
        res_tuple.push_back((*columns[i])[n]);
    }
}

bool ColumnStruct::is_default_at(size_t n) const {
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i) {
        if (!columns[i]->is_default_at(n)) {
            return false;
        }
    }
    return true;
}

StringRef ColumnStruct::get_data_at(size_t) const {
    throw Exception("Method get_data_at is not supported for " + get_name(),
                    ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnStruct::insert_data(const char*, size_t) {
    throw Exception("Method insert_data is not supported for " + get_name(),
                    ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnStruct::insert(const Field& x) {
    const auto& tuple = x.get<const Tuple&>();
    const size_t tuple_size = columns.size();
    if (tuple.size() != tuple_size) {
        throw Exception("Cannot insert value of different size into tuple",
                        ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE);
    }

    for (size_t i = 0; i < tuple_size; ++i) {
        columns[i]->insert(tuple[i]);
    }
}

void ColumnStruct::insert_from(const IColumn& src_, size_t n) {
    const ColumnStruct& src = assert_cast<const ColumnStruct&>(src_);

    const size_t tuple_size = columns.size();
    if (src.columns.size() != tuple_size) {
        throw Exception("Cannot insert value of different size into tuple",
                        ErrorCodes::CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE);
    }

    for (size_t i = 0; i < tuple_size; ++i) {
        columns[i]->insert_from(*src.columns[i], n);
    }
}

void ColumnStruct::insert_default() {
    for (auto& column : columns) {
        column->insert_default();
    }
}

void ColumnStruct::pop_back(size_t n) {
    for (auto& column : columns) {
        column->pop_back(n);
    }
}

StringRef ColumnStruct::serialize_value_into_arena(size_t n, Arena& arena,
                                                   char const*& begin) const {
    StringRef res(begin, 0);
    for (const auto& column : columns) {
        auto value_ref = column->serialize_value_into_arena(n, arena, begin);
        res.data = value_ref.data - res.size;
        res.size += value_ref.size;
    }

    return res;
}

const char* ColumnStruct::deserialize_and_insert_from_arena(const char* pos) {
    for (auto& column : columns) {
        pos = column->deserialize_and_insert_from_arena(pos);
    }

    return pos;
}

void ColumnStruct::update_hash_with_value(size_t n, SipHash& hash) const {
    for (const auto& column : columns) {
        column->update_hash_with_value(n, hash);
    }
}

// void ColumnStruct::update_weak_hash32(WeakHash32 & hash) const {
//     auto s = size();
//     if (hash.get_data().size() != s) {
//         throw Exception("Size of WeakHash32 does not match size of column: column size is " + std::to_string(s) +
//                         ", hash size is " + std::to_string(hash.getData().size()), ErrorCodes::LOGICAL_ERROR);
//     }

//     for (const auto & column : columns) {
//         column->update_weak_hash32(hash);
//     }
// }

// void ColumnStruct::update_hash_fast(SipHash & hash) const {
//     for (const auto & column : columns) {
//         column->update_hash_fast(hash);
//     }
// }

// const char * ColumnStruct::skip_serialized_in_arena(const char * pos) const {
//     for (const auto & column : columns) {
//         pos = column->skip_serialized_in_arena(pos);
//     }
//     return pos;
// }

// void ColumnStruct::expand(const Filter & mask, bool inverted)
// {
//     for (auto & column : columns) {
//         column->expand(mask, inverted);
//     }
// }

// ColumnPtr ColumnStruct::index(const IColumn & indexes, size_t limit) const
// {
//     const size_t tuple_size = columns.size();
//     Columns new_columns(tuple_size);

//     for (size_t i = 0; i < tuple_size; ++i) {
//         new_columns[i] = columns[i]->index(indexes, limit);
//     }

//     return ColumnStruct::create(new_columns);
// }

void ColumnStruct::insert_range_from(const IColumn& src, size_t start, size_t length) {
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i) {
        columns[i]->insert_range_from(*assert_cast<const ColumnStruct&>(src).columns[i], start,
                                      length);
    }
}

ColumnPtr ColumnStruct::filter(const Filter& filt, ssize_t result_size_hint) const {
    const size_t tuple_size = columns.size();
    Columns new_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i) {
        new_columns[i] = columns[i]->filter(filt, result_size_hint);
    }
    return ColumnStruct::create(new_columns);
}

ColumnPtr ColumnStruct::permute(const Permutation& perm, size_t limit) const {
    const size_t tuple_size = columns.size();
    Columns new_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i) {
        new_columns[i] = columns[i]->permute(perm, limit);
    }

    return ColumnStruct::create(new_columns);
}

ColumnPtr ColumnStruct::replicate(const Offsets& offsets) const {
    const size_t tuple_size = columns.size();
    Columns new_columns(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i) {
        new_columns[i] = columns[i]->replicate(offsets);
    }

    return ColumnStruct::create(new_columns);
}

MutableColumns ColumnStruct::scatter(ColumnIndex num_columns, const Selector& selector) const {
    const size_t tuple_size = columns.size();
    std::vector<MutableColumns> scattered_tuple_elements(tuple_size);

    for (size_t tuple_element_idx = 0; tuple_element_idx < tuple_size; ++tuple_element_idx) {
        scattered_tuple_elements[tuple_element_idx] =
                columns[tuple_element_idx]->scatter(num_columns, selector);
    }

    MutableColumns res(num_columns);

    for (size_t scattered_idx = 0; scattered_idx < num_columns; ++scattered_idx) {
        MutableColumns new_columns(tuple_size);
        for (size_t tuple_element_idx = 0; tuple_element_idx < tuple_size; ++tuple_element_idx) {
            new_columns[tuple_element_idx] =
                    std::move(scattered_tuple_elements[tuple_element_idx][scattered_idx]);
        }
        res[scattered_idx] = ColumnStruct::create(std::move(new_columns));
    }

    return res;
}

// int ColumnStruct::compare_at_impl(size_t n, size_t m, const IColumn& rhs, int nan_direction_hint,
//                                   const Collator* collator) const {
//     const size_t tuple_size = columns.size();
//     for (size_t i = 0; i < tuple_size; ++i) {
//         int res = 0;
//         if (collator && columns[i]->is_collation_supported()) {
//             res = columns[i]->compare_at_with_collation(
//                     n, m, *assert_cast<const ColumnStruct&>(rhs).columns[i], nan_direction_hint,
//                     *collator);
//         } else {
//             res = columns[i]->compare_at(n, m, *assert_cast<const ColumnStruct&>(rhs).columns[i],
//                                          nan_direction_hint);
//         }

//         if (res) {
//             return res;
//         }
//     }
//     return 0;
// }

// int ColumnStruct::compare_at(size_t n, size_t m, const IColumn& rhs, int nan_direction_hint) const {
//     return compare_at_impl(n, m, rhs, nan_direction_hint);
// }

// void ColumnStruct::compare_column(const IColumn& rhs, size_t rhs_row_num,
//                                   PaddedPODArray<UInt64>* row_indexes,
//                                   PaddedPODArray<Int8>& compare_results, int direction,
//                                   int nan_direction_hint) const {
//     return do_compare_column<ColumnStruct>(assert_cast<const ColumnStruct&>(rhs), rhs_row_num,
//                                            row_indexes, compare_results, direction,
//                                            nan_direction_hint);
// }

// int ColumnStruct::compare_at_with_collation(size_t n, size_t m, const IColumn& rhs,
//                                             int nan_direction_hint,
//                                             const Collator& collator) const {
//     return compare_at_impl(n, m, rhs, nan_direction_hint, &collator);
// }

// bool ColumnStruct::has_equal_values() const {
//     return has_equal_values_impl<ColumnStruct>();
// }

// template <bool positive>
// struct ColumnStruct::Less {
//     TupleColumns columns;
//     int nan_direction_hint;
//     const Collator* collator;

//     Less(const TupleColumns& columns_, int nan_direction_hint_, const Collator* collator_ = nullptr)
//             : columns(columns_), nan_direction_hint(nan_direction_hint_), collator(collator_) {}

//     bool operator()(size_t a, size_t b) const {
//         for (const auto& column : columns) {
//             int res;
//             if (collator && column->isCollationSupported()) {
//                 res = column->compareAtWithCollation(a, b, *column, nan_direction_hint, *collator);
//             } else {
//                 res = column->compareAt(a, b, *column, nan_direction_hint);
//             }
//             if (res < 0) {
//                 return positive;
//             } else if (res > 0) {
//                 return !positive;
//             }
//         }
//         return false;
//     }
// };

// void ColumnStruct::get_permutation_impl(IColumn::PermutationSortDirection direction,
//                                         IColumn::PermutationSortStability stability, size_t limit,
//                                         int nan_direction_hint, Permutation& res,
//                                         const Collator* collator) const {
//     size_t rows = size();
//     res.resize(rows);
//     for (size_t i = 0; i < rows; ++i) {
//         res[i] = i;
//     }

//     if (limit >= rows) {
//         limit = 0;
//     }

//     EqualRange ranges;
//     ranges.emplace_back(0, rows);
//     update_permutation_impl(direction, stability, limit, nan_direction_hint, res, ranges, collator);
// }

// void ColumnStruct::update_permutation_impl(IColumn::PermutationSortDirection direction,
//                                            IColumn::PermutationSortStability stability,
//                                            size_t limit, int nan_direction_hint,
//                                            IColumn::Permutation& res, EqualRanges& equal_ranges,
//                                            const Collator* collator) const {
//     if (equal_ranges.empty()) {
//         return;
//     }

//     for (const auto& column : columns) {
//         while (!equal_ranges.empty() && limit && limit <= equal_ranges.back().first) {
//             equal_ranges.pop_back();
//         }

//         if (collator && column->isCollationSupported()) {
//             column->update_permutation_with_collation(*collator, direction, stability, limit,
//                                                       nan_direction_hint, res, equal_ranges);
//         } else {
//             column->update_permutation(direction, stability, limit, nan_direction_hint, res,
//                                        equal_ranges);
//         }
//         if (equal_ranges.empty()) {
//             break;
//         }
//     }
// }

// void ColumnStruct::get_permutation(IColumn::PermutationSortDirection direction,
//                                    IColumn::PermutationSortStability stability, size_t limit,
//                                    int nan_direction_hint, Permutation& res) const {
//     get_permutation_impl(direction, stability, limit, nan_direction_hint, res, nullptr);
// }

// void ColumnStruct::update_permutation(IColumn::PermutationSortDirection direction,
//                                       IColumn::PermutationSortStability stability, size_t limit,
//                                       int nan_direction_hint, IColumn::Permutation& res,
//                                       EqualRanges& equal_ranges) const {
//     update_permutation_impl(direction, stability, limit, nan_direction_hint, res, equal_ranges);
// }

// void ColumnStruct::get_permutation_with_collation(const Collator& collator,
//                                                   IColumn::PermutationSortDirection direction,
//                                                   IColumn::PermutationSortStability stability,
//                                                   size_t limit, int nan_direction_hint,
//                                                   Permutation& res) const {
//     get_permutation_impl(direction, stability, limit, nan_direction_hint, res, &collator);
// }

// void ColumnStruct::update_permutation_with_collation(const Collator& collator,
//                                                      IColumn::PermutationSortDirection direction,
//                                                      IColumn::PermutationSortStability stability,
//                                                      size_t limit, int nan_direction_hint,
//                                                      Permutation& res,
//                                                      EqualRanges& equal_ranges) const {
//     update_permutation_impl(direction, stability, limit, nan_direction_hint, res, equal_ranges,
//                             &collator);
// }

// void ColumnStruct::gather(ColumnGathererStream& gatherer) {
//     gatherer.gather(*this);
// }

void ColumnStruct::reserve(size_t n) {
    const size_t tuple_size = columns.size();
    for (size_t i = 0; i < tuple_size; ++i) {
        get_column(i).reserve(n);
    }
}

size_t ColumnStruct::byte_size() const {
    size_t res = 0;
    for (const auto& column : columns) {
        res += column->byte_size();
    }
    return res;
}

// size_t ColumnStruct::byte_size_at(size_t n) const {
//     size_t res = 0;
//     for (const auto& column : columns) {
//         res += column->byte_size_at(n);
//     }
//     return res;
// }

// void ColumnStruct::ensure_ownership() {
//     const size_t tuple_size = columns.size();
//     for (size_t i = 0; i < tuple_size; ++i) {
//         get_column(i).ensure_ownership();
//     }
// }

size_t ColumnStruct::allocated_bytes() const {
    size_t res = 0;
    for (const auto& column : columns) {
        res += column->allocated_bytes();
    }
    return res;
}

void ColumnStruct::protect() {
    for (auto& column : columns) {
        column->protect();
    }
}

void ColumnStruct::get_extremes(Field& min, Field& max) const {
    const size_t tuple_size = columns.size();

    Tuple min_tuple(tuple_size);
    Tuple max_tuple(tuple_size);

    for (size_t i = 0; i < tuple_size; ++i) {
        columns[i]->get_extremes(min_tuple[i], max_tuple[i]);
    }

    min = min_tuple;
    max = max_tuple;
}

void ColumnStruct::for_each_subcolumn(ColumnCallback callback) {
    for (auto& column : columns) {
        callback(column);
    }
}

bool ColumnStruct::structure_equals(const IColumn& rhs) const {
    if (const auto* rhs_tuple = typeid_cast<const ColumnStruct*>(&rhs)) {
        const size_t tuple_size = columns.size();
        if (tuple_size != rhs_tuple->columns.size()) {
            return false;
        }

        for (size_t i = 0; i < tuple_size; ++i) {
            if (!columns[i]->structure_equals(*rhs_tuple->columns[i])) {
                return false;
            }
        }
        return true;
    } else {
        return false;
    }
}

// void ColumnStruct::for_each_subcolumn_recursively(ColumnCallback callback) {
//     for (auto& column : columns) {
//         callback(column);
//         column->for_each_subcolumn_recursively(callback);
//     }
// }

// bool ColumnStruct::is_collation_supported() const {
//     for (const auto& column : columns) {
//         if (column->is_collation_supported()) {
//             return true;
//         }
//     }
//     return false;
// }

// ColumnPtr ColumnStruct::compress() const {
//     size_t byte_size = 0;
//     Columns compressed;
//     compressed.reserve(columns.size());
//     for (const auto& column : columns) {
//         auto compressed_column = column->compress();
//         byte_size += compressed_column->byteSize();
//         compressed.emplace_back(std::move(compressed_column));
//     }

//     return ColumnCompressed::create(size(), byte_size,
//                                     [compressed = std::move(compressed)]() mutable {
//                                         for (auto& column : compressed) {
//                                             column = column->decompress();
//                                         }
//                                         return ColumnStruct::create(compressed);
//                                     });
// }

// double ColumnStruct::get_ratio_of_default_rows(double sample_ratio) const {
//     return get_ratio_of_default_rows_impl<ColumnStruct>(sample_ratio);
// }

// void ColumnStruct::get_indices_of_nondefault_rows(Offsets& indices, size_t from,
//                                                   size_t limit) const {
//     return get_indices_of_nondefault_rows_impl<ColumnStruct>(indices, from, limit);
// }

// void ColumnStruct::finalize() {
//     for (auto& column : columns) {
//         column->finalize();
//     }
// }

// bool ColumnStruct::is_finalized() const {
//     return std::all_of(columns.begin(), columns.end(),
//                        [](const auto& column) { return column->is_finalized(); });
// }

} // namespace doris::vectorized
