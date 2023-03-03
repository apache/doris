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
            LOG(FATAL) << "ColumnStruct cannot have ColumnConst as its element";
        }
        columns.push_back(std::move(column));
    }
}

ColumnStruct::Ptr ColumnStruct::create(const Columns& columns) {
    for (const auto& column : columns) {
        if (is_column_const(*column)) {
            LOG(FATAL) << "ColumnStruct cannot have ColumnConst as its element";
        }
    }
    auto column_struct = ColumnStruct::create(MutableColumns());
    column_struct->columns.assign(columns.begin(), columns.end());
    return column_struct;
}

ColumnStruct::Ptr ColumnStruct::create(const TupleColumns& tuple_columns) {
    for (const auto& column : tuple_columns) {
        if (is_column_const(*column)) {
            LOG(FATAL) << "ColumnStruct cannot have ColumnConst as its element";
        }
    }
    auto column_struct = ColumnStruct::create(MutableColumns());
    column_struct->columns = tuple_columns;
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

void ColumnStruct::insert(const Field& x) {
    const auto& tuple = x.get<const Tuple&>();
    const size_t tuple_size = columns.size();
    if (tuple.size() != tuple_size) {
        LOG(FATAL) << "Cannot insert value of different size into tuple.";
    }

    for (size_t i = 0; i < tuple_size; ++i) {
        columns[i]->insert(tuple[i]);
    }
}

void ColumnStruct::insert_from(const IColumn& src_, size_t n) {
    const ColumnStruct& src = assert_cast<const ColumnStruct&>(src_);

    const size_t tuple_size = columns.size();
    if (src.columns.size() != tuple_size) {
        LOG(FATAL) << "Cannot insert value of different size into tuple.";
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

void ColumnStruct::insert_indices_from(const IColumn& src, const int* indices_begin,
                                       const int* indices_end) {
    const ColumnStruct& src_concrete = assert_cast<const ColumnStruct&>(src);
    for (size_t i = 0; i < columns.size(); ++i) {
        columns[i]->insert_indices_from(src_concrete.get_column(i), indices_begin, indices_end);
    }
}

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

size_t ColumnStruct::filter(const Filter& filter) {
    const size_t tuple_size = columns.size();

    size_t result_size = 0;
    for (size_t i = 0; i < tuple_size; ++i) {
        const auto this_result_size = columns[i]->filter(filter);
        CHECK(result_size == 0 || result_size == this_result_size);
        result_size = this_result_size;
    }
    return result_size;
}

Status ColumnStruct::filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) {
    auto to = reinterpret_cast<vectorized::ColumnStruct*>(col_ptr);
    const size_t tuple_size = columns.size();
    DCHECK_EQ(to->tuple_size(), tuple_size);
    for (size_t i = 0; i < tuple_size; ++i) {
        columns[i]->filter_by_selector(sel, sel_size, &to->get_column(i));
    }
    return Status::OK();
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

} // namespace doris::vectorized
