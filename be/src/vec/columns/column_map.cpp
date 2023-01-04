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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnMap.cpp
// and modified by Doris

#include "vec/columns/column_map.h"

namespace doris::vectorized {

/** A column of map values.
  */
std::string ColumnMap::get_name() const {
    return "Map(" + keys->get_name() + ", " + values->get_name() + ")";
}

ColumnMap::ColumnMap(MutableColumnPtr&& keys, MutableColumnPtr&& values)
        : keys(std::move(keys)), values(std::move(values)) {
    check_size();
}

ColumnArray::Offsets64& ColumnMap::get_offsets() const {
    const ColumnArray & column_keys = assert_cast<const ColumnArray &> (get_keys());
    // todo . did here check size ?
    return const_cast<Offsets64&>(column_keys.get_offsets());
}

void ColumnMap::check_size() const {
    const auto * key_array = typeid_cast<const ColumnArray *>(keys.get());
    const auto * value_array = typeid_cast<const ColumnArray *>(values.get());
    CHECK(key_array) << "ColumnMap keys can be created only from array";
    CHECK(value_array) << "ColumnMap values can be created only from array";
    CHECK_EQ(get_keys_ptr()->size(), get_values_ptr()->size());
}

// todo. here to resize every row map
MutableColumnPtr ColumnMap::clone_resized(size_t to_size) const {
    auto res = ColumnMap::create(keys->clone_resized(to_size), values->clone_resized(to_size));
    return res;
}

// to support field functions
Field ColumnMap::operator[](size_t n) const {
    Map res(2);
    keys->get(n, res[0]);
    values->get(n, res[0]);

    return res;
}

// here to compare to below
void ColumnMap::get(size_t n, Field & res) const {
    Map map(2);
    keys->get(n, map[0]);
    values->get(n, map[1]);

    res = map;
}

StringRef ColumnMap::get_data_at(size_t n) const {
    LOG(FATAL) << "Method get_data_at is not supported for " << get_name();
}

void ColumnMap::insert_data(const char*, size_t) {
    LOG(FATAL) << "Method insert_data is not supported for " << get_name();
}

void ColumnMap::insert(const Field& x) {
    const auto& map = doris::vectorized::get<const Map&>(x);
    // ({}, {}, {})
    // ([], [])
    CHECK_EQ(map.size(), 2);
    keys->insert(map[0]);
    values->insert(map[1]);
}

void ColumnMap::insert_default() {
    keys->insert_default();
    values->insert_default();
}

void ColumnMap::pop_back(size_t n) {
    keys->pop_back(n);
    values->pop_back(n);
}

StringRef ColumnMap::serialize_value_into_arena(size_t n, Arena & arena, char const*& begin)
        const {
    StringRef res(begin, 0);
    auto keys_ref = keys->serialize_value_into_arena(n, arena, begin);
    res.data = keys_ref.data - res.size;
    res.size += keys_ref.size;
    auto value_ref = values->serialize_value_into_arena(n, arena, begin);
    res.data = value_ref.data - res.size;
    res.size += value_ref.size;

    return res;
}

void ColumnMap::insert_from(const IColumn& src_, size_t n) {
    const ColumnMap& src = assert_cast<const ColumnMap&>(src_);

    if ((!get_keys().is_nullable() && src.get_keys().is_nullable())
        || (!get_values().is_nullable() && src.get_values().is_nullable())) {
        DCHECK(false);
    } else if ((get_keys().is_nullable() && !src.get_keys().is_nullable())
               || (get_values().is_nullable() && !src.get_values().is_nullable())) {
        DCHECK(false);
    } else {
        keys->insert_from(*assert_cast<const ColumnMap&>(src_).keys, n);
        values->insert_from(*assert_cast<const ColumnMap&>(src_).values, n);
    }
}

void ColumnMap::insert_indices_from(const IColumn& src, const int* indices_begin,
                                      const int* indices_end) {
    for (auto x = indices_begin; x != indices_end; ++x) {
        if (*x == -1) {
            ColumnMap::insert_default();
        } else {
            ColumnMap::insert_from(src, *x);
        }
    }
}

const char* ColumnMap::deserialize_and_insert_from_arena(const char* pos) {
    pos = keys->deserialize_and_insert_from_arena(pos);
    pos = values->deserialize_and_insert_from_arena(pos);

    return pos;
}

void ColumnMap::update_hash_with_value(size_t n, SipHash & hash) const {
    keys->update_hash_with_value(n, hash);
    values->update_hash_with_value(n, hash);
}

void ColumnMap::insert_range_from(const IColumn& src, size_t start, size_t length) {
    keys->insert_range_from(*assert_cast<const ColumnMap&>(src).keys, start, length);
    values->insert_range_from(*assert_cast<const ColumnMap&>(src).values, start, length);
}

ColumnPtr ColumnMap::filter(const Filter& filt, ssize_t result_size_hint) const {
    return ColumnMap::create(keys->filter(filt, result_size_hint), values->filter(filt, result_size_hint));
}

ColumnPtr ColumnMap::permute(const Permutation& perm, size_t limit) const {
    return ColumnMap::create(keys->permute(perm, limit), values->permute(perm, limit));
}

ColumnPtr ColumnMap::replicate(const Offsets& offsets) const {
    return ColumnMap::create(keys->replicate(offsets), values->replicate(offsets));
}

//MutableColumns ColumnMap::scatter(ColumnIndex num_columns, const Selector& selector) const {
//
//    MutableColumns keys_scatter = keys->scatter(num_columns, selector);
//    MutableColumns values_scatter = values->scatter(num_columns, selector);
//
//    MutableColumns res(num_columns);
//
//    for (size_t scattered_idx = 0; scattered_idx < num_columns; ++scattered_idx)
//    {
//        MutableColumns new_columns(2);
//        for (size_t map_element_idx = 0; map_element_idx < 2; ++map_element_idx)
//            new_columns[map_element_idx] = std::move(scattered_map_elements[map_element_idx][scattered_idx]);
//        res[scattered_idx] = ColumnMap::create(std::move(new_columns));
//    }
//
//
//    return res;
//}


void ColumnMap::reserve(size_t n) {
    get_keys().reserve(n);
    get_values().reserve(n);
}

size_t ColumnMap::byte_size() const {
    return get_keys().byte_size() + get_values().byte_size();
}

size_t ColumnMap::allocated_bytes() const {
    return get_keys().allocated_bytes() + get_values().allocated_bytes();
}

void ColumnMap::protect() {
    get_keys().protect();
    get_values().protect();
}

void ColumnMap::get_extremes(Field & min, Field & max) const {
    Map min_map(2);
    Map max_map(2);

    keys->get_extremes(min_map[0], max_map[0]);
    values->get_extremes(min_map[1], max_map[1]);

    min = min_map;
    max = max_map;
}

}