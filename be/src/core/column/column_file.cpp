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

#include "core/column/column_file.h"

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/data_type/data_type_jsonb.h"

namespace doris {

ColumnFile::ColumnFile(MutableColumnPtr data) : _data(std::move(data)) {
    CHECK(_data->is_column_string() || _data->is_column_string64()) << "ColumnFile must wrap JSONB";
}

ColumnFile::MutablePtr ColumnFile::create(const FileSchemaDescriptor& schema) {
    static_cast<void>(schema);
    return ColumnFile::create_from_jsonb(DataTypeJsonb().create_column());
}

ColumnFile::MutablePtr ColumnFile::create_from_jsonb(MutableColumnPtr data) {
    return Base::create(std::move(data));
}

ColumnPtr ColumnFile::convert_column_if_overflow() {
    _data = WrappedPtr(IColumn::mutate(_data->convert_column_if_overflow()));
    return get_ptr();
}

void ColumnFile::insert(const Field& x) {
    _data->insert(x);
}

void ColumnFile::insert_from(const IColumn& src, size_t n) {
    _data->insert_from(assert_cast<const ColumnFile&>(src).get_jsonb_column(), n);
}

void ColumnFile::insert_range_from(const IColumn& src, size_t start, size_t length) {
    _data->insert_range_from(assert_cast<const ColumnFile&>(src).get_jsonb_column(), start, length);
}

void ColumnFile::insert_range_from_ignore_overflow(const IColumn& src, size_t start,
                                                   size_t length) {
    _data->insert_range_from_ignore_overflow(assert_cast<const ColumnFile&>(src).get_jsonb_column(),
                                             start, length);
}

void ColumnFile::insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                                     const uint32_t* indices_end) {
    _data->insert_indices_from(assert_cast<const ColumnFile&>(src).get_jsonb_column(),
                               indices_begin, indices_end);
}

void ColumnFile::insert_many_from(const IColumn& src, size_t position, size_t length) {
    _data->insert_many_from(assert_cast<const ColumnFile&>(src).get_jsonb_column(), position,
                            length);
}

void ColumnFile::insert_default() {
    _data->insert_default();
}

void ColumnFile::pop_back(size_t n) {
    _data->pop_back(n);
}

MutableColumnPtr ColumnFile::clone_resized(size_t size) const {
    return ColumnFile::create_from_jsonb(_data->clone_resized(size));
}

StringRef ColumnFile::serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const {
    return _data->serialize_value_into_arena(n, arena, begin);
}

const char* ColumnFile::deserialize_and_insert_from_arena(const char* pos) {
    return _data->deserialize_and_insert_from_arena(pos);
}

size_t ColumnFile::serialize_size_at(size_t row) const {
    return _data->serialize_size_at(row);
}

size_t ColumnFile::deserialize_impl(const char* pos) {
    return _data->deserialize_impl(pos);
}

size_t ColumnFile::serialize_impl(char* pos, size_t row) const {
    return _data->serialize_impl(pos, row);
}

int ColumnFile::compare_at(size_t n, size_t m, const IColumn& rhs, int nan_direction_hint) const {
    return _data->compare_at(n, m, assert_cast<const ColumnFile&>(rhs).get_jsonb_column(),
                             nan_direction_hint);
}

void ColumnFile::update_hash_with_value(size_t n, SipHash& hash) const {
    _data->update_hash_with_value(n, hash);
}

void ColumnFile::update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                          const uint8_t* __restrict null_data) const {
    _data->update_xxHash_with_value(start, end, hash, null_data);
}

void ColumnFile::update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                                       const uint8_t* __restrict null_data) const {
    _data->update_crc_with_value(start, end, hash, null_data);
}

void ColumnFile::update_hashes_with_value(uint64_t* __restrict hashes,
                                          const uint8_t* __restrict null_data) const {
    _data->update_hashes_with_value(hashes, null_data);
}

void ColumnFile::update_crcs_with_value(uint32_t* __restrict hash, PrimitiveType type,
                                        uint32_t rows, uint32_t offset,
                                        const uint8_t* __restrict null_data) const {
    _data->update_crcs_with_value(hash, type, rows, offset, null_data);
}

void ColumnFile::update_crc32c_batch(uint32_t* __restrict hashes,
                                     const uint8_t* __restrict null_map) const {
    _data->update_crc32c_batch(hashes, null_map);
}

void ColumnFile::update_crc32c_single(size_t start, size_t end, uint32_t& hash,
                                      const uint8_t* __restrict null_map) const {
    _data->update_crc32c_single(start, end, hash, null_map);
}

ColumnPtr ColumnFile::filter(const Filter& filt, ssize_t result_size_hint) const {
    return ColumnFile::create_from_jsonb(IColumn::mutate(_data->filter(filt, result_size_hint)));
}

size_t ColumnFile::filter(const Filter& filter) {
    return _data->filter(filter);
}

MutableColumnPtr ColumnFile::permute(const Permutation& perm, size_t limit) const {
    return ColumnFile::create_from_jsonb(_data->permute(perm, limit));
}

bool ColumnFile::has_enough_capacity(const IColumn& src) const {
    return _data->has_enough_capacity(assert_cast<const ColumnFile&>(src).get_jsonb_column());
}

bool ColumnFile::structure_equals(const IColumn& rhs) const {
    const auto* rhs_file = check_and_get_column<ColumnFile>(&rhs);
    return rhs_file != nullptr && _data->structure_equals(rhs_file->get_jsonb_column());
}

void ColumnFile::replace_column_data(const IColumn& rhs, size_t row, size_t self_row) {
    _data->replace_column_data(assert_cast<const ColumnFile&>(rhs).get_jsonb_column(), row,
                               self_row);
}

void ColumnFile::get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                                 HybridSorter& sorter, IColumn::Permutation& res) const {
    _data->get_permutation(reverse, limit, nan_direction_hint, sorter, res);
}

void ColumnFile::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                             IColumn::Permutation& perms, EqualRange& range,
                             bool last_column) const {
    _data->sort_column(sorter, flags, perms, range, last_column);
}

void ColumnFile::serialize(StringRef* keys, size_t num_rows) const {
    _data->serialize(keys, num_rows);
}

void ColumnFile::deserialize(StringRef* keys, size_t num_rows) {
    _data->deserialize(keys, num_rows);
}

size_t ColumnFile::get_max_row_byte_size() const {
    return _data->get_max_row_byte_size();
}

void ColumnFile::replace_float_special_values() {
    _data->replace_float_special_values();
}

Status ColumnFile::check_schema(const FileSchemaDescriptor& schema) const {
    static_cast<void>(schema);
    return DataTypeJsonb().check_column(*_data);
}

} // namespace doris
