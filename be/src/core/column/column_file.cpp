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

#include <crc32c/crc32c.h>

#include <algorithm>
#include <limits>

#include "core/arena.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_varbinary.h"
#include "core/column/column_vector.h"
#include "core/column/columns_common.h"
#include "core/custom_allocator.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "exec/common/sip_hash.h"
#include "util/hash_util.hpp"

namespace doris {
namespace {

MutableColumns make_file_children() {
    MutableColumns children;
    children.reserve(ColumnFile::NUM_CHILDREN);
    children.push_back(ColumnNullable::create(ColumnString::create(), ColumnUInt8::create()));
    children.push_back(ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create()));
    children.push_back(ColumnNullable::create(ColumnInt64::create(), ColumnUInt8::create()));
    children.push_back(ColumnNullable::create(ColumnString::create(), ColumnUInt8::create()));
    children.push_back(ColumnNullable::create(ColumnString::create(), ColumnUInt8::create()));
    children.push_back(ColumnNullable::create(ColumnVarbinary::create(), ColumnUInt8::create()));
    return children;
}

// VARBINARY has no column hash implementation. Hash the lossless row encoding,
// which includes every child null flag and length, without exposing SQL hashing.
template <typename Update>
void hash_file_rows(const ColumnFile& column, size_t start, size_t end, const uint8_t* null_map,
                    Update&& update) {
    DorisVector<char> buffer;
    for (size_t row = start; row < end; ++row) {
        if (null_map != nullptr && null_map[row] != 0) {
            continue;
        }
        buffer.resize(column.serialize_size_at(row));
        column.serialize_impl(buffer.data(), row);
        update(row, buffer.data(), buffer.size());
    }
}

uint32_t file_crc(const char* data, size_t size, uint32_t hash) {
    // A complete row can exceed the uint32 length accepted by zlib_crc_hash.
    while (size != 0) {
        const auto length =
                static_cast<uint32_t>(std::min<size_t>(size, std::numeric_limits<uint32_t>::max()));
        hash = HashUtil::zlib_crc_hash(data, length, hash);
        data += length;
        size -= length;
    }
    return hash;
}

} // namespace

ColumnFile::ColumnFile() : ColumnFile(make_file_children()) {}

ColumnFile::ColumnFile(MutableColumns&& columns) {
    if (columns.size() != NUM_CHILDREN) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "ColumnFile requires six children, got {}",
                        columns.size());
    }
    for (size_t i = 0; i < NUM_CHILDREN; ++i) {
        _columns[i] = std::move(columns[i]);
    }
    sanity_check();
    check_const_only_in_top_level();
}

void ColumnFile::sanity_check() const {
    for (size_t i = 0; i < NUM_CHILDREN; ++i) {
        const auto* child = dynamic_cast<const ColumnNullable*>(_columns[i].get());
        if (child == nullptr) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "ColumnFile child {} must be nullable", i);
        }
        const auto& nested = child->get_nested_column();
        bool expected_type;
        if (i == 1 || i == 2) {
            expected_type = dynamic_cast<const ColumnInt64*>(&nested) != nullptr;
        } else if (i == 5) {
            expected_type = dynamic_cast<const ColumnVarbinary*>(&nested) != nullptr;
        } else {
            // Both string offset widths represent the same physical child type.
            expected_type = nested.is_column_string();
        }
        if (!expected_type) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid ColumnFile child {} type {}", i,
                            nested.get_name());
        }
        child->sanity_check();
        if (i != 0 && child->size() != size()) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "ColumnFile child {} has {} rows, expected {}", i, child->size(),
                            size());
        }
    }
}

bool ColumnFile::structure_equals(const IColumn& rhs) const {
    const auto* file = dynamic_cast<const ColumnFile*>(&rhs);
    if (file == nullptr) {
        return false;
    }
    for (size_t i = 0; i < NUM_CHILDREN; ++i) {
        if (!get_column(i).structure_equals(file->get_column(i))) {
            return false;
        }
    }
    return true;
}

MutableColumnPtr ColumnFile::clone_resized(size_t size) const {
    MutableColumns children;
    children.reserve(NUM_CHILDREN);
    for (const auto& column : _columns) {
        children.push_back(column->clone_resized(size));
    }
    return create(std::move(children));
}

Field ColumnFile::operator[](size_t n) const {
    Field result;
    get(n, result);
    return result;
}

void ColumnFile::get(size_t n, Field& res) const {
    File value;
    value.reserve(NUM_CHILDREN);
    for (const auto& column : _columns) {
        value.push_back((*column)[n]);
    }
    res = Field::create_field<TYPE_FILE>(std::move(value));
}

void ColumnFile::insert(const Field& value) {
    DCHECK_EQ(value.get_type(), TYPE_FILE);
    const auto& fields = value.get<TYPE_FILE>();
    if (fields.size() != NUM_CHILDREN) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "FILE Field requires six children, got {}",
                        fields.size());
    }
    for (size_t i = 0; i < NUM_CHILDREN; ++i) {
        _columns[i]->insert(fields[i]);
    }
}

void ColumnFile::insert_from(const IColumn& src, size_t n) {
    const auto& file = assert_cast<const ColumnFile&>(src);
    for (size_t i = 0; i < NUM_CHILDREN; ++i) {
        _columns[i]->insert_from(file.get_column(i), n);
    }
}

void ColumnFile::insert_range_from(const IColumn& src, size_t start, size_t length) {
    const auto& file = assert_cast<const ColumnFile&>(src);
    for (size_t i = 0; i < NUM_CHILDREN; ++i) {
        _columns[i]->insert_range_from(file.get_column(i), start, length);
    }
}

void ColumnFile::insert_range_from_ignore_overflow(const IColumn& src, size_t start,
                                                   size_t length) {
    const auto& file = assert_cast<const ColumnFile&>(src);
    for (size_t i = 0; i < NUM_CHILDREN; ++i) {
        _columns[i]->insert_range_from_ignore_overflow(file.get_column(i), start, length);
    }
}

void ColumnFile::insert_indices_from(const IColumn& src, const uint32_t* begin,
                                     const uint32_t* end) {
    const auto& file = assert_cast<const ColumnFile&>(src);
    for (size_t i = 0; i < NUM_CHILDREN; ++i) {
        _columns[i]->insert_indices_from(file.get_column(i), begin, end);
    }
}

void ColumnFile::insert_many_from(const IColumn& src, size_t position, size_t length) {
    const auto& file = assert_cast<const ColumnFile&>(src);
    for (size_t i = 0; i < NUM_CHILDREN; ++i) {
        _columns[i]->insert_many_from(file.get_column(i), position, length);
    }
}

void ColumnFile::replace_column_data(const IColumn&, size_t, size_t) {
    // Like ColumnString/ColumnStruct, variable-length rows are replaced by rebuilding columns.
    throw Exception(ErrorCode::INTERNAL_ERROR,
                    "Method replace_column_data is not supported for ColumnFile");
}

void ColumnFile::insert_default() {
    for (auto& column : _columns) {
        column->insert_default();
    }
}

void ColumnFile::insert_many_defaults(size_t length) {
    for (auto& column : _columns) {
        column->insert_many_defaults(length);
    }
}

void ColumnFile::pop_back(size_t n) {
    for (auto& column : _columns) {
        column->pop_back(n);
    }
}

void ColumnFile::reserve(size_t n) {
    for (auto& column : _columns) {
        column->reserve(n);
    }
}

void ColumnFile::resize(size_t n) {
    if (n > size()) {
        // ColumnNullable::resize alone does not initialize new null-map entries.
        insert_many_defaults(n - size());
    } else {
        for (auto& column : _columns) {
            column->resize(n);
        }
    }
}

void ColumnFile::clear() {
    for (auto& column : _columns) {
        column->clear();
    }
}

void ColumnFile::erase(size_t start, size_t length) {
    DCHECK_LE(start, size());
    DCHECK_LE(length, size() - start);
    // Filtering also works for the arena-backed VARBINARY child, which has no erase().
    Filter keep(size(), 1);
    std::fill(keep.begin() + start, keep.begin() + start + length, 0);
    filter(keep);
}

ColumnPtr ColumnFile::filter(const Filter& filter, ssize_t result_size_hint) const {
    MutableColumns children;
    children.reserve(NUM_CHILDREN);
    for (const auto& column : _columns) {
        children.push_back(IColumn::mutate(column->filter(filter, result_size_hint)));
    }
    return create(std::move(children));
}

size_t ColumnFile::filter(const Filter& filter) {
    column_match_filter_size(size(), filter.size());
    const size_t result_size = _columns[0]->filter(filter);
    for (size_t i = 1; i < NUM_CHILDREN; ++i) {
        const size_t child_size = _columns[i]->filter(filter);
        DCHECK_EQ(child_size, result_size);
    }
    return result_size;
}

Status ColumnFile::filter_by_selector(const uint16_t* selector, size_t count,
                                      IColumn* destination) const {
    auto& output = assert_cast<ColumnFile&>(*destination);
    DCHECK(output.empty());
    output.reserve(count);
    // Row copies preserve selection order, duplicates and the binary child's owned bytes.
    for (size_t i = 0; i < count; ++i) {
        output.insert_from(*this, selector[i]);
    }
    return Status::OK();
}

MutableColumnPtr ColumnFile::permute(const Permutation& permutation, size_t limit) const {
    MutableColumns children;
    children.reserve(NUM_CHILDREN);
    for (const auto& column : _columns) {
        children.push_back(column->permute(permutation, limit));
    }
    return create(std::move(children));
}

ColumnPtr ColumnFile::convert_column_if_overflow() {
    for (auto& column : _columns) {
        column = column->convert_column_if_overflow();
    }
    return IColumn::convert_column_if_overflow();
}

size_t ColumnFile::byte_size() const {
    size_t size = 0;
    for (const auto& column : _columns) {
        size += column->byte_size();
    }
    return size;
}

size_t ColumnFile::allocated_bytes() const {
    size_t size = 0;
    for (const auto& column : _columns) {
        size += column->allocated_bytes();
    }
    return size;
}

bool ColumnFile::has_enough_capacity(const IColumn& src) const {
    const auto& file = assert_cast<const ColumnFile&>(src);
    for (size_t i = 0; i < NUM_CHILDREN; ++i) {
        if (!_columns[i]->has_enough_capacity(file.get_column(i))) {
            return false;
        }
    }
    return true;
}

void ColumnFile::mutate_subcolumns() {
    for (auto& column : _columns) {
        mutate_subcolumn(column);
    }
}

void ColumnFile::for_each_subcolumn(ColumnCallback callback) const {
    for (const auto& column : _columns) {
        callback(*column);
    }
}

StringRef ColumnFile::serialize_value_into_arena(size_t n, Arena& arena, const char*& begin) const {
    char* pos = arena.alloc_continue(serialize_size_at(n), begin);
    return {pos, serialize_impl(pos, n)};
}

const char* ColumnFile::deserialize_and_insert_from_arena(const char* pos) {
    return pos + deserialize_impl(pos);
}

size_t ColumnFile::serialize_size_at(size_t row) const {
    size_t size = 0;
    for (const auto& column : _columns) {
        size += column->serialize_size_at(row);
    }
    return size;
}

size_t ColumnFile::serialize_impl(char* pos, size_t row) const {
    size_t size = 0;
    for (const auto& column : _columns) {
        size += column->serialize_impl(pos + size, row);
    }
    DCHECK_EQ(size, serialize_size_at(row));
    return size;
}

size_t ColumnFile::deserialize_impl(const char* pos) {
    size_t size = 0;
    for (auto& column : _columns) {
        size += column->deserialize_impl(pos + size);
    }
    return size;
}

void ColumnFile::serialize(StringRef* keys, size_t num_rows) const {
    for (size_t i = 0; i < num_rows; ++i) {
        // The caller owns these mutable row buffers, as in other column serializers.
        keys[i].size += serialize_impl(const_cast<char*>(keys[i].data + keys[i].size), i);
    }
}

void ColumnFile::deserialize(StringRef* keys, size_t num_rows) {
    for (size_t i = 0; i < num_rows; ++i) {
        const auto size = deserialize_impl(keys[i].data);
        keys[i].data += size;
        keys[i].size -= size;
    }
}

size_t ColumnFile::get_max_row_byte_size() const {
    size_t size = 0;
    for (const auto& column : _columns) {
        size += column->get_max_row_byte_size();
    }
    return size;
}

void ColumnFile::update_hash_with_value(size_t n, SipHash& hash) const {
    hash_file_rows(*this, n, n + 1, nullptr,
                   [&](size_t, const char* data, size_t size) { hash.update(data, size); });
}

void ColumnFile::update_hashes_with_value(uint64_t* __restrict hashes,
                                          const uint8_t* __restrict null_data) const {
    hash_file_rows(*this, 0, size(), null_data, [&](size_t row, const char* data, size_t size) {
        hashes[row] = HashUtil::xxHash64WithSeed(data, size, hashes[row]);
    });
}

void ColumnFile::update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                          const uint8_t* __restrict null_data) const {
    hash_file_rows(*this, start, end, null_data, [&](size_t, const char* data, size_t size) {
        hash = HashUtil::xxHash64WithSeed(data, size, hash);
    });
}

void ColumnFile::update_crcs_with_value(uint32_t* __restrict hashes, PrimitiveType /*type*/,
                                        uint32_t rows, uint32_t /*offset*/,
                                        const uint8_t* __restrict null_data) const {
    DCHECK_EQ(rows, size());
    hash_file_rows(*this, 0, rows, null_data, [&](size_t row, const char* data, size_t size) {
        hashes[row] = file_crc(data, size, hashes[row]);
    });
}

void ColumnFile::update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                                       const uint8_t* __restrict null_data) const {
    hash_file_rows(*this, start, end, null_data, [&](size_t, const char* data, size_t size) {
        hash = file_crc(data, size, hash);
    });
}

void ColumnFile::update_crc32c_batch(uint32_t* __restrict hashes,
                                     const uint8_t* __restrict null_map) const {
    hash_file_rows(*this, 0, size(), null_map, [&](size_t row, const char* data, size_t size) {
        hashes[row] = crc32c_extend(hashes[row], reinterpret_cast<const uint8_t*>(data), size);
    });
}

void ColumnFile::update_crc32c_single(size_t start, size_t end, uint32_t& hash,
                                      const uint8_t* __restrict null_map) const {
    hash_file_rows(*this, start, end, null_map, [&](size_t, const char* data, size_t size) {
        hash = crc32c_extend(hash, reinterpret_cast<const uint8_t*>(data), size);
    });
}

int ColumnFile::compare_at(size_t, size_t, const IColumn&, int) const {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "FILE comparison is not supported");
}

void ColumnFile::get_permutation(bool, size_t, int, HybridSorter&, Permutation&) const {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "FILE sorting is not supported");
}

void ColumnFile::sort_column(const ColumnSorter*, EqualFlags&, Permutation&, EqualRange&,
                             bool) const {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "FILE sorting is not supported");
}

} // namespace doris
