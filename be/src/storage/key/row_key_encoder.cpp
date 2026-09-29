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

#include "storage/key/row_key_encoder.h"

#include <algorithm>
#include <cassert>

#include "common/cast_set.h"
#include "common/check.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/consts.h"
#include "common/logging.h"
#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "storage/key_coder.h"
#include "storage/storage_layout.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {
// The column holding the value of row `pos`, nullptr when that value is NULL.
const IColumn* value_column_at(const IColumn* column, size_t pos) {
    if (!is_column_nullable(*column)) {
        return column;
    }
    // Runs once per row and key column, and is_column_nullable has just checked the type.
    const auto& nullable = assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*column);
    return nullable.is_null_at(pos) ? nullptr : &nullable.get_nested_column();
}

// Key columns of the string types store the row's own bytes. This and
// encode_ascending run once per row and key column, so their casts skip the
// release-build type check.
template <FieldType FT>
void full_encode_ascending(const IColumn& column, size_t pos, size_t length, std::string* buf) {
    if constexpr (field_is_slice_type(FT)) {
        const StringRef value =
                assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(column).get_data_at(
                        pos);
        if constexpr (FT == FieldType::OLAP_FIELD_TYPE_CHAR) {
            // Not KeyCoderTraits<CHAR>: it appends the value as it is, so it needs the
            // value already padded to the column length, and the column holds it
            // unpadded. Padding straight into the key gives the same bytes.
            StorageLayout<FT>::append_padded(value, length, buf);
        } else {
            const Slice slice(value.data, value.size);
            KeyCoderTraits<FT>::full_encode_ascending(&slice, buf);
        }
    } else {
        const auto value = StorageLayout<FT>::storage_at(column, pos);
        KeyCoderTraits<FT>::full_encode_ascending(&value, buf);
    }
}

template <FieldType FT>
void encode_ascending(const IColumn& column, size_t pos, size_t length, size_t index_size,
                      std::string* buf) {
    if constexpr (field_is_slice_type(FT)) {
        const StringRef value =
                assert_cast<const ColumnString&, TypeCheckOnRelease::DISABLE>(column).get_data_at(
                        pos);
        if constexpr (FT == FieldType::OLAP_FIELD_TYPE_CHAR) {
            // Not KeyCoderTraits<CHAR> either: it CHECKs that the value is at least
            // index_size long, which only a padded value always is. The first
            // index_size bytes of the value padded to `length`.
            DORIS_CHECK(index_size <= length);
            StorageLayout<FT>::append_padded(
                    StringRef(value.data, std::min(value.size, index_size)), index_size, buf);
        } else {
            const Slice slice(value.data, value.size);
            KeyCoderTraits<FT>::encode_ascending(&slice, index_size, buf);
        }
    } else {
        const auto value = StorageLayout<FT>::storage_at(column, pos);
        KeyCoderTraits<FT>::encode_ascending(&value, index_size, buf);
    }
}
} // namespace

RowKeyEncoder::KeyColumnCoder RowKeyEncoder::_key_column_coder(const TabletColumn& column) {
    KeyColumnCoder coder;
    coder.length = static_cast<size_t>(column.length());
    switch (column.type()) {
#define CASE(FT)                                                             \
    case FieldType::FT:                                                      \
        coder.full_encode_ascending = &full_encode_ascending<FieldType::FT>; \
        coder.encode_ascending = &encode_ascending<FieldType::FT>;           \
        break;
        DORIS_APPLY_FOR_KEY_ENCODABLE_NON_STRING_TYPES(CASE)
        CASE(OLAP_FIELD_TYPE_UNSIGNED_INT)
        CASE(OLAP_FIELD_TYPE_UNSIGNED_BIGINT)
        CASE(OLAP_FIELD_TYPE_CHAR)
        CASE(OLAP_FIELD_TYPE_VARCHAR)
        CASE(OLAP_FIELD_TYPE_STRING)
#undef CASE
    default:
        LOG(FATAL) << "column " << column.name() << " of type " << static_cast<int>(column.type())
                   << " cannot be a key";
    }
    return coder;
}

RowKeyEncoder::RowKeyEncoder(const TabletSchema& schema, bool mow)
        : _num_short_key_columns(schema.num_short_key_columns()) {
    if (mow) {
        _init_mow(schema);
    } else {
        _init_non_mow(schema);
    }
}

void RowKeyEncoder::_init_mow(const TabletSchema& schema) {
    // encode the sequence id into the primary key index
    if (schema.has_sequence_col()) {
        _seq_coder = _key_column_coder(schema.column(schema.sequence_col_idx()));
    }

    // Which columns each view ends up holding:
    //
    //                     _sort_key_coders    _primary_key_coders
    //   no cluster key    key columns         key columns
    //   cluster keys      cluster key cols    key columns
    //
    // The primary key index is built on the schema key columns whatever the segment sorts by, so
    // every mow table gets that view, not just the ones with cluster keys. The sort-key view
    // follows the segment's own order, which is the only column set that differs between the two.
    for (size_t cid = 0; cid < schema.num_key_columns(); ++cid) {
        _primary_key_coders.push_back(_key_column_coder(schema.column(cid)));
    }

    if (schema.cluster_key_uids().empty()) {
        _add_default_sort_key_columns(schema);
        return;
    }
    for (auto uid : schema.cluster_key_uids()) {
        _add_sort_key_column(schema.column_by_uid(uid));
    }
}

void RowKeyEncoder::_init_non_mow(const TabletSchema& schema) {
    _add_default_sort_key_columns(schema);
}

void RowKeyEncoder::_add_default_sort_key_columns(const TabletSchema& schema) {
    for (size_t cid = 0; cid < schema.num_key_columns(); ++cid) {
        _add_sort_key_column(schema.column(cid));
    }
}

void RowKeyEncoder::_add_sort_key_column(const TabletColumn& column) {
    _sort_key_coders.push_back(_key_column_coder(column));
    _sort_key_index_size.push_back(cast_set<uint16_t>(column.index_length()));
}

std::string RowKeyEncoder::full_encode(const std::vector<const IColumn*>& key_columns,
                                       size_t pos) const {
    assert(_sort_key_index_size.size() == _sort_key_coders.size());
    assert(key_columns.size() == _sort_key_coders.size());
    return _full_encode(_sort_key_coders, key_columns, pos);
}

std::string RowKeyEncoder::full_encode_primary_keys(const std::vector<const IColumn*>& key_columns,
                                                    size_t pos) const {
    return _full_encode(_primary_key_coders, key_columns, pos);
}

namespace {
// Shared row-key encoding base: for each key column, write a null marker for
// a null value, otherwise a normal marker followed by whatever `encode_field`
// appends. `encode_field(cid, column, out)` is the only thing that differs between
// the full key encode and the short-key prefix encode.
template <typename EncodeField>
std::string encode_key_columns(const std::vector<const IColumn*>& key_columns, size_t pos,
                               EncodeField&& encode_field) {
    std::string encoded_keys;
    size_t cid = 0;
    for (const auto& column : key_columns) {
        const auto* field = value_column_at(column, pos);
        if (UNLIKELY(!field)) {
            encoded_keys.push_back(KeyConsts::KEY_NULL_FIRST_MARKER);
            ++cid;
            continue;
        }
        encoded_keys.push_back(KeyConsts::KEY_NORMAL_MARKER);
        encode_field(cid, *field, &encoded_keys);
        ++cid;
    }
    return encoded_keys;
}
} // namespace

std::string RowKeyEncoder::_full_encode(const std::vector<KeyColumnCoder>& key_coders,
                                        const std::vector<const IColumn*>& key_columns,
                                        size_t pos) {
    assert(key_columns.size() == key_coders.size());
    return encode_key_columns(key_columns, pos,
                              [&](size_t cid, const IColumn& column, std::string* out) {
                                  const KeyColumnCoder& coder = key_coders[cid];
                                  coder.full_encode_ascending(column, pos, coder.length, out);
                              });
}

std::string RowKeyEncoder::encode_short_keys(const std::vector<const IColumn*>& key_columns,
                                             size_t pos) const {
    assert(key_columns.size() == _num_short_key_columns);
    assert(key_columns.size() <= _sort_key_coders.size());
    return encode_key_columns(
            key_columns, pos, [&](size_t cid, const IColumn& column, std::string* out) {
                const KeyColumnCoder& coder = _sort_key_coders[cid];
                coder.encode_ascending(column, pos, coder.length, _sort_key_index_size[cid], out);
            });
}

void RowKeyEncoder::append_seq_suffix(std::string* encoded_keys, const IColumn* seq_column,
                                      size_t pos) const {
    const auto* field = value_column_at(seq_column, pos);
    // So the primary key index can still use it, encode a null seq column as
    // the smallest value of its length.
    if (UNLIKELY(!field)) {
        encoded_keys->push_back(KeyConsts::KEY_NULL_FIRST_MARKER);
        encoded_keys->append(_seq_coder.length, KeyConsts::KEY_MINIMAL_MARKER);
        return;
    }
    encoded_keys->push_back(KeyConsts::KEY_NORMAL_MARKER);
    _seq_coder.full_encode_ascending(*field, pos, _seq_coder.length, encoded_keys);
}

void RowKeyEncoder::append_rowid_suffix(std::string* encoded_keys, uint32_t rowid) const {
    encoded_keys->push_back(KeyConsts::KEY_NORMAL_MARKER);
    KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>::full_encode_ascending(&rowid,
                                                                                   encoded_keys);
}

} // namespace doris
