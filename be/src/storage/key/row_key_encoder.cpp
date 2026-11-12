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
#include <string_view>
#include <utility>

#include "common/cast_set.h"
#include "common/check.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/consts.h"
#include "common/exception.h"
#include "common/logging.h"
#include "core/assert_cast.h"
#include "core/column/column_decimal.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "storage/key_coder.h"
#include "storage/storage_layout.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {
// The column holding the value of row `pos`, nullptr when that value is NULL.
const IColumn* value_column_at(const IColumn* column, size_t pos) {
    DCHECK(!is_column_const(*column));
    // A key column is never a ColumnConst, so is_nullable() is true only for a ColumnNullable.
    if (!column->is_nullable()) {
        return column;
    }
    const auto& nullable = assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*column);
    return nullable.is_null_at(pos) ? nullptr : &nullable.get_nested_column();
}

// A string key is encoded from the row's own bytes, which CHAR pads below. The key columns'
// types are fixed by the schema and this runs once per row and key column, so the casts here
// and in encode_ascending skip the release-build type check.
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
            RowKeyEncoder::full_encode_string(value.data, value.size, buf);
        }
    } else {
        using Column =
                typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::ColumnType;
        const auto value = StorageLayout<FT>::to_storage(
                assert_cast<const Column&, TypeCheckOnRelease::DISABLE>(column).get_data()[pos]);
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
        using Column =
                typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::ColumnType;
        const auto value = StorageLayout<FT>::to_storage(
                assert_cast<const Column&, TypeCheckOnRelease::DISABLE>(column).get_data()[pos]);
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
        throw Exception(ErrorCode::INTERNAL_ERROR, "column {} of type {} cannot be a key",
                        column.name(), static_cast<int>(column.type()));
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
// appends. `encode_field(cid, column, row_pos, out)` is the only thing that differs between
// the full key encode and the short-key prefix encode.
template <typename EncodeField>
std::string encode_key_columns(const std::vector<const IColumn*>& key_columns, size_t row_pos,
                               EncodeField&& encode_field) {
    std::string encoded_keys;
    size_t cid = 0;
    for (const auto& column : key_columns) {
        const auto* value_column = value_column_at(column, row_pos);
        if (UNLIKELY(!value_column)) {
            encoded_keys.push_back(KeyConsts::KEY_NULL_FIRST_MARKER);
            ++cid;
            continue;
        }
        encoded_keys.push_back(KeyConsts::KEY_NORMAL_MARKER);
        encode_field(cid, *value_column, row_pos, &encoded_keys);
        ++cid;
    }
    return encoded_keys;
}
} // namespace

std::string RowKeyEncoder::_full_encode(const std::vector<KeyColumnCoder>& key_coders,
                                        const std::vector<const IColumn*>& key_columns,
                                        size_t pos) {
    assert(key_columns.size() == key_coders.size());
    return encode_key_columns(
            key_columns, pos,
            [&](size_t cid, const IColumn& column, size_t row_pos, std::string* out) {
                const KeyColumnCoder& coder = key_coders[cid];
                coder.full_encode_ascending(column, row_pos, coder.length, out);
            });
}

std::string RowKeyEncoder::encode_short_keys(const std::vector<const IColumn*>& key_columns,
                                             size_t pos) const {
    assert(key_columns.size() == _num_short_key_columns);
    assert(key_columns.size() <= _sort_key_coders.size());
    return encode_key_columns(
            key_columns, pos,
            [&](size_t cid, const IColumn& column, size_t row_pos, std::string* out) {
                const KeyColumnCoder& coder = _sort_key_coders[cid];
                coder.encode_ascending(column, row_pos, coder.length, _sort_key_index_size[cid],
                                       out);
            });
}

void RowKeyEncoder::append_seq_suffix(std::string* encoded_keys, const IColumn* seq_column,
                                      size_t pos) const {
    const auto* value_column = value_column_at(seq_column, pos);
    // So the primary key index can still use it, encode a null seq column as
    // the smallest value of its length.
    if (UNLIKELY(!value_column)) {
        encoded_keys->push_back(KeyConsts::KEY_NULL_FIRST_MARKER);
        encoded_keys->append(_seq_coder.length, KeyConsts::KEY_MINIMAL_MARKER);
        return;
    }
    encoded_keys->push_back(KeyConsts::KEY_NORMAL_MARKER);
    _seq_coder.full_encode_ascending(*value_column, pos, _seq_coder.length, encoded_keys);
}

void RowKeyEncoder::append_rowid_suffix(std::string* encoded_keys, uint32_t rowid) const {
    encoded_keys->push_back(KeyConsts::KEY_NORMAL_MARKER);
    KeyCoderTraits<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>::full_encode_ascending(&rowid,
                                                                                   encoded_keys);
}

void RowKeyEncoder::full_encode_string(const char* data, size_t size, std::string* buf) {
    const char* end = data + size;
    const char* run = data;
    for (const char* p = data; p != end; ++p) {
        if (static_cast<uint8_t>(*p) <= KeyConsts::KEY_STRING_ESCAPE) {
            buf->append(run, p - run);
            buf->push_back(static_cast<char>(KeyConsts::KEY_STRING_ESCAPE));
            // the escaped byte starts the next run
            run = p;
        }
    }
    buf->append(run, end - run);
}

namespace {
bool is_varlen_key(const TabletColumn& column) {
    return column.type() == FieldType::OLAP_FIELD_TYPE_VARCHAR ||
           column.type() == FieldType::OLAP_FIELD_TYPE_STRING;
}

// Whether a string that ends at a marker in the layout read by cut_to_trusted_prefix() could also
// run on to a later byte from which the columns after it still encode the rest of `key`.
// `string_ends` holds the column and end of each such string.
bool has_another_layout(const TabletSchema& schema, std::string_view key,
                        const std::vector<std::pair<size_t, size_t>>& string_ends) {
    const size_t num_keys = schema.num_key_columns();
    const size_t size = key.size();
    // fits[c][p]: columns c.. encode key[p..) in some layout; fits_from[c][p]: they do so from
    // some p' >= p.
    const size_t stride = size + 2;
    std::vector<uint8_t> fits((num_keys + 1) * stride, 0);
    std::vector<uint8_t> fits_from((num_keys + 1) * stride, 0);
    const auto at = [stride](std::vector<uint8_t>& table, size_t c, size_t p) -> uint8_t& {
        return table[c * stride + p];
    };
    at(fits, num_keys, size) = 1;
    for (size_t c = num_keys + 1; c-- > 0;) {
        for (size_t p = size + 1; p-- > 0;) {
            if (c < num_keys && p < size) {
                const auto marker = static_cast<uint8_t>(key[p]);
                const TabletColumn& column = schema.column(c);
                if (marker == KeyConsts::KEY_NULL_FIRST_MARKER) {
                    at(fits, c, p) = at(fits, c + 1, p + 1);
                } else if (marker == KeyConsts::KEY_NORMAL_MARKER) {
                    if (!is_varlen_key(column)) {
                        const size_t next = p + 1 + static_cast<size_t>(column.length());
                        at(fits, c, p) = next <= size && at(fits, c + 1, next);
                    } else if (c + 1 == num_keys) {
                        at(fits, c, p) = 1;
                    } else {
                        at(fits, c, p) = at(fits_from, c + 1, p + 1);
                    }
                }
            }
            at(fits_from, c, p) = at(fits, c, p) || at(fits_from, c, p + 1);
        }
    }
    return std::ranges::any_of(string_ends, [&](const auto& string_end) {
        return at(fits_from, string_end.first + 1, string_end.second + 1) != 0;
    });
}
} // namespace

void RowKeyEncoder::cut_to_trusted_prefix(const TabletSchema& schema, std::string* key,
                                          bool* truncated) {
    const size_t num_keys = schema.num_key_columns();
    const size_t size = key->size();
    const auto byte = [&](size_t pos) { return static_cast<uint8_t>((*key)[pos]); };
    const auto cut = [&](size_t length) {
        key->resize(std::min(length, size));
        *truncated = true;
    };

    // Read `key` the way a key whose strings hold no byte up to KEY_STRING_ESCAPE is laid out:
    // a string ends at the first such byte, the marker of the next column. Every layout reads
    // the bytes before the first such end alike; past it, an unescaped string holding a byte up
    // to 0x02 may have put the column boundaries elsewhere.
    std::vector<std::pair<size_t, size_t>> string_ends; // column, end of each non-last string
    size_t first_string_end = size;
    size_t pos = 0;
    size_t cid = 0;
    for (; cid < num_keys && pos < size; ++cid) {
        if (byte(pos) == KeyConsts::KEY_NULL_FIRST_MARKER) {
            ++pos;
            continue;
        }
        if (byte(pos) != KeyConsts::KEY_NORMAL_MARKER) {
            cut(std::min(pos, first_string_end));
            return;
        }
        ++pos;
        const TabletColumn& column = schema.column(cid);
        if (!is_varlen_key(column)) {
            pos += static_cast<size_t>(column.length());
            continue;
        }
        size_t end = pos;
        while (end < size && byte(end) > KeyConsts::KEY_STRING_ESCAPE) {
            ++end;
        }
        if (end == size) {
            pos = end;
            continue;
        }
        // Only the next column's marker can end a string inside the key. Any other byte up to
        // KEY_STRING_ESCAPE is a string byte, which the two encodings write differently.
        if (cid + 1 == num_keys || (byte(end) != KeyConsts::KEY_NULL_FIRST_MARKER &&
                                    byte(end) != KeyConsts::KEY_NORMAL_MARKER)) {
            cut(std::min(end, first_string_end));
            return;
        }
        first_string_end = std::min(first_string_end, end);
        string_ends.emplace_back(cid, end);
        pos = end;
    }
    // A string may run on past the end of a cut key, and a whole key not in this layout has a
    // string holding a byte up to 0x02.
    if (*truncated || cid != num_keys || pos != size) {
        cut(std::min(pos, first_string_end));
        return;
    }
    if (!string_ends.empty() && has_another_layout(schema, *key, string_ends)) {
        cut(first_string_end);
    }
}

} // namespace doris
