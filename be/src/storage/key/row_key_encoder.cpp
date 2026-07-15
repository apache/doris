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

#include <cassert>

#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/consts.h"
#include "common/logging.h"
#include "storage/iterator/olap_data_convertor.h"
#include "storage/key_coder.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

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
        const auto& column = schema.column(schema.sequence_col_idx());
        _seq_coder = get_key_coder(column.type());
        _seq_col_length = column.length();
    }

    if (schema.cluster_key_uids().empty()) {
        _add_default_sort_key_columns(schema);
        return;
    }

    for (size_t cid = 0; cid < schema.num_key_columns(); ++cid) {
        _primary_key_coders.push_back(get_key_coder(schema.column(cid).type()));
    }
    _rowid_coder = get_key_coder(FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT);
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
    _sort_key_coders.push_back(get_key_coder(column.type()));
    _sort_key_index_size.push_back(cast_set<uint16_t>(column.index_length()));
}

std::string RowKeyEncoder::full_encode(const std::vector<IOlapColumnDataAccessor*>& key_columns,
                                       size_t pos) const {
    assert(_sort_key_index_size.size() == _sort_key_coders.size());
    assert(key_columns.size() == _sort_key_coders.size());
    return _full_encode(_sort_key_coders, key_columns, pos);
}

std::string RowKeyEncoder::full_encode_primary_keys(
        const std::vector<IOlapColumnDataAccessor*>& key_columns, size_t pos) const {
    return _full_encode(_primary_key_coders, key_columns, pos);
}

namespace {
// Shared row-key encoding base: for each key column, write a null marker for
// a null value, otherwise a normal marker followed by whatever `encode_field`
// appends. `encode_field(cid, field, out)` is the only thing that differs between
// the full key encode and the short-key prefix encode.
template <typename EncodeField>
std::string encode_key_columns(const std::vector<IOlapColumnDataAccessor*>& key_columns, size_t pos,
                               EncodeField&& encode_field) {
    std::string encoded_keys;
    size_t cid = 0;
    for (const auto& column : key_columns) {
        const auto* field = column->get_data_at(pos);
        if (UNLIKELY(!field)) {
            encoded_keys.push_back(KeyConsts::KEY_NULL_FIRST_MARKER);
            ++cid;
            continue;
        }
        encoded_keys.push_back(KeyConsts::KEY_NORMAL_MARKER);
        encode_field(cid, field, &encoded_keys);
        ++cid;
    }
    return encoded_keys;
}
} // namespace

std::string RowKeyEncoder::_full_encode(const std::vector<const KeyCoder*>& key_coders,
                                        const std::vector<IOlapColumnDataAccessor*>& key_columns,
                                        size_t pos) {
    assert(key_columns.size() == key_coders.size());
    return encode_key_columns(key_columns, pos,
                              [&](size_t cid, const void* field, std::string* out) {
                                  DCHECK(key_coders[cid] != nullptr);
                                  key_coders[cid]->full_encode_ascending(field, out);
                              });
}

std::string RowKeyEncoder::encode_short_keys(
        const std::vector<IOlapColumnDataAccessor*>& key_columns, size_t pos) const {
    assert(key_columns.size() == _num_short_key_columns);
    assert(key_columns.size() <= _sort_key_coders.size());
    return encode_key_columns(
            key_columns, pos, [&](size_t cid, const void* field, std::string* out) {
                _sort_key_coders[cid]->encode_ascending(field, _sort_key_index_size[cid], out);
            });
}

void RowKeyEncoder::append_seq_suffix(std::string* encoded_keys,
                                      const IOlapColumnDataAccessor* seq_column, size_t pos) const {
    const auto* field = seq_column->get_data_at(pos);
    // So the primary key index can still use it, encode a null seq column as
    // the smallest value of its length.
    if (UNLIKELY(!field)) {
        encoded_keys->push_back(KeyConsts::KEY_NULL_FIRST_MARKER);
        encoded_keys->append(_seq_col_length, KeyConsts::KEY_MINIMAL_MARKER);
        return;
    }
    encoded_keys->push_back(KeyConsts::KEY_NORMAL_MARKER);
    _seq_coder->full_encode_ascending(field, encoded_keys);
}

void RowKeyEncoder::append_rowid_suffix(std::string* encoded_keys, uint32_t rowid) const {
    encoded_keys->push_back(KeyConsts::KEY_NORMAL_MARKER);
    _rowid_coder->full_encode_ascending(&rowid, encoded_keys);
}

} // namespace doris
