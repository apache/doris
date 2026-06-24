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

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace doris {

class IOlapColumnDataAccessor;
class KeyCoder;
class TabletColumn;
class TabletSchema;

// Encodes rows into the sortable binary key format shared by the short key
// index and the primary key index. Each column is encoded as a one byte
// marker (KeyConsts) followed by the KeyCoder encoded value, a null column
// is encoded as KEY_NULL_FIRST_MARKER without value bytes.
class RowKeyEncoder {
public:
    RowKeyEncoder(const TabletSchema& schema, bool mow);

    // Encode the sort key columns at `pos` with full length.
    std::string full_encode(const std::vector<IOlapColumnDataAccessor*>& key_columns,
                            size_t pos) const;

    // Encode the primary key columns at `pos` with full length, producing the
    // key stored in and probed against the primary key index.
    std::string full_encode_primary_keys(const std::vector<IOlapColumnDataAccessor*>& key_columns,
                                         size_t pos) const;

    // Encode the short key columns at `pos`, each column truncated to its
    // index length.
    std::string encode_short_keys(const std::vector<IOlapColumnDataAccessor*>& key_columns,
                                  size_t pos) const;

    // Append the encoded sequence column at `pos` to `encoded_keys`. A null
    // sequence value is encoded as the minimal value of the column length so
    // that it sorts first in the primary key index.
    void append_seq_suffix(std::string* encoded_keys, const IOlapColumnDataAccessor* seq_column,
                           size_t pos) const;

    // Append the encoded row id to `encoded_keys`, only used by mow tables
    // with cluster keys.
    void append_rowid_suffix(std::string* encoded_keys, uint32_t rowid) const;

    size_t num_sort_key_columns() const { return _sort_key_coders.size(); }

private:
    static std::string _full_encode(const std::vector<const KeyCoder*>& key_coders,
                                    const std::vector<IOlapColumnDataAccessor*>& key_columns,
                                    size_t pos);

    void _add_sort_key_column(const TabletColumn& column);

    // The sort-key view: whatever the segment sorts by. Cluster key columns
    // for mow tables with cluster keys, primary key columns otherwise. Used by
    // full_encode() and encode_short_keys().
    std::vector<const KeyCoder*> _sort_key_coders;
    std::vector<uint16_t> _sort_key_index_size;
    // The primary-key view: always the primary key columns. The primary key
    // index is built on these even when the segment sorts by cluster keys;
    // used by full_encode_primary_keys().
    std::vector<const KeyCoder*> _primary_key_coders;
    const KeyCoder* _seq_coder = nullptr;
    const KeyCoder* _rowid_coder = nullptr;
    size_t _seq_col_length = 0;
    size_t _num_short_key_columns = 0;
};

} // namespace doris
