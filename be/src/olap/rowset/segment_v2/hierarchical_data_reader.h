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

#include <unordered_map>

#include "io/io_common.h"
#include "olap/iterators.h"
#include "olap/schema.h"
#include "vec/columns/subcolumn_tree.h"
#include "vec/json/path_in_data.h"

namespace doris {
namespace segment_v2 {
class ColumnIterator;

struct StreamReader {
    vectorized::ColumnPtr column;
    std::unique_ptr<ColumnIterator> iterator;
    std::shared_ptr<const vectorized::IDataType> type;
    bool inited = false;
    size_t rows_read = 0;
};

using SubstreamCache = vectorized::SubcolumnsTree<StreamReader>;

// Reader for hierarchical data for variant
class HierarchicalDataReader {
public:
    enum class ReadType {
        NONE = 0,                     //
        DIRECT_READ_MATERIALIZED = 1, // direct read column data by column reader
        EXTRACT_FROM_ROOT = 2,        // extract from root columm
        COMPOUND = 3,                 // compound read with subcolumns
    };
    HierarchicalDataReader(const Schema& schema, const StorageReadOptions& opts)
            : _schema(schema), _opts(opts) {
        _is_finalized.resize(_schema.columns().size(), false);
    }
    void clear();
    void reset();
    const SubstreamCache::Node* find_leaf(const vectorized::PathInData& path);
    bool add(const vectorized::PathInData& path, StreamReader&& sub_reader);
    Status finalize(const std::vector<ColumnId>& column_ids, vectorized::MutableColumns& result);
    Status filter(uint16_t* sel_rowid_idx, uint16_t selected_size,
                  const std::vector<ColumnId>& filtered_cids, vectorized::Block* block);
    ReaderType get_reader_type() const { return _opts.io_ctx.reader_type; }
    void set_read_type(const vectorized::PathInData& path, ReadType type) {
        _column_read_type_map[path] = type;
    }

private:
    SubstreamCache _substream_cache;
    std::vector<bool> _is_finalized;
    const Schema& _schema;
    const StorageReadOptions& _opts;
    // column path -> read type
    std::unordered_map<vectorized::PathInData, ReadType, vectorized::PathInData::Hash>
            _column_read_type_map;
};

} // namespace segment_v2
} // namespace doris
