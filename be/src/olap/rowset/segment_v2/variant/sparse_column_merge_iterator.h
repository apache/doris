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

#include <parallel_hashmap/phmap.h>

#include <memory>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "common/exception.h"
#include "common/status.h"
#include "io/io_common.h"
#include "olap/field.h"
#include "olap/iterators.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/stream_reader.h"
#include "olap/rowset/segment_v2/variant/sparse_column_extract_iterator.h"
#include "olap/schema.h"
#include "olap/tablet_schema.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_variant.h"
#include "vec/columns/subcolumn_tree.h"
#include "vec/common/assert_cast.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_variant.h"
#include "vec/functions/function_helpers.h"
#include "vec/json/path_in_data.h"

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

// Implementation for merge processor
class SparseColumnMergeIterator : public BaseSparseColumnProcessor {
public:
    SparseColumnMergeIterator(const TabletSchema::PathsSetInfo& path_set_info,
                              std::unique_ptr<ColumnIterator>&& sparse_column_reader,
                              SubstreamReaderTree&& src_subcolumns_for_sparse,
                              StorageReadOptions* opts, const TabletColumn& col)
            : BaseSparseColumnProcessor(std::move(sparse_column_reader), opts, col),
              _src_subcolumn_map(path_set_info.sub_path_set),
              _src_subcolumns_for_sparse(src_subcolumns_for_sparse) {}
    Status init(const ColumnIteratorOptions& opts) override;

    // Batch processing using template method
    Status next_batch(size_t* n, vectorized::MutableColumnPtr& dst, bool* has_null) override {
        // read subcolumns first
        RETURN_IF_ERROR(_read_subcolumns([&](SubstreamReaderTree::Node* entry) {
            bool has_null = false;
            return entry->data.iterator->next_batch(n, entry->data.column, &has_null);
        }));
        // then read sparse column
        return _process_batch(
                [&]() { return _sparse_column_reader->next_batch(n, _sparse_column, has_null); },
                *n, dst);
    }

    // RowID-based read using template method
    Status read_by_rowids(const rowid_t* rowids, const size_t count,
                          vectorized::MutableColumnPtr& dst) override {
        // read subcolumns first
        RETURN_IF_ERROR(_read_subcolumns([&](SubstreamReaderTree::Node* entry) {
            return entry->data.iterator->read_by_rowids(rowids, count, entry->data.column);
        }));
        // then read sparse column
        return _process_batch(
                [&]() {
                    return _sparse_column_reader->read_by_rowids(rowids, count, _sparse_column);
                },
                count, dst);
    }

    Status seek_to_ordinal(ordinal_t ord) override;

private:
    template <typename ReadFunction>
    Status _read_subcolumns(ReadFunction&& read_func) {
        // clear previous data
        for (auto& entry : _src_subcolumns_for_sparse) {
            entry->data.column->clear();
        }
        // read subcolumns
        for (auto& entry : _src_subcolumns_for_sparse) {
            RETURN_IF_ERROR(read_func(entry.get()));
        }
        return Status::OK();
    }

    // subcolumns in src tablet schema, which will be filtered
    PathSet _src_subcolumn_map;
    // subcolumns to merge to sparse column
    SubstreamReaderTree _src_subcolumns_for_sparse;
    std::vector<std::pair<StringRef, std::shared_ptr<SubstreamReaderTree::Node>>>
            _sorted_src_subcolumn_for_sparse;

    // Path filtering implementation
    void _process_data_with_existing_sparse_column(vectorized::MutableColumnPtr& dst,
                                                   size_t num_rows) override {
        _merge_to(dst);
    }

    void _merge_to(vectorized::MutableColumnPtr& dst);

    void _process_data_without_sparse_column(vectorized::MutableColumnPtr& dst,
                                             size_t num_rows) override;

    void _serialize_nullable_column_to_sparse(const SubstreamReaderTree::Node* src_subcolumn,
                                              vectorized::ColumnString& dst_sparse_column_paths,
                                              vectorized::ColumnString& dst_sparse_column_values,
                                              const StringRef& src_path, size_t row);
};

#include "common/compile_check_end.h"

} // namespace doris::segment_v2
