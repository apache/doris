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

#include "olap/rowset/segment_v2/variant/sparse_column_merge_iterator.h"

#include <memory>
#include <string_view>
#include <unordered_map>

#include "common/status.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/stream_reader.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_variant.h"
#include "vec/columns/subcolumn_tree.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::segment_v2 {

#include "common/compile_check_begin.h"

Status SparseColumnMergeIterator::seek_to_ordinal(ordinal_t ord) {
    RETURN_IF_ERROR(_sparse_column_reader->seek_to_ordinal(ord));
    for (auto& entry : _src_subcolumns_for_sparse) {
        RETURN_IF_ERROR(entry->data.iterator->seek_to_ordinal(ord));
    }
    return Status::OK();
}

Status SparseColumnMergeIterator::init(const ColumnIteratorOptions& opts) {
    RETURN_IF_ERROR(_sparse_column_reader->init(opts));
    for (auto& entry : _src_subcolumns_for_sparse) {
        entry->data.serde = entry->data.type->get_serde();
        RETURN_IF_ERROR(entry->data.iterator->init(opts));
        const auto& path = entry->path.get_path();
        _sorted_src_subcolumn_for_sparse.emplace_back(StringRef(path.data(), path.size()), entry);
    }

    // sort src subcolumns by path
    std::sort(
            _sorted_src_subcolumn_for_sparse.begin(), _sorted_src_subcolumn_for_sparse.end(),
            [](const auto& lhsItem, const auto& rhsItem) { return lhsItem.first < rhsItem.first; });
    return Status::OK();
}

void SparseColumnMergeIterator::_serialize_nullable_column_to_sparse(
        const SubstreamReaderTree::Node* src_subcolumn,
        vectorized::ColumnString& dst_sparse_column_paths,
        vectorized::ColumnString& dst_sparse_column_values, const StringRef& src_path, size_t row) {
    // every subcolumn is always Nullable
    const auto& nullable_serde =
            assert_cast<vectorized::DataTypeNullableSerDe&>(*src_subcolumn->data.serde);
    const auto& nullable_col =
            assert_cast<const vectorized::ColumnNullable&, TypeCheckOnRelease::DISABLE>(
                    *src_subcolumn->data.column);
    if (nullable_col.is_null_at(row)) {
        return;
    }
    // insert key
    dst_sparse_column_paths.insert_data(src_path.data, src_path.size);
    // insert value
    vectorized::ColumnString::Chars& chars = dst_sparse_column_values.get_chars();
    nullable_serde.get_nested_serde()->write_one_cell_to_binary(nullable_col.get_nested_column(),
                                                                chars, row);
    dst_sparse_column_values.get_offsets().push_back(chars.size());
}

void SparseColumnMergeIterator::_process_data_without_sparse_column(
        vectorized::MutableColumnPtr& dst, size_t num_rows) {
    if (_src_subcolumns_for_sparse.empty()) {
        dst->insert_many_defaults(num_rows);
    } else {
        // merge subcolumns to sparse column
        // Otherwise insert required src dense columns into sparse column.
        auto& map_column = assert_cast<vectorized::ColumnMap&>(*dst);
        auto& sparse_column_keys = assert_cast<vectorized::ColumnString&>(map_column.get_keys());
        auto& sparse_column_values =
                assert_cast<vectorized::ColumnString&>(map_column.get_values());
        auto& sparse_column_offsets = map_column.get_offsets();
        for (size_t i = 0; i != num_rows; ++i) {
            // Paths in sorted_src_subcolumn_for_sparse_column are already sorted.
            for (const auto& entry : _sorted_src_subcolumn_for_sparse) {
                const auto& path = entry.first;
                _serialize_nullable_column_to_sparse(entry.second.get(), sparse_column_keys,
                                                     sparse_column_values, path, i);
            }
            sparse_column_offsets.push_back(sparse_column_keys.size());
        }
    }
}

void SparseColumnMergeIterator::_merge_to(vectorized::MutableColumnPtr& dst) {
    auto& column_map = assert_cast<vectorized::ColumnMap&>(*dst);
    auto& dst_sparse_column_paths = assert_cast<vectorized::ColumnString&>(column_map.get_keys());
    auto& dst_sparse_column_values =
            assert_cast<vectorized::ColumnString&>(column_map.get_values());
    auto& dst_sparse_column_offsets = column_map.get_offsets();

    const auto& src_column_map = assert_cast<const vectorized::ColumnMap&>(*_sparse_column);
    const auto& src_sparse_column_paths =
            assert_cast<const vectorized::ColumnString&>(*src_column_map.get_keys_ptr());
    const auto& src_sparse_column_values =
            assert_cast<const vectorized::ColumnString&>(*src_column_map.get_values_ptr());
    const auto& src_serialized_sparse_column_offsets = src_column_map.get_offsets();
    DCHECK_EQ(src_sparse_column_paths.size(), src_sparse_column_values.size());
    // Src object column contains some paths in serialized sparse column in specified range.
    // Iterate over this range and insert all required paths into serialized sparse column or subcolumns.
    for (size_t row = 0; row != _sparse_column->size(); ++row) {
        // Use separate index to iterate over sorted sorted_src_subcolumn_for_sparse_column.
        size_t sorted_src_subcolumn_for_sparse_column_idx = 0;
        size_t sorted_src_subcolumn_for_sparse_column_size = _src_subcolumns_for_sparse.size();

        size_t offset = src_serialized_sparse_column_offsets[row - 1];
        size_t end = src_serialized_sparse_column_offsets[row];
        // Iterator over [path, binary value]
        for (size_t i = offset; i != end; ++i) {
            const StringRef src_sparse_path_string = src_sparse_column_paths.get_data_at(i);
            // Check if we have this path in subcolumns. This path already materialized in subcolumns.
            // So we don't need to insert it into sparse column.
            if (!_src_subcolumn_map.contains(src_sparse_path_string)) {
                // Before inserting this path into sparse column check if we need to
                // insert subcolumns from sorted_src_subcolumn_for_sparse_column before.
                while (sorted_src_subcolumn_for_sparse_column_idx <
                               sorted_src_subcolumn_for_sparse_column_size &&
                       _sorted_src_subcolumn_for_sparse[sorted_src_subcolumn_for_sparse_column_idx]
                                       .first < src_sparse_path_string) {
                    auto& [src_path, src_subcolumn] = _sorted_src_subcolumn_for_sparse
                            [sorted_src_subcolumn_for_sparse_column_idx++];
                    _serialize_nullable_column_to_sparse(src_subcolumn.get(),
                                                         dst_sparse_column_paths,
                                                         dst_sparse_column_values, src_path, row);
                }

                /// Insert path and value from src sparse column to our sparse column.
                dst_sparse_column_paths.insert_from(src_sparse_column_paths, i);
                dst_sparse_column_values.insert_from(src_sparse_column_values, i);
            }
        }

        // Insert remaining dynamic paths from src_dynamic_paths_for_sparse_data.
        while (sorted_src_subcolumn_for_sparse_column_idx <
               sorted_src_subcolumn_for_sparse_column_size) {
            auto& [src_path, src_subcolumn] =
                    _sorted_src_subcolumn_for_sparse[sorted_src_subcolumn_for_sparse_column_idx++];
            _serialize_nullable_column_to_sparse(src_subcolumn.get(), dst_sparse_column_paths,
                                                 dst_sparse_column_values, src_path, row);
        }

        // All the sparse columns in this row are null.
        dst_sparse_column_offsets.push_back(dst_sparse_column_paths.size());
    }
}

#include "common/compile_check_end.h"

} // namespace doris::segment_v2
