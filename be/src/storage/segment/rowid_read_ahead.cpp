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

#include "storage/segment/rowid_read_ahead.h"

#include <algorithm>
#include <array>

#include "common/config.h"
#include "runtime/descriptors.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_read_ahead.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {
namespace {

Status prepare_column_iterator(Segment& segment, const TabletColumn& column,
                               StorageReadOptions& read_options,
                               std::unique_ptr<ColumnIterator>& iterator,
                               SegmentReadAhead& read_ahead) {
    DORIS_CHECK(read_options.stats != nullptr);
    if (iterator == nullptr) {
        RETURN_IF_ERROR(segment.new_column_iterator(column, &iterator, &read_options));
        auto io_ctx = read_options.io_ctx;
        io_ctx.reader_type = ReaderType::READER_QUERY;
        io_ctx.file_cache_stats = &read_options.stats->file_cache_stats;
        ColumnIteratorOptions options {
                .use_page_cache = !config::disable_storage_page_cache,
                .file_reader = read_ahead.file_reader().get(),
                .stats = read_options.stats,
                .io_ctx = io_ctx,
        };
        RETURN_IF_ERROR(iterator->init(options));
    }
    return Status::OK();
}

Status prepare_slot_iterator(Segment& segment, const TabletSchema& schema, RowIdColumnRead& column,
                             SegmentReadAhead& read_ahead, ColumnIterator** prepared) {
    DORIS_CHECK(column.slot != nullptr);
    DORIS_CHECK(column.read_options != nullptr);
    DORIS_CHECK(column.iterator != nullptr);
    DORIS_CHECK(prepared != nullptr);
    *prepared = nullptr;

    // Variant columns need Segment's existing path resolution and casting logic. Leaving their
    // iterator empty makes the subsequent seek_and_read_by_rowid() follow that path unchanged.
    if (!column.slot->column_paths().empty()) {
        return Status::OK();
    }
    const int index = column.slot->col_unique_id() >= 0
                              ? schema.field_index(column.slot->col_unique_id())
                              : schema.field_index(column.slot->col_name());
    if (index < 0) {
        return Status::OK();
    }
    const auto& tablet_column = schema.column(index);
    if (tablet_column.type() == FieldType::OLAP_FIELD_TYPE_VARIANT) {
        return Status::OK();
    }

    RETURN_IF_ERROR(prepare_column_iterator(segment, tablet_column, *column.read_options,
                                            *column.iterator, read_ahead));
    *prepared = column.iterator->get();
    return Status::OK();
}

} // namespace

Status prepare_columns_by_rowids_with_read_ahead(Segment& segment, const TabletSchema& schema,
                                                 const std::vector<uint32_t>& row_ids,
                                                 std::span<RowIdColumnRead> columns,
                                                 SegmentReadAhead& read_ahead) {
    if (row_ids.empty()) {
        return Status::OK();
    }

    std::vector<ColumnIterator*> prepared;
    prepared.reserve(columns.size());
    for (auto& column : columns) {
        ColumnIterator* iterator = nullptr;
        RETURN_IF_ERROR(prepare_slot_iterator(segment, schema, column, read_ahead, &iterator));
        if (iterator != nullptr) {
            prepared.push_back(iterator);
        }
    }
    if (!prepared.empty()) {
        static_cast<void>(read_ahead.prefetch_by_rowids(row_ids.data(), row_ids.size(), prepared));
    }

    return Status::OK();
}

Status read_columns_by_rowids_with_read_ahead(Segment& segment, const TabletSchema& schema,
                                              const std::vector<uint32_t>& row_ids,
                                              std::span<RowIdColumnRead> columns,
                                              SegmentReadAhead& read_ahead) {
    RETURN_IF_ERROR(prepare_columns_by_rowids_with_read_ahead(segment, schema, row_ids, columns,
                                                              read_ahead));

    for (auto& column : columns) {
        DORIS_CHECK(column.result != nullptr);
        RETURN_IF_ERROR(segment.seek_and_read_by_rowid(schema, column.slot, row_ids, *column.result,
                                                       *column.read_options, *column.iterator));
    }
    return Status::OK();
}

Status read_column_by_rowids_with_read_ahead(Segment& segment, const TabletColumn& column,
                                             const std::vector<uint32_t>& row_ids,
                                             MutableColumnPtr& result,
                                             StorageReadOptions& read_options,
                                             std::unique_ptr<ColumnIterator>& iterator,
                                             SegmentReadAhead& read_ahead) {
    if (row_ids.empty()) {
        return Status::OK();
    }
    DORIS_CHECK(std::is_sorted(row_ids.begin(), row_ids.end()));
    DORIS_CHECK(std::adjacent_find(row_ids.begin(), row_ids.end()) == row_ids.end());
    RETURN_IF_ERROR(prepare_column_iterator(segment, column, read_options, iterator, read_ahead));
    std::array<ColumnIterator*, 1> columns {iterator.get()};
    static_cast<void>(read_ahead.prefetch_by_rowids(row_ids.data(), row_ids.size(), columns));
    return iterator->read_by_rowids(row_ids.data(), row_ids.size(), result);
}

} // namespace doris::segment_v2
