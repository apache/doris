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

#include <cstdint>
#include <memory>
#include <span>
#include <vector>

#include "common/status.h"
#include "core/column/column.h"

namespace doris {

class SlotDescriptor;
class StorageReadOptions;
class TabletColumn;
class TabletSchema;

namespace segment_v2 {

class ColumnIterator;
class Segment;
class SegmentReadAhead;

struct RowIdColumnRead {
    /// Logical slot used to resolve the physical TabletColumn.
    SlotDescriptor* slot {nullptr};
    /// Destination column populated by the exact row-id read.
    MutableColumnPtr* result {nullptr};
    /// Per-column read options updated to use the read-ahead reader.
    StorageReadOptions* read_options {nullptr};
    /// Iterator initialized on demand and reused for decoding.
    std::unique_ptr<ColumnIterator>* iterator {nullptr};
};

/// Initializes supported column iterators on the read-ahead reader and submits all of their data
/// pages together. Variant columns keep using the original Segment path.
Status prepare_columns_by_rowids_with_read_ahead(Segment& segment, const TabletSchema& schema,
                                                 const std::vector<uint32_t>& row_ids,
                                                 std::span<RowIdColumnRead> columns,
                                                 SegmentReadAhead& read_ahead);

/// Prepares all columns, then delegates decoding to Segment::seek_and_read_by_rowid().
Status read_columns_by_rowids_with_read_ahead(Segment& segment, const TabletSchema& schema,
                                              const std::vector<uint32_t>& row_ids,
                                              std::span<RowIdColumnRead> columns,
                                              SegmentReadAhead& read_ahead);

/// Reads a physical column whose TabletColumn is already known, such as the row-store column.
Status read_column_by_rowids_with_read_ahead(Segment& segment, const TabletColumn& column,
                                             const std::vector<uint32_t>& row_ids,
                                             MutableColumnPtr& result,
                                             StorageReadOptions& read_options,
                                             std::unique_ptr<ColumnIterator>& iterator,
                                             SegmentReadAhead& read_ahead);

} // namespace segment_v2
} // namespace doris
