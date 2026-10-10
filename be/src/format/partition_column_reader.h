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
#include <memory>

#include "format/count_reader.h"
#include "format/table/table_format_reader.h"

namespace doris {

// Decorates an initialized Hive/Hudi reader so a partition-only duplicate-insensitive aggregate is
// answered from partition metadata instead of file data.
//
// One row of partition values is emitted per scan range, unconditionally. A range whose file turns
// out to hold zero rows still contributes its partition value, so MAX/GROUP BY/DISTINCT can name a
// partition that a full scan would not return. A partition with no file at all contributes nothing,
// because it produces no scan range.
class PartitionColumnReader final : public CountReader {
public:
    // The reader must be initialized (footer parsed) before it can fill the typed partition values,
    // but its row count is irrelevant now, so V1 no longer requires the whole file: the whole-range
    // rule existed only because a partial range's count was unreliable (Parquet row groups are not
    // filtered by range when counting, and ORC's count reads 0 until its row reader exists).
    static bool supports_range(const TFileRangeDesc& range, TFileFormatType::type format_type) {
        return range.__isset.table_format_params &&
               (range.table_format_params.table_format_type == "hive" ||
                range.table_format_params.table_format_type == "hudi") &&
               (format_type == TFileFormatType::FORMAT_PARQUET ||
                format_type == TFileFormatType::FORMAT_ORC);
    }

    explicit PartitionColumnReader(std::unique_ptr<TableFormatReader> inner_reader)
            : CountReader(1, 1, std::move(inner_reader)) {
        DORIS_CHECK(this->inner_reader() != nullptr);
        set_push_down_agg_type(TPushAggOp::type::PARTITION_VALUE);
    }

protected:
    Status on_after_read_block(Block* block, size_t* read_rows) override {
        if (*read_rows > 0) {
            // CountReader supplies cardinality; the initialized reader owns typed partition values.
            // Fill helpers append, so discard the default cells before materializing constants.
            block->clear_column_data();
            RETURN_IF_ERROR(static_cast<TableFormatReader*>(inner_reader())
                                    ->fill_remaining_columns(block, *read_rows));
        }
        return Status::OK();
    }
};

} // namespace doris
