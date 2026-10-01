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

// Decorates an initialized Hive reader after its footer proves the range cardinality.
// Partition-only duplicate-insensitive aggregates need one row from a nonempty range,
// but a valid empty file must contribute no partition value.
class PartitionColumnReader final : public CountReader {
public:
    static bool supports_range(const TFileRangeDesc& range, TFileFormatType::type format_type) {
        return range.__isset.table_format_params &&
               range.table_format_params.table_format_type == "hive" &&
               (format_type == TFileFormatType::FORMAT_PARQUET ||
                format_type == TFileFormatType::FORMAT_ORC) &&
               range.start_offset == 0 && range.file_size >= 0 && range.size == range.file_size;
    }

    PartitionColumnReader(int64_t total_rows, std::unique_ptr<TableFormatReader> inner_reader)
            : CountReader(total_rows > 0 ? 1 : 0, 1, std::move(inner_reader)) {
        DORIS_CHECK(total_rows >= 0);
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
