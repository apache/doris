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

#include <memory>
#include <string>

#include "format_v2/parquet/parquet_type.h"
#include "format_v2/parquet/reader/arrow_leaf_reader_adapter.h"
#include "format_v2/parquet/reader/column_reader.h"

namespace parquet {
class ColumnDescriptor;

namespace internal {
class RecordReader;
} // namespace internal
} // namespace parquet

namespace doris::parquet {

class ScalarColumnReader final : public ParquetColumnReader {
public:
    ScalarColumnReader(const ParquetColumnSchema& column_schema,
                       std::shared_ptr<::parquet::internal::RecordReader> record_reader,
                       const ParquetPageSkipPlan* page_skip_plan = nullptr,
                       ParquetColumnReaderProfile profile = {});

    Status read(int64_t rows, MutableColumnPtr& column, int64_t* rows_read) override;
    Status skip(int64_t rows) override;

    const ::parquet::ColumnDescriptor* descriptor() const { return _descriptor; }
    ArrowLeafReaderContext leaf_context() const {
        return ArrowLeafReaderContext {_descriptor, _type_descriptor, _type,
                                       _name,       _record_reader,   _profile};
    }
    void advance_rows_read(int64_t rows);

private:
    Status skip_records(int64_t rows);
    int64_t page_filtered_rows_to_skip(int64_t rows) const;

    const ::parquet::ColumnDescriptor* _descriptor = nullptr;
    ParquetTypeDescriptor _type_descriptor;
    std::shared_ptr<::parquet::internal::RecordReader> _record_reader;
    const ParquetPageSkipPlan* _page_skip_plan = nullptr;
    int64_t _row_group_rows_read = 0;
};

} // namespace doris::parquet
