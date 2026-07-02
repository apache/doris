// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "common/status.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/parquet_column_schema.h"
#include "format_v2/parquet/parquet_file_context.h"
#include "format_v2/parquet/parquet_profile.h"
#include "format_v2/parquet/parquet_scan.h"

namespace doris {
namespace io {
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::format::parquet {

struct ParquetReaderScanState;

// ============================================================================
// ============================================================================
//   init() -> get_schema() -> open(request) -> get_block() [loop] -> close()
// ============================================================================
class ParquetReader : public format::FileReader {
public:
    ParquetReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                  std::unique_ptr<io::FileDescription>& file_description,
                  std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                  std::optional<format::GlobalRowIdContext> global_rowid_context = std::nullopt,
                  bool enable_mapping_timestamp_tz = false);
    ~ParquetReader() override;

    Status init(RuntimeState* state) override;

    void set_batch_size(size_t batch_size) override;

    Status get_schema(std::vector<format::ColumnDefinition>* file_schema) const override;

    std::unique_ptr<format::TableColumnMapper> create_column_mapper(
            format::TableColumnMapperOptions options) const override;

    Status open(std::shared_ptr<format::FileScanRequest> request) override;

    Status get_block(Block* file_block, size_t* rows, bool* eof) override;

    Status get_aggregate_result(const format::FileAggregateRequest& request,
                                format::FileAggregateResult* result) override;

    void set_condition_cache_context(std::shared_ptr<ConditionCacheContext> ctx) override;

    int64_t get_total_rows() const override;

    Status close() override;

protected:
    void _init_profile() override;

private:
    void _sync_page_cache_profile();

    void _fill_column_definition(const ParquetColumnSchema& column_schema,
                                 format::ColumnDefinition* field) const;

    std::unique_ptr<ParquetReaderScanState>
            _state;                  // complete scan state (file_context + schema + scheduler)
    ParquetProfile _parquet_profile; // RuntimeProfile counter set
    ParquetPageCacheStats _reported_page_cache_stats;
    std::optional<format::GlobalRowIdContext> _global_rowid_context; // global RowId context
    size_t _batch_size = ParquetScanScheduler::DEFAULT_READ_BATCH_SIZE;
    bool _enable_mapping_timestamp_tz = false; // whether UTC timestamps are mapped to TIMESTAMPTZ
};

} // namespace doris::format::parquet
