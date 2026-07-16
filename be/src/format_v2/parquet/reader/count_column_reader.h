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

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "format_v2/column_data.h"
#include "format_v2/parquet/parquet_profile.h"
#include "format_v2/parquet/reader/native/level_reader.h"
#include "io/fs/file_reader_writer_fwd.h"

namespace doris {
class FileMetaData;
namespace io {
struct IOContext;
}
} // namespace doris

namespace doris::format::parquet {
struct ParquetColumnSchema;
// Shape-only COUNT(nullable_col) reader. It uses v2's native LevelReader, so BYTE_ARRAY payloads
// are skipped in the encoding stream and never copied into Arrow builders or Doris strings.
class CountColumnReader {
public:
    ~CountColumnReader();

    static Status create(io::FileReaderSPtr file, const FileMetaData* metadata, int row_group_id,
                         const ParquetColumnSchema& root_schema,
                         const format::LocalColumnIndex* projection, io::IOContext* io_ctx,
                         bool enable_page_cache, const std::string& page_cache_file_key,
                         ParquetColumnReaderProfile profile,
                         std::unique_ptr<CountColumnReader>* reader);

    Status skip(int64_t rows);
    Status read_levels(int64_t rows, int64_t* rows_read);

    const std::vector<int16_t>& definition_levels() const { return _definition_levels; }
    const std::vector<int16_t>& repetition_levels() const { return _repetition_levels; }
    int64_t levels_written() const { return _levels_written; }

private:
    CountColumnReader(std::string name, std::unique_ptr<native::LevelReader> level_reader,
                      ParquetColumnReaderProfile profile);
    void sync_profile();

    std::unique_ptr<native::LevelReader> _level_reader;
    ParquetColumnReaderProfile _profile;
    std::string _name;
    std::vector<int16_t> _definition_levels;
    std::vector<int16_t> _repetition_levels;
    native::ColumnChunkReaderStatistics _reported_native_stats;
    int64_t _levels_written = 0;
};

} // namespace doris::format::parquet
