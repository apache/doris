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

#include <gen_cpp/parquet_types.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "common/status.h"
#include "format_v2/parquet/reader/native/column_chunk_reader.h"
#include "io/fs/file_reader_writer_fwd.h"

namespace doris {
struct FieldSchema;
namespace io {
struct IOContext;
}
} // namespace doris

namespace doris::format::parquet::native {

// A physical-leaf reader that advances all three Parquet streams (repetition levels, definition
// levels and encoded values) but retains only the two level streams. It is used for shape-only
// operations such as COUNT(nullable_col), where materializing a large BYTE_ARRAY value would be
// both unnecessary and potentially unbounded.
//
// This deliberately shares ColumnChunkReader with the value path. Page V1/V2 parsing,
// decompression, dictionary-page handling, page cache semantics and corruption checks therefore
// cannot drift between an aggregate shortcut and an ordinary scan.
class LevelReader {
public:
    class Impl;

    static Status create(io::FileReaderSPtr file, tparquet::ColumnChunk column_chunk,
                         FieldSchema* field, size_t total_rows, size_t max_buffer_size,
                         io::IOContext* io_ctx, bool enable_page_cache,
                         std::unique_ptr<LevelReader>* reader);

    ~LevelReader();

    Status read_rows(size_t rows, std::vector<int16_t>* repetition_levels,
                     std::vector<int16_t>* definition_levels, size_t* rows_read);
    Status skip_rows(size_t rows);
    ColumnChunkReaderStatistics statistics();

private:
    explicit LevelReader(std::unique_ptr<Impl> impl);

    std::unique_ptr<Impl> _impl;
    // COUNT range gaps can call skip repeatedly as the adaptive row cap changes. Keep these
    // throw-away level buffers on the persistent reader so only their logical sizes are reset.
    std::vector<int16_t> _skip_repetition_levels;
    std::vector<int16_t> _skip_definition_levels;
};

} // namespace doris::format::parquet::native
