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

#include <arrow/array.h>
#include <exec/arrow/arrow_reader.h>
#include <exec/arrow/orc_reader.h>

#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "exec/base_scanner.h"
#include "util/runtime_profile.h"
#include "vec/exec/file_scanner.h"

namespace doris::vectorized {

// VArrow scanner convert the data read from orc|parquet to doris's columns.
class FileArrowScanner : public FileScanner {
public:
    FileArrowScanner(RuntimeState* state, RuntimeProfile* profile,
                     const TFileScanRangeParams& params, const std::vector<TFileRangeDesc>& ranges,
                     const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);

    ~FileArrowScanner() override;

    // Open this scanner, will initialize information need to
    Status open() override;

    Status get_next(Block* block, bool* eof) override;

    void close() override;

protected:
    virtual ArrowReaderWrap* _new_arrow_reader(FileReader* file_reader, int64_t batch_size,
                                               int32_t num_of_columns_from_file) = 0;

private:
    // Read next buffer from reader
    Status _open_next_reader();
    Status _next_arrow_batch();
    Status _init_arrow_batch_if_necessary();
    Status _append_batch_to_block(Block* block);

private:
    // Reader
    ArrowReaderWrap* _cur_file_reader;
    bool _cur_file_eof; // is read over?
    std::shared_ptr<arrow::RecordBatch> _batch;
    size_t _arrow_batch_cur_idx;
};

class VFileParquetScanner final : public FileArrowScanner {
public:
    VFileParquetScanner(RuntimeState* state, RuntimeProfile* profile,
                        const TFileScanRangeParams& params,
                        const std::vector<TFileRangeDesc>& ranges,
                        const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);

    ~VFileParquetScanner() override = default;

protected:
    ArrowReaderWrap* _new_arrow_reader(FileReader* file_reader, int64_t batch_size,
                                       int32_t num_of_columns_from_file) override;
};

class VFileORCScanner final : public FileArrowScanner {
public:
    VFileORCScanner(RuntimeState* state, RuntimeProfile* profile,
                    const TFileScanRangeParams& params, const std::vector<TFileRangeDesc>& ranges,
                    const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);

    ~VFileORCScanner() override = default;

protected:
    ArrowReaderWrap* _new_arrow_reader(FileReader* file_reader, int64_t batch_size,
                                       int32_t num_of_columns_from_file) override;
};

} // namespace doris::vectorized
