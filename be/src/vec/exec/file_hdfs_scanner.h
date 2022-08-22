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

#include "common/status.h"
#include "file_scanner.h"
#include "vec/core/block.h"
#include "vec/exec/format/parquet/vparquet_reader.h"

namespace doris::vectorized {

class HdfsFileScanner : public FileScanner {
public:
    HdfsFileScanner(RuntimeState* state, RuntimeProfile* profile,
                    const TFileScanRangeParams& params, const std::vector<TFileRangeDesc>& ranges,
                    const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter)
            : FileScanner(state, profile, params, ranges, pre_filter_texprs, counter) {};
};

class ParquetFileHdfsScanner : public HdfsFileScanner {
public:
    ParquetFileHdfsScanner(RuntimeState* state, RuntimeProfile* profile,
                           const TFileScanRangeParams& params,
                           const std::vector<TFileRangeDesc>& ranges,
                           const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);
    ~ParquetFileHdfsScanner();
    Status open() override;

    Status get_next(vectorized::Block* block, bool* eof) override;
    void close() override;

protected:
    void _init_profiles(RuntimeProfile* profile) override;

private:
    Status _get_next_reader(int _next_range);

private:
    std::shared_ptr<ParquetReader> _reader;
    int64_t _current_range_offset;
};

} // namespace doris::vectorized