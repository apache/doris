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

#include "common/consts.h"
#include "exec/base_scanner.h"
#include "exec/decompressor.h"
#include "exec/line_reader.h"
#include "exec/plain_binary_line_reader.h"
#include "exec/plain_text_line_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/file_factory.h"
#include "io/file_reader.h"
#include "vec/exec/file_scanner.h"

namespace doris::vectorized {
class FileTextScanner final : public FileScanner {
public:
    FileTextScanner(RuntimeState* state, RuntimeProfile* profile,
                    const TFileScanRangeParams& params, const std::vector<TFileRangeDesc>& ranges,
                    const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);

    ~FileTextScanner() override;

    Status open() override;

    Status get_next(Block* block, bool* eof) override;
    void close() override;

protected:
    void _init_profiles(RuntimeProfile* profile) override {}

private:
    Status _fill_file_columns(const Slice& line, vectorized::Block* _block);
    Status _open_next_reader();
    Status _open_file_reader();
    Status _open_line_reader();
    Status _line_split_to_values(const Slice& line);
    Status _split_line(const Slice& line);
    // Reader
    std::unique_ptr<FileReader> _cur_file_reader;
    LineReader* _cur_line_reader;
    bool _cur_line_reader_eof;

    // When we fetch range start from 0, header_type="csv_with_names" skip first line
    // When we fetch range start from 0, header_type="csv_with_names_and_types" skip first two line
    // When we fetch range doesn't start
    int _skip_lines;
    std::vector<Slice> _split_values;
    std::string _value_separator;
    std::string _line_delimiter;
    int _value_separator_length;
    int _line_delimiter_length;

    bool _success;
};
} // namespace doris::vectorized
