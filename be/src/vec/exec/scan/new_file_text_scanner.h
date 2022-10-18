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

#include <exec/arrow/arrow_reader.h>

#include "exec/line_reader.h"
#include "exprs/bloomfilter_predicate.h"
#include "exprs/function_filter.h"
#include "vec/exec/scan/new_file_scanner.h"
#include "vec/exec/scan/vscanner.h"

namespace doris::vectorized {
class NewFileTextScanner : public NewFileScanner {
public:
    NewFileTextScanner(RuntimeState* state, NewFileScanNode* parent, int64_t limit,
                       const TFileScanRange& scan_range, RuntimeProfile* profile,
                       const std::vector<TExpr>& pre_filter_texprs);

    Status open(RuntimeState* state) override;

protected:
    void _init_profiles(RuntimeProfile* profile) override {}
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eos) override;
    Status _convert_to_output_block(Block* output_block);

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
