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

#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/status.h"
#include "exec/base_scanner.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/mem_pool.h"
#include "util/runtime_profile.h"
#include "util/slice.h"

namespace doris {

class Tuple;
class SlotDescriptor;
struct Slice;
class TextConverter;
class FileReader;
class LineReader;
class Decompressor;
class RuntimeState;
class ExprContext;
class TupleDescriptor;
class TupleRow;
class RowDescriptor;
class RuntimeProfile;
class StreamLoadPipe;

// Broker scanner convert the data read from broker to doris's tuple.
class BrokerScanner : public BaseScanner {
public:
    BrokerScanner(RuntimeState* state, RuntimeProfile* profile,
                  const TBrokerScanRangeParams& params, const std::vector<TBrokerRangeDesc>& ranges,
                  const std::vector<TNetworkAddress>& broker_addresses,
                  const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);
    ~BrokerScanner() override;

    // Open this scanner, will initialize information need to
    Status open() override;

    // Get next tuple
    virtual Status get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof,
                            bool* fill_tuple) override;

    Status get_next(vectorized::Block* block, bool* eof) override {
        return Status::NotSupported("Not Implemented get block");
    }

    // Close this scanner
    void close() override;

protected:
    // Read next buffer from reader
    Status open_next_reader();

    Status _line_to_src_tuple(const Slice& line);

    Status _line_split_to_values(const Slice& line);

private:
    Status open_file_reader();
    Status create_decompressor(TFileFormatType::type type);
    Status open_line_reader();

    // Split one text line to values
    void split_line(const Slice& line);

    void fill_fix_length_string(const Slice& value, MemPool* pool, char** new_value_p,
                                int new_value_length);

    bool check_decimal_input(const Slice& value, int precision, int scale,
                             std::stringstream* error_msg);

    // Convert one row to one tuple
    //  'ptr' and 'len' is csv text line
    //  output is tuple
    Status _convert_one_row(const Slice& line, Tuple* tuple, MemPool* tuple_pool, bool* fill_tuple);

protected:
    std::string _value_separator;
    std::string _line_delimiter;
    TFileFormatType::type _file_format_type;
    int _value_separator_length;
    int _line_delimiter_length;

    // Reader
    // _cur_file_reader_s is for stream load pipe reader,
    // and _cur_file_reader is for other file reader.
    // TODO: refactor this to use only shared_ptr or unique_ptr
    std::unique_ptr<FileReader> _cur_file_reader;
    std::shared_ptr<FileReader> _cur_file_reader_s;
    FileReader* _real_reader;
    LineReader* _cur_line_reader;
    Decompressor* _cur_decompressor;
    bool _cur_line_reader_eof;

    // When we fetch range start from 0, header_type="csv_with_names" skip first line
    // When we fetch range start from 0, header_type="csv_with_names_and_types" skip first two line
    // When we fetch range doesn't start from 0 will always skip the first line
    int _skip_lines;

    std::vector<Slice> _split_values;
};

} // namespace doris
