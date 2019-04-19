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
#include <vector>
#include <string>
#include <map>
#include <sstream>

#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/mem_pool.h"
#include "util/slice.h"
#include "util/runtime_profile.h"

namespace doris {

class Tuple;
class SlotDescriptor;
class Slice;
class TextConverter;
class FileReader;
class LineReader;
class Decompressor;
class RuntimeState;
class ExprContext;
class TupleDescriptor;
class TupleRow;
class RowDescriptor;
class MemTracker;
class RuntimeProfile;
class StreamLoadPipe;

struct BrokerScanCounter {
    BrokerScanCounter() :
        num_rows_total(0),
        // num_rows_returned(0),
        num_rows_filtered(0),
        num_rows_unselected(0) {
    }
    
    int64_t num_rows_total; // total read rows (read from source)
    // int64_t num_rows_returned;  // qualified rows (match the dest schema)
    int64_t num_rows_filtered;  // unqualified rows (unmatch the dest schema, or no partition)
    int64_t num_rows_unselected; // rows filterd by predicates
};

// Broker scanner convert the data read from broker to doris's tuple.
class BrokerScanner {
public:
    BrokerScanner(
        RuntimeState* state,
        RuntimeProfile* profile,
        const TBrokerScanRangeParams& params, 
        const std::vector<TBrokerRangeDesc>& ranges,
        const std::vector<TNetworkAddress>& broker_addresses,
        BrokerScanCounter* counter);
    ~BrokerScanner();

    // Open this scanner, will initialize informtion need to 
    Status open();

    // Get next tuple 
    Status get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof);

    // Close this scanner
    void close();

private:
    Status open_file_reader();
    Status create_decompressor(TFileFormatType::type type);
    Status open_line_reader();
    // Read next buffer from reader
    Status open_next_reader();

    // Split one text line to values
    void split_line(
        const Slice& line, std::vector<Slice>* values);

    // Writes a slot in _tuple from an value containing text data.
    bool write_slot(
        const std::string& column_name, const TColumnType& column_type,
        const Slice& value, const SlotDescriptor* slot,
        Tuple* tuple, MemPool* tuple_pool, std::stringstream* error_msg);

    void fill_fix_length_string(
        const Slice& value, MemPool* pool,
        char** new_value_p, int new_value_length);

    bool check_decimal_input(
        const Slice& value,
        int precision, int scale,
        std::stringstream* error_msg);

    // Convert one row to one tuple
    //  'ptr' and 'len' is csv text line
    //  output is tuple
    bool convert_one_row(const Slice& line, Tuple* tuple, MemPool* tuple_pool);

    Status init_expr_ctxes();

    Status line_to_src_tuple();
    bool line_to_src_tuple(const Slice& line);
    bool fill_dest_tuple(const Slice& line, Tuple* dest_tuple, MemPool* mem_pool);
private:
    RuntimeState* _state;
    RuntimeProfile* _profile;
    const TBrokerScanRangeParams& _params;
    const std::vector<TBrokerRangeDesc>& _ranges;
    const std::vector<TNetworkAddress>& _broker_addresses;

    std::unique_ptr<TextConverter> _text_converter;

    char _value_separator;
    char _line_delimiter;

    // Reader
    FileReader* _cur_file_reader;
    LineReader* _cur_line_reader;
    Decompressor* _cur_decompressor;
    int _next_range;
    bool _cur_line_reader_eof;

    bool _scanner_eof;

    // When we fetch range doesn't start from 0, 
    // we will read to one ahead, and skip the first line
    bool _skip_next_line;

    // Used for constructing tuple
    // slots for value read from broker file
    std::vector<SlotDescriptor*> _src_slot_descs;
    std::unique_ptr<RowDescriptor> _row_desc;
    Tuple* _src_tuple;
    TupleRow* _src_tuple_row;

    std::unique_ptr<MemTracker> _mem_tracker;
    // Mem pool used to allocate _src_tuple and _src_tuple_row
    MemPool _mem_pool;

    // Dest tuple descriptor and dest expr context
    const TupleDescriptor* _dest_tuple_desc;
    std::vector<ExprContext*> _dest_expr_ctx;

    // used to hold current StreamLoadPipe
    std::shared_ptr<StreamLoadPipe> _stream_load_pipe;

    // used for process stat
    BrokerScanCounter* _counter;

    // Profile
    RuntimeProfile::Counter* _rows_read_counter;
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _materialize_timer;
};

}
