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

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "io/fs/file_reader.h"
#include "vec/exec/format/generic_reader.h"
namespace doris {

class FileReader;
struct JsonPath;
class LineReader;
class SlotDescriptor;

namespace vectorized {

struct ScannerCounter;

class NewJsonReader : public GenericReader {
public:
    NewJsonReader(RuntimeState* state, RuntimeProfile* profile, ScannerCounter* counter,
                  const TFileScanRangeParams& params, const TFileRangeDesc& range,
                  const std::vector<SlotDescriptor*>& file_slot_descs, bool* scanner_eof);

    NewJsonReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                  const TFileRangeDesc& range, const std::vector<SlotDescriptor*>& file_slot_descs);
    ~NewJsonReader() override = default;

    Status init_reader();
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;
    Status get_parsed_schema(std::vector<std::string>* col_names,
                             std::vector<TypeDescriptor>* col_types) override;

private:
    Status _get_range_params();
    Status _open_file_reader();
    Status _open_line_reader();
    Status _parse_jsonpath_and_json_root();

    Status _read_json_column(std::vector<MutableColumnPtr>& columns,
                             const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row,
                             bool* eof);

    Status _vhandle_simple_json(std::vector<MutableColumnPtr>& columns,
                                const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row,
                                bool* eof);

    Status _vhandle_flat_array_complex_json(std::vector<MutableColumnPtr>& columns,
                                            const std::vector<SlotDescriptor*>& slot_descs,
                                            bool* is_empty_row, bool* eof);

    Status _vhandle_nested_complex_json(std::vector<MutableColumnPtr>& columns,
                                        const std::vector<SlotDescriptor*>& slot_descs,
                                        bool* is_empty_row, bool* eof);

    Status _parse_json(bool* is_empty_row, bool* eof);
    Status _parse_json_doc(size_t* size, bool* eof);

    Status _set_column_value(rapidjson::Value& objectValue, std::vector<MutableColumnPtr>& columns,
                             const std::vector<SlotDescriptor*>& slot_descs, bool* valid);

    Status _write_data_to_column(rapidjson::Value::ConstValueIterator value,
                                 SlotDescriptor* slot_desc, vectorized::IColumn* column_ptr,
                                 bool* valid);

    Status _write_columns_by_jsonpath(rapidjson::Value& objectValue,
                                      const std::vector<SlotDescriptor*>& slot_descs,
                                      std::vector<MutableColumnPtr>& columns, bool* valid);

    Status _append_error_msg(const rapidjson::Value& objectValue, std::string error_msg,
                             std::string col_name, bool* valid);

    std::string _print_json_value(const rapidjson::Value& value);

    Status _read_one_message(std::unique_ptr<uint8_t[]>* file_buf, size_t* read_size);

    Status (NewJsonReader::*_vhandle_json_callback)(
            std::vector<vectorized::MutableColumnPtr>& columns,
            const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row, bool* eof);
    RuntimeState* _state;
    RuntimeProfile* _profile;
    ScannerCounter* _counter;
    const TFileScanRangeParams& _params;
    const TFileRangeDesc& _range;
    const std::vector<SlotDescriptor*>& _file_slot_descs;

    std::unique_ptr<io::FileSystem> _file_system;
    io::FileReaderSPtr _file_reader;
    std::unique_ptr<LineReader> _line_reader;
    bool _reader_eof;

    // When we fetch range doesn't start from 0 will always skip the first line
    bool _skip_first_line;

    std::string _line_delimiter;
    int _line_delimiter_length;

    int _next_row;
    int _total_rows;

    std::string _jsonpaths;
    std::string _json_root;
    bool _read_json_by_line;
    bool _strip_outer_array;
    bool _num_as_string;
    bool _fuzzy_parse;

    std::vector<std::vector<JsonPath>> _parsed_jsonpaths;
    std::vector<JsonPath> _parsed_json_root;

    char _value_buffer[4 * 1024 * 1024]; // 4MB
    char _parse_buffer[512 * 1024];      // 512KB

    using Document = rapidjson::GenericDocument<rapidjson::UTF8<>, rapidjson::MemoryPoolAllocator<>,
                                                rapidjson::MemoryPoolAllocator<>>;
    rapidjson::MemoryPoolAllocator<> _value_allocator;
    rapidjson::MemoryPoolAllocator<> _parse_allocator;
    Document _origin_json_doc;   // origin json document object from parsed json string
    rapidjson::Value* _json_doc; // _json_doc equals _final_json_doc iff not set `json_root`
    std::unordered_map<std::string, int> _name_map;

    bool* _scanner_eof;

    size_t _current_offset;

    RuntimeProfile::Counter* _bytes_read_counter;
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _file_read_timer;
};
} // namespace vectorized
} // namespace doris
