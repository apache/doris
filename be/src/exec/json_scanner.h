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

#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "exec/base_scanner.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/stream_load/stream_load_pipe.h"
#include "runtime/tuple.h"
#include "util/runtime_profile.h"

namespace doris {
class Tuple;
class SlotDescriptor;
class RuntimeState;
class TupleDescriptor;
class JsonReader;
class LineReader;
class FileReader;

class JsonScanner : public BaseScanner {
public:
    JsonScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRangeParams& params,
                const std::vector<TBrokerRangeDesc>& ranges,
                const std::vector<TNetworkAddress>& broker_addresses,
                const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);
    ~JsonScanner();

    // Open this scanner, will initialize information needed
    Status open() override;

    // Get next tuple
    Status get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof, bool* fill_tuple) override;

    Status get_next(vectorized::Block* block, bool* eof) override {
        return Status::NotSupported("Not Implemented get block");
    }

    // Close this scanner
    void close() override;

protected:
    Status open_file_reader();
    Status open_line_reader();
    Status open_json_reader();
    Status open_next_reader();

    Status open_based_reader();
    Status get_range_params(std::string& jsonpath, std::string& json_root, bool& strip_outer_array,
                            bool& num_as_string, bool& fuzzy_parse);

protected:
    std::string _jsonpath;
    std::string _jsonpath_file;

    std::string _line_delimiter;
    int _line_delimiter_length;

    // Reader
    // _cur_file_reader_s is for stream load pipe reader,
    // and _cur_file_reader is for other file reader.
    // TODO: refactor this to use only shared_ptr or unique_ptr
    std::unique_ptr<FileReader> _cur_file_reader;
    std::shared_ptr<FileReader> _cur_file_reader_s;
    FileReader* _real_reader;
    LineReader* _cur_line_reader;
    JsonReader* _cur_json_reader;
    bool _cur_reader_eof;
    bool _read_json_by_line;

    // When we fetch range doesn't start from 0,
    // we will read to one ahead, and skip the first line
    bool _skip_next_line;
};

class JsonDataInternal {
public:
    JsonDataInternal(rapidjson::Value* v);
    ~JsonDataInternal() {}
    rapidjson::Value::ConstValueIterator get_next();
    bool is_null() const { return _json_values == nullptr; }

private:
    rapidjson::Value* _json_values;
    rapidjson::Value::ConstValueIterator _iterator;
};

struct JsonPath;
// Reader to parse the json.
// For most of its methods which return type is Status,
// return Status::OK() if process succeed or encounter data quality error.
// return other error Status if encounter other errors.
class JsonReader {
public:
    JsonReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile,
               bool strip_outer_array, bool num_as_string, bool fuzzy_parse, bool* scanner_eof,
               FileReader* file_reader = nullptr, LineReader* line_reader = nullptr);

    ~JsonReader();

    Status init(const std::string& jsonpath, const std::string& json_root); // must call before use

    Status read_json_row(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs,
                         MemPool* tuple_pool, bool* is_empty_row, bool* eof);

protected:
    Status (JsonReader::*_handle_json_callback)(Tuple* tuple,
                                                const std::vector<SlotDescriptor*>& slot_descs,
                                                MemPool* tuple_pool, bool* is_empty_row, bool* eof);
    Status _handle_simple_json(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs,
                               MemPool* tuple_pool, bool* is_empty_row, bool* eof);
    Status _handle_flat_array_complex_json(Tuple* tuple,
                                           const std::vector<SlotDescriptor*>& slot_descs,
                                           MemPool* tuple_pool, bool* is_empy_row, bool* eof);
    Status _handle_nested_complex_json(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs,
                                       MemPool* tuple_pool, bool* is_empty_row, bool* eof);

    void _fill_slot(Tuple* tuple, SlotDescriptor* slot_desc, MemPool* mem_pool,
                    const uint8_t* value, int32_t len);
    Status _parse_json_doc(size_t* size, bool* eof);
    Status _set_tuple_value(rapidjson::Value& objectValue, Tuple* tuple,
                            const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool,
                            bool* valid);
    Status _write_data_to_tuple(rapidjson::Value::ConstValueIterator value, SlotDescriptor* desc,
                                Tuple* tuple, MemPool* tuple_pool, bool* valid);
    Status _write_values_by_jsonpath(rapidjson::Value& objectValue, MemPool* tuple_pool,
                                     Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs,
                                     bool* valid);
    std::string _print_json_value(const rapidjson::Value& value);
    std::string _print_jsonpath(const std::vector<JsonPath>& path);

    void _close();
    Status _generate_json_paths(const std::string& jsonpath,
                                std::vector<std::vector<JsonPath>>* vect);
    Status _parse_jsonpath_and_json_root(const std::string& jsonpath, const std::string& json_root);

protected:
    int _next_line;
    int _total_lines;
    RuntimeState* _state;
    ScannerCounter* _counter;
    RuntimeProfile* _profile;
    FileReader* _file_reader;
    LineReader* _line_reader;
    bool _closed;
    bool _strip_outer_array;
    bool _num_as_string;
    bool _fuzzy_parse;
    RuntimeProfile::Counter* _bytes_read_counter;
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _file_read_timer;

    std::vector<std::vector<JsonPath>> _parsed_jsonpaths;
    std::vector<JsonPath> _parsed_json_root;

    char _value_buffer[4 * 1024 * 1024];
    char _parse_buffer[512 * 1024];

    typedef rapidjson::GenericDocument<rapidjson::UTF8<>, rapidjson::MemoryPoolAllocator<>,
                                       rapidjson::MemoryPoolAllocator<>>
            Document;
    rapidjson::MemoryPoolAllocator<> _value_allocator;
    rapidjson::MemoryPoolAllocator<> _parse_allocator;
    Document _origin_json_doc;   // origin json document object from parsed json string
    rapidjson::Value* _json_doc; // _json_doc equals _final_json_doc iff not set `json_root`
    std::unordered_map<std::string, int> _name_map;

    // point to the _scanner_eof of JsonScanner
    bool* _scanner_eof;
};

} // namespace doris
