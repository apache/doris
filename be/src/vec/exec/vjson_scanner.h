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
#include <simdjson.h>

#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "exec/base_scanner.h"
#include "exec/line_reader.h"
#include "exprs/json_functions.h"
#include "io/file_reader.h"
#include "runtime/descriptors.h"
#include "util/runtime_profile.h"

namespace doris {
class ExprContext;
class RuntimeState;
struct ScannerCounter;

namespace vectorized {
class VJsonReader;

template <typename JsonReader>
class VJsonScanner : public BaseScanner {
public:
    VJsonScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRangeParams& params,
                 const std::vector<TBrokerRangeDesc>& ranges,
                 const std::vector<TNetworkAddress>& broker_addresses,
                 const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);

    ~VJsonScanner() override;

    // Open this scanner, will initialize information needed
    Status open() override;

    Status get_next(vectorized::Block* output_block, bool* eof) override;

    void close() override;

private:
    Status _open_vjson_reader();
    Status _open_next_reader();

    Status _open_file_reader();
    Status _open_line_reader();
    Status _open_json_reader();

    Status _open_based_reader();
    Status _get_range_params(std::string& jsonpath, std::string& json_root, bool& strip_outer_array,
                             bool& num_as_string, bool& fuzzy_parse);
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
    std::unique_ptr<JsonReader> _cur_vjson_reader = nullptr;
};

class VJsonReader {
public:
    VJsonReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile,
                bool strip_outer_array, bool num_as_string, bool fuzzy_parse, bool* scanner_eof,
                FileReader* file_reader = nullptr, LineReader* line_reader = nullptr);

    ~VJsonReader();

    Status init(const std::string& jsonpath, const std::string& json_root);

    Status read_json_column(std::vector<MutableColumnPtr>& columns,
                            const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row,
                            bool* eof);

private:
    Status (VJsonReader::*_vhandle_json_callback)(
            std::vector<vectorized::MutableColumnPtr>& columns,
            const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row, bool* eof);

    Status _vhandle_simple_json(std::vector<MutableColumnPtr>& columns,
                                const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row,
                                bool* eof);

    Status _vhandle_flat_array_complex_json(std::vector<MutableColumnPtr>& columns,
                                            const std::vector<SlotDescriptor*>& slot_descs,
                                            bool* is_empty_row, bool* eof);

    Status _vhandle_nested_complex_json(std::vector<MutableColumnPtr>& columns,
                                        const std::vector<SlotDescriptor*>& slot_descs,
                                        bool* is_empty_row, bool* eof);

    Status _write_columns_by_jsonpath(rapidjson::Value& objectValue,
                                      const std::vector<SlotDescriptor*>& slot_descs,
                                      std::vector<MutableColumnPtr>& columns, bool* valid);

    Status _set_column_value(rapidjson::Value& objectValue, std::vector<MutableColumnPtr>& columns,
                             const std::vector<SlotDescriptor*>& slot_descs, bool* valid);

    Status _write_data_to_column(rapidjson::Value::ConstValueIterator value,
                                 SlotDescriptor* slot_desc, vectorized::IColumn* column_ptr,
                                 bool* valid);

    Status _parse_json(bool* is_empty_row, bool* eof);

    Status _append_error_msg(const rapidjson::Value& objectValue, std::string error_msg,
                             std::string col_name, bool* valid);

    void _fill_slot(doris::Tuple* tuple, SlotDescriptor* slot_desc, MemPool* mem_pool,
                    const uint8_t* value, int32_t len);
    Status _parse_json_doc(size_t* size, bool* eof);
    Status _set_tuple_value(rapidjson::Value& objectValue, doris::Tuple* tuple,
                            const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool,
                            bool* valid);
    Status _write_data_to_tuple(rapidjson::Value::ConstValueIterator value, SlotDescriptor* desc,
                                doris::Tuple* tuple, MemPool* tuple_pool, bool* valid);
    std::string _print_json_value(const rapidjson::Value& value);

    void _close();
    Status _generate_json_paths(const std::string& jsonpath,
                                std::vector<std::vector<JsonPath>>* vect);
    Status _parse_jsonpath_and_json_root(const std::string& jsonpath, const std::string& json_root);

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

    using Document = rapidjson::GenericDocument<rapidjson::UTF8<>, rapidjson::MemoryPoolAllocator<>,
                                                rapidjson::MemoryPoolAllocator<>>;
    rapidjson::MemoryPoolAllocator<> _value_allocator;
    rapidjson::MemoryPoolAllocator<> _parse_allocator;
    Document _origin_json_doc;   // origin json document object from parsed json string
    rapidjson::Value* _json_doc; // _json_doc equals _final_json_doc iff not set `json_root`
    std::unordered_map<std::string, int> _name_map;

    // point to the _scanner_eof of JsonScanner
    bool* _scanner_eof;
};

class VSIMDJsonReader {
public:
    VSIMDJsonReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile,
                    bool strip_outer_array, bool num_as_string, bool fuzzy_parse, bool* scanner_eof,
                    FileReader* file_reader = nullptr, LineReader* line_reader = nullptr);

    ~VSIMDJsonReader();

    Status init(const std::string& jsonpath, const std::string& json_root);

    Status read_json_column(Block& block, const std::vector<SlotDescriptor*>& slot_descs,
                            bool* is_empty_row, bool* eof);

private:
    Status (VSIMDJsonReader::*_vhandle_json_callback)(
            Block& block, const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row,
            bool* eof);

    Status _vhandle_simple_json(Block& block, const std::vector<SlotDescriptor*>& slot_descs,
                                bool* is_empty_row, bool* eof);

    Status _vhandle_flat_array_complex_json(Block& block,
                                            const std::vector<SlotDescriptor*>& slot_descs,
                                            bool* is_empty_row, bool* eof);

    Status _vhandle_nested_complex_json(Block& block,
                                        const std::vector<SlotDescriptor*>& slot_descs,
                                        bool* is_empty_row, bool* eof);

    Status _write_columns_by_jsonpath(simdjson::ondemand::value value,
                                      const std::vector<SlotDescriptor*>& slot_descs, Block& block,
                                      bool* valid);

    Status _set_column_value(simdjson::ondemand::value value, Block& block,
                             const std::vector<SlotDescriptor*>& slot_descs, bool* valid);

    Status _write_data_to_column(simdjson::ondemand::value value, SlotDescriptor* slot_desc,
                                 vectorized::IColumn* column_ptr, bool* valid);

    Status _parse_json(bool* is_empty_row, bool* eof);

    Status _parse_json_doc(size_t* size, bool* eof);

    Status _parse_jsonpath_and_json_root(const std::string& jsonpath, const std::string& json_root);

    Status _generate_json_paths(const std::string& jsonpath, std::vector<std::string>* vect);

    Status _append_error_msg(std::string error_msg, std::string col_name, bool* valid);

    std::unique_ptr<simdjson::ondemand::parser> _json_parser = nullptr;
    simdjson::ondemand::document _original_json_doc;
    simdjson::ondemand::value _json_value;
    // for strip outer array
    simdjson::ondemand::array_iterator _array_iter;

    int _next_line;
    int _total_lines;
    doris::RuntimeState* _state;
    doris::ScannerCounter* _counter;
    RuntimeProfile* _profile;
    FileReader* _file_reader;
    LineReader* _line_reader;
    bool _strip_outer_array;
    RuntimeProfile::Counter* _bytes_read_counter;
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _file_read_timer;

    // simdjson pointer string, eg.
    //        jsonpath             simdjson pointer
    // `["$.k1[0]", "$.k2.a"]` -> ["/k1/0", "/k2/a"]
    // notice array index not support `*`
    // so we are not fully compatible with previous implementation by rapidjson
    std::vector<std::string> _parsed_jsonpaths;
    std::string _parsed_json_root;

    bool* _scanner_eof;

    static constexpr size_t _buffer_size = 1024 * 1024 * 8;
    static constexpr size_t _padded_size = _buffer_size + simdjson::SIMDJSON_PADDING;
    char _simdjson_ondemand_padding_buffer[_padded_size];
};

} // namespace vectorized
} // namespace doris
