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
#include "exec/json_scanner.h"
#include "runtime/descriptors.h"
#include "util/runtime_profile.h"

namespace doris {
class ExprContext;
class RuntimeState;
struct ScannerCounter;

namespace vectorized {
class VJsonReader;

template <typename JsonReader>
class VJsonScanner : public JsonScanner {
public:
    VJsonScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRangeParams& params,
                 const std::vector<TBrokerRangeDesc>& ranges,
                 const std::vector<TNetworkAddress>& broker_addresses,
                 const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);

    Status get_next(doris::Tuple* tuple, MemPool* tuple_pool, bool* eof,
                    bool* fill_tuple) override {
        return Status::NotSupported("Not Implemented get tuple");
    }
    Status get_next(vectorized::Block* output_block, bool* eof) override;

private:
    Status open_vjson_reader();
    Status open_next_reader();

private:
    std::unique_ptr<JsonReader> _cur_vjson_reader = nullptr;
};

class VJsonReader : public JsonReader {
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
