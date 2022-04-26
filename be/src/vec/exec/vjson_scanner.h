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

#ifndef BE_SRC_VJSON_SCANNER_H_
#define BE_SRC_VJSON_SCANNER_H_

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
#include "exec/json_scanner.h"
#include "exec/exec_node.h"
#include "runtime/row_batch.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/tuple.h"
#include "runtime/mem_tracker.h"
#include "util/runtime_profile.h"
#include "exprs/expr_context.h"

namespace doris {
class ExprContext;

namespace vectorized {
class VJsonReader;

class VJsonScanner : public JsonScanner {
public:
    VJsonScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRangeParams& params,
                const std::vector<TBrokerRangeDesc>& ranges,
                const std::vector<TNetworkAddress>& broker_addresses,
                const std::vector<TExpr>& pre_filter_texprs,
                ScannerCounter* counter);
        
    ~VJsonScanner();
    
    Status open() override;

    void close() override;

    // Status get_next(std::vector<MutableColumnPtr>& columns, bool* eof) override;
    Status get_next(vectorized::Block& output_block, bool* eof) override;

private:
    Status open_vjson_reader();
    Status open_next_reader();

private:
    const std::vector<TBrokerRangeDesc>& _ranges;
    const std::vector<TNetworkAddress>& _broker_addresses;

    // Reader
    FileReader* _cur_file_reader;
    LineReader* _cur_line_reader;
    VJsonReader* _cur_vjson_reader;

    int _next_range;
    bool _cur_reader_eof;
    bool _read_json_by_line;

    // When we fetch range doesn't start from 0,
    // we will read to one ahead, and skip the first line
    bool _skip_next_line;
};

class VJsonReader : public JsonReader {
public:
    VJsonReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile,
               bool strip_outer_array, bool num_as_string,bool fuzzy_parse,
               bool* scanner_eof, FileReader* file_reader = nullptr, LineReader* line_reader = nullptr);

    ~VJsonReader();

    Status init(const std::string& jsonpath, const std::string& json_root);

    Status read_json_column(std::vector<MutableColumnPtr>& columns, 
                            const std::vector<SlotDescriptor*>& slot_descs,
                            bool* is_empty_row, bool* eof);

private:
    Status (VJsonReader::*_vhandle_json_callback)(std::vector<vectorized::MutableColumnPtr>& columns,
                                                const std::vector<SlotDescriptor*>& slot_descs,
                                                bool* is_empty_row, bool* eof);

    Status _vhandle_simple_json(std::vector<MutableColumnPtr>& columns, 
                                const std::vector<SlotDescriptor*>& slot_descs,
                                bool* is_empty_row, bool* eof);

    Status _vhandle_flat_array_complex_json(std::vector<MutableColumnPtr>& columns,
                                        const std::vector<SlotDescriptor*>& slot_descs,
                                        bool* is_empty_row, bool* eof);

    Status _vhandle_nested_complex_json(std::vector<MutableColumnPtr>& columns,
                                        const std::vector<SlotDescriptor*>& slot_descs,
                                        bool* is_empty_row, bool* eof);
    
    Status _write_columns_by_jsonpath(rapidjson::Value& objectValue,
                                    const std::vector<SlotDescriptor*>& slot_descs,
                                    std::vector<MutableColumnPtr>& columns,
                                    bool* valid);
    
    Status _set_column_value(rapidjson::Value& objectValue, std::vector<MutableColumnPtr>& columns,
                            const std::vector<SlotDescriptor*>& slot_descs, bool* valid);
    
    Status _write_data_to_column(rapidjson::Value::ConstValueIterator value, SlotDescriptor* slot_desc,
                            vectorized::IColumn* column_ptr, bool* valid);
    
    Status _insert_to_column(vectorized::IColumn* column_ptr, SlotDescriptor* slot_desc,
                            const char* value_ptr, int32_t& wbytes);

private:
    int _next_line;
    int _total_lines;
    RuntimeState* _state;
    ScannerCounter* _counter;
    RuntimeProfile* _profile;

    bool _strip_outer_array;
    bool _fuzzy_parse;
    RuntimeProfile::Counter* _read_timer;

    std::vector<std::vector<JsonPath>> _parsed_jsonpaths;
    std::vector<JsonPath> _parsed_json_root;

    rapidjson::Document* _origin_json_doc_ptr; // origin json document object from parsed json string
    rapidjson::Value* _json_doc; // _json_doc equals _final_json_doc iff not set `json_root`
    std::unordered_map<std::string, int> _name_map;
    // point to the _scanner_eof of JsonScanner
    bool* _scanner_eof;
};

} // vectorized
} // namespace doris
#endif
