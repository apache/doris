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

#ifndef BE_SRC_JSON_SCANNER_H_
#define BE_SRC_JSON_SCANNER_H_

#include <memory>
#include <vector>
#include <string>
#include <map>
#include <sstream>
#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include "exec/base_scanner.h"
#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "util/slice.h"
#include "util/runtime_profile.h"
#include "runtime/mem_pool.h"
#include "runtime/tuple.h"
#include "runtime/descriptors.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/small_file_mgr.h"

namespace doris {
class Tuple;
class SlotDescriptor;
class RuntimeState;
class TupleDescriptor;
class MemTracker;
class JsonReader;

class JsonScanner : public BaseScanner {
public:
    JsonScanner(
        RuntimeState* state,
        RuntimeProfile* profile,
        const TBrokerScanRangeParams& params,
        const std::vector<TBrokerRangeDesc>& ranges,
        const std::vector<TNetworkAddress>& broker_addresses,
        ScannerCounter* counter);
    ~JsonScanner();

    // Open this scanner, will initialize information needed
    Status open() override;

    // Get next tuple
    Status get_next(Tuple* tuple, MemPool* tuple_pool, bool* eof) override;

    // Close this scanner
    void close() override;
private:
    Status open_next_reader();
private:
    const std::vector<TBrokerRangeDesc>& _ranges;
    const std::vector<TNetworkAddress>& _broker_addresses;

    std::string _jsonpath;
    std::string _jsonpath_file;

    // used to hold current StreamLoadPipe
    std::shared_ptr<StreamLoadPipe> _stream_load_pipe;
    // Reader
    JsonReader* _cur_file_reader;
    int _next_range;
    bool _cur_file_eof; // is read over?
    bool _scanner_eof;
};

class JsonDataInternal {
public:
    JsonDataInternal(rapidjson::Value* v);
    ~JsonDataInternal() {}
    rapidjson::Value::ConstValueIterator get_next();
    rapidjson::Value* get_value() { return _json_values; }
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
    JsonReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile, FileReader* file_reader,
            std::string& jsonpath, bool strip_outer_array);
    ~JsonReader();

    Status init(); // must call before use

    Status read(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool* eof);

private:
    Status _handle_simple_json(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool* eof);
    Status _handle_complex_json(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool* eof);
    Status _handle_flat_array_complex_json(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool* eof);
    Status _handle_nested_complex_json(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool* eof);

    void _fill_slot(Tuple* tuple, SlotDescriptor* slot_desc, MemPool* mem_pool, const uint8_t* value, int32_t len);
    void _assemble_jmap(const std::vector<SlotDescriptor*>& slot_descs);
    Status _parse_json_doc(bool* eof);
    void _set_tuple_value(rapidjson::Value& objectValue, Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool *valid);
    Status _set_tuple_value_from_jmap(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool *valid);
    void _write_data_to_tuple(rapidjson::Value::ConstValueIterator value, SlotDescriptor* desc, Tuple* tuple, MemPool* tuple_pool, bool* valid);
    std::string _print_json_value(const rapidjson::Value& value);
    std::string _print_jsonpath(const std::vector<JsonPath>& path);

    void _close();

private:
    // origin json path
    std::string _jsonpath;
    int _next_line;
    int _total_lines;
    RuntimeState* _state;
    ScannerCounter* _counter;
    RuntimeProfile* _profile;
    FileReader*_file_reader;
    bool _closed;
    bool _strip_outer_array;
    RuntimeProfile::Counter* _bytes_read_counter;
    RuntimeProfile::Counter* _read_timer;
    std::vector<std::vector<JsonPath>> _parsed_jsonpaths;
    rapidjson::Document _json_doc;
    //key: column name
    std::unordered_map<std::string, JsonDataInternal> _jmap;
};


} // end namesapce
#endif
