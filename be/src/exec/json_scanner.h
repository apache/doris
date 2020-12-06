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

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "common/status.h"
#include "exec/base_scanner.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/small_file_mgr.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/tuple.h"
#include "util/runtime_profile.h"
#include "util/slice.h"

namespace doris {
class Tuple;
class SlotDescriptor;
class RuntimeState;
class TupleDescriptor;
class MemTracker;
class JsonReader;

class JsonScanner : public BaseScanner {
public:
    JsonScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRangeParams& params,
                const std::vector<TBrokerRangeDesc>& ranges,
                const std::vector<TNetworkAddress>& broker_addresses, ScannerCounter* counter);
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
               bool strip_outer_array, bool num_as_string);

    ~JsonReader();

    Status init(const std::string& jsonpath, const std::string& json_root); // must call before use

    Status read(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool,
                bool* eof);

private:
    Status (JsonReader::*_handle_json_callback)(Tuple* tuple,
                                                const std::vector<SlotDescriptor*>& slot_descs,
                                                MemPool* tuple_pool, bool* eof);
    Status _handle_simple_json(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs,
                               MemPool* tuple_pool, bool* eof);
    Status _handle_flat_array_complex_json(Tuple* tuple,
                                           const std::vector<SlotDescriptor*>& slot_descs,
                                           MemPool* tuple_pool, bool* eof);
    Status _handle_nested_complex_json(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs,
                                       MemPool* tuple_pool, bool* eof);

    void _fill_slot(Tuple* tuple, SlotDescriptor* slot_desc, MemPool* mem_pool,
                    const uint8_t* value, int32_t len);
    Status _parse_json_doc(bool* eof);
    void _set_tuple_value(rapidjson::Value& objectValue, Tuple* tuple,
                          const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool,
                          bool* valid);
    void _write_data_to_tuple(rapidjson::Value::ConstValueIterator value, SlotDescriptor* desc,
                              Tuple* tuple, MemPool* tuple_pool, bool* valid);
    bool _write_values_by_jsonpath(rapidjson::Value& objectValue, MemPool* tuple_pool, Tuple* tuple,
                                   const std::vector<SlotDescriptor*>& slot_descs);
    std::string _print_json_value(const rapidjson::Value& value);
    std::string _print_jsonpath(const std::vector<JsonPath>& path);

    void _close();
    Status _generate_json_paths(const std::string& jsonpath,
                                std::vector<std::vector<JsonPath>>* vect);

private:
    int _next_line;
    int _total_lines;
    RuntimeState* _state;
    ScannerCounter* _counter;
    RuntimeProfile* _profile;
    FileReader* _file_reader;
    bool _closed;
    bool _strip_outer_array;
    bool _num_as_string;
    RuntimeProfile::Counter* _bytes_read_counter;
    RuntimeProfile::Counter* _read_timer;

    std::vector<std::vector<JsonPath>> _parsed_jsonpaths;
    std::vector<JsonPath> _parsed_json_root;

    rapidjson::Document _origin_json_doc; // origin json document object from parsed json string
    rapidjson::Value* _json_doc; // _json_doc equals _final_json_doc iff not set `json_root`
};

} // namespace doris
#endif
