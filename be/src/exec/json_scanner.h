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
    ~JsonDataInternal();
    rapidjson::Value::ConstValueIterator get_next();

private:
    rapidjson::Value* _json_values;
    rapidjson::Value::ConstValueIterator  _iterator;
};

class JsonReader {
public:
    JsonReader(RuntimeState* state, ScannerCounter* counter, RuntimeProfile* profile, FileReader* file_reader,
            std::string& jsonpath, bool strip_outer_array);
    ~JsonReader();
    Status read(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool* eof);

private:
    void init_jsonpath(std::string& jsonpath);
    void fill_slot(Tuple* tuple, SlotDescriptor* slot_desc, MemPool* mem_pool, const uint8_t* value, int32_t len);
    size_t get_data_by_jsonpath(const std::vector<SlotDescriptor*>& slot_descs);
    Status parse_json_doc(bool* eof);
    Status set_tuple_value(rapidjson::Value& objectValue, Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool *valid);
    Status set_tuple_value_from_map(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool *valid);
    Status handle_simple_json(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool* eof);

    Status handle_nest_complex_json(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool* eof);
    Status handle_flat_array_complex_json(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool* eof);
    Status handle_complex_json(Tuple* tuple, const std::vector<SlotDescriptor*>& slot_descs, MemPool* tuple_pool, bool* eof);
    Status write_data_to_tuple(rapidjson::Value::ConstValueIterator value, SlotDescriptor* desc, Tuple* tuple, MemPool* tuple_pool);
    void close();

private:
    int _next_line;
    int _total_lines;
    RuntimeState* _state;
    ScannerCounter* _counter;
    RuntimeProfile* _profile;
    FileReader*_file_reader;
    bool _closed;
    bool _strip_outer_array;
    /**
     * _parse_jsonpath_flag == 1, jsonpath is valid
     * _parse_jsonpath_flag == 0, jsonpath is empty, default
     * _parse_jsonpath_flag == -1, jsonpath parse is error, it will return ERROR
     */
    short _parse_jsonpath_flag;
    RuntimeProfile::Counter* _bytes_read_counter;
    RuntimeProfile::Counter* _read_timer;
    rapidjson::Document _jsonpaths_doc;
    rapidjson::Document _json_doc;
    //key: column name
    std::unordered_map<std::string, JsonDataInternal> _jmap;
};


} // end namesapce
#endif
