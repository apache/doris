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

#include <rapidjson/allocators.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/rapidjson.h>
#include <simdjson/common_defs.h>
#include <simdjson/simdjson.h> // IWYU pragma: keep
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "exec/line_reader.h"
#include "exprs/json_functions.h"
#include "io/file_factory.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "util/runtime_profile.h"
#include "vec/common/hash_table/hash_map.h"
#include "vec/common/string_ref.h"
#include "vec/core/types.h"
#include "vec/exec/format/generic_reader.h"
#include "vec/json/json_parser.h"
#include "vec/json/simd_json_parser.h"

namespace simdjson {
namespace fallback {
namespace ondemand {
class object;
} // namespace ondemand
} // namespace fallback
} // namespace simdjson

namespace doris {

class SlotDescriptor;
class RuntimeState;
class TFileRangeDesc;
class TFileScanRangeParams;

namespace io {
class FileSystem;
struct IOContext;
} // namespace io
struct TypeDescriptor;

namespace vectorized {

struct ScannerCounter;
class Block;
class IColumn;

class NewJsonReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(NewJsonReader);

public:
    NewJsonReader(RuntimeState* state, RuntimeProfile* profile, ScannerCounter* counter,
                  const TFileScanRangeParams& params, const TFileRangeDesc& range,
                  const std::vector<SlotDescriptor*>& file_slot_descs, bool* scanner_eof,
                  io::IOContext* io_ctx);

    NewJsonReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                  const TFileRangeDesc& range, const std::vector<SlotDescriptor*>& file_slot_descs,
                  io::IOContext* io_ctx);
    ~NewJsonReader() override = default;

    Status init_reader(const std::unordered_map<std::string, vectorized::VExprContextSPtr>&
                               col_default_value_ctx);
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;
    Status get_parsed_schema(std::vector<std::string>* col_names,
                             std::vector<TypeDescriptor>* col_types) override;

private:
    Status _get_range_params();
    void _init_system_properties();
    void _init_file_description();
    Status _open_file_reader(bool need_schema);
    Status _open_line_reader();
    Status _parse_jsonpath_and_json_root();

    Status _read_json_column(RuntimeState* state, Block& block,
                             const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row,
                             bool* eof);

    Status _vhandle_simple_json(RuntimeState* /*state*/, Block& block,
                                const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row,
                                bool* eof);

    Status _vhandle_flat_array_complex_json(RuntimeState* /*state*/, Block& block,
                                            const std::vector<SlotDescriptor*>& slot_descs,
                                            bool* is_empty_row, bool* eof);

    Status _vhandle_nested_complex_json(RuntimeState* /*state*/, Block& block,
                                        const std::vector<SlotDescriptor*>& slot_descs,
                                        bool* is_empty_row, bool* eof);

    Status _parse_json(bool* is_empty_row, bool* eof);
    Status _parse_json_doc(size_t* size, bool* eof);

    Status _set_column_value(rapidjson::Value& objectValue, Block& block,
                             const std::vector<SlotDescriptor*>& slot_descs, bool* valid);

    Status _write_data_to_column(rapidjson::Value::ConstValueIterator value,
                                 SlotDescriptor* slot_desc, vectorized::IColumn* column_ptr,
                                 bool* valid);

    Status _write_columns_by_jsonpath(rapidjson::Value& objectValue,
                                      const std::vector<SlotDescriptor*>& slot_descs, Block& block,
                                      bool* valid);

    Status _append_error_msg(const rapidjson::Value& objectValue, std::string error_msg,
                             std::string col_name, bool* valid);

    std::string _print_json_value(const rapidjson::Value& value);

    Status _read_one_message(std::unique_ptr<uint8_t[]>* file_buf, size_t* read_size);

    // simdjson, replace none simdjson function if it is ready
    Status _simdjson_init_reader();
    Status _simdjson_parse_json(bool* is_empty_row, bool* eof);
    Status _simdjson_parse_json_doc(size_t* size, bool* eof);

    Status _simdjson_handle_simple_json(RuntimeState* state, Block& block,
                                        const std::vector<SlotDescriptor*>& slot_descs,
                                        bool* is_empty_row, bool* eof);

    Status _simdjson_handle_flat_array_complex_json(RuntimeState* state, Block& block,
                                                    const std::vector<SlotDescriptor*>& slot_descs,
                                                    bool* is_empty_row, bool* eof);

    Status _simdjson_handle_nested_complex_json(RuntimeState* state, Block& block,
                                                const std::vector<SlotDescriptor*>& slot_descs,
                                                bool* is_empty_row, bool* eof);

    Status _simdjson_set_column_value(simdjson::ondemand::object* value, Block& block,
                                      const std::vector<SlotDescriptor*>& slot_descs, bool* valid);

    Status _simdjson_write_data_to_column(simdjson::ondemand::value& value,
                                          SlotDescriptor* slot_desc,
                                          vectorized::IColumn* column_ptr, bool* valid);

    Status _simdjson_write_columns_by_jsonpath(simdjson::ondemand::object* value,
                                               const std::vector<SlotDescriptor*>& slot_descs,
                                               Block& block, bool* valid);
    Status _append_error_msg(simdjson::ondemand::object* obj, std::string error_msg,
                             std::string col_name, bool* valid);

    size_t _column_index(const StringRef& name, size_t key_index);

    Status (NewJsonReader::*_vhandle_json_callback)(RuntimeState* state, Block& block,
                                                    const std::vector<SlotDescriptor*>& slot_descs,
                                                    bool* is_empty_row, bool* eof);
    Status _get_column_default_value(
            const std::vector<SlotDescriptor*>& slot_descs,
            const std::unordered_map<std::string, vectorized::VExprContextSPtr>&
                    col_default_value_ctx);

    Status _fill_missing_column(SlotDescriptor* slot_desc, vectorized::IColumn* column_ptr,
                                bool* valid);

    RuntimeState* _state;
    RuntimeProfile* _profile;
    ScannerCounter* _counter;
    const TFileScanRangeParams& _params;
    const TFileRangeDesc& _range;
    io::FileSystemProperties _system_properties;
    io::FileDescription _file_description;
    const std::vector<SlotDescriptor*>& _file_slot_descs;

    std::shared_ptr<io::FileSystem> _file_system;
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

    io::IOContext* _io_ctx;

    RuntimeProfile::Counter* _bytes_read_counter;
    RuntimeProfile::Counter* _read_timer;
    RuntimeProfile::Counter* _file_read_timer;

    // ======SIMD JSON======
    // name mapping
    /// Hash table match `field name -> position in the block`. NOTE You can use perfect hash map.
    using NameMap = HashMap<StringRef, size_t, StringRefHash>;
    NameMap _slot_desc_index;
    /// Cached search results for previous row (keyed as index in JSON object) - used as a hint.
    std::vector<NameMap::LookupResult> _prev_positions;
    /// Set of columns which already met in row. Exception is thrown if there are more than one column with the same name.
    std::vector<UInt8> _seen_columns;
    // simdjson
    static constexpr size_t _init_buffer_size = 1024 * 1024 * 8;
    size_t _padded_size = _init_buffer_size + simdjson::SIMDJSON_PADDING;
    size_t _original_doc_size = 0;
    std::string _simdjson_ondemand_padding_buffer;
    std::string _simdjson_ondemand_unscape_padding_buffer;
    // char _simdjson_ondemand_padding_buffer[_padded_size];
    simdjson::ondemand::document _original_json_doc;
    simdjson::ondemand::value _json_value;
    // for strip outer array
    // array_iter pointed to _array
    simdjson::ondemand::array_iterator _array_iter;
    simdjson::ondemand::array _array;
    std::unique_ptr<JSONDataParser<SimdJSONParser>> _json_parser;
    std::unique_ptr<simdjson::ondemand::parser> _ondemand_json_parser = nullptr;
    // column to default value string map
    std::unordered_map<std::string, std::string> _col_default_value_map;
};

} // namespace vectorized
} // namespace doris
