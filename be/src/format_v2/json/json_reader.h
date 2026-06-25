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

#include <simdjson/simdjson.h> // IWYU pragma: keep

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "core/custom_allocator.h"
#include "core/data_type_serde/data_type_serde.h"
#include "exprs/json_functions.h"
#include "format_v2/file_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_profile.h"

namespace doris {
class Decompressor;
class LineReader;
class SlotDescriptor;
class IColumn;
} // namespace doris

namespace doris::format::json {

// FileScannerV2 JSON reader.
//
// JSON files do not carry an embedded physical schema. The v2 table layer still needs a
// file-local schema and FileScanRequest contract, so this reader exposes FE-provided file slots as
// v2 file-local columns and performs JSON parsing/materialization directly in the v2 path.
class JsonReader final : public FileReader {
public:
    // `file_slot_descs` is the FE-planned file schema. JSON has no physical schema, so the reader
    // exposes these slots as synthetic file-local columns and materializes only the columns
    // requested by FileScanRequest.
    JsonReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
               std::unique_ptr<io::FileDescription>& file_description,
               std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
               const TFileScanRangeParams* scan_params, const TFileRangeDesc& range,
               const std::vector<SlotDescriptor*>& file_slot_descs,
               TFileCompressType::type range_compress_type = TFileCompressType::UNKNOWN,
               std::optional<TUniqueId> stream_load_id = std::nullopt);
    ~JsonReader() override;

    // Initializes scan attributes and builds the synthetic schema from FE slots.
    Status init(RuntimeState* state) override;
    Status get_schema(std::vector<ColumnDefinition>* file_schema) const override;
    std::unique_ptr<TableColumnMapper> create_column_mapper(
            TableColumnMapperOptions options) const override;
    // Opens the underlying file or stream and binds requested local column ids to output block
    // positions. After this call, `get_block` can be called until it returns eof.
    Status open(std::shared_ptr<FileScanRequest> request) override;
    // Appends rows into `file_block` according to the FileScanRequest order. The block must already
    // contain columns matching the requested positions.
    Status get_block(Block* file_block, size_t* rows, bool* eof) override;
    Status close() override;

private:
    // A requested column keeps both identities:
    // - `source_index`: index in FE file slots, used for jsonpaths and SerDe lookup.
    // - `block_position`: index in the caller's output block, used for materialization.
    struct RequestedColumn {
        LocalColumnId file_column_id = LocalColumnId::invalid();
        LocalIndex block_position;
        size_t source_index = 0;
        SlotDescriptor* slot_desc = nullptr;
        DataTypeSerDeSPtr serde;
    };

    Status _build_requested_columns(const FileScanRequest& request,
                                    std::vector<RequestedColumn>* columns) const;
    // Reconciles TableReader's split/range descriptor with FileReader's concrete file description.
    TFileRangeDesc _json_range() const;
    Status _open_file_reader();
    Status _create_decompressor();
    Status _create_line_reader();
    Status _parse_jsonpath_and_json_root();
    // Reads one logical JSON document: one line for JSON Lines, or the whole range/pipe payload for
    // single-document mode.
    Status _read_one_document(size_t* size, bool* eof);
    Status _read_one_document_from_pipe(size_t* read_size);
    // Moves the logical document into a simdjson-padded buffer and creates an ondemand document.
    Status _parse_next_json(size_t* size, bool* eof);
    // Applies json_root and validates the object/array shape required by strip_outer_array.
    Status _extract_json_value(size_t size, bool* eof, bool* is_empty_row);
    Status _append_rows_from_current_value(Block* block, bool* is_empty_row, bool* eof);
    Status _append_simple_json_rows(Block* block, bool* is_empty_row, bool* eof);
    Status _append_flat_array_jsonpath_rows(Block* block, bool* is_empty_row, bool* eof);
    Status _append_nested_jsonpath_row(Block* block, bool* is_empty_row, bool* eof);
    Status _set_column_values_from_object(simdjson::ondemand::object* object_value, Block* block,
                                          bool* valid);
    Status _write_columns_by_jsonpath(simdjson::ondemand::object* object_value, Block* block,
                                      bool* valid);
    template <bool use_string_cache>
    Status _write_data_to_column(simdjson::ondemand::value& value, const DataTypePtr& type_desc,
                                 IColumn* column_ptr, const std::string& column_name,
                                 const DataTypeSerDeSPtr& serde, bool* valid);
    Status _fill_missing_column(const RequestedColumn& column, IColumn* column_ptr, bool* valid);
    Status _handle_json_error(const Status& status, Block* block, size_t original_rows,
                              bool* is_empty_row);
    Status _apply_filters(Block* file_block, size_t* rows);
    void _truncate_block_to_rows(Block* block, size_t num_rows);
    void _pop_back_last_inserted_value(Block* block, size_t column_index);
    size_t _column_index(std::string_view key, size_t key_index);
    bool _is_root_path_for_column(const RequestedColumn& column) const;

    const TFileScanRangeParams* _scan_params = nullptr;
    TFileRangeDesc _range;
    TFileRangeDesc _reader_range;
    std::vector<SlotDescriptor*> _source_file_slot_descs;
    DataTypeSerDeSPtrs _source_serdes;
    std::vector<ColumnDefinition> _file_schema;
    RuntimeState* _runtime_state = nullptr;
    TFileCompressType::type _range_compress_type = TFileCompressType::UNKNOWN;
    std::optional<TUniqueId> _stream_load_id;
    std::vector<RequestedColumn> _requested_columns;
    std::unordered_map<std::string, size_t> _slot_name_to_index;
    std::vector<size_t> _previous_positions;

    io::FileReaderSPtr _physical_file_reader;
    std::unique_ptr<Decompressor> _decompressor;
    std::unique_ptr<LineReader> _line_reader;
    int64_t _current_offset = 0;
    bool _reader_eof = false;
    bool _skip_first_line = false;
    bool _single_document_read = false;

    std::string _line_delimiter;
    size_t _line_delimiter_length = 0;
    std::string _jsonpaths;
    std::string _json_root;
    bool _read_json_by_line = false;
    bool _strip_outer_array = false;
    bool _num_as_string = false;
    bool _fuzzy_parse = false;
    bool _is_hive_table = false;
    bool _openx_json_ignore_malformed = false;
    TFileCompressType::type _file_compress_type = TFileCompressType::UNKNOWN;

    std::vector<std::vector<JsonPath>> _parsed_jsonpaths;
    std::vector<JsonPath> _parsed_json_root;
    bool _parsed_from_json_root = false;
    DataTypeSerDe::FormatOptions _serde_options;

    // simdjson ondemand values point into `_padding_buffer`, so the buffer must outlive all values
    // created from the current document.
    std::unique_ptr<simdjson::ondemand::parser> _json_parser;
    simdjson::ondemand::document _original_json_doc;
    simdjson::ondemand::value _json_value;
    simdjson::ondemand::array _array;
    simdjson::ondemand::array_iterator _array_iter;
    std::string _document_buffer;
    std::string _padding_buffer;
    size_t _original_doc_size = 0;
    size_t _padded_size = 1024 * 1024 * 8 + simdjson::SIMDJSON_PADDING;
    std::unordered_map<std::string_view, std::string_view> _cached_string_values;
};

} // namespace doris::format::json
