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

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "core/data_type_serde/data_type_serde.h"
#include "format_v2/file_reader.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/runtime_profile.h"
#include "util/slice.h"

namespace doris {
class Decompressor;
class LineReader;
class SlotDescriptor;
} // namespace doris

namespace doris::format {

// Shared FileReader implementation for delimited text-like formats in FileScannerV2.
//
// CSV and Hive text have different row parsing and cell serde rules, but their v2 FileReader
// control flow is the same: expose a file-local schema from FE slot descriptors, resolve
// FileScanRequest local positions, read physical lines, materialize requested columns, apply
// file-local conjuncts, and optionally count rows by scanning. This base keeps that contract in one
// place while derived readers provide only format-specific hooks.
class DelimitedTextReader : public FileReader {
public:
    ~DelimitedTextReader() override;

    Status init(RuntimeState* state) override;
    Status get_schema(std::vector<ColumnDefinition>* file_schema) const override;
    std::unique_ptr<TableColumnMapper> create_column_mapper(
            TableColumnMapperOptions options) const override;
    Status open(std::shared_ptr<FileScanRequest> request) override;
    Status get_block(Block* file_block, size_t* rows, bool* eof) override;
    Status get_aggregate_result(const FileAggregateRequest& request,
                                FileAggregateResult* result) override;
    Status close() override;

protected:
    struct DelimitedTextProfile {
        RuntimeProfile::Counter* open_file_time = nullptr;
        RuntimeProfile::Counter* create_line_reader_time = nullptr;
        RuntimeProfile::Counter* read_line_time = nullptr;
        RuntimeProfile::Counter* split_line_time = nullptr;
        RuntimeProfile::Counter* deserialize_time = nullptr;
        RuntimeProfile::Counter* conjunct_filter_time = nullptr;
        RuntimeProfile::Counter* delete_conjunct_filter_time = nullptr;
        RuntimeProfile::Counter* raw_lines_read = nullptr;
        RuntimeProfile::Counter* rows_read_before_filter = nullptr;
        RuntimeProfile::Counter* rows_filtered_by_conjunct = nullptr;
        RuntimeProfile::Counter* rows_filtered_by_delete_conjunct = nullptr;
        RuntimeProfile::Counter* rows_returned = nullptr;
        RuntimeProfile::Counter* empty_lines_read = nullptr;
        RuntimeProfile::Counter* skipped_lines = nullptr;
        RuntimeProfile::Counter* cells_deserialized = nullptr;
    };

    struct RequestedColumn {
        LocalColumnId file_column_id = LocalColumnId::invalid();
        LocalIndex block_position;
        const SlotDescriptor* slot_desc = nullptr;
        DataTypeSerDeSPtr serde;
        bool nullable_string_fast_path = false;
    };

    DelimitedTextReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                        std::unique_ptr<io::FileDescription>& file_description,
                        std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                        const TFileScanRangeParams* scan_params,
                        const std::vector<SlotDescriptor*>& file_slot_descs,
                        TFileCompressType::type range_compress_type,
                        std::optional<TUniqueId> stream_load_id, std::string reader_name);

    // Initialize format-specific options after the common init path has validated scan params and
    // runtime state. Implementations must fill `_value_separator`, `_line_delimiter`,
    // `_file_compress_type`, `_options`, and any parser-specific state before the common schema
    // construction reads column_idxs.
    virtual Status _init_format_state() = 0;
    // Create the decompressor used by the line reader. CSV may infer compression from the file
    // format enum, while Hive text uses only the explicit compress_type.
    virtual Status _create_decompressor() = 0;
    // Create the physical line reader. Implementations choose plain/enclosed/binary line contexts,
    // but must store the result in `_line_reader` for the common get_block/count paths.
    virtual Status _create_line_reader() = 0;
    // Validate one logical line before splitting. CSV enforces UTF-8 for query reads; Hive text
    // deliberately accepts arbitrary bytes and uses the default OK implementation.
    virtual Status _validate_line(const Slice& line);
    // Split one logical line into `_split_values`. The common materialization path then resolves
    // requested field ordinals against `_split_values`.
    virtual void _split_line(const Slice& line) = 0;
    // Deserialize a single normalized field into the requested output column using the
    // format-specific serde API.
    virtual Status _deserialize_one_cell(const RequestedColumn& column, IColumn* output,
                                         Slice value) = 0;
    // Let formats rewrite a raw field before serde. CSV uses this for empty_field_as_null; Hive
    // text keeps the raw field because empty string and NULL are distinct unless null_format
    // matches exactly.
    virtual Slice _normalize_value(Slice value) const;
    // Whether an empty physical line is one logical record. CSV keeps the existing default
    // skip behavior, while Hive TEXTFILE treats an empty line as a record with one empty field.
    virtual bool _empty_line_as_record() const;
    // Whether this file can start at a non-zero split offset. Compressed delimited files cannot be
    // split because the decompressor needs the stream from the beginning.
    virtual bool _can_split() const;

    Status _append_null(IColumn* output);
    // Match the generic nullable serde semantics exactly: a field is NULL when its raw slice is
    // byte-for-byte equal to null_format. This also covers Hive tables that set
    // serialization.null.format to the empty string.
    bool _is_null_format(Slice value) const;
    const uint8_t* _remove_bom(const uint8_t* ptr, size_t* size);
    void _init_profile() override;

    const TFileScanRangeParams* _scan_params = nullptr;
    std::vector<SlotDescriptor*> _source_file_slot_descs;
    std::vector<int32_t> _source_column_idxs;
    DataTypeSerDeSPtrs _source_serdes;
    std::vector<ColumnDefinition> _file_schema;
    RuntimeState* _runtime_state = nullptr;

    std::vector<RequestedColumn> _requested_columns;
    std::unique_ptr<Decompressor> _decompressor;
    std::unique_ptr<LineReader> _line_reader;
    std::vector<Slice> _split_values;
    DataTypeSerDe::FormatOptions _options;

    std::string _value_separator;
    std::string _line_delimiter;
    TFileCompressType::type _file_compress_type = TFileCompressType::UNKNOWN;
    TFileCompressType::type _range_compress_type = TFileCompressType::UNKNOWN;
    std::optional<TUniqueId> _stream_load_id;
    int64_t _start_offset = 0;
    int64_t _size = -1;
    int _skip_lines = 0;
    char _escape = 0;
    bool _line_reader_eof = false;
    bool _bom_removed = false;
    // FE exposes this as an optional text-file attribute. Keep the default strict so missing thrift
    // fields do not accidentally accept arbitrary bytes; CSV can still opt out through the session
    // variable or TVF/file-format property `enable_text_validate_utf8=false`.
    bool _enable_text_validate_utf8 = true;
    DelimitedTextProfile _text_profile;

private:
    Status _build_requested_columns(const FileScanRequest& request,
                                    std::vector<RequestedColumn>* columns) const;
    Status _open_file();
    Status _read_next_line(Slice* line, bool* eof);
    Status _fill_columns_from_line(const Slice& line, std::vector<MutableColumnPtr>* columns,
                                   size_t* rows);

    std::string _reader_name;
};

} // namespace doris::format
