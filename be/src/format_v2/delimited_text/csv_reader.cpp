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

#include "format_v2/delimited_text/csv_reader.h"

#include <algorithm>
#include <cstring>
#include <utility>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type_serde/data_type_string_serde.h"
#include "format/file_reader/new_plain_binary_line_reader.h"
#include "format/file_reader/new_plain_text_line_reader.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/decompressor.h"
#include "util/utf8_check.h"

namespace doris::format::csv {

enum class CsvReaderState { START, NORMAL, PRE_MATCH_ENCLOSE, MATCH_ENCLOSE };

struct CsvReaderStateWrapper {
    void forward_to(CsvReaderState state) {
        prev_state = curr_state;
        curr_state = state;
    }

    void reset() {
        curr_state = CsvReaderState::START;
        prev_state = CsvReaderState::START;
    }

    CsvReaderState curr_state = CsvReaderState::START;
    CsvReaderState prev_state = CsvReaderState::START;
};

// The v1 CSV context cannot distinguish a column separator that overlaps the line delimiter and
// may rewind into a separator that was already emitted when the output buffer is refilled. Keep
// the stricter behavior local to format v2 so the v1 reader remains unchanged.
class EncloseCsvLineReaderV2Ctx final
        : public BaseTextLineReaderContext<EncloseCsvLineReaderV2Ctx> {
    using FindDelimiterFunc = const uint8_t* (*)(const uint8_t*, size_t, const char*, size_t);

public:
    EncloseCsvLineReaderV2Ctx(const std::string& line_delimiter_, size_t line_delimiter_len_,
                              std::string column_separator, size_t column_separator_len,
                              size_t column_separator_count, char enclose, char escape,
                              bool keep_cr_, bool skip_utf8_bom)
            : BaseTextLineReaderContext(line_delimiter_, line_delimiter_len_, keep_cr_),
              _enclose(enclose),
              _escape(escape),
              _skip_utf8_bom(skip_utf8_bom),
              _column_separator_len(column_separator_len),
              _column_separator(std::move(column_separator)) {
        if (_column_separator_len == 1) {
            _find_column_separator = &look_for_column_separator<true>;
        } else {
            _find_column_separator = &look_for_column_separator<false>;
        }
        _column_separator_positions.reserve(column_separator_count);
    }

    void refresh_impl() {
        _idx = 0;
        _should_escape = false;
        _quote_escape = false;
        _result = nullptr;
        _column_separator_positions.clear();
        _state.reset();
    }

    [[nodiscard]] const std::vector<size_t>& column_sep_positions() const {
        return _column_separator_positions;
    }

    void adjust_column_sep_positions(size_t offset) {
        for (auto& position : _column_separator_positions) {
            position -= offset;
        }
    }

    const uint8_t* read_line_impl(const uint8_t* start, size_t length) {
        if (_skip_utf8_bom && !_first_record_prefix_checked && _idx == 0) {
            constexpr uint8_t UTF8_BOM[] = {0xEF, 0xBB, 0xBF};
            constexpr size_t UTF8_BOM_SIZE = sizeof(UTF8_BOM);
            const size_t prefix_size = std::min(length, UTF8_BOM_SIZE);
            if (std::memcmp(start, UTF8_BOM, prefix_size) != 0) {
                _first_record_prefix_checked = true;
            } else if (length < UTF8_BOM_SIZE) {
                return nullptr;
            } else {
                _idx = UTF8_BOM_SIZE;
                _first_record_prefix_checked = true;
            }
        }

        if (_state.curr_state == CsvReaderState::NORMAL ||
            _state.curr_state == CsvReaderState::MATCH_ENCLOSE) {
            const size_t last_separator_end =
                    _column_separator_positions.empty()
                            ? 0
                            : _column_separator_positions.back() + _column_separator_len;
            DORIS_CHECK_LE(last_separator_end, _idx);
            _idx -= std::min(_column_separator_len - 1, _idx - last_separator_end);
        }

        _total_len = length;
        size_t bound = update_reading_bound(start);
        while (_idx != bound) {
            switch (_state.curr_state) {
            case CsvReaderState::START:
                on_start(start);
                break;
            case CsvReaderState::NORMAL:
                on_normal(start, bound);
                break;
            case CsvReaderState::PRE_MATCH_ENCLOSE:
                on_pre_match_enclose(start, bound);
                break;
            case CsvReaderState::MATCH_ENCLOSE:
                on_match_enclose(start, bound);
                break;
            }
        }
        return _result;
    }

private:
    template <bool SingleChar>
    static const uint8_t* look_for_column_separator(const uint8_t* start, size_t length,
                                                    const char* separator, size_t separator_len) {
        if constexpr (SingleChar) {
            for (size_t i = 0; i < length; ++i) {
                if (start[i] == separator[0]) {
                    return start + i;
                }
            }
            return nullptr;
        }
        return static_cast<const uint8_t*>(memmem(start, length, separator, separator_len));
    }

    size_t update_reading_bound(const uint8_t* start) {
        _result = call_find_line_sep(start + _idx, _total_len - _idx);
        return _result == nullptr ? _total_len : _result - start + line_delimiter_length();
    }

    void on_column_separator_found(const uint8_t* start, const uint8_t* separator_position) {
        const uint8_t* field_start = start + _idx;
        _column_separator_positions.push_back(separator_position - start);
        _idx += separator_position + _column_separator_len - field_start;
    }

    void on_start(const uint8_t* start) {
        if (start[_idx] == _enclose) [[unlikely]] {
            _state.forward_to(CsvReaderState::PRE_MATCH_ENCLOSE);
            ++_idx;
        } else {
            _state.forward_to(CsvReaderState::NORMAL);
        }
    }

    void on_normal(const uint8_t* start, size_t bound) {
        const size_t search_bound = _result == nullptr ? bound : _result - start;
        DORIS_CHECK_LE(_idx, search_bound);
        const uint8_t* separator_position =
                _find_column_separator(start + _idx, search_bound - _idx, _column_separator.c_str(),
                                       _column_separator_len);
        if (separator_position != nullptr) [[likely]] {
            on_column_separator_found(start, separator_position);
            _state.forward_to(CsvReaderState::START);
            return;
        }
        _idx = bound;
    }

    void on_pre_match_enclose(const uint8_t* start, size_t& bound) {
        do {
            do {
                if (_escape != _enclose && start[_idx] == _escape) [[unlikely]] {
                    _should_escape = !_should_escape;
                } else if (_should_escape) [[unlikely]] {
                    _should_escape = false;
                } else if (_quote_escape) {
                    if (start[_idx] == _enclose) {
                        _quote_escape = false;
                    } else {
                        _quote_escape = false;
                        _state.forward_to(CsvReaderState::MATCH_ENCLOSE);
                        return;
                    }
                } else if (start[_idx] == _enclose) {
                    _quote_escape = true;
                } else {
                    _quote_escape = false;
                }
                ++_idx;
            } while (_idx != bound);

            if (_idx != _total_len) {
                bound = update_reading_bound(start);
            } else {
                _result = nullptr;
                break;
            }
        } while (true);
    }

    void on_match_enclose(const uint8_t* start, size_t bound) {
        const size_t search_bound = _result == nullptr ? bound : _result - start;
        DORIS_CHECK_LE(_idx, search_bound);
        const uint8_t* separator_position =
                _find_column_separator(start + _idx, search_bound - _idx, _column_separator.c_str(),
                                       _column_separator_len);
        if (separator_position != nullptr) [[likely]] {
            on_column_separator_found(start, separator_position);
            _state.forward_to(CsvReaderState::START);
            return;
        }
        _idx = bound;
    }

    CsvReaderStateWrapper _state;
    const char _enclose;
    const char _escape;
    const bool _skip_utf8_bom;
    bool _first_record_prefix_checked = false;
    const uint8_t* _result = nullptr;
    size_t _total_len = 0;
    const size_t _column_separator_len;
    size_t _idx = 0;
    bool _should_escape = false;
    bool _quote_escape = false;
    const std::string _column_separator;
    std::vector<size_t> _column_separator_positions;
    FindDelimiterFunc _find_column_separator;
};

namespace {

bool starts_with_at(const Slice& line, size_t pos, const std::string& needle) {
    return !needle.empty() && pos + needle.size() <= line.size &&
           std::memcmp(line.data + pos, needle.data(), needle.size()) == 0;
}

bool is_csv_text_format(TFileFormatType::type format_type) {
    switch (format_type) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
    case TFileFormatType::FORMAT_CSV_GZ:
    case TFileFormatType::FORMAT_CSV_BZ2:
    case TFileFormatType::FORMAT_CSV_LZ4FRAME:
    case TFileFormatType::FORMAT_CSV_LZ4BLOCK:
    case TFileFormatType::FORMAT_CSV_LZOP:
    case TFileFormatType::FORMAT_CSV_SNAPPYBLOCK:
    case TFileFormatType::FORMAT_CSV_DEFLATE:
        return true;
    default:
        return false;
    }
}

} // namespace

CsvReader::CsvReader(std::shared_ptr<io::FileSystemProperties>& system_properties,
                     std::unique_ptr<io::FileDescription>& file_description,
                     std::shared_ptr<io::IOContext> io_ctx, RuntimeProfile* profile,
                     const TFileScanRangeParams* scan_params,
                     const std::vector<SlotDescriptor*>& file_slot_descs,
                     TFileCompressType::type range_compress_type,
                     std::optional<TUniqueId> stream_load_id)
        : DelimitedTextReader(system_properties, file_description, std::move(io_ctx), profile,
                              scan_params, file_slot_descs, range_compress_type,
                              std::move(stream_load_id), "CSV") {}

CsvReader::~CsvReader() = default;

Status CsvReader::_init_format_state() {
    _file_format_type = _scan_params->format_type;
    _file_compress_type =
            _range_compress_type != TFileCompressType::UNKNOWN
                    ? _range_compress_type
                    : (_scan_params->__isset.compress_type ? _scan_params->compress_type
                                                           : TFileCompressType::UNKNOWN);
    if (_file_compress_type == TFileCompressType::UNKNOWN &&
        _file_format_type == TFileFormatType::FORMAT_CSV_PLAIN) {
        // FORMAT_CSV_PLAIN is an uncompressed byte stream even when FE does not fill
        // compress_type. Non-first splits rely on this normalization; otherwise UNKNOWN would be
        // rejected by the split-compressed-file guard in the shared reader base.
        _file_compress_type = TFileCompressType::PLAIN;
    }

    const auto& text_params = _scan_params->file_attributes.text_params;
    _value_separator = text_params.column_separator;
    _line_delimiter = text_params.line_delimiter;
    if (text_params.__isset.enclose) {
        _enclose = text_params.enclose;
    }
    if (text_params.__isset.escape) {
        _escape = text_params.escape;
    }
    _trim_tailing_spaces = _runtime_state != nullptr &&
                           _runtime_state->trim_tailing_spaces_for_external_table_query();
    _options.escape_char = _escape;
    _options.quote_char = _enclose;
    _options.collection_delim =
            text_params.collection_delimiter.empty() ? ',' : text_params.collection_delimiter[0];
    _options.map_key_delim =
            text_params.mapkv_delimiter.empty() ? ':' : text_params.mapkv_delimiter[0];
    if (text_params.__isset.null_format) {
        _options.null_format = text_params.null_format.data();
        _options.null_len = text_params.null_format.length();
    }
    if (_scan_params->file_attributes.__isset.trim_double_quotes) {
        _trim_double_quotes = _scan_params->file_attributes.trim_double_quotes;
    }
    _options.converted_from_string = _trim_double_quotes;
    if (_runtime_state != nullptr) {
        _keep_cr = _runtime_state->query_options().keep_carriage_return;
    }
    if (text_params.__isset.empty_field_as_null) {
        _empty_field_as_null = text_params.empty_field_as_null;
    }
    return Status::OK();
}

Status CsvReader::_create_decompressor() {
    if (_file_compress_type != TFileCompressType::UNKNOWN) {
        return Decompressor::create_decompressor(_file_compress_type, &_decompressor);
    }
    return Decompressor::create_decompressor(_file_format_type, &_decompressor);
}

Status CsvReader::_create_line_reader() {
    if (is_csv_text_format(_file_format_type)) {
        std::shared_ptr<TextLineReaderContextIf> text_line_reader_ctx;
        if (_enclose == 0) {
            text_line_reader_ctx = std::make_shared<PlainTextLineReaderCtx>(
                    _line_delimiter, _line_delimiter.size(), _keep_cr);
        } else {
            const size_t col_sep_num =
                    _source_file_slot_descs.size() > 1 ? _source_file_slot_descs.size() - 1 : 0;
            _enclose_reader_ctx = std::make_shared<EncloseCsvLineReaderV2Ctx>(
                    _line_delimiter, _line_delimiter.size(), _value_separator,
                    _value_separator.size(), col_sep_num, _enclose, _escape, _keep_cr,
                    _start_offset == 0);
            text_line_reader_ctx = _enclose_reader_ctx;
        }
        _line_reader = NewPlainTextLineReader::create_unique(
                _profile, _file_reader, _decompressor.get(), std::move(text_line_reader_ctx), _size,
                _start_offset);
        return Status::OK();
    }
    if (_file_format_type == TFileFormatType::FORMAT_PROTO) {
        _line_reader = NewPlainBinaryLineReader::create_unique(_file_reader);
        return Status::OK();
    }
    return Status::InternalError<false>("Unknown CSV format type {}", _file_format_type);
}

Status CsvReader::_validate_line(const Slice& line) {
    if (_file_format_type != TFileFormatType::FORMAT_PROTO && _enable_text_validate_utf8 &&
        !validate_utf8(line.data, line.size)) {
        return Status::InternalError<false>("Only support csv data in utf8 codec");
    }
    return Status::OK();
}

void CsvReader::_split_line(const Slice& line) {
    _split_values.clear();
    if (_file_format_type == TFileFormatType::FORMAT_PROTO) {
        auto** row_ptr = reinterpret_cast<PDataRow**>(line.data);
        PDataRow* row = *row_ptr;
        for (const PDataColumn& col : row->col()) {
            _split_values.emplace_back(col.value());
        }
        return;
    }

    const auto append_value = [&](size_t value_start, size_t value_len) {
        while (_trim_tailing_spaces && value_len > 0 &&
               line.data[value_start + value_len - 1] == ' ') {
            --value_len;
        }
        if (_enclose != 0 && value_len > 1 && line.data[value_start] == _enclose &&
            line.data[value_start + value_len - 1] == _enclose) {
            ++value_start;
            value_len -= 2;
        }
        _split_values.emplace_back(line.data + value_start, value_len);
    };

    size_t value_start = 0;
    if (_enclose_reader_ctx != nullptr) {
        for (const size_t separator_position : _enclose_reader_ctx->column_sep_positions()) {
            DORIS_CHECK_LE(value_start, separator_position);
            DORIS_CHECK_LE(separator_position, line.size);
            append_value(value_start, separator_position - value_start);
            value_start = separator_position + _value_separator.size();
        }
    } else {
        for (size_t i = 0; i < line.size;) {
            if (starts_with_at(line, i, _value_separator)) {
                append_value(value_start, i - value_start);
                i += _value_separator.size();
                value_start = i;
            } else {
                ++i;
            }
        }
    }
    DORIS_CHECK_LE(value_start, line.size);
    append_value(value_start, line.size - value_start);
}

Status CsvReader::_deserialize_one_cell(const RequestedColumn& column, IColumn* output,
                                        Slice value) {
    DORIS_CHECK(output != nullptr);
    if (column.nullable_string_fast_path) {
        auto& null_column = assert_cast<ColumnNullable&>(*output);
        // String is the hottest CSV type. Avoid the generic nullable serde wrapper here:
        // deserialize directly into the nested string column and append the null map bit ourselves.
        if (_empty_field_as_null && value.size == 0) {
            null_column.insert_data(nullptr, 0);
            return Status::OK();
        }
        // CSV keeps empty-field handling separate from null_format matching. An empty
        // null_format must not turn every empty CSV field into NULL unless FE explicitly sets
        // empty_field_as_null; OpenCSV-compatible tables expect empty fields to stay empty strings.
        const bool quoted = _options.converted_from_string && value.trim_double_quotes();
        if (!quoted && _options.null_len > 0 && value.size == _options.null_len &&
            std::memcmp(value.data, _options.null_format, value.size) == 0) {
            null_column.insert_data(nullptr, 0);
            return Status::OK();
        }
        static DataTypeStringSerDe string_serde(TYPE_STRING);
        auto status = string_serde.deserialize_one_cell_from_csv(null_column.get_nested_column(),
                                                                 value, _options);
        if (!status.ok()) {
            null_column.insert_data(nullptr, 0);
            return Status::OK();
        }
        null_column.get_null_map_data().push_back(0);
        return Status::OK();
    }
    return column.serde->deserialize_one_cell_from_csv(*output, value, _options);
}

Slice CsvReader::_normalize_value(Slice value) const {
    if (_empty_field_as_null && value.size == 0) {
        return Slice(_options.null_format, _options.null_len);
    }
    return value;
}

bool CsvReader::_can_split() const {
    return (_file_compress_type == TFileCompressType::PLAIN) ||
           (_file_compress_type == TFileCompressType::UNKNOWN &&
            _file_format_type == TFileFormatType::FORMAT_CSV_PLAIN);
}

void CsvReader::_on_bom_removed(size_t bom_size) {
    if (_enclose_reader_ctx != nullptr) {
        _enclose_reader_ctx->adjust_column_sep_positions(bom_size);
    }
}

} // namespace doris::format::csv
