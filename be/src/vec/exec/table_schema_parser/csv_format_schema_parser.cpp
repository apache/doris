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

#include "vec/exec/table_schema_parser/csv_format_schema_parser.h"

#include <gen_cpp/PaloBrokerService_types.h>
#include <gen_cpp/types.pb.h>

#include <cstddef>
#include <cstring>
#include <memory>
#include <string>

#include "common/status.h"
#include "exec/decompressor.h"
#include "exec/plain_text_line_reader.h"
#include "runtime/types.h"
#include "util/string_util.h"
namespace doris {
Status CsvFormatSchemaParser::open(const std::unique_ptr<FileReader>& file_reader) {
    _runtime_profile.reset(new RuntimeProfile("CsvFormatSchemaParser"));
    Decompressor* decompressor = nullptr;
    RETURN_IF_ERROR(Decompressor::create_decompressor(CompressType::UNCOMPRESSED, &decompressor));
    _line_reader.reset(new PlainTextLineReader(_runtime_profile.get(), file_reader.get(),
                                               decompressor, _min_length, _line_delimiter,
                                               _line_delimiter_length));
    return Status::OK();
}

Status CsvFormatSchemaParser::parse(PFetchTableSchemaResult* result) {
    const uint8_t* ptr = nullptr;
    size_t size = 0;
    RETURN_IF_ERROR(_get_next_line(&ptr, &size));
    RETURN_IF_ERROR(_parse_column_nums(Slice(ptr, size), result));
    RETURN_IF_ERROR(_parse_column_names(Slice(ptr, size), result));
    RETURN_IF_ERROR(_parse_column_types(Slice(ptr, size), result));
    return Status::OK();
}

Status CsvFormatSchemaParser::_get_next_line(const uint8_t** ptr, size_t* size) {
    while (_line_reader_eof == false) {
        LOG(INFO) << "--ftw: read line";
        RETURN_IF_ERROR(_line_reader->read_line(ptr, size, &_line_reader_eof));
        if (*size == 0) {
            // Read empty row, just continue
            LOG(INFO) << "--ftw: read 0 size of line";
            continue;
        } else {
            break;
        }
    }
    if (*size == 0 && _line_reader_eof == true) {
        return Status::InvalidArgument("can not read a non-empty line");
    }
    return Status::OK();
}

Status CsvFormatSchemaParser::_parse_column_nums(const Slice& line,
                                                 PFetchTableSchemaResult* result) {
    std::vector<std::string> split_col =
            split(std::string((const char*)line.data, line.size), _column_delimiter);
    result->set_column_nums(split_col.size());
    return Status::OK();
}

Status CsvFormatSchemaParser::_parse_column_names(const Slice& line,
                                                  PFetchTableSchemaResult* result) {
    if (!result->has_column_nums()) {
        return Status::InternalError("can not parse column nums");
    }
    for (int col_idx = 0; col_idx < result->column_nums(); ++col_idx) {
        std::string col_name = "c" + std::to_string(col_idx + 1);
        result->add_column_names(col_name);
    }
    return Status::OK();
}

Status CsvFormatSchemaParser::_parse_column_types(const Slice& line,
                                                  PFetchTableSchemaResult* result) {
    if (!result->has_column_nums()) {
        return Status::InternalError("can not parse column nums");
    }
    for (int col_idx = 0; col_idx < result->column_nums(); ++col_idx) {
        TypeDescriptor string_type = TypeDescriptor::create_string_type();
        PTypeDesc* type_desc = result->add_column_types();
        string_type.to_protobuf(type_desc);
    }
    return Status::OK();
}
} // namespace doris
