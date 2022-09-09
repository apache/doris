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

#include "common/status.h"
#include "exec/line_reader.h"
#include "io/file_reader.h"
#include "util/runtime_profile.h"
#include "util/slice.h"
#include "vec/exec/table_schema_parser/format_schema_parser.h"
namespace doris {

class CsvFormatSchemaParser : public FormatSchemaParser {
public:
    CsvFormatSchemaParser()
            : _min_length(-1),
              _line_delimiter("\n"),
              _line_delimiter_length(1),
              _column_delimiter(","),
              _column_delimiter_length(1) {};
    ~CsvFormatSchemaParser() override = default;
    Status open(const std::unique_ptr<FileReader>& file_reader) override;
    Status parse(PFetchTableSchemaResult* result) override;

private:
    // modify the method name
    Status _parse_column_nums(const Slice& line, PFetchTableSchemaResult* result);
    Status _parse_column_names(const Slice& line, PFetchTableSchemaResult* result);
    Status _parse_column_types(const Slice& line, PFetchTableSchemaResult* result);
    Status _get_next_line(const uint8_t** ptr, size_t* size);

private:
    std::unique_ptr<LineReader> _line_reader;
    bool _line_reader_eof = false;

    size_t _min_length;
    // default delimiter is "\n"
    std::string _line_delimiter;
    size_t _line_delimiter_length;
    std::string _column_delimiter;
    size_t _column_delimiter_length;

    std::unique_ptr<RuntimeProfile> _runtime_profile;
};
} // namespace doris
