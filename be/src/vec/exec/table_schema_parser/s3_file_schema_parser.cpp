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

#include "s3_file_schema_parser.h"

#include "common/status.h"
#include "gutil/strings/split.h"
#include "io/s3_reader.h"
#include "vec/exec/table_schema_parser/csv_format_schema_parser.h"
#include "vec/exec/table_schema_parser/format_schema_parser.h"

namespace doris {
const std::string S3FileSchemaParser::_SCHEME_DELIM = "://";
const std::string S3FileSchemaParser::_PATH_DELIM = "/";
const std::string S3FileSchemaParser::_S3_SCHEMA = "s3";
const StringCaseSet S3FileSchemaParser::_VALID_SCHEMES = {"http", "https"};

Status S3FileSchemaParser::parse() {
    RETURN_IF_ERROR(_parse_path());
    std::map<std::string, std::string> properties(_request->properties().begin(),
                                                  _request->properties().end());
    properties["AWS_ENDPOINT"] = _s3_endpoint;
    properties["AWS_REGION"] = "";
    properties["use_path_style"] = "true";
    _cur_file_reader.reset(new S3Reader(properties, _s3_uri, 0));
    RETURN_IF_ERROR(_cur_file_reader->open());
    std::unique_ptr<FormatSchemaParser> format_schema_parser;
    switch (_request->format_type()) {
    case CSV:
        format_schema_parser.reset(new CsvFormatSchemaParser());
        break;
    default:
        return Status::InvalidArgument("no support format");
    }
    RETURN_IF_ERROR(format_schema_parser->open(_cur_file_reader));
    RETURN_IF_ERROR(format_schema_parser->parse(_result));
    return Status::OK();
}

Status S3FileSchemaParser::_parse_path() {
    if (_path.empty()) {
        return Status::InvalidArgument("Invalid path, please check path.");
    }
    std::vector<std::string> scheme_split = strings::Split(_path, _SCHEME_DELIM);
    if (scheme_split.size() != 2) {
        return Status::InvalidArgument("Invalid path, please check path.");
    }
    std::string schema = scheme_split[0];
    if (_VALID_SCHEMES.find(schema) == _VALID_SCHEMES.end()) {
        return Status::InvalidArgument("Invalid path schema, please check path.");
    }
    std::vector<std::string> endpoint_split =
            strings::Split(scheme_split[1], strings::delimiter::Limit(_PATH_DELIM, 1));
    if (endpoint_split.size() != 2) {
        return Status::InvalidArgument("Invalid path endpoint, please check path.");
    }
    _s3_endpoint = schema + _SCHEME_DELIM + endpoint_split[0];
    _s3_uri = _S3_SCHEMA + _SCHEME_DELIM + endpoint_split[1];
    return Status::OK();
}
} // namespace doris
