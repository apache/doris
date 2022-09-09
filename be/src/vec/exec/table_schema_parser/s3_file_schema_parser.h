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

#include "common/status.h"
#include "exec/line_reader.h"
#include "file_schema_parser.h"
#include "io/file_reader.h"
#include "util/s3_uri.h"
namespace doris {
class LineReader;

class S3FileSchemaParser : public FileSchemaParser {
public:
    S3FileSchemaParser(const PFetchTableSchemaRequest* request, PFetchTableSchemaResult* result,
                       std::string path)
            : FileSchemaParser(request, result, path), _s3_uri(path) {}

    Status parse() override;

private:
    /**
    * parse the path to _s3_endpoint and _s3_uri
    * example: 
    *   parse path: http://127.0.0.1:9312/test2/student1.csv to:
    *   _s3_endpoint = http://127.0.0.1:9312
    *   _s3_uri = s3://test2/student1.csv
    */
    Status _parse_path();

private:
    static const std::string _SCHEME_DELIM;
    static const std::string _PATH_DELIM;
    static const std::string _S3_SCHEMA;
    static const StringCaseSet _VALID_SCHEMES;

    std::string _s3_endpoint;
    std::string _s3_uri;
    std::unique_ptr<FileReader> _cur_file_reader;
    std::unique_ptr<LineReader> _cur_line_reader;
};
} // namespace doris
