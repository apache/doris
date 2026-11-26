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

#include <cstddef>
#include <string>
#include <vector>

#include "util/slice.h"

namespace doris::format::csv {

// OpenCSVSerde runs one OpenCSV 2.3 reader per Hadoop TextInputFormat record.
// Returned slices reference this parser's decoded buffer until the next parse().
// The field limit bounds delimiter metadata by the requested column prefix.
class HiveCsvParser {
public:
    HiveCsvParser(std::string separator, char quote, char escape, size_t field_limit);
    void parse(const Slice& line, std::vector<Slice>* fields);

private:
    std::string _separator;
    char _quote;
    char _escape;
    size_t _field_limit;
    std::string _decoded;
    std::vector<size_t> _field_ends;
};

} // namespace doris::format::csv
