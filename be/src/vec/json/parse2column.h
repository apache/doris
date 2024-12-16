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

#include <common/status.h>

#include <vector>

#include "vec/columns/column.h"
#include "vec/common/string_ref.h"
#include "vec/json/json_parser.h"

namespace doris::vectorized {

class SimdJSONParser;
enum class ExtractType;
template <typename ParserImpl>
class JSONDataParser;
template <typename T>
class ColumnStr;
using ColumnString = ColumnStr<UInt32>;
using JsonParser = JSONDataParser<SimdJSONParser>;

// parse a batch of json strings into column object, throws doris::Execption when failed
void parse_json_to_variant(IColumn& column, const ColumnString& raw_json_column,
                           const ParseConfig& config);

// parse a single json, throws doris::Execption when failed
void parse_json_to_variant(IColumn& column, const StringRef& jsons, JsonParser* parser,
                           const ParseConfig& config);
} // namespace doris::vectorized
