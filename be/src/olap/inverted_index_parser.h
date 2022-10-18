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

#include <string>
#include <map>

namespace doris {

enum class InvertedIndexParserType {
    PARSER_UNKNOWN = 0,
    PARSER_NOT_SET = 1,
    PARSER_NONE = 2,
    PARSER_STANDARD = 3,
    PARSER_ENGLISH = 4,
    PARSER_CHINESE = 5,
};

const std::string INVERTED_INDEX_PARSER_KEY = "parser";
const std::string INVERTED_INDEX_PARSER_UNKNOWN = "unknown";
const std::string INVERTED_INDEX_PARSER_NOT_SET = "not_set";
const std::string INVERTED_INDEX_PARSER_NONE = "none";
const std::string INVERTED_INDEX_PARSER_STANDARD = "standard";
const std::string INVERTED_INDEX_PARSER_ENGLISH = "english";
const std::string INVERTED_INDEX_PARSER_CHINESE = "chinese";

std::string inverted_index_parser_type_to_string(InvertedIndexParserType parser_type);

InvertedIndexParserType get_inverted_index_parser_type_from_string(const std::string& parser_str);

std::string get_parser_string_from_properties(const std::map<std::string, std::string>& properties);

} // namespace doris
