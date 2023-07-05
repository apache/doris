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

namespace doris {
namespace segment_v2 {

enum class InvertedIndexQueryType {
    UNKNOWN_QUERY = -1,
    EQUAL_QUERY = 0,
    LESS_THAN_QUERY = 1,
    LESS_EQUAL_QUERY = 2,
    GREATER_THAN_QUERY = 3,
    GREATER_EQUAL_QUERY = 4,
    MATCH_ANY_QUERY = 5,
    MATCH_ALL_QUERY = 6,
    MATCH_PHRASE_QUERY = 7,
};

inline std::string InvertedIndexQueryType_toString(InvertedIndexQueryType query_type) {
    switch (query_type) {
    case InvertedIndexQueryType::UNKNOWN_QUERY: {
        return "UNKNOWN";
    }
    case InvertedIndexQueryType::EQUAL_QUERY: {
        return "EQ";
    }
    case InvertedIndexQueryType::LESS_THAN_QUERY: {
        return "LT";
    }
    case InvertedIndexQueryType::LESS_EQUAL_QUERY: {
        return "LE";
    }
    case InvertedIndexQueryType::GREATER_THAN_QUERY: {
        return "GT";
    }
    case InvertedIndexQueryType::GREATER_EQUAL_QUERY: {
        return "GE";
    }
    case InvertedIndexQueryType::MATCH_ANY_QUERY: {
        return "MANY";
    }
    case InvertedIndexQueryType::MATCH_ALL_QUERY: {
        return "MALL";
    }
    case InvertedIndexQueryType::MATCH_PHRASE_QUERY: {
        return "MPHRASE";
    }
    default:
        return "";
    }
}
} // namespace segment_v2
} // namespace doris
