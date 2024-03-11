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

enum class InvertedIndexReaderType {
    UNKNOWN = -1,
    FULLTEXT = 0,
    STRING_TYPE = 1,
    BKD = 2,
};

template <InvertedIndexReaderType T>
constexpr const char* InvertedIndexReaderTypeToString();

template <>
constexpr const char* InvertedIndexReaderTypeToString<InvertedIndexReaderType::UNKNOWN>() {
    return "UNKNOWN";
}

template <>
constexpr const char* InvertedIndexReaderTypeToString<InvertedIndexReaderType::FULLTEXT>() {
    return "FULLTEXT";
}

template <>
constexpr const char* InvertedIndexReaderTypeToString<InvertedIndexReaderType::STRING_TYPE>() {
    return "STRING_TYPE";
}

template <>
constexpr const char* InvertedIndexReaderTypeToString<InvertedIndexReaderType::BKD>() {
    return "BKD";
}

inline std::string reader_type_to_string(InvertedIndexReaderType query_type) {
    switch (query_type) {
    case InvertedIndexReaderType::UNKNOWN:
        return InvertedIndexReaderTypeToString<InvertedIndexReaderType::UNKNOWN>();
    case InvertedIndexReaderType::FULLTEXT:
        return InvertedIndexReaderTypeToString<InvertedIndexReaderType::FULLTEXT>();
    case InvertedIndexReaderType::STRING_TYPE:
        return InvertedIndexReaderTypeToString<InvertedIndexReaderType::STRING_TYPE>();
    case InvertedIndexReaderType::BKD:
        return InvertedIndexReaderTypeToString<InvertedIndexReaderType::BKD>();
    }
    return ""; // Explicitly handle all cases
}

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
    MATCH_PHRASE_PREFIX_QUERY = 8,
    MATCH_REGEXP_QUERY = 9,
    MATCH_PHRASE_EDGE_QUERY = 10,
};

inline bool is_range_query(InvertedIndexQueryType query_type) {
    return (query_type == InvertedIndexQueryType::GREATER_THAN_QUERY ||
            query_type == InvertedIndexQueryType::GREATER_EQUAL_QUERY ||
            query_type == InvertedIndexQueryType::LESS_THAN_QUERY ||
            query_type == InvertedIndexQueryType::LESS_EQUAL_QUERY);
}

inline bool is_match_query(InvertedIndexQueryType query_type) {
    return (query_type == InvertedIndexQueryType::MATCH_ANY_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_ALL_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_PHRASE_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_REGEXP_QUERY ||
            query_type == InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY);
}

inline std::string query_type_to_string(InvertedIndexQueryType query_type) {
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
    case InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY: {
        return "MPHRASEPREFIX";
    }
    case InvertedIndexQueryType::MATCH_REGEXP_QUERY: {
        return "MREGEXP";
    }
    case InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY: {
        return "MPHRASEEDGE";
    }
    default:
        return "";
    }
}
} // namespace segment_v2
} // namespace doris
