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

#include <fmt/core.h>
#include <gen_cpp/Opcodes_types.h>
#include <glog/logging.h>
#include <math.h>

#include "common/logging.h"
#include "olap/olap_tuple.h"
#include "runtime/primitive_type.h"

namespace doris {

using CompareLargeFunc = bool (*)(const void*, const void*);

static const char* NEGATIVE_INFINITY = "-oo";
static const char* POSITIVE_INFINITY = "+oo";

struct OlapScanRange {
public:
    OlapScanRange() : begin_include(true), end_include(true) {
        begin_scan_range.add_value(NEGATIVE_INFINITY);
        end_scan_range.add_value(POSITIVE_INFINITY);
    }
    OlapScanRange(bool begin, bool end, std::vector<std::string>& begin_range,
                  std::vector<std::string>& end_range)
            : begin_include(begin),
              end_include(end),
              begin_scan_range(begin_range),
              end_scan_range(end_range) {}

    bool begin_include;
    bool end_include;
    OlapTuple begin_scan_range;
    OlapTuple end_scan_range;

    std::string debug_string() const {
        fmt::memory_buffer buf;
        DCHECK_EQ(begin_scan_range.size(), end_scan_range.size());
        for (int i = 0; i < begin_scan_range.size(); i++) {
            fmt::format_to(buf, "({}, {})\n", begin_scan_range[i], end_scan_range[i]);
        }
        return fmt::to_string(buf);
    }
};

static char encoding_table[] = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
                                'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
                                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                                'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
                                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};

static int mod_table[] = {0, 2, 1};
static const char base64_pad = '=';

inline size_t base64_encode(const char* data, size_t length, char* encoded_data) {
    size_t output_length = (size_t)(4.0 * ceil((double)length / 3.0));

    if (encoded_data == nullptr) {
        return 0;
    }

    for (uint32_t i = 0, j = 0; i < length;) {
        uint32_t octet_a = i < length ? (unsigned char)data[i++] : 0;
        uint32_t octet_b = i < length ? (unsigned char)data[i++] : 0;
        uint32_t octet_c = i < length ? (unsigned char)data[i++] : 0;

        uint32_t triple = (octet_a << 0x10) + (octet_b << 0x08) + octet_c;

        encoded_data[j++] = encoding_table[(triple >> 3 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 2 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 1 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 0 * 6) & 0x3F];
    }

    for (int i = 0; i < mod_table[length % 3]; i++) {
        encoded_data[output_length - 1 - i] = base64_pad;
    }

    return output_length;
}

enum SQLFilterOp {
    FILTER_LARGER = 0,
    FILTER_LARGER_OR_EQUAL = 1,
    FILTER_LESS = 2,
    FILTER_LESS_OR_EQUAL = 3,
    FILTER_IN = 4,
    FILTER_NOT_IN = 5
};

template <PrimitiveType>
constexpr bool always_false_v = false;

inline SQLFilterOp to_olap_filter_type(TExprOpcode::type type, bool opposite) {
    switch (type) {
    case TExprOpcode::LT:
        return opposite ? FILTER_LARGER : FILTER_LESS;

    case TExprOpcode::LE:
        return opposite ? FILTER_LARGER_OR_EQUAL : FILTER_LESS_OR_EQUAL;

    case TExprOpcode::GT:
        return opposite ? FILTER_LESS : FILTER_LARGER;

    case TExprOpcode::GE:
        return opposite ? FILTER_LESS_OR_EQUAL : FILTER_LARGER_OR_EQUAL;

    case TExprOpcode::EQ:
        return opposite ? FILTER_NOT_IN : FILTER_IN;

    case TExprOpcode::NE:
        return opposite ? FILTER_IN : FILTER_NOT_IN;

    case TExprOpcode::EQ_FOR_NULL:
        return FILTER_IN;

    default:
        VLOG_CRITICAL << "TExprOpcode: " << type;
        DCHECK(false);
    }

    return FILTER_IN;
}

inline SQLFilterOp to_olap_filter_type(const std::string& function_name, bool opposite) {
    if (function_name == "lt") {
        return opposite ? FILTER_LARGER : FILTER_LESS;
    } else if (function_name == "gt") {
        return opposite ? FILTER_LESS : FILTER_LARGER;
    } else if (function_name == "le") {
        return opposite ? FILTER_LARGER_OR_EQUAL : FILTER_LESS_OR_EQUAL;
    } else if (function_name == "ge") {
        return opposite ? FILTER_LESS_OR_EQUAL : FILTER_LARGER_OR_EQUAL;
    } else if (function_name == "eq") {
        return opposite ? FILTER_NOT_IN : FILTER_IN;
    } else if (function_name == "ne") {
        return opposite ? FILTER_IN : FILTER_NOT_IN;
    } else {
        DCHECK(false) << "Function Name: " << function_name;
        return FILTER_IN;
    }
}

enum class MatchType {
    UNKNOWN = -1,
    MATCH_ANY = 0,
    MATCH_ALL = 1,
    MATCH_PHRASE = 2,
    MATCH_ELEMENT_EQ = 3,
    MATCH_ELEMENT_LT = 4,
    MATCH_ELEMENT_GT = 5,
    MATCH_ELEMENT_LE = 6,
    MATCH_ELEMENT_GE = 7,
};

inline MatchType to_match_type(TExprOpcode::type type) {
    switch (type) {
    case TExprOpcode::type::MATCH_ANY:
        return MatchType::MATCH_ANY;
        break;
    case TExprOpcode::type::MATCH_ALL:
        return MatchType::MATCH_ALL;
        break;
    case TExprOpcode::type::MATCH_PHRASE:
        return MatchType::MATCH_PHRASE;
        break;
    case TExprOpcode::type::MATCH_ELEMENT_EQ:
        return MatchType::MATCH_ELEMENT_EQ;
        break;
    case TExprOpcode::type::MATCH_ELEMENT_LT:
        return MatchType::MATCH_ELEMENT_LT;
        break;
    case TExprOpcode::type::MATCH_ELEMENT_GT:
        return MatchType::MATCH_ELEMENT_GT;
        break;
    case TExprOpcode::type::MATCH_ELEMENT_LE:
        return MatchType::MATCH_ELEMENT_LE;
        break;
    case TExprOpcode::type::MATCH_ELEMENT_GE:
        return MatchType::MATCH_ELEMENT_GE;
        break;
    default:
        VLOG_CRITICAL << "TExprOpcode: " << type;
        DCHECK(false);
    }
    return MatchType::MATCH_ANY;
}

inline MatchType to_match_type(const std::string& condition_op) {
    if (condition_op.compare("match_any") == 0) {
        return MatchType::MATCH_ANY;
    } else if (condition_op.compare("match_all") == 0) {
        return MatchType::MATCH_ALL;
    } else if (condition_op.compare("match_phrase") == 0) {
        return MatchType::MATCH_PHRASE;
    } else if (condition_op.compare("match_element_eq") == 0) {
        return MatchType::MATCH_ELEMENT_EQ;
    } else if (condition_op.compare("match_element_lt") == 0) {
        return MatchType::MATCH_ELEMENT_LT;
    } else if (condition_op.compare("match_element_gt") == 0) {
        return MatchType::MATCH_ELEMENT_GT;
    } else if (condition_op.compare("match_element_le") == 0) {
        return MatchType::MATCH_ELEMENT_LE;
    } else if (condition_op.compare("match_element_ge") == 0) {
        return MatchType::MATCH_ELEMENT_GE;
    }
    return MatchType::UNKNOWN;
}

inline bool is_match_condition(const std::string& op) {
    if (0 == strcasecmp(op.c_str(), "match_any") || 0 == strcasecmp(op.c_str(), "match_all") ||
        0 == strcasecmp(op.c_str(), "match_phrase") ||
        0 == strcasecmp(op.c_str(), "match_element_eq") ||
        0 == strcasecmp(op.c_str(), "match_element_lt") ||
        0 == strcasecmp(op.c_str(), "match_element_gt") ||
        0 == strcasecmp(op.c_str(), "match_element_le") ||
        0 == strcasecmp(op.c_str(), "match_element_ge")) {
        return true;
    }
    return false;
}

inline bool is_match_operator(const TExprOpcode::type& op_type) {
    return TExprOpcode::MATCH_ANY == op_type || TExprOpcode::MATCH_ALL == op_type ||
           TExprOpcode::MATCH_PHRASE == op_type || TExprOpcode::MATCH_ELEMENT_EQ == op_type ||
           TExprOpcode::MATCH_ELEMENT_LT == op_type || TExprOpcode::MATCH_ELEMENT_GT == op_type ||
           TExprOpcode::MATCH_ELEMENT_LE == op_type || TExprOpcode::MATCH_ELEMENT_GE == op_type;
}

} // namespace doris
