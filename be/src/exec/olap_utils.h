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

#ifndef DORIS_BE_SRC_QUERY_EXEC_OLAP_UTILS_H
#define DORIS_BE_SRC_QUERY_EXEC_OLAP_UTILS_H

#include <math.h>

#include "common/logging.h"
#include "gen_cpp/Opcodes_types.h"
#include "olap/tuple.h"
#include "runtime/datetime_value.h"
#include "runtime/primitive_type.h"

namespace doris {

typedef bool (*CompareLargeFunc)(const void*, const void*);

template <class T>
inline bool compare_large(const void* lhs, const void* rhs) {
    return *reinterpret_cast<const T*>(lhs) > *reinterpret_cast<const T*>(rhs);
}

inline CompareLargeFunc get_compare_func(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
        return compare_large<bool>;

    case TYPE_TINYINT:
        return compare_large<int8_t>;

    case TYPE_SMALLINT:
        return compare_large<int16_t>;

    case TYPE_INT:
        return compare_large<int32_t>;

    case TYPE_BIGINT:
        return compare_large<int64_t>;

    case TYPE_LARGEINT:
        return compare_large<__int128>;

    case TYPE_FLOAT:
        return compare_large<float>;

    case TYPE_DOUBLE:
        return compare_large<double>;

    case TYPE_DATE:
    case TYPE_DATETIME:
        return compare_large<DateTimeValue>;

    case TYPE_DECIMALV2:
        return compare_large<DecimalV2Value>;

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
        return compare_large<StringValue>;

    default:
        DCHECK(false) << "Unsupported Compare type";
    }
    __builtin_unreachable();
}

static const char* NEGATIVE_INFINITY = "-oo";
static const char* POSITIVE_INFINITY = "+oo";

typedef struct OlapScanRange {
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
} OlapScanRange;

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

inline int get_olap_size(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT: {
        return 1;
    }

    case TYPE_SMALLINT: {
        return 2;
    }

    case TYPE_DATE: {
        return 3;
    }

    case TYPE_INT:
    case TYPE_FLOAT: {
        return 4;
    }

    case TYPE_BIGINT:
    case TYPE_LARGEINT:
    case TYPE_DOUBLE:
    case TYPE_DATETIME: {
        return 8;
    }

    case TYPE_DECIMALV2: {
        return 12;
    }

    default: {
        DCHECK(false);
    }
    }

    return 0;
}

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

} // namespace doris

#endif
