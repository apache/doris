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

#include <gen_cpp/Opcodes_types.h>
#include <glog/logging.h>
#include <math.h>

#include <sstream>

#include "common/logging.h"
#include "core/data_type/primitive_type.h"
#include "storage/olap_tuple.h"

namespace doris {

using CompareLargeFunc = bool (*)(const void*, const void*);

/// OlapScanRange represents a single key-range interval used to scan an OLAP tablet.
///
/// It is the final product of the scan-key generation pipeline:
///
///   SQL WHERE conjuncts
///     -> ColumnValueRange  (per-column value constraints, see olap_scan_common.h)
///     -> OlapScanKeys::extend_scan_key()  (combine columns into multi-column prefix keys)
///     -> OlapScanKeys::get_key_range()    (emit one OlapScanRange per key pair)
///     -> OlapScanner / tablet reader      (use ranges for short-key index lookup)
///
/// Example – table t(k1 INT, k2 INT, v INT) with key columns (k1, k2):
///
///   WHERE k1 IN (1, 2) AND k2 = 10
///     => two OlapScanRange objects:
///        range0: begin=(1, 10)  end=(1, 10)  include=[true, true]  -- point lookup
///        range1: begin=(2, 10)  end=(2, 10)  include=[true, true]  -- point lookup
///
///   WHERE k1 >= 5 AND k1 < 10
///     => one OlapScanRange:
///        begin=(5)  end=(10)  begin_include=true  end_include=false
///
///   No key predicates at all (full table scan):
///     => one default-constructed OlapScanRange with has_lower_bound=false, has_upper_bound=false.
///        Consumers detect this and skip pushing key range to the reader (fall back to full scan).
///
struct OlapScanRange {
public:
    OlapScanRange()
            : begin_include(true),
              end_include(true),
              has_lower_bound(false),
              has_upper_bound(false) {}

    bool begin_include;
    bool end_include;

    /// Whether this range carries real begin/end bounds.
    /// false only for the default-constructed "full scan" placeholder
    /// (created when no key predicates exist at all).
    bool has_lower_bound;
    bool has_upper_bound;

    OlapTuple begin_scan_range;
    OlapTuple end_scan_range;

    std::string debug_string() const {
        std::ostringstream buf;
        buf << "begin=(" << begin_scan_range.debug_string() << "), end=("
            << end_scan_range.debug_string() << ")";
        return buf.str();
    }
};

enum SQLFilterOp {
    FILTER_LARGER = 0,
    FILTER_LARGER_OR_EQUAL = 1,
    FILTER_LESS = 2,
    FILTER_LESS_OR_EQUAL = 3,
    FILTER_IN = 4,
    FILTER_NOT_IN = 5,
    FILTER_EQ = 6,
    FILTER_NE = 7
};

template <PrimitiveType>
constexpr bool always_false_v = false;

inline SQLFilterOp to_olap_filter_type(const std::string& function_name) {
    if (function_name == "lt") {
        return FILTER_LESS;
    } else if (function_name == "gt") {
        return FILTER_LARGER;
    } else if (function_name == "le") {
        return FILTER_LESS_OR_EQUAL;
    } else if (function_name == "ge") {
        return FILTER_LARGER_OR_EQUAL;
    } else if (function_name == "eq") {
        return FILTER_EQ;
    } else if (function_name == "ne") {
        return FILTER_NE;
    } else if (function_name == "in") {
        return FILTER_IN;
    } else if (function_name == "not_in") {
        return FILTER_NOT_IN;
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
    MATCH_PHRASE_PREFIX = 8,
    MATCH_REGEXP = 9,
    MATCH_PHRASE_EDGE = 10,
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
    case TExprOpcode::type::MATCH_PHRASE_PREFIX:
        return MatchType::MATCH_PHRASE_PREFIX;
        break;
    case TExprOpcode::type::MATCH_REGEXP:
        return MatchType::MATCH_REGEXP;
        break;
    case TExprOpcode::type::MATCH_PHRASE_EDGE:
        return MatchType::MATCH_PHRASE_EDGE;
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
    } else if (condition_op.compare("match_phrase_prefix") == 0) {
        return MatchType::MATCH_PHRASE_PREFIX;
    } else if (condition_op.compare("match_regexp") == 0) {
        return MatchType::MATCH_REGEXP;
    } else if (condition_op.compare("match_phrase_edge") == 0) {
        return MatchType::MATCH_PHRASE_EDGE;
    }
    return MatchType::UNKNOWN;
}

inline bool is_match_condition(const std::string& op) {
    if (0 == strcasecmp(op.c_str(), "match_any") || 0 == strcasecmp(op.c_str(), "match_all") ||
        0 == strcasecmp(op.c_str(), "match_phrase") ||
        0 == strcasecmp(op.c_str(), "match_phrase_prefix") ||
        0 == strcasecmp(op.c_str(), "match_regexp") ||
        0 == strcasecmp(op.c_str(), "match_phrase_edge")) {
        return true;
    }
    return false;
}

inline bool is_match_operator(const TExprOpcode::type& op_type) {
    return TExprOpcode::MATCH_ANY == op_type || TExprOpcode::MATCH_ALL == op_type ||
           TExprOpcode::MATCH_PHRASE == op_type || TExprOpcode::MATCH_PHRASE_PREFIX == op_type ||
           TExprOpcode::MATCH_REGEXP == op_type || TExprOpcode::MATCH_PHRASE_EDGE == op_type;
}

} // namespace doris
