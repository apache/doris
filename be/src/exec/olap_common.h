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

#include <gen_cpp/PaloInternalService_types.h>
#include <glog/logging.h>
#include <stddef.h>

#include <boost/container/detail/std_fwd.hpp>
#include <boost/lexical_cast.hpp>
#include <cstdint>
#include <iterator>
#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include "common/status.h"
#include "exec/olap_utils.h"
#include "olap/olap_common.h"
#include "olap/olap_tuple.h"
#include "runtime/define_primitive_type.h"
#include "runtime/primitive_type.h"
#include "runtime/type_limit.h"
#include "vec/core/types.h"
#include "vec/io/io_helper.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {

template <PrimitiveType primitive_type, class T>
std::string cast_to_string(T value, int scale) {
    if constexpr (primitive_type == TYPE_DECIMAL32) {
        return ((vectorized::Decimal<int32_t>)value).to_string(scale);
    } else if constexpr (primitive_type == TYPE_DECIMAL64) {
        return ((vectorized::Decimal<int64_t>)value).to_string(scale);
    } else if constexpr (primitive_type == TYPE_DECIMAL128I) {
        return ((vectorized::Decimal<int128_t>)value).to_string(scale);
    } else if constexpr (primitive_type == TYPE_TINYINT) {
        return std::to_string(static_cast<int>(value));
    } else if constexpr (primitive_type == TYPE_LARGEINT) {
        return vectorized::int128_to_string(value);
    } else if constexpr (primitive_type == TYPE_DATETIMEV2) {
        DateV2Value<DateTimeV2ValueType> datetimev2_val =
                static_cast<DateV2Value<DateTimeV2ValueType>>(value);
        char buf[30];
        datetimev2_val.to_string(buf);
        std::stringstream ss;
        ss << buf;
        return ss.str();
    } else {
        return boost::lexical_cast<std::string>(value);
    }
}

/**
 * @brief Column's value range
 **/
template <PrimitiveType primitive_type>
class ColumnValueRange {
public:
    using CppType = typename PrimitiveTypeTraits<primitive_type>::CppType;
    using IteratorType = typename std::set<CppType>::iterator;

    ColumnValueRange();

    ColumnValueRange(std::string col_name);

    ColumnValueRange(std::string col_name, const CppType& min, const CppType& max,
                     bool contain_null);

    ColumnValueRange(std::string col_name, int precision, int scale);

    ColumnValueRange(std::string col_name, bool is_nullable_col, int precision, int scale);

    ColumnValueRange(std::string col_name, const CppType& min, const CppType& max,
                     bool is_nullable_col, bool contain_null, int precision, int scale);

    // should add fixed value before add range
    Status add_fixed_value(const CppType& value);

    // should remove fixed value after add fixed value
    void remove_fixed_value(const CppType& value);

    Status add_range(SQLFilterOp op, CppType value);

    Status add_compound_value(SQLFilterOp op, CppType value);

    bool is_in_compound_value_range() const;

    Status add_match_value(MatchType match_type, const CppType& value);

    bool is_fixed_value_range() const;

    bool is_match_value_range() const;

    bool is_scope_value_range() const;

    bool is_empty_value_range() const;

    bool is_fixed_value_convertible() const;

    bool is_range_value_convertible() const;

    size_t get_convertible_fixed_value_size() const;

    void convert_to_fixed_value();

    void convert_to_range_value();

    bool convert_to_avg_range_value(std::vector<OlapTuple>& begin_scan_keys,
                                    std::vector<OlapTuple>& end_scan_keys, bool& begin_include,
                                    bool& end_include, int32_t max_scan_key_num);

    bool convert_to_close_range(std::vector<OlapTuple>& begin_scan_keys,
                                std::vector<OlapTuple>& end_scan_keys, bool& begin_include,
                                bool& end_include);

    constexpr bool is_reject_split_type() const { return _is_reject_split_type; }

    bool has_intersection(ColumnValueRange<primitive_type>& range);

    void intersection(ColumnValueRange<primitive_type>& range);

    void set_empty_value_range() {
        _fixed_values.clear();
        _low_value = TYPE_MAX;
        _high_value = TYPE_MIN;
        _contain_null = false;
    }

    const std::set<CppType>& get_fixed_value_set() const { return _fixed_values; }

    CppType get_range_max_value() const { return _high_value; }

    CppType get_range_min_value() const { return _low_value; }

    const CppType* get_range_max_value_ptr() const { return &_high_value; }

    const CppType* get_range_min_value_ptr() const { return &_low_value; }

    SQLFilterOp get_range_high_op() const { return _high_op; }

    SQLFilterOp get_range_low_op() const { return _low_op; }

    bool is_low_value_mininum() const { return _low_value == TYPE_MIN; }

    bool is_low_value_maximum() const { return _low_value == TYPE_MAX; }

    bool is_high_value_maximum() const { return _high_value == TYPE_MAX; }

    bool is_high_value_mininum() const { return _high_value == TYPE_MIN; }

    bool is_begin_include() const { return _low_op == FILTER_LARGER_OR_EQUAL; }

    bool is_end_include() const { return _high_op == FILTER_LESS_OR_EQUAL; }

    PrimitiveType type() const { return _column_type; }

    const std::string& column_name() const { return _column_name; }

    bool is_nullable_col() const { return _is_nullable_col; }

    bool contain_null() const { return _contain_null; }

    size_t get_fixed_value_size() const { return _fixed_values.size(); }

    void to_olap_filter(std::vector<TCondition>& filters) {
        if (is_fixed_value_range() || is_match_value_range()) {
            // 1. convert to in filter condition
            if (is_fixed_value_range()) {
                to_in_condition(filters, true);
            }

            if (is_match_value_range()) {
                to_match_condition(filters);
            }
        } else if (_low_value < _high_value) {
            // 2. convert to min max filter condition
            TCondition null_pred;
            if (TYPE_MAX == _high_value && _high_op == FILTER_LESS_OR_EQUAL &&
                TYPE_MIN == _low_value && _low_op == FILTER_LARGER_OR_EQUAL && _is_nullable_col &&
                !contain_null()) {
                null_pred.__set_column_name(_column_name);
                null_pred.__set_condition_op("is");
                null_pred.condition_values.emplace_back("not null");
            }

            if (null_pred.condition_values.size() != 0) {
                filters.push_back(std::move(null_pred));
                return;
            }

            TCondition low;
            if (TYPE_MIN != _low_value || FILTER_LARGER_OR_EQUAL != _low_op) {
                low.__set_column_name(_column_name);
                low.__set_condition_op((_low_op == FILTER_LARGER_OR_EQUAL ? ">=" : ">>"));
                low.__set_marked_by_runtime_filter(_marked_runtime_filter_predicate);
                low.condition_values.push_back(
                        cast_to_string<primitive_type, CppType>(_low_value, _scale));
            }

            if (low.condition_values.size() != 0) {
                filters.push_back(std::move(low));
            }

            TCondition high;
            if (TYPE_MAX != _high_value || FILTER_LESS_OR_EQUAL != _high_op) {
                high.__set_column_name(_column_name);
                high.__set_condition_op((_high_op == FILTER_LESS_OR_EQUAL ? "<=" : "<<"));
                high.__set_marked_by_runtime_filter(_marked_runtime_filter_predicate);
                high.condition_values.push_back(
                        cast_to_string<primitive_type, CppType>(_high_value, _scale));
            }

            if (high.condition_values.size() != 0) {
                filters.push_back(std::move(high));
            }
        } else {
            // 3. convert to is null and is not null filter condition
            TCondition null_pred;
            if (TYPE_MAX == _low_value && TYPE_MIN == _high_value && _is_nullable_col &&
                contain_null()) {
                null_pred.__set_column_name(_column_name);
                null_pred.__set_condition_op("is");
                null_pred.condition_values.emplace_back("null");
            }

            if (null_pred.condition_values.size() != 0) {
                filters.push_back(std::move(null_pred));
            }
        }
    }

    void to_in_condition(std::vector<TCondition>& filters, bool is_in = true) {
        TCondition condition;
        condition.__set_column_name(_column_name);
        condition.__set_condition_op(is_in ? "*=" : "!*=");
        condition.__set_marked_by_runtime_filter(_marked_runtime_filter_predicate);

        for (const auto& value : _fixed_values) {
            condition.condition_values.push_back(
                    cast_to_string<primitive_type, CppType>(value, _scale));
        }

        if (condition.condition_values.size() != 0) {
            filters.push_back(std::move(condition));
        }
    }

    void to_condition_in_compound(std::vector<TCondition>& filters) {
        for (const auto& value : _compound_values) {
            TCondition condition;
            condition.__set_column_name(_column_name);
            if (value.first == FILTER_LARGER) {
                condition.__set_condition_op(">>");
            } else if (value.first == FILTER_LARGER_OR_EQUAL) {
                condition.__set_condition_op(">=");
            } else if (value.first == FILTER_LESS) {
                condition.__set_condition_op("<<");
            } else if (value.first == FILTER_LESS_OR_EQUAL) {
                condition.__set_condition_op("<=");
            } else if (value.first == FILTER_IN) {
                condition.__set_condition_op("*=");
            } else if (value.first == FILTER_NOT_IN) {
                condition.__set_condition_op("!*=");
            }
            condition.condition_values.push_back(
                    cast_to_string<primitive_type, CppType>(value.second, _scale));
            if (condition.condition_values.size() != 0) {
                filters.push_back(std::move(condition));
            }
        }
    }

    void to_match_condition(std::vector<TCondition>& filters) {
        for (const auto& value : _match_values) {
            TCondition condition;
            condition.__set_column_name(_column_name);

            if (value.first == MatchType::MATCH_ANY) {
                condition.__set_condition_op("match_any");
            } else if (value.first == MatchType::MATCH_ALL) {
                condition.__set_condition_op("match_all");
            } else if (value.first == MatchType::MATCH_PHRASE) {
                condition.__set_condition_op("match_phrase");
            } else if (value.first == MatchType::MATCH_ELEMENT_EQ) {
                condition.__set_condition_op("match_element_eq");
            } else if (value.first == MatchType::MATCH_ELEMENT_LT) {
                condition.__set_condition_op("match_element_lt");
            } else if (value.first == MatchType::MATCH_ELEMENT_GT) {
                condition.__set_condition_op("match_element_gt");
            } else if (value.first == MatchType::MATCH_ELEMENT_LE) {
                condition.__set_condition_op("match_element_le");
            } else if (value.first == MatchType::MATCH_ELEMENT_GE) {
                condition.__set_condition_op("match_element_ge");
            }
            condition.condition_values.push_back(
                    cast_to_string<primitive_type, CppType>(value.second, _scale));
            if (condition.condition_values.size() != 0) {
                filters.push_back(std::move(condition));
            }
        }
    }

    void set_whole_value_range() {
        _fixed_values.clear();
        _low_value = TYPE_MIN;
        _high_value = TYPE_MAX;
        _low_op = FILTER_LARGER_OR_EQUAL;
        _high_op = FILTER_LESS_OR_EQUAL;
        _contain_null = _is_nullable_col;
    }

    bool is_whole_value_range() const {
        DCHECK(_is_nullable_col || !contain_null())
                << "Non-nullable column cannot contains null value";

        return _fixed_values.empty() && _low_value == TYPE_MIN && _high_value == TYPE_MAX &&
               _low_op == FILTER_LARGER_OR_EQUAL && _high_op == FILTER_LESS_OR_EQUAL &&
               _is_nullable_col == contain_null();
    }

    // only two case will set range contain null, call by temp_range in olap scan node
    // 'is null' and 'is not null'
    // 1. if the pred is 'is null' means the range should be
    // empty in fixed_range and _high_value < _low_value
    // 2. if the pred is 'is not null' means the range should be whole range and
    // 'is not null' be effective
    void set_contain_null(bool contain_null) {
        if (contain_null) {
            set_empty_value_range();
        } else {
            set_whole_value_range();
        }
        _contain_null = _is_nullable_col && contain_null;
    }

    void mark_runtime_filter_predicate(bool is_runtime_filter_predicate) {
        _marked_runtime_filter_predicate = is_runtime_filter_predicate;
    }

    bool get_marked_by_runtime_filter() const { return _marked_runtime_filter_predicate; }

    int precision() const { return _precision; }

    int scale() const { return _scale; }

    static void add_fixed_value_range(ColumnValueRange<primitive_type>& range, CppType* value) {
        static_cast<void>(range.add_fixed_value(*value));
    }

    static void remove_fixed_value_range(ColumnValueRange<primitive_type>& range, CppType* value) {
        range.remove_fixed_value(*value);
    }

    static void add_value_range(ColumnValueRange<primitive_type>& range, SQLFilterOp op,
                                CppType* value) {
        static_cast<void>(range.add_range(op, *value));
    }

    static void add_compound_value_range(ColumnValueRange<primitive_type>& range, SQLFilterOp op,
                                         CppType* value) {
        static_cast<void>(range.add_compound_value(op, *value));
    }

    static void add_match_value_range(ColumnValueRange<primitive_type>& range, MatchType match_type,
                                      CppType* match_value) {
        static_cast<void>(range.add_match_value(match_type, *match_value));
    }

    static ColumnValueRange<primitive_type> create_empty_column_value_range(bool is_nullable_col,
                                                                            int precision,
                                                                            int scale) {
        return ColumnValueRange<primitive_type>::create_empty_column_value_range(
                "", is_nullable_col, precision, scale);
    }

    static ColumnValueRange<primitive_type> create_empty_column_value_range(
            const std::string& col_name, bool is_nullable_col, int precision, int scale) {
        return ColumnValueRange<primitive_type>(col_name, TYPE_MAX, TYPE_MIN, is_nullable_col,
                                                false, precision, scale);
    }

protected:
    bool is_in_range(const CppType& value);

private:
    const static CppType TYPE_MIN; // Column type's min value
    const static CppType TYPE_MAX; // Column type's max value

    std::string _column_name;
    PrimitiveType _column_type; // Column type (eg: TINYINT,SMALLINT,INT,BIGINT)
    CppType _low_value;         // Column's low value, closed interval at left
    CppType _high_value;        // Column's high value, open interval at right
    SQLFilterOp _low_op;
    SQLFilterOp _high_op;
    std::set<CppType> _fixed_values;                       // Column's fixed int value
    std::set<std::pair<MatchType, CppType>> _match_values; // match value using in full-text search

    bool _is_nullable_col;
    bool _contain_null;
    int _precision;
    int _scale;

    static constexpr bool _is_reject_split_type = primitive_type == PrimitiveType::TYPE_LARGEINT ||
                                                  primitive_type == PrimitiveType::TYPE_DECIMALV2 ||
                                                  primitive_type == PrimitiveType::TYPE_HLL ||
                                                  primitive_type == PrimitiveType::TYPE_VARCHAR ||
                                                  primitive_type == PrimitiveType::TYPE_CHAR ||
                                                  primitive_type == PrimitiveType::TYPE_STRING ||
                                                  primitive_type == PrimitiveType::TYPE_BOOLEAN ||
                                                  primitive_type == PrimitiveType::TYPE_DATETIME ||
                                                  primitive_type == PrimitiveType::TYPE_DATETIMEV2;

    // range value except leaf node of and node in compound expr tree
    std::set<std::pair<SQLFilterOp, CppType>> _compound_values;
    bool _marked_runtime_filter_predicate = false;
};

class OlapScanKeys {
public:
    OlapScanKeys()
            : _has_range_value(false),
              _begin_include(true),
              _end_include(true),
              _is_convertible(true) {}

    template <PrimitiveType primitive_type>
    Status extend_scan_key(ColumnValueRange<primitive_type>& range, int32_t max_scan_key_num,
                           bool* exact_value, bool* eos);

    Status get_key_range(std::vector<std::unique_ptr<OlapScanRange>>* key_range);

    bool has_range_value() const { return _has_range_value; }

    void clear() {
        _has_range_value = false;
        _begin_scan_keys.clear();
        _end_scan_keys.clear();
    }

    std::string debug_string() {
        std::stringstream ss;
        DCHECK(_begin_scan_keys.size() == _end_scan_keys.size());
        ss << "ScanKeys:";

        for (int i = 0; i < _begin_scan_keys.size(); ++i) {
            ss << "ScanKey=" << (_begin_include ? "[" : "(") << _begin_scan_keys[i] << " : "
               << _end_scan_keys[i] << (_end_include ? "]" : ")");
        }
        return ss.str();
    }

    size_t size() {
        DCHECK(_begin_scan_keys.size() == _end_scan_keys.size());
        return _begin_scan_keys.size();
    }

    void set_begin_include(bool begin_include) { _begin_include = begin_include; }

    bool begin_include() const { return _begin_include; }

    void set_end_include(bool end_include) { _end_include = end_include; }

    bool end_include() const { return _end_include; }

    void set_is_convertible(bool is_convertible) { _is_convertible = is_convertible; }

    // now, only use in UT
    static std::string to_print_key(const OlapTuple& scan_keys) {
        std::stringstream sstream;
        sstream << scan_keys;
        return sstream.str();
    }

private:
    std::vector<OlapTuple> _begin_scan_keys;
    std::vector<OlapTuple> _end_scan_keys;
    bool _has_range_value;
    bool _begin_include;
    bool _end_include;
    bool _is_convertible;
};

using ColumnValueRangeType =
        std::variant<ColumnValueRange<TYPE_TINYINT>, ColumnValueRange<TYPE_SMALLINT>,
                     ColumnValueRange<TYPE_INT>, ColumnValueRange<TYPE_BIGINT>,
                     ColumnValueRange<TYPE_LARGEINT>, ColumnValueRange<TYPE_CHAR>,
                     ColumnValueRange<TYPE_VARCHAR>, ColumnValueRange<TYPE_STRING>,
                     ColumnValueRange<TYPE_DATE>, ColumnValueRange<TYPE_DATEV2>,
                     ColumnValueRange<TYPE_DATETIME>, ColumnValueRange<TYPE_DATETIMEV2>,
                     ColumnValueRange<TYPE_DECIMALV2>, ColumnValueRange<TYPE_BOOLEAN>,
                     ColumnValueRange<TYPE_HLL>, ColumnValueRange<TYPE_DECIMAL32>,
                     ColumnValueRange<TYPE_DECIMAL64>, ColumnValueRange<TYPE_DECIMAL128I>>;

template <PrimitiveType primitive_type>
const typename ColumnValueRange<primitive_type>::CppType
        ColumnValueRange<primitive_type>::TYPE_MIN =
                type_limit<typename ColumnValueRange<primitive_type>::CppType>::min();
template <PrimitiveType primitive_type>
const typename ColumnValueRange<primitive_type>::CppType
        ColumnValueRange<primitive_type>::TYPE_MAX =
                type_limit<typename ColumnValueRange<primitive_type>::CppType>::max();

template <PrimitiveType primitive_type>
ColumnValueRange<primitive_type>::ColumnValueRange()
        : _column_type(INVALID_TYPE), _precision(-1), _scale(-1) {}

template <PrimitiveType primitive_type>
ColumnValueRange<primitive_type>::ColumnValueRange(std::string col_name)
        : ColumnValueRange(std::move(col_name), TYPE_MIN, TYPE_MAX, true) {}

template <PrimitiveType primitive_type>
ColumnValueRange<primitive_type>::ColumnValueRange(std::string col_name, const CppType& min,
                                                   const CppType& max, bool contain_null)
        : _column_name(std::move(col_name)),
          _column_type(primitive_type),
          _low_value(min),
          _high_value(max),
          _low_op(FILTER_LARGER_OR_EQUAL),
          _high_op(FILTER_LESS_OR_EQUAL),
          _is_nullable_col(true),
          _contain_null(contain_null),
          _precision(-1),
          _scale(-1) {}

template <PrimitiveType primitive_type>
ColumnValueRange<primitive_type>::ColumnValueRange(std::string col_name, const CppType& min,
                                                   const CppType& max, bool is_nullable_col,
                                                   bool contain_null, int precision, int scale)
        : _column_name(std::move(col_name)),
          _column_type(primitive_type),
          _low_value(min),
          _high_value(max),
          _low_op(FILTER_LARGER_OR_EQUAL),
          _high_op(FILTER_LESS_OR_EQUAL),
          _is_nullable_col(is_nullable_col),
          _contain_null(is_nullable_col && contain_null),
          _precision(precision),
          _scale(scale) {}

template <PrimitiveType primitive_type>
ColumnValueRange<primitive_type>::ColumnValueRange(std::string col_name, int precision, int scale)
        : ColumnValueRange(std::move(col_name), TYPE_MIN, TYPE_MAX, true, true, precision, scale) {}

template <PrimitiveType primitive_type>
ColumnValueRange<primitive_type>::ColumnValueRange(std::string col_name, bool is_nullable_col,
                                                   int precision, int scale)
        : ColumnValueRange(std::move(col_name), TYPE_MIN, TYPE_MAX, is_nullable_col,
                           is_nullable_col, precision, scale) {}

template <PrimitiveType primitive_type>
Status ColumnValueRange<primitive_type>::add_fixed_value(const CppType& value) {
    if (INVALID_TYPE == _column_type) {
        return Status::InternalError("AddFixedValue failed, Invalid type");
    }

    _fixed_values.insert(value);
    _contain_null = false;

    _high_value = TYPE_MIN;
    _low_value = TYPE_MAX;

    return Status::OK();
}

template <PrimitiveType primitive_type>
Status ColumnValueRange<primitive_type>::add_compound_value(SQLFilterOp op, CppType value) {
    std::pair<SQLFilterOp, CppType> val_with_op(op, value);
    _compound_values.insert(val_with_op);
    _contain_null = false;

    _high_value = TYPE_MIN;
    _low_value = TYPE_MAX;
    return Status::OK();
}

template <PrimitiveType primitive_type>
Status ColumnValueRange<primitive_type>::add_match_value(MatchType match_type,
                                                         const CppType& value) {
    std::pair<MatchType, CppType> match_value(match_type, value);
    _match_values.insert(match_value);
    _contain_null = false;

    // _high_value = TYPE_MIN;
    // _low_value = TYPE_MAX;
    return Status::OK();
}

template <PrimitiveType primitive_type>
void ColumnValueRange<primitive_type>::remove_fixed_value(const CppType& value) {
    _fixed_values.erase(value);
}

template <PrimitiveType primitive_type>
bool ColumnValueRange<primitive_type>::is_fixed_value_range() const {
    return _fixed_values.size() != 0;
}

template <PrimitiveType primitive_type>
bool ColumnValueRange<primitive_type>::is_in_compound_value_range() const {
    return _compound_values.size() != 0;
}

template <PrimitiveType primitive_type>
bool ColumnValueRange<primitive_type>::is_match_value_range() const {
    return _match_values.size() != 0;
}

template <PrimitiveType primitive_type>
bool ColumnValueRange<primitive_type>::is_scope_value_range() const {
    return _high_value > _low_value;
}

template <PrimitiveType primitive_type>
bool ColumnValueRange<primitive_type>::is_empty_value_range() const {
    if (INVALID_TYPE == _column_type) {
        return true;
    }

    return (!is_fixed_value_range() && !is_scope_value_range() && !contain_null() &&
            !is_match_value_range());
}

template <PrimitiveType primitive_type>
bool ColumnValueRange<primitive_type>::is_fixed_value_convertible() const {
    if (is_fixed_value_range()) {
        return false;
    }

    if (!is_enumeration_type(_column_type)) {
        return false;
    }

    return true;
}

template <PrimitiveType primitive_type>
bool ColumnValueRange<primitive_type>::is_range_value_convertible() const {
    if (!is_fixed_value_range()) {
        return false;
    }

    if (TYPE_NULL == _column_type || TYPE_BOOLEAN == _column_type) {
        return false;
    }

    return true;
}

template <PrimitiveType primitive_type>
size_t ColumnValueRange<primitive_type>::get_convertible_fixed_value_size() const {
    if (!is_fixed_value_convertible()) {
        return 0;
    }

    return _high_value - _low_value;
}

// The return value indicates whether eos.
template <PrimitiveType primitive_type>
bool ColumnValueRange<primitive_type>::convert_to_close_range(
        std::vector<OlapTuple>& begin_scan_keys, std::vector<OlapTuple>& end_scan_keys,
        bool& begin_include, bool& end_include) {
    if constexpr (!_is_reject_split_type) {
        begin_include = true;
        end_include = true;

        bool is_empty = false;

        if (!is_begin_include()) {
            if (_low_value == TYPE_MIN) {
                is_empty = true;
            } else {
                ++_low_value;
            }
        }
        if (!is_end_include()) {
            if (_high_value == TYPE_MAX) {
                is_empty = true;
            } else {
                --_high_value;
            }
        }

        if (_high_value < _low_value) {
            is_empty = true;
        }

        if (is_empty && !contain_null()) {
            begin_scan_keys.clear();
            end_scan_keys.clear();
            return true;
        }
    }
    return false;
}

// The return value indicates whether the split result is range or fixed value.
template <PrimitiveType primitive_type>
bool ColumnValueRange<primitive_type>::convert_to_avg_range_value(
        std::vector<OlapTuple>& begin_scan_keys, std::vector<OlapTuple>& end_scan_keys,
        bool& begin_include, bool& end_include, int32_t max_scan_key_num) {
    if constexpr (!_is_reject_split_type) {
        auto no_split = [&]() -> bool {
            begin_scan_keys.emplace_back();
            begin_scan_keys.back().add_value(
                    cast_to_string<primitive_type, CppType>(get_range_min_value(), scale()),
                    contain_null());
            end_scan_keys.emplace_back();
            end_scan_keys.back().add_value(
                    cast_to_string<primitive_type, CppType>(get_range_max_value(), scale()));
            return true;
        };

        CppType min_value = get_range_min_value();
        CppType max_value = get_range_max_value();
        if constexpr (primitive_type == PrimitiveType::TYPE_DATE) {
            min_value.set_type(TimeType::TIME_DATE);
            max_value.set_type(TimeType::TIME_DATE);
        }

        if (min_value > max_value || max_scan_key_num == 1) {
            return no_split();
        }

        auto cast = [](const CppType& value) {
            if constexpr (primitive_type == PrimitiveType::TYPE_DATE ||
                          primitive_type == PrimitiveType::TYPE_DATEV2) {
                return value;
            } else {
                return (int128_t)value;
            }
        };

        // When CppType is date, we can not convert it to integer number and calculate distance.
        // In other case, we convert element to int128 to avoit overflow.
        size_t step_size = (cast(max_value) - min_value) / max_scan_key_num;

        constexpr size_t MAX_STEP_SIZE = 1 << 20;
        // When the step size is too large, the range is easy to not really contain data.
        if (step_size > MAX_STEP_SIZE) {
            return no_split();
        }

        // Add null key if contain null, must do after no_split check
        if (contain_null()) {
            begin_scan_keys.emplace_back();
            begin_scan_keys.back().add_null();
            end_scan_keys.emplace_back();
            end_scan_keys.back().add_null();
        }
        while (true) {
            begin_scan_keys.emplace_back();
            begin_scan_keys.back().add_value(
                    cast_to_string<primitive_type, CppType>(min_value, scale()));

            if (cast(max_value) - min_value < step_size) {
                min_value = max_value;
            } else {
                min_value += step_size;
            }

            end_scan_keys.emplace_back();
            end_scan_keys.back().add_value(
                    cast_to_string<primitive_type, CppType>(min_value, scale()));

            if (min_value == max_value) {
                break;
            }
            ++min_value;
        }

        return step_size != 0;
    }
    return false;
}

template <PrimitiveType primitive_type>
void ColumnValueRange<primitive_type>::convert_to_range_value() {
    if (!is_range_value_convertible()) {
        return;
    }

    if (!_fixed_values.empty()) {
        _low_value = *_fixed_values.begin();
        _low_op = FILTER_LARGER_OR_EQUAL;
        _high_value = *_fixed_values.rbegin();
        _high_op = FILTER_LESS_OR_EQUAL;
        _fixed_values.clear();
    }
}

template <PrimitiveType primitive_type>
Status ColumnValueRange<primitive_type>::add_range(SQLFilterOp op, CppType value) {
    if (INVALID_TYPE == _column_type) {
        return Status::InternalError("AddRange failed, Invalid type");
    }

    // add range means range should not contain null
    _contain_null = false;

    if (is_fixed_value_range()) {
        std::pair<IteratorType, IteratorType> bound_pair = _fixed_values.equal_range(value);

        switch (op) {
        case FILTER_LARGER: {
            _fixed_values.erase(_fixed_values.begin(), bound_pair.second);
            break;
        }

        case FILTER_LARGER_OR_EQUAL: {
            _fixed_values.erase(_fixed_values.begin(), bound_pair.first);
            break;
        }

        case FILTER_LESS: {
            if (bound_pair.first == _fixed_values.find(value)) {
                _fixed_values.erase(bound_pair.first, _fixed_values.end());
            } else {
                _fixed_values.erase(bound_pair.second, _fixed_values.end());
            }

            break;
        }

        case FILTER_LESS_OR_EQUAL: {
            _fixed_values.erase(bound_pair.second, _fixed_values.end());
            break;
        }

        default: {
            return Status::InternalError("Add Range fail! Unsupported SQLFilterOp.");
        }
        }

        _high_value = TYPE_MIN;
        _low_value = TYPE_MAX;
    } else {
        if (_high_value > _low_value) {
            switch (op) {
            case FILTER_LARGER: {
                if (value >= _low_value) {
                    _low_value = value;
                    _low_op = op;
                }

                break;
            }

            case FILTER_LARGER_OR_EQUAL: {
                if (value > _low_value) {
                    _low_value = value;
                    _low_op = op;
                }

                break;
            }

            case FILTER_LESS: {
                if (value <= _high_value) {
                    _high_value = value;
                    _high_op = op;
                }

                break;
            }

            case FILTER_LESS_OR_EQUAL: {
                if (value < _high_value) {
                    _high_value = value;
                    _high_op = op;
                }

                break;
            }

            default: {
                return Status::InternalError("Add Range fail! Unsupported SQLFilterOp.");
            }
            }
        }

        if (FILTER_LARGER_OR_EQUAL == _low_op && FILTER_LESS_OR_EQUAL == _high_op &&
            _high_value == _low_value) {
            static_cast<void>(add_fixed_value(_high_value));
            _high_value = TYPE_MIN;
            _low_value = TYPE_MAX;
        }
    }

    return Status::OK();
}

template <PrimitiveType primitive_type>
bool ColumnValueRange<primitive_type>::is_in_range(const CppType& value) {
    switch (_high_op) {
    case FILTER_LESS: {
        switch (_low_op) {
        case FILTER_LARGER: {
            return value < _high_value && value > _low_value;
        }

        case FILTER_LARGER_OR_EQUAL: {
            return value < _high_value && value >= _low_value;
        }

        default: {
            DCHECK(false);
        }
        }

        break;
    }

    case FILTER_LESS_OR_EQUAL: {
        switch (_low_op) {
        case FILTER_LARGER: {
            return value <= _high_value && value > _low_value;
        }

        case FILTER_LARGER_OR_EQUAL: {
            return value <= _high_value && value >= _low_value;
        }

        default: {
            DCHECK(false);
        }
        }
    }

    default: {
        DCHECK(false);
    }
    }

    return false;
}

template <PrimitiveType primitive_type>
void ColumnValueRange<primitive_type>::intersection(ColumnValueRange<primitive_type>& range) {
    // 1. clear if column type not match
    if (_column_type != range._column_type) {
        set_empty_value_range();
    }

    // 2. clear if any range is empty
    if (is_empty_value_range() || range.is_empty_value_range()) {
        set_empty_value_range();
    }

    std::set<CppType> result_values;
    // 3. fixed_value intersection, fixed value range do not contain null
    if (is_fixed_value_range() || range.is_fixed_value_range() || range.is_match_value_range()) {
        if (is_fixed_value_range() && range.is_fixed_value_range()) {
            set_intersection(_fixed_values.begin(), _fixed_values.end(),
                             range._fixed_values.begin(), range._fixed_values.end(),
                             std::inserter(result_values, result_values.begin()));
        } else if (is_fixed_value_range() && !range.is_fixed_value_range()) {
            IteratorType iter = _fixed_values.begin();

            while (iter != _fixed_values.end()) {
                if (range.is_in_range(*iter)) {
                    result_values.insert(*iter);
                }
                ++iter;
            }
        } else if (!is_fixed_value_range() && range.is_fixed_value_range()) {
            IteratorType iter = range._fixed_values.begin();
            while (iter != range._fixed_values.end()) {
                if (this->is_in_range(*iter)) {
                    result_values.insert(*iter);
                }
                ++iter;
            }
        }

        if (!result_values.empty()) {
            _fixed_values = std::move(result_values);
            _contain_null = false;
            _high_value = TYPE_MIN;
            _low_value = TYPE_MAX;
        } else if (range.is_match_value_range()) {
            for (auto& value : range._match_values) {
                static_cast<void>(add_match_value(value.first, value.second));
            }
        } else {
            set_empty_value_range();
        }
    } else {
        if (contain_null() && range.contain_null()) {
            // if both is_whole_range to keep the same, else set_contain_null
            if (!is_whole_value_range() || !range.is_whole_value_range()) {
                set_contain_null(true);
            }
        } else {
            static_cast<void>(add_range(range._high_op, range._high_value));
            static_cast<void>(add_range(range._low_op, range._low_value));
        }
    }
}

template <PrimitiveType primitive_type>
bool ColumnValueRange<primitive_type>::has_intersection(ColumnValueRange<primitive_type>& range) {
    // 1. return false if column type not match
    if (_column_type != range._column_type) {
        return false;
    }

    // 2. return false if any range is empty
    if (is_empty_value_range() || range.is_empty_value_range()) {
        return false;
    }

    // 3.1 return false if two int fixedRange has no intersection
    if (is_fixed_value_range() && range.is_fixed_value_range()) {
        std::set<CppType> result_values;
        set_intersection(_fixed_values.begin(), _fixed_values.end(), range._fixed_values.begin(),
                         range._fixed_values.end(),
                         std::inserter(result_values, result_values.begin()));

        if (result_values.size() != 0) {
            return true;
        } else {
            return false;
        }
    } // 3.2
    else if (is_fixed_value_range() && !range.is_fixed_value_range()) {
        IteratorType iter = _fixed_values.begin();

        while (iter != _fixed_values.end()) {
            if (range.is_in_range(*iter)) {
                return true;
            }

            ++iter;
        }

        return false;
    } else if (!is_fixed_value_range() && range.is_fixed_value_range()) {
        IteratorType iter = range._fixed_values.begin();

        while (iter != range._fixed_values.end()) {
            if (this->is_in_range(*iter)) {
                return true;
            }

            ++iter;
        }

        return false;
    } else {
        if (_low_value > range._high_value || range._low_value > _high_value) {
            return false;
        } else if (_low_value == range._high_value) {
            if (FILTER_LARGER_OR_EQUAL == _low_op && FILTER_LESS_OR_EQUAL == range._high_op) {
                return true;
            } else {
                return false;
            }
        } else if (range._low_value == _high_value) {
            if (FILTER_LARGER_OR_EQUAL == range._low_op && FILTER_LESS_OR_EQUAL == _high_op) {
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }
}

template <PrimitiveType primitive_type>
Status OlapScanKeys::extend_scan_key(ColumnValueRange<primitive_type>& range,
                                     int32_t max_scan_key_num, bool* exact_value, bool* eos) {
    using CppType = typename PrimitiveTypeTraits<primitive_type>::CppType;
    using ConstIterator = typename std::set<CppType>::const_iterator;

    // 1. clear ScanKey if some column range is empty
    if (range.is_empty_value_range()) {
        _begin_scan_keys.clear();
        _end_scan_keys.clear();
        return Status::OK();
    }

    // 2. stop extend ScanKey when it's already extend a range value
    if (_has_range_value) {
        return Status::OK();
    }

    //if a column doesn't have any predicate, we will try converting the range to fixed values
    auto scan_keys_size = _begin_scan_keys.empty() ? 1 : _begin_scan_keys.size();
    if (range.is_fixed_value_range()) {
        if (range.get_fixed_value_size() > max_scan_key_num / scan_keys_size) {
            if (range.is_range_value_convertible()) {
                range.convert_to_range_value();
                *exact_value = false;
            } else {
                return Status::OK();
            }
        }
    } else {
        if (_begin_scan_keys.empty() && range.is_fixed_value_convertible() && _is_convertible &&
            !range.is_reject_split_type()) {
            *eos |= range.convert_to_close_range(_begin_scan_keys, _end_scan_keys, _begin_include,
                                                 _end_include);

            if (range.convert_to_avg_range_value(_begin_scan_keys, _end_scan_keys, _begin_include,
                                                 _end_include, max_scan_key_num)) {
                _has_range_value = true;
            }
            return Status::OK();
        }
    }

    // extend ScanKey with MatchValueRange
    if (range.is_match_value_range() && _begin_scan_keys.empty()) {
        _begin_scan_keys.emplace_back();
        _begin_scan_keys.back().add_value(
                cast_to_string<primitive_type, CppType>(type_limit<CppType>::min(), 0));
        _end_scan_keys.emplace_back();
        _end_scan_keys.back().add_value(
                cast_to_string<primitive_type, CppType>(type_limit<CppType>::max(), 0));
        _begin_include = true;
        _end_include = true;
        *exact_value = false;
        // not empty, do nothing
        return Status::OK();
    }

    // 3.1 extend ScanKey with FixedValueRange
    if (range.is_fixed_value_range()) {
        // 3.1.1 construct num of fixed value ScanKey (begin_key == end_key)
        if (_begin_scan_keys.empty()) {
            auto fixed_value_set = range.get_fixed_value_set();
            ConstIterator iter = fixed_value_set.begin();

            for (; iter != fixed_value_set.end(); ++iter) {
                _begin_scan_keys.emplace_back();
                _begin_scan_keys.back().add_value(
                        cast_to_string<primitive_type, CppType>(*iter, range.scale()));
                _end_scan_keys.emplace_back();
                _end_scan_keys.back().add_value(
                        cast_to_string<primitive_type, CppType>(*iter, range.scale()));
            }

            if (range.contain_null()) {
                _begin_scan_keys.emplace_back();
                _begin_scan_keys.back().add_null();
                _end_scan_keys.emplace_back();
                _end_scan_keys.back().add_null();
            }
        } // 3.1.2 produces the Cartesian product of ScanKey and fixed_value
        else {
            auto fixed_value_set = range.get_fixed_value_set();
            int original_key_range_size = _begin_scan_keys.size();

            for (int i = 0; i < original_key_range_size; ++i) {
                OlapTuple start_base_key_range = _begin_scan_keys[i];
                OlapTuple end_base_key_range = _end_scan_keys[i];

                ConstIterator iter = fixed_value_set.begin();

                for (; iter != fixed_value_set.end(); ++iter) {
                    // alter the first ScanKey in original place
                    if (iter == fixed_value_set.begin()) {
                        _begin_scan_keys[i].add_value(
                                cast_to_string<primitive_type, CppType>(*iter, range.scale()));
                        _end_scan_keys[i].add_value(
                                cast_to_string<primitive_type, CppType>(*iter, range.scale()));
                    } // append follow ScanKey
                    else {
                        _begin_scan_keys.push_back(start_base_key_range);
                        _begin_scan_keys.back().add_value(
                                cast_to_string<primitive_type, CppType>(*iter, range.scale()));
                        _end_scan_keys.push_back(end_base_key_range);
                        _end_scan_keys.back().add_value(
                                cast_to_string<primitive_type, CppType>(*iter, range.scale()));
                    }
                }

                if (range.contain_null()) {
                    _begin_scan_keys.push_back(start_base_key_range);
                    _begin_scan_keys.back().add_null();
                    _end_scan_keys.push_back(end_base_key_range);
                    _end_scan_keys.back().add_null();
                }
            }
        }

        _begin_include = true;
        _end_include = true;
    } // Extend ScanKey with range value
    else {
        _has_range_value = true;

        /// if max < min, this range should only contains a null value.
        if (range.get_range_max_value() < range.get_range_min_value()) {
            CHECK(range.contain_null());
            if (_begin_scan_keys.empty()) {
                _begin_scan_keys.emplace_back();
                _begin_scan_keys.back().add_null();
                _end_scan_keys.emplace_back();
                _end_scan_keys.back().add_null();
            } else {
                for (int i = 0; i < _begin_scan_keys.size(); ++i) {
                    _begin_scan_keys[i].add_null();
                    _end_scan_keys[i].add_null();
                }
            }
        } else if (_begin_scan_keys.empty()) {
            _begin_scan_keys.emplace_back();
            _begin_scan_keys.back().add_value(cast_to_string<primitive_type, CppType>(
                                                      range.get_range_min_value(), range.scale()),
                                              range.contain_null());
            _end_scan_keys.emplace_back();
            _end_scan_keys.back().add_value(cast_to_string<primitive_type, CppType>(
                    range.get_range_max_value(), range.scale()));
        } else {
            for (int i = 0; i < _begin_scan_keys.size(); ++i) {
                _begin_scan_keys[i].add_value(cast_to_string<primitive_type, CppType>(
                                                      range.get_range_min_value(), range.scale()),
                                              range.contain_null());
            }

            for (int i = 0; i < _end_scan_keys.size(); ++i) {
                _end_scan_keys[i].add_value(cast_to_string<primitive_type, CppType>(
                        range.get_range_max_value(), range.scale()));
            }
        }
        _begin_include = range.is_begin_include();
        _end_include = range.is_end_include();
    }

    return Status::OK();
}

struct ScanPredicate {
    TCondition condition;
    PrimitiveType primitiveType;
};

} // namespace doris
