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

#ifndef  DORIS_BE_SRC_QUERY_EXEC_OLAP_COMMON_H
#define  DORIS_BE_SRC_QUERY_EXEC_OLAP_COMMON_H

#include <boost/variant.hpp>
#include <boost/lexical_cast.hpp>
#include <map>
#include <string>
#include <stdint.h>

#include "common/logging.h"
#include "exec/olap_utils.h"
#include "exec/scan_node.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/string_value.hpp"
#include "runtime/datetime_value.h"

#include "olap/tuple.h"

namespace doris {

template<class T>
std::string cast_to_string(T value) {
    return boost::lexical_cast<std::string>(value);
}

/**
 * @brief Column's value range
 **/
template<class T>
class ColumnValueRange {
public:
    typedef typename std::set<T>::iterator iterator_type;
    ColumnValueRange();
    ColumnValueRange(std::string col_name, PrimitiveType type, T min, T max);

    // should add fixed value before add range
    Status add_fixed_value(T value);

    Status add_range(SQLFilterOp op, T value);

    bool is_fixed_value_range() const;

    bool is_empty_value_range() const;

    bool is_fixed_value_convertible() const;

    bool is_range_value_convertible() const;

    size_t get_convertible_fixed_value_size() const;

    void convert_to_fixed_value();

    void convert_to_range_value();

    bool has_intersection(ColumnValueRange<T>& range);

    void set_empty_value_range() {
        _fixed_values.clear();
        _low_value = _type_max;
        _high_value = _type_min;
    }

    const std::set<T>& get_fixed_value_set() const {
        return _fixed_values;
    }

    T get_range_max_value() const {
        return _high_value;
    }

    T get_range_min_value() const {
        return _low_value;
    }

    bool is_low_value_mininum() const {
        return _low_value == _type_min;
    }

    bool is_high_value_maximum() const {
        return _high_value == _type_max;
    }

    bool is_begin_include() const {
        return _low_op == FILTER_LARGER_OR_EQUAL;
    }

    bool is_end_include() const {
        return _high_op == FILTER_LESS_OR_EQUAL;
    }

    PrimitiveType type() const {
        return _column_type;
    }

    size_t get_fixed_value_size() const {
        return _fixed_values.size();
    }

    std::string to_olap_filter(std::list<TCondition> &filters) {
        if (is_fixed_value_range()) {
            TCondition condition;
            condition.__set_column_name(_column_name);
            condition.__set_condition_op("*=");

            for (auto value : _fixed_values) {
                condition.condition_values.push_back(cast_to_string(value));
            }

            if (condition.condition_values.size() != 0) {
                filters.push_back(condition);
            }
        } else {
            TCondition low;
            if (_type_min != _low_value || FILTER_LARGER_OR_EQUAL != _low_op) {
                low.__set_column_name(_column_name);
                low.__set_condition_op((_low_op == FILTER_LARGER_OR_EQUAL ? ">=" : ">>"));
                low.condition_values.push_back(cast_to_string(_low_value));
            }

            if (low.condition_values.size() != 0) {
                filters.push_back(low);
            }

            TCondition high;
            if (_type_max != _high_value || FILTER_LESS_OR_EQUAL != _high_op) {
                high.__set_column_name(_column_name);
                high.__set_condition_op((_high_op == FILTER_LESS_OR_EQUAL ? "<=" : "<<"));
                high.condition_values.push_back(cast_to_string(_high_value));
            }

            if (high.condition_values.size() != 0) {
                filters.push_back(high);
            }
        }

        return "";
    }

    void clear() {
        _fixed_values.clear();
        _low_value = _type_min;
        _high_value = _type_max;
        _low_op = FILTER_LARGER_OR_EQUAL;
        _high_op = FILTER_LESS_OR_EQUAL;
    }
protected:
    bool is_in_range(const T& value);

private:
    std::string _column_name;
    PrimitiveType _column_type;  // Column type (eg: TINYINT,SMALLINT,INT,BIGINT)
    T _type_min;                 // Column type's min value
    T _type_max;                 // Column type's max value
    T _low_value;                // Column's low value, closed interval at left
    T _high_value;               // Column's high value, open interval at right
    SQLFilterOp _low_op;
    SQLFilterOp _high_op;
    std::set<T> _fixed_values;   // Column's fixed int value
};

class OlapScanKeys {
public:
    OlapScanKeys() :
        _has_range_value(false),
        _begin_include(true),
        _end_include(true),
        _is_convertible(true) {}

    template<class T>
    Status extend_scan_key(ColumnValueRange<T>& range);

    Status get_key_range(std::vector<OlapScanRange>* key_range);

    bool has_range_value() {
        return _has_range_value;
    }

    void clear() {
        _has_range_value = false;
        _begin_scan_keys.clear();
        _end_scan_keys.clear();
    }

    void debug() {
        DCHECK(_begin_scan_keys.size() == _end_scan_keys.size());
        VLOG(1) << "ScanKeys:";

        for (int i = 0; i < _begin_scan_keys.size(); ++i) {
            VLOG(1) << "ScanKey=" << (_begin_include ? "[" : "(")
                    << _begin_scan_keys[i] << " : "
                    << _end_scan_keys[i]
                    << (_end_include ? "]" : ")");
        }
    }

    size_t size() {
        DCHECK(_begin_scan_keys.size() == _end_scan_keys.size());
        return _begin_scan_keys.size();
    }

    void set_begin_include(bool begin_include) {
        _begin_include = begin_include;
    }

    bool begin_include() const {
        return _begin_include;
    }

    void set_end_include(bool end_include) {
        _end_include = end_include;
    }

    bool end_include() const {
        return _end_include;
    }

    void set_is_convertible(bool is_convertible) {
        _is_convertible = is_convertible;
    }

private:
    std::vector<OlapTuple> _begin_scan_keys;
    std::vector<OlapTuple> _end_scan_keys;
    bool _has_range_value;
    bool _begin_include;
    bool _end_include;
    bool _is_convertible;
};

typedef boost::variant <
        ColumnValueRange<int8_t>,
        ColumnValueRange<int16_t>,
        ColumnValueRange<int32_t>,
        ColumnValueRange<int64_t>,
        ColumnValueRange<__int128>,
        ColumnValueRange<StringValue>,
        ColumnValueRange<DateTimeValue>,
        ColumnValueRange<DecimalValue> > ColumnValueRangeType;

class DorisScanRange {
public:
    DorisScanRange(const TPaloScanRange& doris_scan_range)
        : _scan_range(doris_scan_range)  {
    }

    Status init();

    const TPaloScanRange& scan_range() {
        return _scan_range;
    }

    /**
     * @brief return -1 if column is not partition column
     *        return 0 if column's value range has NO intersection with partition column
     *        return 1 if column's value range has intersection with partition column
     **/
    int has_intersection(const std::string column_name, ColumnValueRangeType& value_range);

    class IsEmptyValueRangeVisitor : public boost::static_visitor<bool> {
    public:
        template<class T>
        bool operator()(T& v) {
            return v.is_empty_value_range();
        }
    };

    class HasIntersectionVisitor : public boost::static_visitor<bool> {
    public:
        template<class T, class P>
        bool operator()(T& , P&) {
            return false;
        }
        template<class T>
        bool operator()(T& v1, T& v2) {
            return v1.has_intersection(v2);
        }
    };
private:
    const TPaloScanRange _scan_range;
    std::map<std::string, ColumnValueRangeType > _partition_column_range;
};

template<class T>
ColumnValueRange<T>::ColumnValueRange() : _column_type(INVALID_TYPE) {
}

template<class T>
ColumnValueRange<T>::ColumnValueRange(std::string col_name, PrimitiveType type, T min, T max)
    : _column_name(col_name),
      _column_type(type),
      _type_min(min),
      _type_max(max),
      _low_value(min),
      _high_value(max),
      _low_op(FILTER_LARGER_OR_EQUAL),
      _high_op(FILTER_LESS_OR_EQUAL) {
}

template<class T>
Status ColumnValueRange<T>::add_fixed_value(T value) {
    if (INVALID_TYPE == _column_type) {
        return Status("AddFixedValue failed, Invalid type");
    }

    _fixed_values.insert(value);
    return Status::OK;
}

template<class T>
bool ColumnValueRange<T>::is_fixed_value_range() const {
    return _fixed_values.size() != 0;
}

template<class T>
bool ColumnValueRange<T>::is_empty_value_range() const {
    if (INVALID_TYPE == _column_type) {
        return true;
    } else {
        if (0 == _fixed_values.size()) {
            if (_high_value > _low_value) {
                return false;
            } else {
                return true;
            }
        } else {
            return false;
        }
    }
}

template<class T>
bool ColumnValueRange<T>::is_fixed_value_convertible() const {
    if (is_fixed_value_range()) {
        return false;
    }

    if (!is_enumeration_type(_column_type)) {
        return false;
    }

    return true;
}

template<class T>
bool ColumnValueRange<T>::is_range_value_convertible() const {
    if (!is_fixed_value_range()) {
        return false;
    }

    if (TYPE_NULL == _column_type
            || TYPE_BOOLEAN == _column_type) {
        return false;
    }

    return true;
}

template<class T>
size_t ColumnValueRange<T>::get_convertible_fixed_value_size() const {
    if (!is_fixed_value_convertible()) {
        return 0;
    }

    return _high_value - _low_value;
}

template<>
void ColumnValueRange<StringValue>::convert_to_fixed_value();

template<>
void ColumnValueRange<DecimalValue>::convert_to_fixed_value();

template<>
void ColumnValueRange<__int128>::convert_to_fixed_value();

template<class T>
void ColumnValueRange<T>::convert_to_fixed_value() {
    if (!is_fixed_value_convertible()) {
        return;
    }

    if (_low_op == FILTER_LARGER) {
        ++_low_value;
    }

    if (_high_op == FILTER_LESS) {
        for (T v = _low_value; v < _high_value; ++v) {
            _fixed_values.insert(v);
        }
    } else {
        for (T v = _low_value; v <= _high_value; ++v) {
            _fixed_values.insert(v);
        }
    }
}

template<class T>
void ColumnValueRange<T>::convert_to_range_value() {
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

template<class T>
Status ColumnValueRange<T>::add_range(SQLFilterOp op, T value) {
    if (INVALID_TYPE == _column_type) {
        return Status("AddRange failed, Invalid type");
    }

    if (is_fixed_value_range()) {
        std::pair<iterator_type, iterator_type> bound_pair
            = _fixed_values.equal_range(value);

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
            return Status("AddRangefail! Unsupport SQLFilterOp.");
        }
        }

        _high_value = _type_min;
        _low_value = _type_max;
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
                break;
            }

            default: {
                return Status("AddRangefail! Unsupport SQLFilterOp.");
            }
            }
        }

        if (FILTER_LARGER_OR_EQUAL == _low_op &&
                FILTER_LESS_OR_EQUAL == _high_op &&
                _high_value == _low_value) {
            add_fixed_value(_high_value);
            _high_value = _type_min;
            _low_value = _type_max;
        }
    }

    return Status::OK;
}

template<class T>
bool ColumnValueRange<T>::is_in_range(const T& value) {
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

template<class T>
bool ColumnValueRange<T>::has_intersection(ColumnValueRange<T>& range) {
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
        std::set<T> result_values;
        set_intersection(
            _fixed_values.begin(),
            _fixed_values.end(),
            range._fixed_values.begin(),
            range._fixed_values.end(),
            std::inserter(result_values, result_values.begin()));

        if (result_values.size() != 0) {
            return true;
        } else {
            return false;
        }
    } // 3.2
    else if (is_fixed_value_range() && !range.is_fixed_value_range()) {
        iterator_type iter = _fixed_values.begin();

        while (iter != _fixed_values.end()) {
            if (range.is_in_range(*iter)) {
                return true;
            }

            ++iter;
        }

        return false;
    } else if (!is_fixed_value_range() && range.is_fixed_value_range()) {
        iterator_type iter = range._fixed_values.begin();

        while (iter != range._fixed_values.end()) {
            if (this->is_in_range(*iter)) {
                return true;
            }

            ++iter;
        }

        return false;
    } else {
        if (_low_value > range._high_value
                || range._low_value > _high_value) {
            return false;
        } else if (_low_value == range._high_value) {
            if (FILTER_LARGER_OR_EQUAL == _low_op &&
                    FILTER_LESS_OR_EQUAL == range._high_op) {
                return true;
            } else {
                return false;
            }
        } else if (range._low_value == _high_value) {
            if (FILTER_LARGER_OR_EQUAL == range._low_op &&
                    FILTER_LESS_OR_EQUAL == _high_op) {
                return true;
            } else {
                return false;
            }
        } else {
            return true;
        }
    }
}

template<class T>
Status OlapScanKeys::extend_scan_key(ColumnValueRange<T>& range) {
    using namespace std;
    typedef typename set<T>::const_iterator const_iterator_type;

    // 1. clear ScanKey if some column range is empty
    if (range.is_empty_value_range()) {
        _begin_scan_keys.clear();
        _end_scan_keys.clear();
        return Status::OK;
    }

    // 2. stop extend ScanKey when it's already extend a range value
    if (_has_range_value) {
        return Status::OK;
    }

    //if a column doesn't have any predicate, we will try converting the range to fixed values
    //for this case, we need to add null value to fixed values
    bool has_converted = false;

    if (range.is_fixed_value_range()) {
        if ((_begin_scan_keys.empty() && range.get_fixed_value_size() > config::doris_max_scan_key_num)
                || range.get_fixed_value_size() * _begin_scan_keys.size() > config::doris_max_scan_key_num) {
            if (range.is_range_value_convertible()) {
                range.convert_to_range_value();
            } else {
                return Status::OK;
            }
        }
    } else {
        if (range.is_fixed_value_convertible() && _is_convertible) {
            if (_begin_scan_keys.empty()) {
                if (range.get_convertible_fixed_value_size() < config::doris_max_scan_key_num) {
                    if (range.is_low_value_mininum() && range.is_high_value_maximum()) {
                        has_converted = true;
                    }

                    range.convert_to_fixed_value();
                }
            } else {
                if (range.get_convertible_fixed_value_size() * _begin_scan_keys.size()
                        < config::doris_max_scan_key_num) {
                    if (range.is_low_value_mininum() && range.is_high_value_maximum()) {
                        has_converted = true;
                    }

                    range.convert_to_fixed_value();
                }
            }
        }
    }

    // 3.1 extend ScanKey with FixedValueRange
    if (range.is_fixed_value_range()) {
        // 3.1.1 construct num of fixed value ScanKey (begin_key == end_key)
        if (_begin_scan_keys.empty()) {
            const set<T>& fixed_value_set = range.get_fixed_value_set();
            const_iterator_type iter = fixed_value_set.begin();

            for (; iter != fixed_value_set.end(); ++iter) {
                _begin_scan_keys.emplace_back();
                _begin_scan_keys.back().add_value(cast_to_string(*iter));
                _end_scan_keys.emplace_back();
                _end_scan_keys.back().add_value(cast_to_string(*iter));
            }

            if (has_converted) {
                 _begin_scan_keys.emplace_back();
                 _begin_scan_keys.back().add_null();
                 _end_scan_keys.emplace_back();
                 _end_scan_keys.back().add_null();
            }
        } // 3.1.2 produces the Cartesian product of ScanKey and fixed_value
        else {
            const set<T>& fixed_value_set = range.get_fixed_value_set();
            int original_key_range_size = _begin_scan_keys.size();

            for (int i = 0; i < original_key_range_size; ++i) {
                OlapTuple start_base_key_range = _begin_scan_keys[i];
                OlapTuple end_base_key_range = _end_scan_keys[i];

                const_iterator_type iter = fixed_value_set.begin();

                for (; iter != fixed_value_set.end(); ++iter) {
                    // alter the first ScanKey in original place
                    if (iter == fixed_value_set.begin()) {
                        _begin_scan_keys[i].add_value(cast_to_string(*iter));
                        _end_scan_keys[i].add_value(cast_to_string(*iter));
                    } // append follow ScanKey
                    else {
                        _begin_scan_keys.push_back(start_base_key_range);
                        _begin_scan_keys.back().add_value(cast_to_string(*iter));
                        _end_scan_keys.push_back(end_base_key_range);
                        _end_scan_keys.back().add_value(cast_to_string(*iter));
                    }
                }

                if (has_converted) {
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

        if (_begin_scan_keys.empty()) {
            _begin_scan_keys.emplace_back();
            _begin_scan_keys.back().add_value(
                cast_to_string(range.get_range_min_value()),
                range.is_low_value_mininum());
            _end_scan_keys.emplace_back();
            _end_scan_keys.back().add_value(
                cast_to_string(range.get_range_max_value()));
        } else {
            for (int i = 0; i < _begin_scan_keys.size(); ++i) {
                _begin_scan_keys[i].add_value(
                    cast_to_string(range.get_range_min_value()),
                    range.is_low_value_mininum());
            }

            for (int i = 0; i < _end_scan_keys.size(); ++i) {
                _end_scan_keys[i].add_value(
                    cast_to_string(range.get_range_max_value()));
            }
        }

        _begin_include = range.is_begin_include();
        _end_include = range.is_end_include();
    }

    return Status::OK;
}

}  // namespace doris

#endif

/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
