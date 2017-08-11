// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

#include <algorithm>
#include <map>
#include <sstream>
#include <string>
#include "olap/field.h"
#include "exprs/aggregate_functions.h"

using std::map;
using std::nothrow;
using std::string;
using std::stringstream;

namespace palo {

FieldType FieldInfo::get_field_type_by_string(const string& type_str) {
    string upper_type_str = type_str;
    std::transform(type_str.begin(), type_str.end(), upper_type_str.begin(), toupper);
    FieldType type;

    if (0 == upper_type_str.compare("TINYINT")) {
        type = OLAP_FIELD_TYPE_TINYINT;
    } else if (0 == upper_type_str.compare("SMALLINT")) {
        type = OLAP_FIELD_TYPE_SMALLINT;
    } else if (0 == upper_type_str.compare("INT")) {
        type = OLAP_FIELD_TYPE_INT;
    } else if (0 == upper_type_str.compare("BIGINT")) {
        type = OLAP_FIELD_TYPE_BIGINT;
    } else if (0 == upper_type_str.compare("LARGEINT")) {
        type = OLAP_FIELD_TYPE_LARGEINT;
    } else if (0 == upper_type_str.compare("UNSIGNED_TINYINT")) {
        type = OLAP_FIELD_TYPE_UNSIGNED_TINYINT;
    } else if (0 == upper_type_str.compare("UNSIGNED_SMALLINT")) {
        type = OLAP_FIELD_TYPE_UNSIGNED_SMALLINT;
    } else if (0 == upper_type_str.compare("UNSIGNED_INT")) {
        type = OLAP_FIELD_TYPE_UNSIGNED_INT;
    } else if (0 == upper_type_str.compare("UNSIGNED_BIGINT")) {
        type = OLAP_FIELD_TYPE_UNSIGNED_BIGINT;
    } else if (0 == upper_type_str.compare("FLOAT")) {
        type = OLAP_FIELD_TYPE_FLOAT;
    } else if (0 == upper_type_str.compare("DISCRETE_DOUBLE")) {
        type = OLAP_FIELD_TYPE_DISCRETE_DOUBLE;
    } else if (0 == upper_type_str.compare("DOUBLE")) {
        type = OLAP_FIELD_TYPE_DOUBLE;
    } else if (0 == upper_type_str.compare("CHAR")) {
        type = OLAP_FIELD_TYPE_CHAR;
    } else if (0 == upper_type_str.compare("DATE")) {
        type = OLAP_FIELD_TYPE_DATE;
    } else if (0 == upper_type_str.compare("DATETIME")) {
        type = OLAP_FIELD_TYPE_DATETIME;
    } else if (0 == upper_type_str.compare(0, 7, "DECIMAL")) {
        type = OLAP_FIELD_TYPE_DECIMAL;
    } else if (0 == upper_type_str.compare(0, 7, "VARCHAR")) {
        type = OLAP_FIELD_TYPE_VARCHAR;
    } else if (0 == upper_type_str.compare(0, 3, "HLL")) {
        type = OLAP_FIELD_TYPE_HLL;
    } else if (0 == upper_type_str.compare("STRUCT")) {
        type = OLAP_FIELD_TYPE_STRUCT;
    } else if (0 == upper_type_str.compare("LIST")) {
        type = OLAP_FIELD_TYPE_LIST;
    } else if (0 == upper_type_str.compare("MAP")) {
        type = OLAP_FIELD_TYPE_MAP;
    } else {
        OLAP_LOG_WARNING("invalid type string. [type='%s']", type_str.c_str());
        type = OLAP_FIELD_TYPE_UNKNOWN;
    }

    return type;
}

FieldAggregationMethod FieldInfo::get_aggregation_type_by_string(const string& str) {
    string upper_str = str;
    std::transform(str.begin(), str.end(), upper_str.begin(), toupper);
    FieldAggregationMethod aggregation_type;

    if (0 == upper_str.compare("NONE")) {
        aggregation_type = OLAP_FIELD_AGGREGATION_NONE;
    } else if (0 == upper_str.compare("SUM")) {
        aggregation_type = OLAP_FIELD_AGGREGATION_SUM;
    } else if (0 == upper_str.compare("MIN")) {
        aggregation_type = OLAP_FIELD_AGGREGATION_MIN;
    } else if (0 == upper_str.compare("MAX")) {
        aggregation_type = OLAP_FIELD_AGGREGATION_MAX;
    } else if (0 == upper_str.compare("REPLACE")) {
        aggregation_type = OLAP_FIELD_AGGREGATION_REPLACE;
    } else if (0 == upper_str.compare("HLL_UNION")) {
        aggregation_type = OLAP_FIELD_AGGREGATION_HLL_UNION;
    } else {
        OLAP_LOG_WARNING("invalid aggregation type string. [aggregation='%s']", str.c_str());
        aggregation_type = OLAP_FIELD_AGGREGATION_UNKNOWN;
    }

    return aggregation_type;
}

string FieldInfo::get_string_by_field_type(FieldType type) {
    switch (type) {
    case OLAP_FIELD_TYPE_TINYINT:
        return "TINYINT";

    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        return "UNSIGNED_TINYINT";

    case OLAP_FIELD_TYPE_SMALLINT:
        return "SMALLINT";

    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        return "UNSIGNED_SMALLINT";

    case OLAP_FIELD_TYPE_INT:
        return "INT";

    case OLAP_FIELD_TYPE_UNSIGNED_INT:
        return "UNSIGNED_INT";

    case OLAP_FIELD_TYPE_BIGINT:
        return "BIGINT";

    case OLAP_FIELD_TYPE_LARGEINT:
        return "LARGEINT";

    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        return "UNSIGNED_BIGINT";

    case OLAP_FIELD_TYPE_FLOAT:
        return "FLOAT";

    case OLAP_FIELD_TYPE_DOUBLE:
        return "DOUBLE";

    case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
        return "DISCRETE_DOUBLE";

    case OLAP_FIELD_TYPE_CHAR:
        return "CHAR";

    case OLAP_FIELD_TYPE_DATE:
        return "DATE";

    case OLAP_FIELD_TYPE_DATETIME:
        return "DATETIME";

    case OLAP_FIELD_TYPE_DECIMAL:
        return "DECIMAL";

    case OLAP_FIELD_TYPE_VARCHAR:
        return "VARCHAR";
    
    case OLAP_FIELD_TYPE_HLL:
        return "HLL";

    case OLAP_FIELD_TYPE_STRUCT:
        return "STRUCT";

    case OLAP_FIELD_TYPE_LIST:
        return "LIST";

    case OLAP_FIELD_TYPE_MAP:
        return "MAP";

    default:
        return "UNKNOWN";
    }
}

string FieldInfo::get_string_by_aggregation_type(FieldAggregationMethod type) {
    switch (type) {
    case OLAP_FIELD_AGGREGATION_NONE:
        return "NONE";

    case OLAP_FIELD_AGGREGATION_SUM:
        return "SUM";

    case OLAP_FIELD_AGGREGATION_MIN:
        return "MIN";

    case OLAP_FIELD_AGGREGATION_MAX:
        return "MAX";

    case OLAP_FIELD_AGGREGATION_REPLACE:
        return "REPLACE";
            
    case OLAP_FIELD_AGGREGATION_HLL_UNION:
        return "HLL_UNION";

    default:
        return "UNKNOWN";
    }
}

uint32_t FieldInfo::get_field_length_by_type(TPrimitiveType::type type, uint32_t string_length) {
    switch (type) {
    case TPrimitiveType::TINYINT:
        return 1;
    case TPrimitiveType::SMALLINT:
        return 2;
    case TPrimitiveType::INT:
        return 4;
    case TPrimitiveType::BIGINT:
        return 8;
    case TPrimitiveType::LARGEINT:
        return 16;
    case TPrimitiveType::DATE:
        return 3;
    case TPrimitiveType::DATETIME:
        return 8;
    case TPrimitiveType::FLOAT:
        return 4;
    case TPrimitiveType::DOUBLE:
        return 8;
    case TPrimitiveType::CHAR:
        return string_length;
    case TPrimitiveType::VARCHAR:
    case TPrimitiveType::HLL:
        return string_length + sizeof(VarCharField::LengthValueType);
    case TPrimitiveType::DECIMAL:    
        return 12; // use 12 bytes in olap engine.
    default:
        OLAP_LOG_WARNING("unknown field type. [type=%d]", type);
        return 0;
    }
}

Field* Field::create(const FieldInfo& field_info) {
    Field* field = NULL;

    switch (field_info.type) {
    case OLAP_FIELD_TYPE_TINYINT:
        field = new(nothrow) NumericField<int8_t>(field_info);
        break;

    case OLAP_FIELD_TYPE_SMALLINT:
        field = new(nothrow) NumericField<int16_t>(field_info);
        break;

    case OLAP_FIELD_TYPE_INT:
        field = new(nothrow) NumericField<int>(field_info);
        break;

    case OLAP_FIELD_TYPE_BIGINT:
        field = new(nothrow) NumericField<int64_t>(field_info);
        break;

    case OLAP_FIELD_TYPE_LARGEINT:
        field = new(nothrow) NumericField<int128_t>(field_info);
        break;

    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        field = new(nothrow) NumericField<uint8_t>(field_info);
        break;

    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        field = new(nothrow) NumericField<uint16_t>(field_info);
        break;

    case OLAP_FIELD_TYPE_UNSIGNED_INT:
        field = new(nothrow) NumericField<uint32_t>(field_info);
        break;

    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        field = new(nothrow) NumericField<uint64_t>(field_info);
        break;

    case OLAP_FIELD_TYPE_FLOAT:
        field = new(nothrow) NumericField<float>(field_info);
        break;

    case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
        field = new(nothrow) DiscreteDoubleField(field_info);
        break;

    case OLAP_FIELD_TYPE_DOUBLE:
        field = new(nothrow) NumericField<double>(field_info);
        break;

    case OLAP_FIELD_TYPE_CHAR:
        field = new(nothrow) CharField(field_info);
        break;

    case OLAP_FIELD_TYPE_DATE:
        field = new(nothrow) DateField(field_info);
        break;

    case OLAP_FIELD_TYPE_DATETIME:
        field = new(nothrow) DateTimeField(field_info);
        break;

    case OLAP_FIELD_TYPE_DECIMAL:
        field = new(nothrow) DecimalField(field_info);
        break;

    case OLAP_FIELD_TYPE_VARCHAR:
        field = new(nothrow) VarCharField(field_info);
        break;
    case OLAP_FIELD_TYPE_HLL:
        field = new(nothrow) HllField(field_info);
        break;
    case OLAP_FIELD_TYPE_STRUCT:
    case OLAP_FIELD_TYPE_LIST:
    case OLAP_FIELD_TYPE_MAP:
        OLAP_LOG_WARNING("unsupported field type. [type='%s']",
                         FieldInfo::get_string_by_field_type(field_info.type).c_str());
        return NULL;

    case OLAP_FIELD_TYPE_UNKNOWN:
    default:
        OLAP_LOG_WARNING("unknown field type. [type=%d]", field_info.type);
        return NULL;
    }

    if (NULL == field) {
        OLAP_LOG_WARNING("fail to malloc Field.");
        return NULL;
    }

    field->_field_type = field_info.type;

    if (field->init() != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init Field.");
        SAFE_DELETE(field);
        return NULL;
    }

    return field;
}

// 这个函数目前不支持字符串类型
Field* Field::create_by_type(const FieldType& field_type) {
    Field* field = NULL;
    FieldInfo field_info;
    field_info.aggregation = OLAP_FIELD_AGGREGATION_NONE;

    switch (field_type) {
    case OLAP_FIELD_TYPE_TINYINT:
        field = new(nothrow) NumericField<int8_t>(field_info);
        break;

    case OLAP_FIELD_TYPE_SMALLINT:
        field = new(nothrow) NumericField<int16_t>(field_info);
        break;

    case OLAP_FIELD_TYPE_INT:
        field = new(nothrow) NumericField<int>(field_info);
        break;

    case OLAP_FIELD_TYPE_BIGINT:
        field = new(nothrow) NumericField<int64_t>(field_info);
        break;

    case OLAP_FIELD_TYPE_LARGEINT:
        field = new(nothrow) NumericField<int128_t>(field_info);
        break;

    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        field = new(nothrow) NumericField<uint8_t>(field_info);
        break;

    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        field = new(nothrow) NumericField<uint16_t>(field_info);
        break;

    case OLAP_FIELD_TYPE_UNSIGNED_INT:
        field = new(nothrow) NumericField<uint32_t>(field_info);
        break;

    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        field = new(nothrow) NumericField<uint64_t>(field_info);
        break;

    case OLAP_FIELD_TYPE_FLOAT:
        field = new(nothrow) NumericField<float>(field_info);
        break;

    case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
        field = new(nothrow) DiscreteDoubleField(field_info);
        break;

    case OLAP_FIELD_TYPE_DOUBLE:
        field = new(nothrow) NumericField<double>(field_info);
        break;

    case OLAP_FIELD_TYPE_DATE:
        field = new(nothrow) DateField(field_info);
        break;

    case OLAP_FIELD_TYPE_DATETIME:
        field = new(nothrow) DateTimeField(field_info);
        break;

    case OLAP_FIELD_TYPE_DECIMAL:
        field = new(nothrow) DecimalField(field_info);
        break;

    case OLAP_FIELD_TYPE_STRUCT:
    case OLAP_FIELD_TYPE_LIST:
    case OLAP_FIELD_TYPE_MAP:
        OLAP_LOG_DEBUG("not supported field type. [type=%d]", field_type);
        return NULL;

    case OLAP_FIELD_TYPE_CHAR:
    case OLAP_FIELD_TYPE_VARCHAR:
    case OLAP_FIELD_TYPE_HLL:
        return NULL;

    case OLAP_FIELD_TYPE_UNKNOWN:
    default:
        OLAP_LOG_DEBUG("not supported field type. [type=%d]", field_type);
        return NULL;
    }

    if (NULL == field) {
        OLAP_LOG_WARNING("fail to malloc Field.");
        return NULL;
    }

    field->_field_type = field_type;

    if (field->init() != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init Field.");
        SAFE_DELETE(field);
        return NULL;
    }

    return field;
}

template <typename T>
OLAPStatus BaseField<T>::init() {
    switch (_aggregation) {
    case OLAP_FIELD_AGGREGATION_SUM:
        _aggregator = new(nothrow) FieldAddAggregator<T*>();
        break;

    case OLAP_FIELD_AGGREGATION_MIN:
        _aggregator = new(nothrow) FieldMinAggregator<T*>();
        break;

    case OLAP_FIELD_AGGREGATION_MAX:
        _aggregator = new(nothrow) FieldMaxAggregator<T*>();
        break;

    case OLAP_FIELD_AGGREGATION_REPLACE:
        _aggregator = new(nothrow) FieldReplaceAggregator<T*>();
        break;

    case OLAP_FIELD_AGGREGATION_NONE:
        _aggregator = new(nothrow) FieldNoneAggregator<T*>();
        break;
    case OLAP_FIELD_AGGREGATION_HLL_UNION:
        _aggregator = new(nothrow) FieldHllUnionAggreator<T*>();
        break;
    case OLAP_FIELD_AGGREGATION_UNKNOWN:
    default:
        OLAP_LOG_WARNING("unknown aggregation method, use FieldAddAggregator for default."
                         " [aggregation=%d]", _aggregation);
        return OLAP_ERR_OTHER_ERROR;
    }

    if (NULL == _aggregator) {
        OLAP_LOG_WARNING("fail to malloc aggregator.");
        return OLAP_ERR_MALLOC_ERROR;
    }

    return OLAP_SUCCESS;
}

// 下面是一些系列类型转换函数
// 所有的函数均没有考虑对异常情况的处理
// 对于strto*和ato*系列函数，除非传入的不是合法的C风格字符串
// 例如是NULL或者没有以\0结尾，否则不会导致严重错误的发生
template <>
OLAPStatus BaseField<int8_t>::from_string(const std::string& value_string) {
    int8_t value = 0;

    if (value_string.length() > 0) {
        value = static_cast<int8_t>(strtol(value_string.c_str(), NULL, 10));
    }

    from_storage(reinterpret_cast<char*>(&value));

    return OLAP_SUCCESS;
}

template <>
OLAPStatus BaseField<int16_t>::from_string(const std::string& value_string) {
    int16_t value = 0;

    if (value_string.length() > 0) {
        value = static_cast<int16_t>(strtol(value_string.c_str(), NULL, 10));
    }

    from_storage(reinterpret_cast<char*>(&value));

    return OLAP_SUCCESS;
}

template <>
OLAPStatus BaseField<int>::from_string(const std::string& value_string) {
    int value = 0;

    if (value_string.length() > 0) {
        value = static_cast<int>(strtol(value_string.c_str(), NULL, 10));
    }

    from_storage(reinterpret_cast<char*>(&value));

    return OLAP_SUCCESS;
}

template <>
OLAPStatus BaseField<int64_t>::from_string(const std::string& value_string) {
    int64_t value = 0;

    if (value_string.length() > 0) {
        value = strtol(value_string.c_str(), NULL, 10);
    }

    from_storage(reinterpret_cast<char*>(&value));

    return OLAP_SUCCESS;
}

template <>
OLAPStatus BaseField<int128_t>::from_string(const std::string& str) {
    int128_t value = 0;

    const char* value_string = str.c_str();
    char* end = NULL;
    value = strtol(value_string, &end, 10);
    if (*end != 0) {
        value = 0;
    } else if (value > LONG_MIN && value < LONG_MAX) {
        // use strtol result directly
    } else {
        bool is_negative = false;
        if (*value_string == '-' || *value_string == '+') {
            if (*(value_string++) == '-') {
                is_negative = true;
            }
        }

        uint128_t current = 0;
        uint128_t max_int128 = ~((int128_t)(1) << 127);
        while (*value_string != 0) {
            if (current > max_int128 / 10) {
                break;
            }

            current = current * 10 + (*(value_string++) - '0');
        }

        if (*value_string != 0
                || (!is_negative && current > max_int128)
                || ( is_negative&& current > max_int128 + 1)) {
            current = 0;
        }

        value = is_negative ? -current : current;
    }

    from_storage(reinterpret_cast<char*>(&value));

    return OLAP_SUCCESS;
}

template <>
OLAPStatus BaseField<uint32_t>::from_string(const std::string& value_string) {
    uint32_t value = 0;

    if (value_string.length() > 0) {
        value = static_cast<uint32_t>(strtoul(value_string.c_str(), NULL, 10));
    }

    from_storage(reinterpret_cast<char*>(&value));

    return OLAP_SUCCESS;
}

template <>
OLAPStatus BaseField<uint64_t>::from_string(const std::string& value_string) {
    uint64_t value = 0;

    if (value_string.length() > 0) {
        value = strtoul(value_string.c_str(), NULL, 10);
    }

    from_storage(reinterpret_cast<char*>(&value));

    return OLAP_SUCCESS;
}

template <>
OLAPStatus BaseField<uint16_t>::from_string(const std::string& value_string) {
    uint16_t value = 0;

    if (value_string.length() > 0) {
        value = static_cast<uint16_t>(strtoul(value_string.c_str(), NULL, 10));
    }

    from_storage(reinterpret_cast<char*>(&value));

    return OLAP_SUCCESS;
}

template <>
OLAPStatus BaseField<uint8_t>::from_string(const std::string& value_string) {
    uint8_t value = 0;

    if (value_string.length() > 0) {
        value = static_cast<uint8_t>(strtoul(value_string.c_str(), NULL, 10));
    }

    from_storage(reinterpret_cast<char*>(&value));

    return OLAP_SUCCESS;
}

template <>
OLAPStatus BaseField<float>::from_string(const std::string& value_string) {
    float value = 0.0f;

    if (value_string.length() > 0) {
        value = static_cast<float>(atof(value_string.c_str()));
    }

    from_storage(reinterpret_cast<char*>(&value));

    return OLAP_SUCCESS;
}

template <>
OLAPStatus BaseField<double>::from_string(const std::string& value_string) {
    double value = 0.0;

    if (value_string.length() > 0) {
        value = atof(value_string.c_str());
    }

    from_storage(reinterpret_cast<char*>(&value));

    return OLAP_SUCCESS;
}

template <typename T>
string BaseField<T>::to_string() const {
    stringstream stream;
    stream << *_value();
    return stream.str();
}

template <typename T>
string BaseField<T>::to_buf() const {
    stringstream stream;
    stream << *_value();
    return stream.str();
}

template <>
string BaseField<uint8_t>::to_string() const {
    char buf[1024] = {'\0'};
    snprintf(buf, sizeof(buf), "%u", *_value());
    return string(buf);
}

template <>
string BaseField<uint8_t>::to_buf() const {
    char buf[1024] = {'\0'};
    snprintf(buf, sizeof(buf), "%u", *_value());
    return string(buf);
}

template <>
string BaseField<int8_t>::to_string() const {
    char buf[1024] = {'\0'};
    snprintf(buf, sizeof(buf), "%d", *_value());
    return string(buf);
}

template <>
string BaseField<int8_t>::to_buf() const {
    char buf[1024] = {'\0'};
    snprintf(buf, sizeof(buf), "%d", *_value());
    return string(buf);
}

template <>
string BaseField<double>::to_string() const {
    char buf[1024] = {'\0'};
    snprintf(buf, sizeof(buf), "%.10f", *_value());
    return string(buf);
}

template <>
string BaseField<double>::to_buf() const {
    char buf[1024] = {'\0'};
    snprintf(buf, sizeof(buf), "%.10f", *_value());
    return string(buf);
}

template <>
string BaseField<decimal12_t>::to_string() const {
    decimal12_t* data_ptr = _value();
    return data_ptr->to_string();
}

template <>
string BaseField<decimal12_t>::to_buf() const {
    decimal12_t* data_ptr = _value();
    return data_ptr->to_string();
}

template <>
string BaseField<int128_t>::to_string() const {
    char buf[1024];
    int128_t value = *_value();
    if (value >= std::numeric_limits<int64_t>::min()
            && value <= std::numeric_limits<int64_t>::max()) {
        snprintf(buf, sizeof(buf), "%ld", (int64_t)value);
    } else {
        char* current = buf;
        uint128_t abs_value = value;
        if (value < 0) {
            *(current++) = '-';
            abs_value = -value;
        }

        // the max value of uint64_t is 18446744073709551615UL,
        // so use Z19_UINT64 to divide uint128_t 
        const static uint64_t Z19_UINT64 = 10000000000000000000ULL;
        uint64_t suffix = abs_value % Z19_UINT64;
        uint64_t middle = abs_value / Z19_UINT64 % Z19_UINT64;
        uint64_t prefix = abs_value / Z19_UINT64 / Z19_UINT64;

        char* end = buf + sizeof(buf);
        if (prefix > 0) {
            current += snprintf(current, end - current, "%" PRIu64, prefix);
            current += snprintf(current, end - current, "%.19" PRIu64, middle);
            current += snprintf(current, end - current, "%.19" PRIu64, suffix);
        } else if (OLAP_LIKELY(middle > 0)) {
            current += snprintf(current, end - current, "%" PRIu64, middle);
            current += snprintf(current, end - current, "%.19" PRIu64, suffix);
        } else {
            current += snprintf(current, end - current, "%" PRIu64, suffix);
        }
    }

    return std::string(buf);
}

template <>
string BaseField<int128_t>::to_buf() const {
    return to_string();
}

template <>
void NumericField<int128_t>::set_to_min() {
    int128_t min_value = (int128_t)(1) << 127;
    memcpy(this->_buf, reinterpret_cast<const char*>(&min_value), sizeof(int128_t));
}

template<>
bool NumericField<int128_t>::is_min() {
    int128_t min_value = (int128_t)(1) << 127;
    if (*_value() == min_value) {
        return true;
    } else {
        return false;
    }
}

template <>
void NumericField<int128_t>::set_to_max() {
    int128_t max_value = ~((int128_t)(1) << 127);
    memcpy(this->_buf, reinterpret_cast<const char*>(&max_value), sizeof(int128_t));
}

OLAPStatus CharField::init() {
    OLAPStatus res = BaseField<char>::init();

    return res;
}

OLAPStatus VarCharField::from_string(const std::string& value_string) {
    size_t value_len = value_string.length();

    if (value_len > (_buf_size - sizeof(LengthValueType))) {
        OLAP_LOG_WARNING("the len of value string is too log[len=%lu, buf_size=%lu].",
                value_len, _buf_size);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    if (_buf == NULL) {
        OLAP_LOG_WARNING("the buf is NULL!");
        return OLAP_ERR_NOT_INITED;
    }
    
    memcpy(_buf + sizeof(LengthValueType), value_string.c_str(), value_len);
    *_length_ptr = value_len;

    return OLAP_SUCCESS;
}

string VarCharField::to_string() const {
    if (NULL == _buf) {
        return "";
    }
    
    string res(reinterpret_cast<const char*>(_buf + sizeof(LengthValueType)), *_length_ptr);
    
    return res;
}

string VarCharField::to_buf() const {
    if (NULL == _buf) {
        return "";
    }

    string res(reinterpret_cast<const char*>(_buf + sizeof(LengthValueType)), *_length_ptr);
    return res;
}

OLAPStatus CharField::from_string(const std::string& value_string) {
    if (_length == 0) {
        return OLAP_ERR_NOT_INITED;
    }

    size_t value_len = value_string.length();
    
    if (value_len > _length) {
        OLAP_LOG_WARNING("the len of value string is too long[len=%lu, m_aclloate_len=%lu].",
                value_len, _length);
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    if (_buf != NULL) {
        memset(_buf, 0, _length);
        memcpy((void*)buf(), (const void*)value_string.c_str(), value_len);
    }

    return OLAP_SUCCESS;
}

string CharField::to_string() const {
    if (NULL == _buf || _length == 0) {
        return "";
    }

    string res(_buf, _length);
    return res;
}

string CharField::to_buf() const {
    if (NULL == _buf || _length == 0) {
        return "";
    }

    string res(reinterpret_cast<const char*>(_buf), _length);
    return res;
}
    
void HllSetResolver::parse() {
    // skip LengthValueType
    char*  pdata = _buf_ref;
    _set_type = (HllDataType)pdata[0];
    char* sparse_data = NULL;
    switch (_set_type) {
        case HLL_DATA_EXPLICIT:
            // first byte : type
            // second～five byte : hash values's number
            // five byte later : hash value
            _expliclit_num = (ExpliclitLengthValueType) (pdata[sizeof(SetTypeValueType)]);
            _expliclit_value = (uint64_t*)(pdata + sizeof(SetTypeValueType) 
                                                 + sizeof(ExpliclitLengthValueType));
            break;
        case HLL_DATA_SPRASE:
            // first byte : type
            // second ～（2^HLL_COLUMN_PRECISION)/8 byte : bitmap mark which is not zero
            // 2^HLL_COLUMN_PRECISION)/8 ＋ 1以后value
            _sparse_count = (SparseLengthValueType*)(pdata + sizeof (SetTypeValueType));
            sparse_data = pdata + sizeof(SetTypeValueType) + sizeof(SparseLengthValueType);
            for (int i = 0; i < *_sparse_count; i++) {
                SparseIndexType* index = (SparseIndexType*)sparse_data;
                sparse_data += sizeof(SparseIndexType);
                SparseValueType* value = (SparseValueType*)sparse_data;
                _sparse_map[*index] = *value;
                sparse_data += sizeof(SetTypeValueType);
            }
            break;
        case HLL_DATA_FULL:
            // first byte : type
            // second byte later : hll register value
            _full_value_position = pdata + sizeof (SetTypeValueType);
            break;
        default:
            // HLL_DATA_EMPTY
            break;
    }
}

void HllSetResolver::fill_registers(char* registers, int len) {

    if (_set_type == HLL_DATA_EXPLICIT) {
        for (int i = 0; i < get_expliclit_count(); ++i) {
            uint64_t hash_value = get_expliclit_value(i);
            int idx = hash_value % len;
            uint8_t first_one_bit = __builtin_ctzl(hash_value >> HLL_COLUMN_PRECISION) + 1;
            registers[idx] = std::max((uint8_t)registers[idx], first_one_bit);
        }
    } else if (_set_type == HLL_DATA_SPRASE) {
        std::map<SparseIndexType, SparseValueType>& sparse_map = get_sparse_map();
        for (std::map<SparseIndexType, SparseValueType>::iterator iter = sparse_map.begin();
                                                 iter != sparse_map.end(); iter++) {
            registers[iter->first] = std::max((uint8_t)registers[iter->first], (uint8_t)iter->second);
        }
    } else if (_set_type == HLL_DATA_FULL) {
        char* full_value = get_full_value();
        for (int i = 0; i < len; i++) {
            registers[i] = std::max((uint8_t)registers[i], (uint8_t)full_value[i]);
        }
        
    } else {
      // HLL_DATA_EMPTY
    }
}

void HllSetResolver::fill_index_to_value_map(std::map<int, uint8_t>* index_to_value, int len) {

    if (_set_type == HLL_DATA_EXPLICIT) {
        for (int i = 0; i < get_expliclit_count(); ++i) {
            uint64_t hash_value = get_expliclit_value(i);
            int idx = hash_value % len; 
            uint8_t first_one_bit = __builtin_ctzl(hash_value >> HLL_COLUMN_PRECISION) + 1; 
            if (index_to_value->find(idx) != index_to_value->end()) {
                (*index_to_value)[idx] = (*index_to_value)[idx] < first_one_bit ? first_one_bit : (*index_to_value)[idx];
            } else {
                (*index_to_value)[idx] = first_one_bit;
            }    
        }    
    } else if (_set_type == HLL_DATA_SPRASE) {
        std::map<SparseIndexType, SparseValueType>& sparse_map = get_sparse_map();
        for (std::map<SparseIndexType, SparseValueType>::iterator iter = sparse_map.begin();
                                                 iter != sparse_map.end(); iter++) {
            if (index_to_value->find(iter->first) != index_to_value->end()) {
                (*index_to_value)[iter->first] = (*index_to_value)[iter->first] < iter->second ? iter->second : (*index_to_value)[iter->first];
            } else {
                (*index_to_value)[iter->first] = iter->second;
            }        
        }        
    } else if (_set_type == HLL_DATA_FULL) {
       char* registers = get_full_value();
       for (int i = 0; i < len; i++) {
           if (registers[i] != 0) {
               if (index_to_value->find(i) != index_to_value->end()) {
                   (*index_to_value)[i] = (*index_to_value)[i] < registers[i] ? registers[i]  : (*index_to_value)[i];
               } else {
                   (*index_to_value)[i] = registers[i];
               } 
           }
       } 
    } else { 
      // HLL_DATA_EMPTY
    }      
}

void HllSetResolver::fill_hash64_set(std::set<uint64_t>* hash_set) {
    if (_set_type == HLL_DATA_EXPLICIT) {
        for (int i = 0; i < get_expliclit_count(); ++i) {
            uint64_t hash_value = get_expliclit_value(i);
            hash_set->insert(hash_value);
        }    
    } 
}
 
void HllSetHelper::set_sparse(char *result, const std::map<int, uint8_t>& index_to_value, int& len) {
    result[0] = HLL_DATA_SPRASE;
    len = sizeof(HllSetResolver::SetTypeValueType) + sizeof(HllSetResolver::SparseLengthValueType);
    char* write_value_pos = result + len;
    for (std::map<int, uint8_t>::const_iterator iter = index_to_value.begin();
         iter != index_to_value.end(); iter++) {
        write_value_pos[0] = (char)(iter->first & 0xff);
        write_value_pos[1] = (char)(iter->first >> 8 & 0xff);
        write_value_pos[2] = iter->second;
        write_value_pos += 3;
    }
    int registers_count = index_to_value.size();
    len += registers_count * (sizeof(HllSetResolver::SparseIndexType) + sizeof(HllSetResolver::SparseValueType));
    *(int*)(result + 1) = registers_count;
}

void HllSetHelper::set_expliclit(char* result, const std::set<uint64_t>& hash_value_set, int& len) {
    result[0] = HLL_DATA_EXPLICIT;
    result[1] = (HllSetResolver::ExpliclitLengthValueType)hash_value_set.size();
    len = sizeof(HllSetResolver::SetTypeValueType) + sizeof(HllSetResolver::ExpliclitLengthValueType);
    char* writePosition = result + len;
    for (std::set<uint64_t>::const_iterator iter = hash_value_set.begin();
         iter != hash_value_set.end(); iter++) {
        uint64_t hash_value = *iter;
        *(uint64_t*)writePosition = hash_value;
        writePosition += 8;
    }
    len += sizeof(uint64_t) * hash_value_set.size();
}

void HllSetHelper::set_full(char* result, const char* registers, const int registers_len, int& len) {
    result[0] = HLL_DATA_FULL;
    memcpy(result + 1, registers, registers_len);
    len = registers_len + sizeof(HllSetResolver::SetTypeValueType);
}

void HllSetHelper::set_full(char* result, 
                            const std::map<int, uint8_t>& index_to_value, 
                            const int registers_len, int& len) {
    result[0] = HLL_DATA_FULL;
    for (std::map<int, uint8_t>::const_iterator iter = index_to_value.begin();
         iter != index_to_value.end(); iter++) {
        result[1 + iter->first] = iter->second;
    }
    len = registers_len + sizeof(HllSetResolver::SetTypeValueType);
}

void HllSetHelper::set_max_register(char* registers, int registers_len, 
                                    const std::set<uint64_t>& hash_set) {
    for (std::set<uint64_t>::const_iterator iter = hash_set.begin();
         iter != hash_set.end(); iter++) { 
        uint64_t hash_value = *iter;
        int idx = hash_value % registers_len;
        uint8_t first_one_bit = __builtin_ctzl(hash_value >> HLL_COLUMN_PRECISION) + 1;     
        registers[idx] = std::max((uint8_t)registers[idx], first_one_bit);
    }    
}

template <typename T>
void FieldHllUnionAggreator<T>::finalize_one_merge(T t) {
    char* buf = (char*)t;
    if (!_has_value) {
        return;
    }
    std::map<int, uint8_t> index_to_value;
    if (_hash64_set.size() > HLL_EXPLICLIT_INT64_NUM 
              || _has_sparse_or_full) {
        HllSetHelper::set_max_register(_registers, HLL_REGISTERS_COUNT, _hash64_set);
        for (int i = 0; i < HLL_REGISTERS_COUNT; i++) {
            if (_registers[i] != 0) { 
                index_to_value[i] = _registers[i];
            }    
        } 
    }    
    int length_value_type_len = sizeof(VarCharField::LengthValueType);
    int sparse_set_len = index_to_value.size() *
                         (sizeof(HllSetResolver::SparseIndexType) 
                         + sizeof(HllSetResolver::SparseValueType))
                         + sizeof(HllSetResolver::SparseLengthValueType); 
    int result_len = 0;

    if (sparse_set_len >= HLL_COLUMN_DEFAULT_LEN) {
        // full set
        HllSetHelper::set_full(buf + length_value_type_len, _registers, 
                               HLL_REGISTERS_COUNT, result_len);
    } else if (index_to_value.size() > 0) {
        // sparse set
        HllSetHelper::set_sparse(buf + length_value_type_len, 
                                 index_to_value, result_len); 
    } else if (_hash64_set.size() > 0) {
        // expliclit set
        HllSetHelper::set_expliclit(buf + length_value_type_len,
                                    _hash64_set, result_len);
    } 
 
    VarCharField::LengthValueType* length_value = (VarCharField::LengthValueType*)buf;
    *length_value = result_len & 0xffff;

    reset();
}
}  // namespace palo
