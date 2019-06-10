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

#ifndef DORIS_BE_SRC_OLAP_FIELD_INFO_H
#define DORIS_BE_SRC_OLAP_FIELD_INFO_H

#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/utils.h"

namespace doris {

// 定义uint24_t类型和需要的一些方法
struct uint24_t {
public:
    uint24_t() {
        memset(data, 0, sizeof(data));
    }

    uint24_t(const uint24_t& value) {
        data[0] = value.data[0];
        data[1] = value.data[1];
        data[2] = value.data[2];
    }

    uint24_t(const int32_t& value) {
        data[0] = static_cast<uint8_t>(value);
        data[1] = static_cast<uint8_t>(value >> 8);
        data[2] = static_cast<uint8_t>(value >> 16);
    }

    uint24_t& operator+=(const uint24_t& value) {
        *this = static_cast<int>(*this) + static_cast<int>(value);
        return *this;
    }

    operator int() const {
        int value = static_cast<uint8_t>(data[0]);
        value += (static_cast<uint32_t>(static_cast<uint8_t>(data[1]))) << 8;
        value += (static_cast<uint32_t>(static_cast<uint8_t>(data[2]))) << 16;
        return value;
    }

    uint24_t& operator=(const int& value) {
        data[0] = static_cast<uint8_t>(value);
        data[1] = static_cast<uint8_t>(value >> 8);
        data[2] = static_cast<uint8_t>(value >> 16);
        return *this;
    }

    uint24_t& operator=(const int64_t& value) {
        data[0] = static_cast<uint8_t>(value);
        data[1] = static_cast<uint8_t>(value >> 8);
        data[2] = static_cast<uint8_t>(value >> 16);
        return *this;
    }

    bool operator==(const uint24_t& value) const {
        return cmp(value) == 0;
    }

    bool operator!=(const uint24_t& value) const {
        return cmp(value) != 0;
    }

    bool operator<(const uint24_t& value) const {
        return cmp(value) < 0;
    }

    bool operator<=(const uint24_t& value) const {
        return cmp(value) <= 0;
    }

    bool operator>(const uint24_t& value) const {
        return cmp(value) > 0;
    }

    bool operator>=(const uint24_t& value) const {
        return cmp(value) >= 0;
    }

    int32_t cmp(const uint24_t& other) const {
        if (data[2] > other.data[2]) {
            return 1;
        } else if (data[2] < other.data[2]) {
            return -1;
        }

        if (data[1] > other.data[1]) {
            return 1;
        } else if (data[1] < other.data[1]) {
            return -1;
        }

        if (data[0] > other.data[0]) {
            return 1;
        } else if (data[0] < other.data[0]) {
            return -1;
        }

        return 0;
    }

private:
    uint8_t data[3];
} __attribute__((packed));

inline std::ostream& operator<<(std::ostream& os, const uint24_t& val) {
    return os;
}

// the sign of integer must be same as fraction
struct decimal12_t {
    decimal12_t() : integer(0), fraction(0) {}
    decimal12_t(int64_t int_part, int32_t frac_part) {
        integer = int_part;
        fraction = frac_part;
    }

    decimal12_t(const decimal12_t& value) {
        integer = value.integer;
        fraction = value.fraction;
    }

    decimal12_t& operator+=(const decimal12_t& value) {
        fraction += value.fraction;
        integer += value.integer;

        if (fraction >= FRAC_RATIO) {
            integer += 1;
            fraction -= FRAC_RATIO;
        } else if (fraction <= -FRAC_RATIO) {
            integer -= 1;
            fraction += FRAC_RATIO;
        }

        // if sign of fraction is different from integer
        if ((fraction != 0) && (integer != 0) && (fraction ^ integer) < 0) {
            bool sign = integer < 0;
            integer += (sign ? 1 : -1);
            fraction += (sign ? -FRAC_RATIO : FRAC_RATIO);
        }

        //OLAP_LOG_WARNING("agg: int=%ld, frac=%d", integer, fraction);
        //_set_flag();
        return *this;
    }

    // call field::copy
    decimal12_t& operator=(const decimal12_t& value) {
        integer = value.integer;
        fraction = value.fraction;
        return *this;
    }

    bool operator<(const decimal12_t& value) const {
        return cmp(value) < 0;
    }

    bool operator<=(const decimal12_t& value) const {
        return cmp(value) <= 0;
    }

    bool operator>(const decimal12_t& value) const {
        return cmp(value) > 0;
    }

    bool operator>=(const decimal12_t& value) const {
        return cmp(value) >= 0;
    }

    bool operator==(const decimal12_t& value) const {
        return cmp(value) == 0;
    }

    bool operator!=(const decimal12_t& value) const {
        return cmp(value) != 0;
    }

    int32_t cmp(const decimal12_t& other) const {
        if (integer > other.integer) {
            return 1;
        } else if (integer == other.integer) {
            if (fraction > other.fraction) {
                return 1;
            } else if (fraction == other.fraction) {
                return 0;
            }
        }

        return -1;
    }

    std::string to_string() {
        char buf[128] = {'\0'};

        if (integer < 0 || fraction < 0) {
            snprintf(buf, sizeof(buf), "-%lu.%09u",
                     std::abs(integer), std::abs(fraction));
        } else {
            snprintf(buf, sizeof(buf), "%lu.%09u",
                     std::abs(integer), std::abs(fraction));
        }

        return std::string(buf);
    }

    OLAPStatus from_string(const std::string& str) {
        integer = 0;
        fraction = 0;
        const char* value_string = str.c_str();
        const char* sign = strchr(value_string, '-');

        if (sign != NULL) {
            if (sign != value_string) {
                return OLAP_ERR_INPUT_PARAMETER_ERROR;
            } else {
                ++value_string;
            }
        }

        const char* sepr = strchr(value_string, '.');
        if ((sepr != NULL && sepr - value_string > MAX_INT_DIGITS_NUM)
                || (sepr == NULL && strlen(value_string) > MAX_INT_DIGITS_NUM)) {
            integer = 999999999999999999;
            fraction = 999999999;
        } else {
            if (sepr == value_string) {
                sscanf(value_string, ".%9d", &fraction);
                integer = 0;
            } else {
                sscanf(value_string, "%18ld.%9d", &integer, &fraction);
            }

            int32_t frac_len = (NULL != sepr) ?
                               MAX_FRAC_DIGITS_NUM - strlen(sepr + 1) : MAX_FRAC_DIGITS_NUM;
            frac_len = frac_len > 0 ? frac_len : 0;
            fraction *= g_power_table[frac_len];
        }

        if (sign != NULL) {
            fraction = -fraction;
            integer = -integer;
        }

        return OLAP_SUCCESS;
    }

    static const int32_t FRAC_RATIO = 1000000000;
    static const int32_t MAX_INT_DIGITS_NUM = 18;
    static const int32_t MAX_FRAC_DIGITS_NUM = 9;

    int64_t integer;
    int32_t fraction;
} __attribute__((packed));

inline std::ostream& operator<<(std::ostream& os, const decimal12_t& val) {
    return os;
}

// 保存Field元信息的结构体
struct FieldInfo {
public:
    // 名称
    std::string name;
    // 数据类型
    FieldType type;
    // 聚集方式
    FieldAggregationMethod aggregation;
    // 长度，单位为字节
    // 除字符串外，其它类型都是确定的
    uint32_t length;
    // 前缀索引长度，如果为0，表示不使用前缀索引，
    // 否则则按照需要设置前缀索引,目前只对字符串起作用。
    uint32_t index_length;
    // 是否是Primary Key
    bool is_key;

    bool has_default_value;
    std::string default_value;

    bool has_referenced_column;
    std::string referenced_column;

    // used to creating decimal data type
    uint32_t precision;
    uint32_t frac;

    bool is_allow_null;
    // 全局唯一id
    uint32_t unique_id;
    // 子列的index
    std::vector<uint32_t> sub_columns;
    // 是否是其他列的子列
    bool is_root_column;

    // is bloom filter column
    bool is_bf_column;
public:
    static std::string get_string_by_field_type(FieldType type);
    static std::string get_string_by_aggregation_type(FieldAggregationMethod aggregation_type);
    static FieldType get_field_type_by_string(const std::string& str);
    static FieldAggregationMethod get_aggregation_type_by_string(const std::string& str);
    static uint32_t get_field_length_by_type(TPrimitiveType::type type, uint32_t string_length);

    OLAPStatus set_default_value(const char* str) {
        default_value = str;

        return OLAP_SUCCESS;
    }

    std::string to_string() const {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "{name='%s' type='%s' aggregation='%s' length=%u is_key=%d "
                 "is_allow_null=%d}",
                 name.c_str(),
                 get_string_by_field_type(type).c_str(),
                 get_string_by_aggregation_type(aggregation).c_str(),
                 length,
                 is_key,
                 is_allow_null);
        return std::string(buf);
    }

    std::string to_json() const {
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "{\"name\":\"%s\",\"type\":\"%s\",\"aggregation\":\"%s\","
                 "\"length\":%u,\"is_key\":%d,\"is_allow_null\":%d}",
                 name.c_str(),
                 get_string_by_field_type(type).c_str(),
                 get_string_by_aggregation_type(aggregation).c_str(),
                 length,
                 is_key,
                 is_allow_null);
        return std::string(buf);
    }
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_FIELD_INFO_H
