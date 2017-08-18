// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BDG_PALO_BE_SRC_OLAP_FIELD_H
#define BDG_PALO_BE_SRC_OLAP_FIELD_H

#include <math.h>
#include <stdio.h>

#include <limits>
#include <sstream>
#include <string>

#include "gen_cpp/AgentService_types.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/utils.h"
#include "util/mem_util.hpp"
#include "olap/column_file/bloom_filter.hpp"
#include "common/config.h"
#include "runtime/string_value.h"
#include "runtime/mem_pool.h"

namespace palo {

const static int HLL_COLUMN_PRECISION = 14;
const static int HLL_EXPLICLIT_INT64_NUM = 160;
const static int HLL_REGISTERS_COUNT = 16384;
// regisers (2^14) + 1 (type)
const static int HLL_COLUMN_DEFAULT_LEN = 16385;

class AggregateFunctions;
 
// 定义uint24_t类型和需要的一些方法
struct uint24_t {
public:
    uint24_t() {
        memset(data, 0, sizeof(data));
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

private:
    uint8_t data[3];
} __attribute__((packed));

struct decimal12_t {
    decimal12_t() : integer(0), fraction(0) {}
    decimal12_t(int64_t int_part, int32_t frac_part) {
        integer = int_part;
        fraction = frac_part;
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

        if (fraction * integer < 0) {
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

    bool operator>(const decimal12_t& value) const {
        return cmp(value) > 0;
    }

    bool operator>=(const decimal12_t& value) const {
        return cmp(value) >= 0;
    }

    bool operator==(const decimal12_t& value) const {
        return cmp(value) == 0;
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

    std::string to_buf() {
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

// 聚集方法的纯虚模板基类
template <typename T>
class FieldAggregator {
public:
    // 聚集结果存储在left上
    virtual void operator()(T left, const T right, uint32_t length) = 0;

    virtual ~FieldAggregator() {}

    virtual void finalize_one_merge(T t) {}
};

// 什么都不做
template <typename T>
class FieldNoneAggregator : public FieldAggregator<T> {
public:
    void operator()(T left, const T right, uint32_t length) {}
};

// MIN聚集，两个值中取最小的一个
template <typename T>
class FieldMinAggregator : public FieldAggregator<T> {
public:
    void operator()(T left, const T right, uint32_t length) {
        *left > *right ? *left = *right : *left;
    }
};

// MAX聚集，两个值中取最大的一个
template <typename T>
class FieldMaxAggregator : public FieldAggregator<T> {
public:
    void operator()(T left, const T right, uint32_t length) {
        *right > *left ? *left = *right : *left;
    }
};

// SUM聚集，将右值累加到左值中
template <typename T>
class FieldAddAggregator : public FieldAggregator<T> {
public:
    void operator()(T left, const T right, uint32_t length) {
        *left += *right;
    }
};

// REPLACE聚集，使用右值替换左值
template <typename T>
class FieldReplaceAggregator : public FieldAggregator<T> {
public:
    void operator()(T left, const T right, uint32_t length) {
        memcpy(left, right, length);
    }
};


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

    std::string to_buf() const {
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

// Field的基类，定义接口，同时也提供创建特定类型的Field实例的静态方法
// Field内部参数为Field*的方法都要求实例类型和当前类型一致，否则会产生无法预知的错误
// 出于效率的考虑，大部分函数实现均没有对参数进行检查
class Field {
public:
    // 使用FieldInfo创建一个Field对象的实例
    // 根据类型的不同，使用不同的类模板参数或者子类
    // 对于没有预料到的类型，会返回NULL
    static Field* create(const FieldInfo& field_info);
    static Field* create_by_type(const FieldType& field_type);

    Field() {
        _buf = NULL;
        _is_allocated = false;
        _overhead = 0;
        _length_ptr = &_length;
    }
    virtual ~Field() {
        if (_is_allocated) {
            SAFE_DELETE_ARRAY(_field_buf);
        }
    }

    // 初始化Field
    virtual OLAPStatus init() = 0;

    virtual bool allocate() {
        _is_allocated = true;

        if (NULL == (_field_buf = new(std::nothrow) char[_buf_size + sizeof(char)])) {
            OLAP_LOG_FATAL("failed to malloc _field_buf. [size=%ld]", _buf_size + sizeof(char));
            return false;
        }
        memset(_field_buf, 0, _buf_size + sizeof(char));
        _is_null = _field_buf;
        _buf = _field_buf + 1;

        return true;
    }

    inline void set_buf_size(size_t size) {
        _buf_size = size;
    }

    inline void set_string_length(uint16_t len) {
       _length = len; 
    } 

    inline uint16_t get_string_length() const{
        return _length;  
    }

    inline size_t get_buf_size() const{
        return _buf_size;
    }

    // 判断两个Field是否相等
    inline bool equal(const Field* field);

    // 返回-1，0，1，分别代表当前field小于，等于，大于传入参数中的field
    inline int cmp(const Field* field) const;

    inline int index_cmp(const Field* field) const;

    // 实际的比较方法的实现, 直接调用会有虚函数开销
    virtual int real_cmp(const Field* field) const = 0;

    // 调用对应的聚集方法
    // 具体的行为取决于使用上面定义的哪一种聚集方法
    // 调用方为左值，传入的为右值
    virtual void aggregate(const Field* field) = 0;
    
    virtual void finalize_one_merge() = 0;

    virtual FieldAggregationMethod get_aggregation_method() = 0;

    // 返回当前字段的长度, 单位是字节
    inline size_t size() const {
        return *_length_ptr + _overhead;
    }

    inline size_t field_size() const {
        //add the byte occupied by _is_null
        return size() + sizeof(char);
    }

    // 返回作为index的长度，由于有前缀索引的存在, 所以可能和实际字段的长度不同
    virtual size_t index_size() const {
        return _index_length;
    }

    // 返回当前字段在MySQL中的长度, 单位是字节
    virtual size_t mysql_size() const {
        return size();
    }

    // 从另一个field对象中拷贝值到内部指向的buffer中
    inline void copy(const Field* field);

    // 将内部的value转成string输出
    // 没有考虑实现的性能，仅供DEBUG使用
    virtual std::string to_string() const = 0;
    virtual std::string to_buf() const = 0;

    // 把当前指向的buf中的数据转为MySQL的格式
    // 由于目前所有Field类型的长度都和MySQL一致, 因此实现原地转换以避免多数情况下的memcpy
    inline void to_mysql();

    // 转为内部存储格式并写入buf
    virtual void to_storage(char* buf) const = 0;
    // 转为索引格式并写入buf，限定输出长度
    virtual void to_index(char* buf) = 0;

    // 转为MySQL格式并写入buf
    virtual void to_mysql(char* buf) const = 0;

    // 从传入的字符串反序列化field的值
    // 参数必须是一个\0结尾的字符串
    virtual OLAPStatus from_string(const std::string& value_string) = 0;

    // 从数据文件中读取一个值，并写入内部buffer
    virtual void from_storage(const char* buf) = 0;

    // attach到一段buf
    virtual void attach_buf(char* buf) {
        _buf = buf;
    }

    virtual void attach_field(char* field) {
        _is_null = field;
        _buf = field + sizeof(char);
    }

    // 获取指向内部buf的指针
    char* buf() const {
        return _buf;
    }

    char* get_null() const {
        return _is_null;
    }

    bool is_null() const {
        return *reinterpret_cast<bool*>(_is_null);
    }

    void set_null() {
        _is_null[0] |= 1;
    }

    void set_not_null() {
        _is_null[0] &= ~1;
    }

    FieldType type() const {
        return _field_type;
    }

    virtual void set_to_max() {}
    virtual void set_to_min() {}
    virtual bool is_min() {
        return false;
    }

protected:
    char* _is_null;
    // 存储数据的Buffer
    char* _buf;
    char* _field_buf;//store _buf and _is_null
    // buf是否由内部分配管理
    bool _is_allocated;
    // Field的长度，单位为字节
    uint16_t _length;
    uint16_t* _length_ptr;
    uint16_t _overhead;
    // Field的最大长度，单位为字节，通常等于length， 变长字符串不同
    size_t _index_length;
    //
    size_t _buf_size;
    // Field类型
    FieldType _field_type;
};

// 继承自Field的基础模板类
template <typename T>
class BaseField : public Field {
public:
    BaseField(const FieldInfo& field_info) {
        _length = sizeof(T);
        _buf_size = _length;
        _index_length = field_info.index_length;
        _aggregation = field_info.aggregation;
        _aggregator = NULL;
        _buf = NULL;
    }

    virtual ~BaseField() {
        SAFE_DELETE(_aggregator);
    }

    virtual OLAPStatus init();

    virtual int real_cmp(const Field* field) const {
        T left = *_value();
        T right = *_value(field->buf());

        if (left > right) {
            return 1;
        } else if (right > left) {
            return -1;
        } else {
            return 0;
        }
    }

    virtual FieldAggregationMethod get_aggregation_method() {
        return _aggregation;
    }

    virtual void aggregate(const Field* field) {
        (*_aggregator)(_value(), _value(field->buf()), field->size());
    }

    virtual void finalize_one_merge() {
        _aggregator->finalize_one_merge(_value());
    } 

    virtual std::string to_string() const;
    virtual std::string to_buf() const;
    virtual void to_storage(char* buf) const {
        memcpy(buf, _buf, sizeof(T));
    }
    virtual void to_index(char* index_buf) {
        memcpy(index_buf, _is_null, sizeof(char));
        to_storage(index_buf + sizeof(char));
    }
    virtual void to_mysql(char* buf) const {
        memcpy(buf, _buf, sizeof(T));
    }

    virtual void from_storage(const char* buf) {
        memcpy(_buf, buf, sizeof(T));
    }
    virtual OLAPStatus from_string(const std::string& value_string) {return OLAP_SUCCESS;}

protected:
    T* _value() const {
        return reinterpret_cast<T*>(_buf);
    }
    static T* _value(char* buf) {
        return reinterpret_cast<T*>(buf);
    }

protected:
    // 聚集方法
    FieldAggregationMethod _aggregation;
    // 聚集方法对应的实现类
    FieldAggregator<T*>* _aggregator;
};

// 实现对所有数值类Field类型的处理
// 继承自BaseField
template <typename T>
class NumericField : public BaseField<T> {
public:
    NumericField(const FieldInfo& field_info) : BaseField<T>(field_info) {};
    virtual void set_to_max() {
        T max_value = std::numeric_limits<T>::max();
        memcpy(this->_buf, reinterpret_cast<const char*>(&max_value), sizeof(T));
    };
    virtual void set_to_min() {
        T min_value = std::numeric_limits<T>::min();
        memcpy(this->_buf, reinterpret_cast<const char*>(&min_value), sizeof(T));
    };
    virtual bool is_min() {
        T min_value = std::numeric_limits<T>::min();
        if (*(this->_value()) == min_value) {
            return true;
        } else {
            return false;
        }
    }

};

// 实现对CHAR类型的处理
// 继承自BaseField<char>，可以复用大部分方法
class CharField : public BaseField<char> {
public:
    CharField(const FieldInfo& field_info)
        : BaseField<char>(field_info) {
        _length = field_info.length;
        _buf_size = _length;
    }

    virtual ~CharField() {}

    virtual OLAPStatus init();

    virtual std::string to_string() const;
    virtual std::string to_buf() const;
    virtual OLAPStatus from_string(const std::string& value_string);
    virtual void from_storage(const char* buf) {
        memcpy(_buf, buf, _length);
    }
    virtual void to_storage(char* buf) const {
        memcpy(buf, _buf, _length);
    }
    virtual void to_mysql(char* buf) const {
        memcpy(buf, _buf, _length);
    }

    virtual void set_to_max() {
        if (NULL != _buf) {
            memset(_buf, 0xff, _length);
        }
    }
    virtual void set_to_min() {
        if (NULL != _buf) {
            memset(_buf, 0, _length);
        }
    }

    virtual bool is_min() {
        if (_buf == NULL || strlen(_buf) == 0) {
            return true;
        } else {
            return false;
        }
    }

    int real_cmp(const Field* field) const {
        uint16_t field_len = field->get_string_length();
        uint16_t cmp_len = std::min(_length, field_len);
        int res = strncmp(_buf, field->buf(), cmp_len);

        if (res > 0) {
            return 1;
        } else if (0 == res) {
            if (_length > field_len) {
                return 1;
            } else if (_length < field_len) {
                return -1;
            }
            return 0;
        } else {
            return -1;
        }
    }
};

class VarCharField : public CharField {
public:
    VarCharField(const FieldInfo& field_info)
        : CharField(field_info) {
        _buf_size = field_info.length;
        _overhead = sizeof(uint16_t);
    }

    virtual void attach_field(char* buf) {
        _is_null = buf;
        _buf = buf + sizeof(char);
        _length_ptr = reinterpret_cast<LengthValueType*>(_buf);
    }

    virtual void attach_buf(char* buf) {
        _buf = buf;
        _length_ptr = reinterpret_cast<LengthValueType*>(_buf);
    }

    virtual bool allocate() {
        if (Field::allocate()) {
            attach_buf(_buf);
            *_length_ptr = 0;
            return true;
        }

        return false;
    }

    // 重写size，字符串的长度均使用动态获取

    // 均需要另外实现
    virtual std::string to_string() const;
    virtual std::string to_buf() const;
    virtual OLAPStatus from_string(const std::string& value_string);

    // 重点是，如何知道该拷贝多长，需要解析头
    virtual void from_storage(const char* buf) {
        size_t copy_length = _get_length_from_buf(buf) + sizeof(LengthValueType);
        copy_length = _buf_size < copy_length ? _buf_size : copy_length;
        memcpy(_buf, buf, copy_length);
        _set_length_to_buf(_buf, copy_length - sizeof(LengthValueType));
    }

    void from_storage_length(const char* buf, int length) {
        size_t copy_length = length + sizeof(LengthValueType);
        copy_length = _buf_size < copy_length ? _buf_size : copy_length;
        memcpy(_buf + sizeof(LengthValueType), buf, copy_length - sizeof(LengthValueType));
        _set_length_to_buf(_buf, length);
    }


    virtual void to_storage(char* buf) const {
        memcpy(buf, _buf, size());
    }

    virtual void to_index(char* index_buf) {
        
        size_t copy_size = size() < _index_length?
                           size() : _index_length;
        // 先清零，再拷贝
        memset(index_buf, 0, _index_length + sizeof(char));
        memcpy(index_buf, _is_null, sizeof(char));
        memcpy(index_buf + sizeof(char), _buf, copy_size);
        _set_length_to_buf(index_buf + sizeof(char), copy_size - sizeof(LengthValueType));
    }

    virtual void to_mysql(char* buf) const {
        memcpy(buf, _buf, size());
    }

    virtual void set_to_max() {
        if (NULL != _buf) {
            _set_length_to_buf(_buf, 1);
            memset(_buf + sizeof(LengthValueType), 0xFF, 1);
        }
    }

    virtual void set_to_min() {
        if (NULL != _buf) {
            _set_length_to_buf(_buf, 0);
        }
    }

    virtual bool is_min() {
        if (NULL == _buf || 0 == _get_length_from_buf(_buf)) {
            return true;
        } else {
            return false;
        }
    }

    int real_cmp(const Field* field) const {
        return varchar_cmp(_buf + sizeof(LengthValueType),
                           size() - sizeof(LengthValueType),
                           field->buf() + sizeof(LengthValueType),
                           field->size() - sizeof(LengthValueType));
    }

    static int varchar_cmp(const char* buf1, size_t length1, const char* buf2, size_t length2) {
        size_t compare_size = std::min(length1, length2);
        int res = strncmp(buf1, buf2, compare_size);

        if (res > 0) {
            return 1;
        } else if (0 == res) {
            if (length1 > length2) {
                return 1;
            } else if (length1 == length2) {
                return 0;
            }
        }

        return -1;
    }

    int short_key_cmp(const Field* field) const {
        // 如果field的实际长度比short key长，则仅比较前缀，确保相同short key的所有block都被扫描，
        // 否则，可以直接比较short key和field
        int res = 0;

        if (field->size() > _index_length) {
            int compare_size = _index_length - sizeof(LengthValueType);
            res = strncmp(_buf + sizeof(LengthValueType),
                          field->buf() + sizeof(LengthValueType), compare_size);
        } else {
            res = real_cmp(field);
        }

        return res;
    }

    typedef uint32_t OffsetValueType;
    typedef uint16_t LengthValueType;
protected:
    inline LengthValueType _get_length_from_buf(const char* buf) {
        return *reinterpret_cast<const LengthValueType*>(buf);
    }

    inline void _set_length_to_buf(char* buf, LengthValueType length) {
        *reinterpret_cast<LengthValueType*>(buf) = length;
    }
};

// help parse hll set
class HllSetResolver {
        
public:
    HllSetResolver() : _buf_ref(nullptr),
                       _buf_len(0),
                       _set_type(HLL_DATA_EMPTY),
                       _full_value_position(nullptr),
                       _expliclit_value(nullptr),
                       _expliclit_num(0) {
    }

    typedef uint8_t SetTypeValueType;
    typedef uint8_t ExpliclitLengthValueType;
    typedef int32_t SparseLengthValueType;
    typedef uint16_t SparseIndexType;
    typedef uint8_t SparseValueType;

    // only save pointer 
    void init(char* buf, int len){
        this->_buf_ref = buf;
        this->_buf_len = len;
    }

    // hll set type
    HllDataType get_hll_data_type() { 
        return _set_type;
    };
    
    // expliclit value num
    int get_expliclit_count() { 
        return (int)_expliclit_num; 
    };
    
    // get expliclit index value 64bit
    uint64_t get_expliclit_value(int index) {
        if (index >= _expliclit_num) {
            return -1;
        }
        return _expliclit_value[index];
    };

    // get expliclit index value 64bit
    char* get_expliclit_value() { 
        return (char*)_expliclit_value; 
    };

    // get full register value
    char* get_full_value() { 
        return _full_value_position; 
    };

    // get sparse (index, value) count
    int get_sparse_count() { 
        return (int)*_sparse_count; 
    };

    // get (index, value) map
    std::map<SparseIndexType, SparseValueType>& get_sparse_map() { 
        return _sparse_map; 
    };
    
    // parse set , call after copy() or init()
    void parse();
   
    // fill registers with set
    void fill_registers(char* registers, int len);

    // fill map with set
    void fill_index_to_value_map(std::map<int, uint8_t>* index_to_value, int len);
    
    // fill hash map
    void fill_hash64_set(std::set<uint64_t>* hash_set);
    
private :
    
    char* _buf_ref;    // set
    int _buf_len;      // set len
    HllDataType _set_type;        //set type
    char* _full_value_position;
    uint64_t* _expliclit_value;
    ExpliclitLengthValueType _expliclit_num;
    std::map<SparseIndexType, SparseValueType> _sparse_map;
    SparseLengthValueType* _sparse_count;
};

class HllSetHelper {

public:

    static void set_sparse(char *result,const std::map<int, uint8_t>& index_to_value, int& len);

    static void set_expliclit(char* result, const std::set<uint64_t>& hash_value_set, int& len);

    static void set_full(char* result, const char* registers, const int set_len, int& len);

    static void set_full(char* result, const std::map<int, uint8_t>& index_to_value, 
                         const int set_len, int& len);

    static void set_max_register(char *registers,
                                 int registers_len, 
                                 const std::set<uint64_t>& hash_set);
};

// 通过varchar的变长编码方式实现hll集合
// 实现hll列中间计算结果的处理
// empty 空集合
// expliclit 存储64位hash值的集合
// sparse 存储hll非0的register
// full  存储全部的hll register
// empty -> expliclit -> sparse -> full 四种类型的转换方向不可逆
// 第一个字节存放hll集合的类型 0:empty 1:expliclit 2:sparse 3:full
// 已决定后面的数据怎么解析
class HllField : public VarCharField {
  
public:
    
    HllField(const FieldInfo& field_info)
    : VarCharField(field_info) {
    }
    
    void parse() {
        resolver.init(_buf + sizeof(VarCharField::LengthValueType),*reinterpret_cast<const LengthValueType*>(_buf));
        resolver.parse();
    }
    // hll set type
    HllDataType get_hll_data_type() { return resolver.get_hll_data_type();};
    
    // expliclit value num
    int get_expliclit_count() { return resolver.get_expliclit_count(); };
    
    // get expliclit index value 64bit
    uint64_t get_expliclit_value(int index) {
        return resolver.get_expliclit_value(index);
    };
    
    char* get_expliclit_value() {
        return resolver.get_expliclit_value();
    };
    
    // get full register value
    char* get_full_value() { return resolver.get_full_value(); };
    
    // get sparse bitset
    int get_sparse_count() { return resolver.get_sparse_count();};
    
    // get sparse register value
    std::map<HllSetResolver::SparseIndexType, HllSetResolver::SparseValueType>& get_sparse_map() { 
        return resolver.get_sparse_map();
    };
    
    HllSetResolver* getHllSetResolver() { return &resolver;};
    
private :
    
    HllSetResolver resolver;
};
 

template <typename T>
class FieldHllUnionAggreator : public FieldAggregator<T> {

public: 

    FieldHllUnionAggreator() {
        reset();
    } 
    
    void operator()(T left, const T right, uint32_t length) {
        char* left_buf =  (char*)left;
        char* right_buf = (char*)right;
        // parse set
        if (!_has_value) {
            fill_set(left_buf);
            _has_value = true;
        }
        fill_set(right_buf);
    }

    void fill_set(char* buf) {
        HllSetResolver resolver; 
        int length_value_type_len = sizeof(VarCharField::LengthValueType);
        resolver.init(buf + length_value_type_len,
                           *reinterpret_cast<const VarCharField::LengthValueType*>(buf));
        resolver.parse();
        if (resolver.get_hll_data_type() == HLL_DATA_EXPLICIT) {
            // expliclit set
            resolver.fill_hash64_set(&_hash64_set);
        } else if (resolver.get_hll_data_type() != HLL_DATA_EMPTY) {
            // full or sparse
            _has_sparse_or_full = true;
            resolver.fill_registers(_registers, HLL_REGISTERS_COUNT);
        } else {
            // empty
        }
    }

    virtual void finalize_one_merge(T t);

    void reset() {
        memset(_registers, 0, HLL_REGISTERS_COUNT);
        _hash64_set.clear();
        _has_value = false;
        _has_sparse_or_full = false;
    }

private:

    bool _has_value;
    bool _has_sparse_or_full;
    char _registers[HLL_REGISTERS_COUNT];
    std::set<uint64_t> _hash64_set;
};

class DateTimeField : public BaseField<long> {                
    public:
    DateTimeField(const FieldInfo& field_info) : BaseField<long>(field_info) {}

    virtual OLAPStatus from_string(const std::string& value_string) {
        tm time_tm;
        char* res = strptime(value_string.c_str(), "%Y-%m-%d %H:%M:%S", &time_tm);

        if (NULL != res) {
            long value = ((time_tm.tm_year + 1900) * 10000L
                          + (time_tm.tm_mon + 1) * 100L
                          + time_tm.tm_mday) * 1000000L
                         + time_tm.tm_hour * 10000L
                         + time_tm.tm_min * 100L
                         + time_tm.tm_sec;
            from_storage(reinterpret_cast<const char*>(&value));
        } else {
            // 1400 - 01 - 01
            *_value() = 14000101000000;
        }

        return OLAP_SUCCESS;
    }

    virtual std::string to_string() const {
        tm time_tm;
        long tmp = *_value();
        long part1 = (tmp / 1000000L);
        long part2 = (tmp - part1 * 1000000L);

        time_tm.tm_year = static_cast<int>((part1 / 10000L) % 10000) - 1900;
        time_tm.tm_mon = static_cast<int>((part1 / 100) % 100) - 1;
        time_tm.tm_mday = static_cast<int>(part1 % 100);

        time_tm.tm_hour = static_cast<int>((part2 / 10000L) % 10000);
        time_tm.tm_min = static_cast<int>((part2 / 100) % 100);
        time_tm.tm_sec = static_cast<int>(part2 % 100);

        char buf[20] = {'\0'};
        strftime(buf, 20, "%Y-%m-%d %H:%M:%S", &time_tm);
        return std::string(buf);
    }

    virtual std::string to_buf() const {
        tm time_tm;
        long tmp = *_value();
        long part1 = (tmp / 1000000L);
        long part2 = (tmp - part1 * 1000000L);

        time_tm.tm_year = static_cast<int>((part1 / 10000L) % 10000) - 1900;
        time_tm.tm_mon = static_cast<int>((part1 / 100) % 100) - 1;
        time_tm.tm_mday = static_cast<int>(part1 % 100);

        time_tm.tm_hour = static_cast<int>((part2 / 10000L) % 10000);
        time_tm.tm_min = static_cast<int>((part2 / 100) % 100);
        time_tm.tm_sec = static_cast<int>(part2 % 100);

        char buf[20] = {'\0'};
        strftime(buf, 20, "%Y-%m-%d %H:%M:%S", &time_tm);
        return std::string(buf);
    }

    virtual void set_to_max() {
        // 设置为最大时间，其含义为：9999-12-31 23:59:59
        long value = 99991231235959L;
        from_storage(reinterpret_cast<const char*>(&value));
    }
    virtual void set_to_min() {
        long value = 101000000;
        from_storage(reinterpret_cast<const char*>(&value));
    }
    virtual bool is_min() {
        long value = 101000000;
        if (*(reinterpret_cast<long*>(_buf)) == value) {
            return true;
        } else {
            return false;
        }
    }

};

// 实现对Date类型的处理
// MySQL内部使用3个字节存储
// 具体格式为: year * 16 * 32 + month * 32 + day
// 这里也直接采用MySQL的存储格式
// 继承自BaseFiled<uint24_t>
class DateField : public BaseField<uint24_t> {
public:
    DateField(const FieldInfo& field_info) : BaseField<uint24_t>(field_info) {}

    virtual OLAPStatus from_string(const std::string& value_string) {
        tm time_tm;
        char* res = strptime(value_string.c_str(), "%Y-%m-%d", &time_tm);

        if (NULL != res) {
            int value = (time_tm.tm_year + 1900) * 16 * 32
                        + (time_tm.tm_mon + 1) * 32
                        + time_tm.tm_mday;
            *_value() = value;
        } else {
            // 1400 - 01 - 01
            *_value() = 716833;
        }

        return OLAP_SUCCESS;
    }

    virtual std::string to_string() const {
        tm time_tm;
        int value = *_value();
        memset(&time_tm, 0, sizeof(time_tm));
        time_tm.tm_mday = static_cast<int>(value & 31);
        time_tm.tm_mon = static_cast<int>(value >> 5 & 15) - 1;
        time_tm.tm_year = static_cast<int>(value >> 9) - 1900;
        char buf[20] = {'\0'};
        strftime(buf, sizeof(buf), "%Y-%m-%d", &time_tm);
        return std::string(buf);
    }

    virtual std::string to_buf() const {
        tm time_tm;
        int value = *_value();
        memset(&time_tm, 0, sizeof(time_tm));
        time_tm.tm_mday = static_cast<int>(value & 31);
        time_tm.tm_mon = static_cast<int>(value >> 5 & 15) - 1;
        time_tm.tm_year = static_cast<int>(value >> 9) - 1900;
        char buf[20] = {'\0'};
        strftime(buf, sizeof(buf), "%Y-%m-%d", &time_tm);
        return std::string(buf);
    }

    virtual void set_to_max() {
        int value = 9999 * 16 * 32
                    + 12 * 32
                    + 31;
        *_value() = value;
    }

    virtual void set_to_min() {
        int value = 0 * 16 * 32 + 1 * 32 + 1;
        *_value() = value;
    }

    virtual bool is_min() {
        if (33 == *_value()) {
            return true;
        } else {
            return false;
        }
    }

};

// 实现对DISCRETE DOUBLE类型的处理
// 由于内部存储使用int64实现
// 因此继承自BaseFiled<int64_t>
class DiscreteDoubleField : public NumericField<int64_t> {
public:
    DiscreteDoubleField(const FieldInfo& field_info) : NumericField<int64_t>(field_info) {};

    size_t mysql_size() const {
        return sizeof(double);
    }

    // 把数据转为double
    double to_double() const {
        return *_value() / RATIO;
    }

    virtual void to_mysql(char* buf) const {
        double value = to_double();
        memcpy(buf, &value, sizeof(double));
    }

    // 转换为可打印的字符串
    virtual std::string to_string() const {
        double value = *_value() / RATIO;
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%.10f", value);
        return std::string(buf);
    }

    virtual std::string to_buf() const {
        double value = *_value() / RATIO;
        char buf[1024] = {'\0'};
        snprintf(buf, sizeof(buf), "%.10f", value);
        return std::string(buf);
    }

    // 将值从字符串转换到内部buf中
    // 字符串格式是以double的方式表示
    virtual OLAPStatus from_string(const std::string& value_string) {
        double double_val = atof(value_string.c_str());

        if (double_val < DISCRETE_DOUBLE_MIN || double_val > DISCRETE_DOUBLE_MAX) {
            OLAP_LOG_WARNING("value in disrete double is overflow. [value=%.10f]", double_val);
            return OLAP_ERR_INPUT_PARAMETER_ERROR;
        }

        long value = static_cast<long>(double_val * RATIO);
        from_storage(reinterpret_cast<const char*>(&value));
        return OLAP_SUCCESS;
    }
private:
    // 从double转换为int64_t类型时放大的比例
    // 该值决定了小数部分的精度
    static constexpr double RATIO = 1000000.0;
    static constexpr double DISCRETE_DOUBLE_MAX = LONG_MAX / 1000000.0;
    static constexpr double DISCRETE_DOUBLE_MIN = LONG_MIN / 1000000.0;
};

// decimal
class DecimalField: public BaseField<decimal12_t> {
public:
    DecimalField(const FieldInfo& field_info) : BaseField<decimal12_t>(field_info) {
        _precision = field_info.precision;
        _frac = field_info.frac;
        _length = sizeof(int64_t) + sizeof(int32_t);
    };

    virtual void attach_field(char* buf) {
        _is_null = buf;
        _buf = buf + 1;
    }

    virtual void attach_buf(char* buf) {
        _buf = buf;
    }

    bool allocate() {
        if (Field::allocate()) {
            attach_buf(_buf);
            return true;
        }

        return false;
    }

    size_t mysql_size() const {
        return sizeof(int64_t) + sizeof(int32_t);
    }

    // 把数据转为double
    double to_double() const {
        decimal12_t* data_ptr = _value();
        return data_ptr->integer + double(data_ptr->fraction / decimal12_t::FRAC_RATIO);
    }

    virtual OLAPStatus from_string(const std::string& value_string) {
        decimal12_t* data_ptr = _value();
        return data_ptr->from_string(value_string);
    }

    virtual int real_cmp(const Field* field) const {
        decimal12_t* data_ptr = _value();
        decimal12_t* other = reinterpret_cast<decimal12_t*>(field->buf());
        return data_ptr->cmp(*other);
    }

    virtual void from_storage(const char* buf) {
        memcpy(_buf, buf, _length);
    }
    virtual void to_storage(char* buf) const {
        memcpy(buf, _buf, _length);
    }
    virtual void to_mysql(char* buf) const {
        memcpy(buf, _buf, _length);
    }

    virtual void set_to_max() {
        if (NULL != _buf) {
            decimal12_t* data_ptr = _value();
            data_ptr->integer = 999999999999999999;
            data_ptr->fraction = 999999999;
        }
    }
    virtual void set_to_min() {
        if (NULL != _buf) {
            decimal12_t* data_ptr = _value();
            data_ptr->integer = -999999999999999999;
            data_ptr->fraction = -999999999;
        }
    }
    virtual bool is_min() {
        if (NULL == _buf) {
            return true;
        } else if (_value()->integer == -999999999999999999
            && _value()->fraction == -999999999){
            return true;
        } else {
            return false;
        }
    }

private:
    // using fix ratio, 10^9
    static const int32_t INT_STORE_BYTE = 8;
    static const int32_t FRAC_STORE_BYTE = 4;
    static const int32_t STORE_BYTE = INT_STORE_BYTE + FRAC_STORE_BYTE;

    // frac set by user defination
    int32_t _frac;
    // precision set by user defination
    int32_t _precision;
};

// 这里是一个优化的实现，主要是基于一下几点的考虑
// 1. 大部分数据类型的长度都是1 2 4 8
// 2. memcpy对编译期确定长度的拷贝实现会很快, 大概是非定长的10倍左右
// 3. 去掉了之前实现所带来的虚函数调用的开销
// 4. 对CPU的分支预测和指令流水线处理更友好
// 对单版本扫描性能的整体优化效果大概有15%左右
void Field::copy(const Field* field) {

    *_is_null = *(field->get_null());

    if (OLAP_UNLIKELY(OLAP_FIELD_TYPE_VARCHAR == field->type()) || OLAP_UNLIKELY(OLAP_FIELD_TYPE_HLL == field->type())) {
        memory_copy(_buf, field->buf(), field->size());
    } else {
        switch (_length) {
        case 1:
            *_buf = *field->buf();
            break;

        case 2:
            *reinterpret_cast<int16_t*>(_buf) = *reinterpret_cast<int16_t*>(field->buf());
            break;

        case 4:
            *reinterpret_cast<int32_t*>(_buf) = *reinterpret_cast<int32_t*>(field->buf());
            break;

        case 8:
            *reinterpret_cast<int64_t*>(_buf) = *reinterpret_cast<int64_t*>(field->buf());
            break;

        case 16:
            *reinterpret_cast<int128_t*>(_buf) = *reinterpret_cast<int128_t*>(field->buf());
            break;

        default:
            memory_copy(_buf, field->buf(), field->size());
            //OLAP_LOG_DEBUG("++++ copy length: %lu", field->size());
            break;
        }
    }
}

// 类似于上面copy的优化原理，另外还考虑到了作为Key的类型主要是4种UNSIGNED的INT
// 对多版本扫描的整体优化效果大概有15%左右
int Field::cmp(const Field* field) const {
    bool first = is_null();
    bool second = field->is_null();
    if (first == second && 1 == first) {
        return 0;
    } else if (first != second && 0 == first) {
        return 1;
    } else if (first != second && 1 == first) {
        return -1;
    }

    switch (_field_type) {
    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        return reinterpret_cast<const BaseField<uint8_t>*>(this)->
               BaseField<uint8_t>::real_cmp(field);

    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        return reinterpret_cast<const BaseField<uint16_t>*>(this)->
               BaseField<uint16_t>::real_cmp(field);

    case OLAP_FIELD_TYPE_UNSIGNED_INT:
        return reinterpret_cast<const BaseField<uint32_t>*>(this)->
               BaseField<uint32_t>::real_cmp(field);

    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        return reinterpret_cast<const BaseField<uint64_t>*>(this)->
               BaseField<uint64_t>::real_cmp(field);

    case OLAP_FIELD_TYPE_TINYINT:
        return reinterpret_cast<const BaseField<int8_t>*>(this)->
               BaseField<int8_t>::real_cmp(field);

    case OLAP_FIELD_TYPE_SMALLINT:
        return reinterpret_cast<const BaseField<int16_t>*>(this)->
               BaseField<int16_t>::real_cmp(field);

    case OLAP_FIELD_TYPE_INT:
        return reinterpret_cast<const BaseField<int32_t>*>(this)->
               BaseField<int32_t>::real_cmp(field);

    case OLAP_FIELD_TYPE_BIGINT:
        return reinterpret_cast<const BaseField<int64_t>*>(this)->
               BaseField<int64_t>::real_cmp(field);

    case OLAP_FIELD_TYPE_LARGEINT:
        return reinterpret_cast<const BaseField<int128_t>*>(this)->
               BaseField<int128_t>::real_cmp(field);

    default:
        return real_cmp(field);
    }
}

// 类似于上面copy的优化原理，另外还考虑到了作为Key的类型主要是4种UNSIGNED的INT
// 对多版本扫描的整体优化效果大概有15%左右
int Field::index_cmp(const Field* field) const {
    bool first = is_null();
    bool second = field->is_null();
    if (first == second && 1 == first) {
        return 0;
    } else if (first != second && 0 == first) {
        return 1;
    } else if (first != second && 1 == first) {
        return -1;
    }

    switch (_field_type) {
    case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        return reinterpret_cast<const BaseField<uint8_t>*>(this)->
               BaseField<uint8_t>::real_cmp(field);

    case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        return reinterpret_cast<const BaseField<uint16_t>*>(this)->
               BaseField<uint16_t>::real_cmp(field);

    case OLAP_FIELD_TYPE_UNSIGNED_INT:
        return reinterpret_cast<const BaseField<uint32_t>*>(this)->
               BaseField<uint32_t>::real_cmp(field);

    case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        return reinterpret_cast<const BaseField<uint64_t>*>(this)->
               BaseField<uint64_t>::real_cmp(field);

    case OLAP_FIELD_TYPE_TINYINT:
        return reinterpret_cast<const BaseField<int8_t>*>(this)->
               BaseField<int8_t>::real_cmp(field);

    case OLAP_FIELD_TYPE_SMALLINT:
        return reinterpret_cast<const BaseField<int16_t>*>(this)->
               BaseField<int16_t>::real_cmp(field);

    case OLAP_FIELD_TYPE_INT:
        return reinterpret_cast<const BaseField<int32_t>*>(this)->
               BaseField<int32_t>::real_cmp(field);

    case OLAP_FIELD_TYPE_BIGINT:
        return reinterpret_cast<const BaseField<int64_t>*>(this)->
               BaseField<int64_t>::real_cmp(field);

    case OLAP_FIELD_TYPE_LARGEINT:
        return reinterpret_cast<const BaseField<int128_t>*>(this)->
               BaseField<int128_t>::real_cmp(field);

    case OLAP_FIELD_TYPE_VARCHAR:
        return reinterpret_cast<const VarCharField*>(this)->short_key_cmp(field);

    default:
        return real_cmp(field);
    }
}

bool Field::equal(const Field* field) {
    bool first = is_null();
    bool second = field->is_null();
    if (first == second && 1 == first) {
        return true;
    } else if (first != second && 0 == first) {
        return false;
    } else if (first != second && 1 == first) {
        return false;
    }

    return memcmp_sse(_buf, field->buf(), size()) == 0;
}

void Field::to_mysql() {
    if (OLAP_UNLIKELY(_field_type == OLAP_FIELD_TYPE_DISCRETE_DOUBLE)) {
        reinterpret_cast<DiscreteDoubleField*>(this)->DiscreteDoubleField::to_mysql(_buf);
    }
}

}  // namespace palo

#endif // BDG_PALO_BE_SRC_OLAP_FIELD_H
