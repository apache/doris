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

#include <fcntl.h>
#include <pthread.h>
#include <sys/time.h>
#include <zlib.h>

#include <cstdio>
#include <cstdlib>
#include <exception>
#include <filesystem>
#include <iterator>
#include <limits>
#include <list>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "common/logging.h"
#if defined(__i386) || defined(__x86_64__)
#include "olap/bhp_lib.h"
#endif
#include "olap/olap_common.h"
#include "olap/olap_define.h"

#define TRY_LOCK true

namespace doris {
void write_log_info(char* buf, size_t buf_len, const char* fmt, ...);
static const std::string DELETE_SIGN = "__DORIS_DELETE_SIGN__";

// 用来加速运算
const static int32_t g_power_table[] = {1,      10,      100,      1000,      10000,
                                        100000, 1000000, 10000000, 100000000, 1000000000};

// 计时工具，用于确定一段代码执行的时间，用于性能调优
class OlapStopWatch {
public:
    uint64_t get_elapse_time_us() {
        struct timeval now;
        gettimeofday(&now, 0);
        return (uint64_t)((now.tv_sec - _begin_time.tv_sec) * 1e6 +
                          (now.tv_usec - _begin_time.tv_usec));
    }

    double get_elapse_second() { return get_elapse_time_us() / 1000000.0; }

    void reset() { gettimeofday(&_begin_time, 0); }

    OlapStopWatch() { reset(); }

private:
    struct timeval _begin_time; // 起始时间戳
};

// @brief 切分字符串
// @param base 原串
// @param separator 分隔符
// @param result 切分结果
template <typename T>
Status split_string(const std::string& base, const T separator, std::vector<std::string>* result) {
    if (!result) {
        return Status::OLAPInternalError(OLAP_ERR_OTHER_ERROR);
    }

    // 处理base为空的情况
    // 在删除功能中，当varchar类型列的过滤条件为空时，会出现这种情况
    if (base.size() == 0) {
        result->push_back("");
        return Status::OK();
    }

    size_t offset = 0;
    while (offset < base.length()) {
        size_t next = base.find(separator, offset);
        if (next == std::string::npos) {
            result->push_back(base.substr(offset));
            break;
        } else {
            result->push_back(base.substr(offset, next - offset));
            offset = next + 1;
        }
    }

    return Status::OK();
}

template <typename T>
void _destruct_object(const void* obj, void*) {
    delete ((const T*)obj);
}

template <typename T>
void _destruct_array(const void* array, void*) {
    delete[]((const T*)array);
}

// 根据压缩类型的不同，执行压缩。dest_buf_len是dest_buf的最大长度，
// 通过指针返回的written_len是实际写入的长度。
Status olap_compress(const char* src_buf, size_t src_len, char* dest_buf, size_t dest_len,
                     size_t* written_len, OLAPCompressionType compression_type);

Status olap_decompress(const char* src_buf, size_t src_len, char* dest_buf, size_t dest_len,
                       size_t* written_len, OLAPCompressionType compression_type);

// 计算adler32的包装函数
// 第一次使用的时候第一个参数传宏ADLER32_INIT, 之后的调用传上次计算的结果
#define ADLER32_INIT adler32(0L, Z_NULL, 0)
uint32_t olap_adler32(uint32_t adler, const char* buf, size_t len);

// CRC32仅仅用在RowBlock的校验，性能优异
#define CRC32_INIT 0xFFFFFFFF
uint32_t olap_crc32(uint32_t crc32, const char* buf, size_t len);

// 获取系统当前时间，并将时间转换为字符串
Status gen_timestamp_string(std::string* out_string);

enum ComparatorEnum {
    COMPARATOR_LESS = 0,
    COMPARATOR_LARGER = 1,
};

// 处理comparator functor处理过程中出现的错误
class ComparatorException : public std::exception {
public:
    virtual const char* what() const throw() {
        return "exception happens when doing binary search.";
    }
};

// iterator offset，用于二分查找
using iterator_offset_t = size_t;

class BinarySearchIterator : public std::iterator_traits<iterator_offset_t*> {
public:
    BinarySearchIterator() : _offset(0u) {}
    explicit BinarySearchIterator(iterator_offset_t offset) : _offset(offset) {}

    iterator_offset_t operator*() const { return _offset; }

    BinarySearchIterator& operator++() {
        ++_offset;
        return *this;
    }

    BinarySearchIterator& operator--() {
        --_offset;
        return *this;
    }

    BinarySearchIterator& operator-=(size_t step) {
        _offset = _offset - step;
        return *this;
    }

    BinarySearchIterator& operator+=(size_t step) {
        _offset = _offset + step;
        return *this;
    }

    bool operator!=(const BinarySearchIterator& iterator) {
        return this->_offset != iterator._offset;
    }

private:
    iterator_offset_t _offset;
};

int operator-(const BinarySearchIterator& left, const BinarySearchIterator& right);

// 不用sse4指令的crc32c的计算函数
unsigned int crc32c_lut(char const* b, unsigned int off, unsigned int len, unsigned int crc);

bool check_datapath_rw(const std::string& path);

Status read_write_test_file(const std::string& test_file_path);

//转换两个list
template <typename T1, typename T2>
void static_cast_assign_vector(std::vector<T1>* v1, const std::vector<T2>& v2) {
    if (nullptr != v1) {
        //GCC3.4的模板展开貌似有问题， 这里如果使用迭代器会编译失败
        for (size_t i = 0; i < v2.size(); i++) {
            v1->push_back(static_cast<T1>(v2[i]));
        }
    }
}

// 打印Errno
class Errno {
public:
    // 返回Errno对应的错误信息,线程安全
    static const char* str();
    static const char* str(int no);
    static int no();

private:
    static const int BUF_SIZE = 256;
    static __thread char _buf[BUF_SIZE];
};

#define ENDSWITH(str, suffix) ((str).rfind(suffix) == (str).size() - strlen(suffix))

// 检查int8_t, int16_t, int32_t, int64_t的值是否溢出
template <typename T>
bool valid_signed_number(const std::string& value_str) {
    char* endptr = nullptr;
    errno = 0;
    int64_t value = strtol(value_str.c_str(), &endptr, 10);

    if ((errno == ERANGE && (value == LONG_MAX || value == LONG_MIN)) ||
        (errno != 0 && value == 0) || endptr == value_str || *endptr != '\0') {
        return false;
    }

    if (value < std::numeric_limits<T>::min() || value > std::numeric_limits<T>::max()) {
        return false;
    }

    return true;
}

template <>
bool valid_signed_number<int128_t>(const std::string& value_str);

// 检查uint8_t, uint16_t, uint32_t, uint64_t的值是否溢出
template <typename T>
bool valid_unsigned_number(const std::string& value_str) {
    if (value_str[0] == '-') {
        return false;
    }

    char* endptr = nullptr;
    errno = 0;
    uint64_t value = strtoul(value_str.c_str(), &endptr, 10);

    if ((errno == ERANGE && (value == ULONG_MAX)) || (errno != 0 && value == 0) ||
        endptr == value_str || *endptr != '\0') {
        return false;
    }

    if (value < std::numeric_limits<T>::min() || value > std::numeric_limits<T>::max()) {
        return false;
    }

    return true;
}

bool valid_decimal(const std::string& value_str, const uint32_t precision, const uint32_t frac);

// Validate for date/datetime roughly. The format is 'yyyy-MM-dd HH:mm:ss'
// TODO: support 'yyyy-MM-dd HH:mm:ss.SSS'
bool valid_datetime(const std::string& value_str, const uint32_t scale);

bool valid_bool(const std::string& value_str);

// Util used to get string name of thrift enum item
#define EnumToString(enum_type, index, out)                 \
    do {                                                    \
        std::map<int, const char*>::const_iterator it =     \
                _##enum_type##_VALUES_TO_NAMES.find(index); \
        if (it == _##enum_type##_VALUES_TO_NAMES.end()) {   \
            out = "NULL";                                   \
        } else {                                            \
            out = it->second;                               \
        }                                                   \
    } while (0)

struct RowLocation {
    RowLocation() : segment_id(0), row_id(0) {}
    RowLocation(uint32_t sid, uint32_t rid) : segment_id(sid), row_id(rid) {}
    RowLocation(RowsetId rsid, uint32_t sid, uint32_t rid)
            : rowset_id(rsid), segment_id(sid), row_id(rid) {}
    RowsetId rowset_id;
    uint32_t segment_id;
    uint32_t row_id;
};

} // namespace doris
