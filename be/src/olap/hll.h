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

#ifndef DORIS_BE_SRC_OLAP_HLL_H
#define DORIS_BE_SRC_OLAP_HLL_H

#include <math.h>
#include <stdio.h>
#include <set>
#include <map>

#include "olap/olap_common.h"

namespace doris {

const static int HLL_COLUMN_PRECISION = 14;
const static int HLL_EXPLICLIT_INT64_NUM = 160;
const static int HLL_REGISTERS_COUNT = 16384;
// maximum size in byte of serialized HLL: type(1) + registers (2^14)
const static int HLL_COLUMN_DEFAULT_LEN = 16385;

// Hyperloglog distinct estimate algorithm.
// See these papers for more details.
// 1) Hyperloglog: The analysis of a near-optimal cardinality estimation
// algorithm (2007)
// 2) HyperLogLog in Practice (paper from google with some improvements)

// 通过varchar的变长编码方式实现hll集合
// 实现hll列中间计算结果的处理
// empty 空集合
// explicit 存储64位hash值的集合
// sparse 存储hll非0的register
// full  存储全部的hll register
// empty -> explicit -> sparse -> full 四种类型的转换方向不可逆
// 第一个字节存放hll集合的类型 0:empty 1:explicit 2:sparse 3:full
// 已决定后面的数据怎么解析
class HyperLogLog {
public:
    HyperLogLog(): _type(HLL_DATA_EMPTY){
        memset(_registers, 0, HLL_REGISTERS_COUNT);
    }

    explicit HyperLogLog(uint64_t hash_value): _type(HLL_DATA_EXPLICIT) {
        _hash_set.emplace(hash_value);
    }

    explicit HyperLogLog(char* src);

    typedef uint8_t SetTypeValueType;
    typedef int32_t SparseLengthValueType;
    typedef uint16_t SparseIndexType;
    typedef uint8_t SparseValueType;

    // change the _type to HLL_DATA_FULL directly has two reasons:
    // 1. keep behavior consistent with before
    // 2. make the code logic is simple
    void update(const uint64_t hash_value) {
        _type = HLL_DATA_FULL;
        update_registers(_registers, hash_value);
    }

    void merge(const HyperLogLog& other);

    int serialize(char* dest);

    int64_t estimate_cardinality();

    // only for debug
    std::string to_string() {
        switch (_type) {
            case HLL_DATA_EMPTY:
                return {};
            case HLL_DATA_EXPLICIT:
            case HLL_DATA_SPRASE:
            case HLL_DATA_FULL:
                {
                    std::string str {"hash set size: "};
                    str.append(std::to_string(_hash_set.size()));
                    str.append("\ncardinality:\t");
                    str.append(std::to_string(estimate_cardinality()));
                    str.append("\ntype:\t");
                    str.append(std::to_string(_type));
                    return str;
                }
            default:
                return {};
        }
    }

private:
    HllDataType _type;
    char _registers[HLL_REGISTERS_COUNT];
    std::set<uint64_t> _hash_set;

    static void update_registers(char* registers, uint64_t hash_value) {
        // Use the lower bits to index into the number of streams and then
        // find the first 1 bit after the index bits.
        int idx = hash_value % HLL_REGISTERS_COUNT;
        uint8_t first_one_bit = __builtin_ctzl(hash_value >> HLL_COLUMN_PRECISION) + 1;
        registers[idx] = std::max((uint8_t)registers[idx], first_one_bit);
    }

    static void merge_hash_set_to_registers(char* registers, const std::set<uint64_t>& hash_set) {
        for (auto hash_value: hash_set) {
            update_registers(registers, hash_value);
        }
    }

    static void merge_registers(char* registers, const char* other_registers) {
        for (int i = 0; i < doris::HLL_REGISTERS_COUNT; ++i) {
            registers[i] = std::max(registers[i], other_registers[i]);
        }
    }

    static int serialize_full(char* result, char* registers) {
        result[0] = HLL_DATA_FULL;
        memcpy(result + 1, registers, HLL_REGISTERS_COUNT);
        return HLL_COLUMN_DEFAULT_LEN;
    }

    static int serialize_sparse(char *result, const std::map<int, uint8_t>& index_to_value) {
        result[0] = HLL_DATA_SPRASE;
        int len = sizeof(SetTypeValueType) + sizeof(SparseLengthValueType);
        char* write_value_pos = result + len;
        for (auto iter = index_to_value.begin();
             iter != index_to_value.end(); iter++) {
            write_value_pos[0] = (char)(iter->first & 0xff);
            write_value_pos[1] = (char)(iter->first >> 8 & 0xff);
            write_value_pos[2] = iter->second;
            write_value_pos += 3;
        }
        int registers_count = index_to_value.size();
        len += registers_count * (sizeof(SparseIndexType)+ sizeof(SparseValueType));
        *(int*)(result + 1) = registers_count;
        return len;
    }

    static int serialize_explicit(char* result, const std::set<uint64_t>& hash_value_set) {
        result[0] = HLL_DATA_EXPLICIT;
        result[1] = (uint8_t)(hash_value_set.size());
        int len = sizeof(SetTypeValueType) + sizeof(uint8_t);
        char* write_pos = result + len;
        for (auto iter = hash_value_set.begin();
             iter != hash_value_set.end(); iter++) {
            uint64_t hash_value = *iter;
            *(uint64_t*)write_pos = hash_value;
            write_pos += 8;
        }
        len += sizeof(uint64_t) * hash_value_set.size();
        return len;
    }
};

// todo(kks): remove this when dpp_sink class was removed
class HllSetResolver {
public:
    HllSetResolver() : _buf_ref(nullptr),
                       _buf_len(0),
                       _set_type(HLL_DATA_EMPTY),
                       _full_value_position(nullptr),
                       _explicit_value(nullptr),
                       _explicit_num(0) {}

    ~HllSetResolver() {}

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

    // explicit value num
    int get_explicit_count() {
        return (int)_explicit_num;
    };

    // get explicit index value 64bit
    uint64_t get_explicit_value(int index) {
        if (index >= _explicit_num) {
            return -1;
        }
        return _explicit_value[index];
    };

    // get full register value
    char* get_full_value() {
        return _full_value_position;
    };

    // get (index, value) map
    std::map<SparseIndexType, SparseValueType>& get_sparse_map() {
        return _sparse_map;
    };

    // parse set , call after copy() or init()
    void parse();
private :
    char* _buf_ref;    // set
    int _buf_len;      // set len
    HllDataType _set_type;        //set type
    char* _full_value_position;
    uint64_t* _explicit_value;
    ExpliclitLengthValueType _explicit_num;
    std::map<SparseIndexType, SparseValueType> _sparse_map;
    SparseLengthValueType* _sparse_count;
};

// todo(kks): remove this when dpp_sink class was removed
class HllSetHelper {
public:
    static void set_sparse(char *result, const std::map<int, uint8_t>& index_to_value, int& len);
    static void set_explicit(char* result, const std::set<uint64_t>& hash_value_set, int& len);
    static void set_full(char* result, const std::map<int, uint8_t>& index_to_value,
                         const int set_len, int& len);
};

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_HLL_H
