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

#include "gutil/macros.h"

namespace doris {

const static int HLL_COLUMN_PRECISION = 14;
const static int HLL_ZERO_COUNT_BITS = (64 - HLL_COLUMN_PRECISION);
const static int HLL_EXPLICLIT_INT64_NUM = 160;
const static int HLL_SPARSE_THRESHOLD = 4096;
const static int HLL_REGISTERS_COUNT = 16 * 1024;
// maximum size in byte of serialized HLL: type(1) + registers (2^14)
const static int HLL_COLUMN_DEFAULT_LEN = HLL_REGISTERS_COUNT + 1;

// 1 for type; 1 for hash values count; 8 for hash value
const static int HLL_SINGLE_VALUE_SIZE = 10;
const static int HLL_EMPTY_SIZE = 1;

// Hyperloglog distinct estimate algorithm.
// See these papers for more details.
// 1) Hyperloglog: The analysis of a near-optimal cardinality estimation
// algorithm (2007)
// 2) HyperLogLog in Practice (paper from google with some improvements)

// Each HLL value is a set of values. To save space, Doris store HLL value
// in different format according to its cardinality.
//
// HLL_DATA_EMPTY: when set is empty.
//
// HLL_DATA_EXPLICIT: when there is only few values in set, store these values explicit.
// If the number of hash values is not greater than 160, set is encoded in this format.
// The max space occupied is (1 + 1 + 160 * 8) = 1282. I don't know why 160 is choosed,
// maybe can be other number. If you are interested, you can try other number and see
// if it will be better.
//
// HLL_DATA_SPRASE: only store non-zero registers. If the number of non-zero registers
// is not greater than 4096, set is encoded in this format. The max space occupied is
// (1 + 4 + 3 * 4096) = 12293.
//
// HLL_DATA_FULL: most space-consuming, store all registers
// 
// A HLL value will change in the sequence empty -> explicit -> sparse -> full, and not
// allow reverse.
//
// NOTE: This values are persisted in storage devices, so don't change exist
// enum values.
enum HllDataType {
    HLL_DATA_EMPTY = 0,
    HLL_DATA_EXPLICIT = 1,
    HLL_DATA_SPRASE = 2,
    HLL_DATA_FULL = 3,
};

class HyperLogLog {
public:

    HyperLogLog() = default;
    explicit HyperLogLog(uint64_t hash_value): _type(HLL_DATA_EXPLICIT) {
        _hash_set.emplace(hash_value);
    }
    explicit HyperLogLog(const uint8_t* src);

    ~HyperLogLog() {
        delete[] _registers;
    }

    typedef uint8_t SetTypeValueType;
    typedef int32_t SparseLengthValueType;
    typedef uint16_t SparseIndexType;
    typedef uint8_t SparseValueType;

    // Add a hash value to this HLL value
    // NOTE: input must be a hash_value
    void update(uint64_t hash_value);

    void merge(const HyperLogLog& other);

    int serialize(uint8_t* dest);
    bool deserialize(const uint8_t* ptr);

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
    HllDataType _type = HLL_DATA_EMPTY;
    std::set<uint64_t> _hash_set;

    // This field is much space consumming(HLL_REGISTERS_COUNT), we craete
    // it only when it is really needed.
    uint8_t* _registers = nullptr;

private:
    DISALLOW_COPY_AND_ASSIGN(HyperLogLog);

    void _convert_explicit_to_register();

    // update one hash value into this registers
    void _update_registers(uint64_t hash_value) {
        // Use the lower bits to index into the number of streams and then
        // find the first 1 bit after the index bits.
        int idx = hash_value % HLL_REGISTERS_COUNT;
        hash_value >>= HLL_COLUMN_PRECISION;
        // make sure max first_one_bit is HLL_ZERO_COUNT_BITS + 1
        hash_value |= ((uint64_t)1 << HLL_ZERO_COUNT_BITS);
        uint8_t first_one_bit = __builtin_ctzl(hash_value) + 1;
        _registers[idx] = std::max((uint8_t)_registers[idx], first_one_bit);
    }

    // absorb other registers into this registers
    void _merge_registers(const uint8_t* other_registers) {
        for (int i = 0; i < HLL_REGISTERS_COUNT; ++i) {
            _registers[i] = std::max(_registers[i], other_registers[i]);
        }
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
