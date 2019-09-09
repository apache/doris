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

#include "olap/hll.h"

#include <algorithm>
#include <map>

using std::map;
using std::nothrow;
using std::string;
using std::stringstream;

namespace doris {

HyperLogLog::HyperLogLog(char* src) {
    _type = (HllDataType)src[0];
    memset(_registers, 0, HLL_REGISTERS_COUNT);
    char* sparse_data = nullptr;
    switch (_type) {
        case HLL_DATA_EXPLICIT:
            // first byte : type
            // second～five byte : hash values's number
            // five byte later : hash value
        {
            auto _explicit_num = (uint8_t) (src[sizeof(SetTypeValueType)]);
            auto *_explicit_value = (uint64_t *) (src + sizeof(SetTypeValueType) + sizeof(uint8_t));
            for (int i = 0; i < _explicit_num; ++i) {
                _hash_set.insert(_explicit_value[i]);
            }
        }
            break;
        case HLL_DATA_SPRASE:
            // first byte : type
            // second ～（2^HLL_COLUMN_PRECISION)/8 byte : bitmap mark which is not zero
            // 2^HLL_COLUMN_PRECISION)/8 ＋ 1以后value
        {
            auto* _sparse_count = (SparseLengthValueType*)(src + sizeof (SetTypeValueType));
            sparse_data = src + sizeof(SetTypeValueType) + sizeof(SparseLengthValueType);
            std::map<SparseIndexType, SparseValueType> _sparse_map;
            for (int i = 0; i < *_sparse_count; i++) {
                auto* index = (SparseIndexType*)sparse_data;
                sparse_data += sizeof(SparseIndexType);
                auto* value = (SparseValueType*)sparse_data;
                _sparse_map[*index] = *value;
                sparse_data += sizeof(SetTypeValueType);
            }

            for (auto iter: _sparse_map) {
                _registers[iter.first] = (uint8_t)iter.second;
            }
        }
            break;
        case HLL_DATA_FULL:
            // first byte : type
            // second byte later : hll register value
        {
            char* _full_value_position = src + sizeof (SetTypeValueType);
            memcpy(_registers, _full_value_position, HLL_REGISTERS_COUNT);
        }
            break;
        case HLL_DATA_EMPTY:
            break;
        default:
            break;
    }
}

void HyperLogLog::merge(const HyperLogLog& other) {
    if (other._type == HLL_DATA_EMPTY) {
        return;
    }

    // _type must change
    if (_type == HLL_DATA_EMPTY) {
        _type = other._type;
        switch (other._type) {
            case HLL_DATA_EXPLICIT:
                _hash_set = other._hash_set;
                return;
            case HLL_DATA_SPRASE:
            case HLL_DATA_FULL:
                memcpy(_registers, other._registers, HLL_REGISTERS_COUNT);
                return;
            default:
                return;
        }
    }

    // _type maybe change
    if (_type == HLL_DATA_EXPLICIT) {
        switch (other._type) {
            case HLL_DATA_EXPLICIT:
                _hash_set.insert(other._hash_set.begin(), other._hash_set.end());
                return;
            case HLL_DATA_SPRASE:
            case HLL_DATA_FULL:
                memcpy(_registers, other._registers, HLL_REGISTERS_COUNT);
                _type = other._type;
                return;
            default:
                return;
        }
    }

    // _type maybe change
    if (_type == HLL_DATA_SPRASE) {
        switch (other._type) {
            case HLL_DATA_EXPLICIT:
                _hash_set.insert(other._hash_set.begin(), other._hash_set.end());
                return;
            case HLL_DATA_SPRASE:
            case HLL_DATA_FULL:
                merge_registers(_registers, other._registers);
                _type = other._type;
                return;
            default:
                return;
        }
    }

    // _type not change
    if (_type == HLL_DATA_FULL) {
        switch (other._type) {
            case HLL_DATA_EXPLICIT:
                _hash_set.insert(other._hash_set.begin(), other._hash_set.end());
                return;
            case HLL_DATA_SPRASE:
            case HLL_DATA_FULL:
                merge_registers(_registers, other._registers);
                return;
            default:
                return;
        }
    }
}

int HyperLogLog::serialize(char* dest) {
    if (_type == HLL_DATA_EMPTY) {
        dest[0] = _type;
        return 1;
    }

    std::map<int, uint8_t> index_to_value;
    if (_type == HLL_DATA_SPRASE || _type == HLL_DATA_FULL ||
        _hash_set.size() > HLL_EXPLICLIT_INT64_NUM) {
        merge_hash_set_to_registers(_registers, _hash_set);
        for (int i = 0; i < HLL_REGISTERS_COUNT; i++) {
            if (_registers[i] != 0) {
                index_to_value[i] = _registers[i];
            }
        }
    }

    int sparse_set_len = index_to_value.size() * (sizeof(SparseIndexType)
                                                  + sizeof(SparseValueType) + sizeof(SparseLengthValueType));
    int result_len = 0;
    if (sparse_set_len >= HLL_COLUMN_DEFAULT_LEN) {
        result_len = serialize_full(dest, _registers);
    } else if (index_to_value.size() > 0) {
        result_len = serialize_sparse(dest, index_to_value);
    } else if (_hash_set.size() > 0) {
        result_len = serialize_explicit(dest, _hash_set);
    }

    return result_len & 0xffff;
}

int64_t HyperLogLog::estimate_cardinality() {
    if (_type == HLL_DATA_EMPTY) {
        return 0;
    }

    merge_hash_set_to_registers(_registers, _hash_set);

    const int num_streams = HLL_REGISTERS_COUNT;
    // Empirical constants for the algorithm.
    float alpha = 0;

    if (num_streams == 16) {
        alpha = 0.673f;
    } else if (num_streams == 32) {
        alpha = 0.697f;
    } else if (num_streams == 64) {
        alpha = 0.709f;
    } else {
        alpha = 0.7213f / (1 + 1.079f / num_streams);
    }

    float harmonic_mean = 0;
    int num_zero_registers = 0;

    for (int i = 0; i < HLL_REGISTERS_COUNT; ++i) {
        harmonic_mean += powf(2.0f, -_registers[i]);

        if (_registers[i] == 0) {
            ++num_zero_registers;
        }
    }

    harmonic_mean = 1.0f / harmonic_mean;
    double estimate = alpha * num_streams * num_streams * harmonic_mean;
    // according to HerperLogLog current correction, if E is cardinal
    // E =< num_streams * 2.5 , LC has higher accuracy.
    // num_streams * 2.5 < E , HerperLogLog has higher accuracy.
    // Generally , we can use HerperLogLog to produce value as E.
    if (estimate <= num_streams * 2.5 && num_zero_registers != 0) {
        // Estimated cardinality is too low. Hll is too inaccurate here, instead use
        // linear counting.
        estimate = num_streams * log(static_cast<float>(num_streams) / num_zero_registers);
    } else if (num_streams == 16384 && estimate < 72000) {
        // when Linear Couint change to HerperLoglog according to HerperLogLog Correction,
        // there are relatively large fluctuations, we fixed the problem refer to redis.
        double bias = 5.9119 * 1.0e-18 * (estimate * estimate * estimate * estimate)
                      - 1.4253 * 1.0e-12 * (estimate * estimate * estimate) +
                      1.2940 * 1.0e-7 * (estimate * estimate)
                      - 5.2921 * 1.0e-3 * estimate +
                      83.3216;
        estimate -= estimate * (bias / 100);
    }
    return (int64_t)(estimate + 0.5);
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
            _explicit_num = (ExpliclitLengthValueType) (pdata[sizeof(SetTypeValueType)]);
            _explicit_value = (uint64_t*)(pdata + sizeof(SetTypeValueType)
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

void HllSetHelper::set_sparse(
        char *result, const std::map<int, uint8_t>& index_to_value, int& len) {
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
    len += registers_count * (sizeof(HllSetResolver::SparseIndexType)
            + sizeof(HllSetResolver::SparseValueType));
    *(int*)(result + 1) = registers_count;
}

void HllSetHelper::set_explicit(char* result, const std::set<uint64_t>& hash_value_set, int& len) {
    result[0] = HLL_DATA_EXPLICIT;
    result[1] = (HllSetResolver::ExpliclitLengthValueType)(hash_value_set.size());
    len = sizeof(HllSetResolver::SetTypeValueType)
        + sizeof(HllSetResolver::ExpliclitLengthValueType);
    char* write_pos = result + len;
    for (std::set<uint64_t>::const_iterator iter = hash_value_set.begin();
            iter != hash_value_set.end(); iter++) {
        uint64_t hash_value = *iter;
        *(uint64_t*)write_pos = hash_value;
        write_pos += 8;
    }
    len += sizeof(uint64_t) * hash_value_set.size();
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

}  // namespace doris
