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
#include <sstream>
#include <string>

#include "util/slice.h"

using std::map;
using std::nothrow;
using std::string;
using std::stringstream;

namespace doris {

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

void HllSetResolver::fill_registers(char* registers, int len) {
    if (_set_type == HLL_DATA_EXPLICIT) {
        for (int i = 0; i < get_explicit_count(); ++i) {
            uint64_t hash_value = get_explicit_value(i);
            int idx = hash_value % len;
            uint8_t first_one_bit = __builtin_ctzl(hash_value >> HLL_COLUMN_PRECISION) + 1;
            registers[idx] = std::max((uint8_t)registers[idx], first_one_bit);
        }
    } else if (_set_type == HLL_DATA_SPRASE) {
        std::map<SparseIndexType, SparseValueType>& sparse_map = get_sparse_map();
        for (std::map<SparseIndexType, SparseValueType>::iterator iter = sparse_map.begin();
                iter != sparse_map.end(); iter++) {
            registers[iter->first] =
                std::max((uint8_t)registers[iter->first], (uint8_t)iter->second);
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
        for (int i = 0; i < get_explicit_count(); ++i) {
            uint64_t hash_value = get_explicit_value(i);
            int idx = hash_value % len;
            uint8_t first_one_bit = __builtin_ctzl(hash_value >> HLL_COLUMN_PRECISION) + 1;
            if (index_to_value->find(idx) != index_to_value->end()) {
                (*index_to_value)[idx] =
                    (*index_to_value)[idx] < first_one_bit ? first_one_bit : (*index_to_value)[idx];
            } else {
                (*index_to_value)[idx] = first_one_bit;
            }
        }
    } else if (_set_type == HLL_DATA_SPRASE) {
        std::map<SparseIndexType, SparseValueType>& sparse_map = get_sparse_map();
        for (std::map<SparseIndexType, SparseValueType>::iterator iter = sparse_map.begin();
                iter != sparse_map.end(); iter++) {
            if (index_to_value->find(iter->first) != index_to_value->end()) {
                (*index_to_value)[iter->first] =
                    (*index_to_value)[iter->first]
                        < iter->second ? iter->second : (*index_to_value)[iter->first];
            } else {
                (*index_to_value)[iter->first] = iter->second;
            }
        }
    } else if (_set_type == HLL_DATA_FULL) {
        char* registers = get_full_value();
        for (int i = 0; i < len; i++) {
            if (registers[i] != 0) {
                if (index_to_value->find(i) != index_to_value->end()) {
                    (*index_to_value)[i] =
                        (*index_to_value)[i] < registers[i] ? registers[i]  : (*index_to_value)[i];
                } else {
                    (*index_to_value)[i] = registers[i];
                }
            }
        }
    }
}

void HllSetResolver::fill_hash64_set(std::set<uint64_t>* hash_set) {
    if (_set_type == HLL_DATA_EXPLICIT) {
        for (int i = 0; i < get_explicit_count(); ++i) {
            uint64_t hash_value = get_explicit_value(i);
            hash_set->insert(hash_value);
        }
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

void HllSetHelper::set_full(char* result, const char* registers,
        const int registers_len, int& len) {
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

void HllSetHelper::fill_set(const char* data, HllContext* context) {
    HllSetResolver resolver;
    const Slice* slice = reinterpret_cast<const Slice*>(data);
    if (OLAP_UNLIKELY(slice->data == nullptr)) {
        return;
    }
    resolver.init(slice->data, slice->size);
    resolver.parse();
    if (resolver.get_hll_data_type() == HLL_DATA_EXPLICIT) {
        // expliclit set
        resolver.fill_hash64_set(context->hash64_set);
    } else if (resolver.get_hll_data_type() != HLL_DATA_EMPTY) {
        // full or sparse
        context->has_sparse_or_full = true;
        resolver.fill_registers(context->registers, HLL_REGISTERS_COUNT);
    }
}

void HllSetHelper::init_context(HllContext* context) {
    memset(context->registers, 0, HLL_REGISTERS_COUNT);
    context->hash64_set = new std::set<uint64_t>();
    context->has_value = false;
    context->has_sparse_or_full = false;
}

}  // namespace doris
