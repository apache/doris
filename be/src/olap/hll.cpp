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
