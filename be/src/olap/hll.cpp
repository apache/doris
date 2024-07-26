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

#include <cmath>
#include <map>
#include <ostream>

#include "common/logging.h"
#include "util/coding.h"
#include "util/slice.h"

using std::string;
using std::stringstream;

namespace doris {

HyperLogLog::HyperLogLog(const Slice& src) {
    // When deserialize return false, we make this object a empty
    if (!deserialize(src)) {
        _type = HLL_DATA_EMPTY;
    }
}

// Convert explicit values to register format, and clear explicit values.
// NOTE: this function won't modify _type.
void HyperLogLog::_convert_explicit_to_register() {
    DCHECK(_type == HLL_DATA_EXPLICIT)
            << "_type(" << _type << ") should be explicit(" << HLL_DATA_EXPLICIT << ")";
    _registers = new uint8_t[HLL_REGISTERS_COUNT];
    memset(_registers, 0, HLL_REGISTERS_COUNT);
    for (auto value : _hash_set) {
        _update_registers(value);
    }
    // clear _hash_set
    vectorized::flat_hash_set<uint64_t>().swap(_hash_set);
}

// Change HLL_DATA_EXPLICIT to HLL_DATA_FULL directly, because HLL_DATA_SPARSE
// is implemented in the same way in memory with HLL_DATA_FULL.
void HyperLogLog::update(uint64_t hash_value) {
    switch (_type) {
    case HLL_DATA_EMPTY:
        _hash_set.insert(hash_value);
        _type = HLL_DATA_EXPLICIT;
        break;
    case HLL_DATA_EXPLICIT:
        if (_hash_set.size() < HLL_EXPLICIT_INT64_NUM) {
            _hash_set.insert(hash_value);
            break;
        }
        _convert_explicit_to_register();
        _type = HLL_DATA_FULL;
        // fall through
    case HLL_DATA_SPARSE:
    case HLL_DATA_FULL:
        _update_registers(hash_value);
        break;
    }
}

void HyperLogLog::merge(const HyperLogLog& other) {
    // fast path
    if (other._type == HLL_DATA_EMPTY) {
        return;
    }
    switch (_type) {
    case HLL_DATA_EMPTY: {
        // _type must change
        _type = other._type;
        switch (other._type) {
        case HLL_DATA_EXPLICIT:
            _hash_set = other._hash_set;
            break;
        case HLL_DATA_SPARSE:
        case HLL_DATA_FULL:
            _registers = new uint8_t[HLL_REGISTERS_COUNT];
            memcpy(_registers, other._registers, HLL_REGISTERS_COUNT);
            break;
        default:
            break;
        }
        break;
    }
    case HLL_DATA_EXPLICIT: {
        switch (other._type) {
        case HLL_DATA_EXPLICIT: {
            // Merge other's explicit values first, then check if the number is exceed
            // HLL_EXPLICIT_INT64_NUM. This is OK because the max value is 2 * 160.
            _hash_set.insert(other._hash_set.begin(), other._hash_set.end());
            if (_hash_set.size() > HLL_EXPLICIT_INT64_NUM) {
                _convert_explicit_to_register();
                _type = HLL_DATA_FULL;
            }
        } break;
        case HLL_DATA_SPARSE:
        case HLL_DATA_FULL:
            _convert_explicit_to_register();
            _merge_registers(other._registers);
            _type = HLL_DATA_FULL;
            break;
        default:
            break;
        }
        break;
    }
    case HLL_DATA_SPARSE:
    case HLL_DATA_FULL: {
        switch (other._type) {
        case HLL_DATA_EXPLICIT:
            for (auto hash_value : other._hash_set) {
                _update_registers(hash_value);
            }
            break;
        case HLL_DATA_SPARSE:
        case HLL_DATA_FULL:
            _merge_registers(other._registers);
            break;
        default:
            break;
        }
        break;
    }
    }
}

size_t HyperLogLog::max_serialized_size() const {
    switch (_type) {
    case HLL_DATA_EMPTY:
    default:
        return 1;
    case HLL_DATA_EXPLICIT:
        return 2 + _hash_set.size() * 8;
    case HLL_DATA_SPARSE:
    case HLL_DATA_FULL:
        return 1 + HLL_REGISTERS_COUNT;
    }
}

size_t HyperLogLog::serialize(uint8_t* dst) const {
    uint8_t* ptr = dst;
    switch (_type) {
    case HLL_DATA_EMPTY:
    default: {
        // When the _type is unknown, which may not happen, we encode it as
        // Empty HyperLogLog object.
        *ptr++ = HLL_DATA_EMPTY;
        break;
    }
    case HLL_DATA_EXPLICIT: {
        DCHECK(_hash_set.size() <= HLL_EXPLICIT_INT64_NUM)
                << "Number of explicit elements(" << _hash_set.size()
                << ") should be less or equal than " << HLL_EXPLICIT_INT64_NUM;
        *ptr++ = _type;
        *ptr++ = (uint8_t)_hash_set.size();
        for (auto hash_value : _hash_set) {
            encode_fixed64_le(ptr, hash_value);
            ptr += 8;
        }
        break;
    }
    case HLL_DATA_SPARSE:
    case HLL_DATA_FULL: {
        uint32_t num_non_zero_registers = 0;
        for (int i = 0; i < HLL_REGISTERS_COUNT; ++i) {
            num_non_zero_registers += (_registers[i] != 0);
        }

        // each register in sparse format will occupy 3bytes, 2 for index and
        // 1 for register value. So if num_non_zero_registers is greater than
        // 4K we use full encode format.
        if (num_non_zero_registers > HLL_SPARSE_THRESHOLD) {
            *ptr++ = HLL_DATA_FULL;
            memcpy(ptr, _registers, HLL_REGISTERS_COUNT);
            ptr += HLL_REGISTERS_COUNT;
        } else {
            *ptr++ = HLL_DATA_SPARSE;
            // 2-5(4 byte): number of registers
            encode_fixed32_le(ptr, num_non_zero_registers);
            ptr += 4;

            for (uint32_t i = 0; i < HLL_REGISTERS_COUNT; ++i) {
                if (_registers[i] == 0) {
                    continue;
                }
                // 2 bytes: register index
                // 1 byte: register value
                encode_fixed16_le(ptr, i);
                ptr += 2;
                *ptr++ = _registers[i];
            }
        }
        break;
    }
    }
    return ptr - dst;
}

bool HyperLogLog::is_valid(const Slice& slice) {
    if (slice.size < 1) {
        return false;
    }
    const uint8_t* ptr = (uint8_t*)slice.data;
    const uint8_t* end = (uint8_t*)slice.data + slice.size;
    auto type = (HllDataType)*ptr++;
    switch (type) {
    case HLL_DATA_EMPTY:
        break;
    case HLL_DATA_EXPLICIT: {
        if ((ptr + 1) > end) {
            return false;
        }
        uint8_t num_explicits = *ptr++;
        ptr += num_explicits * 8;
        break;
    }
    case HLL_DATA_SPARSE: {
        if ((ptr + 4) > end) {
            return false;
        }
        uint32_t num_registers = decode_fixed32_le(ptr);
        ptr += 4 + 3 * num_registers;
        break;
    }
    case HLL_DATA_FULL: {
        ptr += HLL_REGISTERS_COUNT;
        break;
    }
    default:
        return false;
    }
    return ptr == end;
}

// TODO(zc): check input string's length
bool HyperLogLog::deserialize(const Slice& slice) {
    // can be called only when type is empty
    DCHECK(_type == HLL_DATA_EMPTY);

    // NOTE(zc): Don't remove this check unless you known what
    // you are doing. Because of history bug, we ingest some
    // invalid HLL data in storage, which ptr is nullptr.
    // we must handle this case to avoid process crash.
    // This bug is in release 0.10, I think we can remove this
    // in release 0.12 or later.
    if (slice.data == nullptr || slice.size <= 0) {
        return false;
    }
    // check if input length is valid
    if (!is_valid(slice)) {
        return false;
    }

    const uint8_t* ptr = (uint8_t*)slice.data;
    // first byte : type
    _type = (HllDataType)*ptr++;
    switch (_type) {
    case HLL_DATA_EMPTY:
        break;
    case HLL_DATA_EXPLICIT: {
        // 2: number of explicit values
        // make sure that num_explicit is positive
        uint8_t num_explicits = *ptr++;
        // 3+: 8 bytes hash value
        for (int i = 0; i < num_explicits; ++i) {
            _hash_set.insert(decode_fixed64_le(ptr));
            ptr += 8;
        }
        break;
    }
    case HLL_DATA_SPARSE: {
        _registers = new uint8_t[HLL_REGISTERS_COUNT];
        memset(_registers, 0, HLL_REGISTERS_COUNT);
        // 2-5(4 byte): number of registers
        uint32_t num_registers = decode_fixed32_le(ptr);
        ptr += 4;
        for (uint32_t i = 0; i < num_registers; ++i) {
            // 2 bytes: register index
            // 1 byte: register value
            uint16_t register_idx = decode_fixed16_le(ptr);
            ptr += 2;
            _registers[register_idx] = *ptr++;
        }
        break;
    }
    case HLL_DATA_FULL: {
        _registers = new uint8_t[HLL_REGISTERS_COUNT];
        // 2+ : hll register value
        memcpy(_registers, ptr, HLL_REGISTERS_COUNT);
        break;
    }
    default:
        // revert type to EMPTY
        _type = HLL_DATA_EMPTY;
        return false;
    }
    return true;
}

int64_t HyperLogLog::estimate_cardinality() const {
    if (_type == HLL_DATA_EMPTY) {
        return 0;
    }
    if (_type == HLL_DATA_EXPLICIT) {
        return _hash_set.size();
    }

    const int num_streams = HLL_REGISTERS_COUNT;
    // Empirical constants for the algorithm.
    float alpha = 0;

    if (num_streams == 16) {
        alpha = 0.673F;
    } else if (num_streams == 32) {
        alpha = 0.697F;
    } else if (num_streams == 64) {
        alpha = 0.709F;
    } else {
        alpha = 0.7213F / (1 + 1.079F / num_streams);
    }

    float harmonic_mean = 0;
    int num_zero_registers = 0;

    for (int i = 0; i < HLL_REGISTERS_COUNT; ++i) {
        harmonic_mean += powf(2.0F, -_registers[i]);

        if (_registers[i] == 0) {
            ++num_zero_registers;
        }
    }

    harmonic_mean = 1.0F / harmonic_mean;
    double estimate = alpha * num_streams * num_streams * harmonic_mean;
    // according to HyperLogLog current correction, if E is cardinal
    // E =< num_streams * 2.5 , LC has higher accuracy.
    // num_streams * 2.5 < E , HyperLogLog has higher accuracy.
    // Generally , we can use HyperLogLog to produce value as E.
    if (estimate <= num_streams * 2.5 && num_zero_registers != 0) {
        // Estimated cardinality is too low. Hll is too inaccurate here, instead use
        // linear counting.
        estimate = num_streams * log(static_cast<float>(num_streams) / num_zero_registers);
    } else if (num_streams == 16384 && estimate < 72000) {
        // when Linear Couint change to HyperLogLog according to HyperLogLog Correction,
        // there are relatively large fluctuations, we fixed the problem refer to redis.
        double bias = 5.9119 * 1.0e-18 * (estimate * estimate * estimate * estimate) -
                      1.4253 * 1.0e-12 * (estimate * estimate * estimate) +
                      1.2940 * 1.0e-7 * (estimate * estimate) - 5.2921 * 1.0e-3 * estimate +
                      83.3216;
        estimate -= estimate * (bias / 100);
    }
    return (int64_t)(estimate + 0.5);
}

void HllSetResolver::parse() {
    // skip LengthValueType
    char* pdata = _buf_ref;
    _set_type = (HllDataType)pdata[0];
    char* sparse_data = nullptr;
    switch (_set_type) {
    case HLL_DATA_EXPLICIT:
        // first byte : type
        // second～five byte : hash values's number
        // five byte later : hash value
        _explicit_num = (ExplicitLengthValueType)(pdata[sizeof(SetTypeValueType)]);
        _explicit_value =
                (uint64_t*)(pdata + sizeof(SetTypeValueType) + sizeof(ExplicitLengthValueType));
        break;
    case HLL_DATA_SPARSE:
        // first byte : type
        // second ～（2^HLL_COLUMN_PRECISION)/8 byte : bitmap mark which is not zero
        // 2^HLL_COLUMN_PRECISION)/8 ＋ 1以后value
        _sparse_count = (SparseLengthValueType*)(pdata + sizeof(SetTypeValueType));
        sparse_data = pdata + sizeof(SetTypeValueType) + sizeof(SparseLengthValueType);
        for (int i = 0; i < *_sparse_count; i++) {
            auto* index = (SparseIndexType*)sparse_data;
            sparse_data += sizeof(SparseIndexType);
            auto* value = (SparseValueType*)sparse_data;
            _sparse_map[*index] = *value;
            sparse_data += sizeof(SetTypeValueType);
        }
        break;
    case HLL_DATA_FULL:
        // first byte : type
        // second byte later : hll register value
        _full_value_position = pdata + sizeof(SetTypeValueType);
        break;
    default:
        // HLL_DATA_EMPTY
        break;
    }
}

} // namespace doris
