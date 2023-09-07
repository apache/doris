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
#include "util/quantile_state.h"

#include <string.h>

#include <cmath>
#include <ostream>
#include <utility>

#include "common/logging.h"
#include "util/coding.h"
#include "util/slice.h"
#include "util/tdigest.h"

namespace doris {

QuantileState::QuantileState() : _type(EMPTY), _compression(QUANTILE_STATE_COMPRESSION_MIN) {}

QuantileState::QuantileState(float compression) : _type(EMPTY), _compression(compression) {}

QuantileState::QuantileState(const Slice& slice) {
    if (!deserialize(slice)) {
        _type = EMPTY;
    }
}

size_t QuantileState::get_serialized_size() {
    size_t size = 1 + sizeof(float); // type(QuantileStateType) + compression(float)
    switch (_type) {
    case EMPTY:
        break;
    case SINGLE:
        size += sizeof(double);
        break;
    case EXPLICIT:
        size += sizeof(uint16_t) + sizeof(double) * _explicit_data.size();
        break;
    case TDIGEST:
        size += _tdigest_ptr->serialized_size();
        break;
    }
    return size;
}

void QuantileState::set_compression(float compression) {
    DCHECK(compression >= QUANTILE_STATE_COMPRESSION_MIN &&
           compression <= QUANTILE_STATE_COMPRESSION_MAX);
    this->_compression = compression;
}

bool QuantileState::is_valid(const Slice& slice) {
    if (slice.size < 1) {
        return false;
    }
    const uint8_t* ptr = (uint8_t*)slice.data;
    const uint8_t* end = (uint8_t*)slice.data + slice.size;
    float compress_value = *reinterpret_cast<const float*>(ptr);
    if (compress_value < QUANTILE_STATE_COMPRESSION_MIN ||
        compress_value > QUANTILE_STATE_COMPRESSION_MAX) {
        return false;
    }
    ptr += sizeof(float);

    auto type = (QuantileStateType)*ptr++;
    switch (type) {
    case EMPTY:
        break;
    case SINGLE: {
        if ((ptr + sizeof(double)) > end) {
            return false;
        }
        ptr += sizeof(double);
        break;
    }
    case EXPLICIT: {
        if ((ptr + sizeof(uint16_t)) > end) {
            return false;
        }
        uint16_t num_explicits = decode_fixed16_le(ptr);
        ptr += sizeof(uint16_t);
        ptr += num_explicits * sizeof(double);
        break;
    }
    case TDIGEST: {
        if ((ptr + sizeof(uint32_t)) > end) {
            return false;
        }
        uint32_t tdigest_serialized_length = decode_fixed32_le(ptr);
        ptr += tdigest_serialized_length;
        break;
    }
    default:
        return false;
    }
    return ptr == end;
}

double QuantileState::get_explicit_value_by_percentile(float percentile) const {
    DCHECK(_type == EXPLICIT);
    int n = _explicit_data.size();
    std::vector<double> sorted_data(_explicit_data.begin(), _explicit_data.end());
    std::sort(sorted_data.begin(), sorted_data.end());

    double index = (n - 1) * percentile;
    int intIdx = (int)index;
    if (intIdx == n - 1) {
        return sorted_data[intIdx];
    }
    return sorted_data[intIdx + 1] * (index - intIdx) + sorted_data[intIdx] * (intIdx + 1 - index);
}

double QuantileState::get_value_by_percentile(float percentile) const {
    DCHECK(percentile >= 0 && percentile <= 1);
    switch (_type) {
    case EMPTY: {
        return NAN;
    }
    case SINGLE: {
        return _single_data;
    }
    case EXPLICIT: {
        return get_explicit_value_by_percentile(percentile);
    }
    case TDIGEST: {
        return _tdigest_ptr->quantile(percentile);
    }
    default:
        break;
    }
    return NAN;
}

bool QuantileState::deserialize(const Slice& slice) {
    DCHECK(_type == EMPTY);

    // in case of insert error data caused be crashed
    if (slice.data == nullptr || slice.size <= 0) {
        return false;
    }
    // check input is valid
    if (!is_valid(slice)) {
        LOG(WARNING) << "QuantileState deserialize failed: slice is invalid";
        return false;
    }

    const uint8_t* ptr = (uint8_t*)slice.data;
    _compression = *reinterpret_cast<const float*>(ptr);
    ptr += sizeof(float);
    // first byte : type
    _type = (QuantileStateType)*ptr++;
    switch (_type) {
    case EMPTY:
        // 1: empty
        break;
    case SINGLE: {
        // 2: single_data value
        _single_data = *reinterpret_cast<const double*>(ptr);
        ptr += sizeof(double);
        break;
    }
    case EXPLICIT: {
        // 3: number of explicit values
        // make sure that num_explicit is positive
        uint16_t num_explicits = decode_fixed16_le(ptr);
        ptr += sizeof(uint16_t);
        _explicit_data.reserve(std::min(num_explicits * 2, QUANTILE_STATE_EXPLICIT_NUM));
        _explicit_data.resize(num_explicits);
        memcpy(&_explicit_data[0], ptr, num_explicits * sizeof(double));
        ptr += num_explicits * sizeof(double);
        break;
    }
    case TDIGEST: {
        // 4: Tdigest object value
        _tdigest_ptr = std::make_shared<TDigest>(0);
        _tdigest_ptr->unserialize(ptr);
        break;
    }
    default:
        // revert type to EMPTY
        _type = EMPTY;
        return false;
    }
    return true;
}

size_t QuantileState::serialize(uint8_t* dst) const {
    uint8_t* ptr = dst;
    *reinterpret_cast<float*>(ptr) = _compression;
    ptr += sizeof(float);
    switch (_type) {
    case EMPTY: {
        *ptr++ = EMPTY;
        break;
    }
    case SINGLE: {
        *ptr++ = SINGLE;
        *reinterpret_cast<double*>(ptr) = _single_data;
        ptr += sizeof(double);
        break;
    }
    case EXPLICIT: {
        *ptr++ = EXPLICIT;
        uint16_t size = _explicit_data.size();
        *reinterpret_cast<uint16_t*>(ptr) = size;
        ptr += sizeof(uint16_t);
        memcpy(ptr, &_explicit_data[0], size * sizeof(double));
        ptr += size * sizeof(double);
        break;
    }
    case TDIGEST: {
        *ptr++ = TDIGEST;
        size_t tdigest_size = _tdigest_ptr->serialize(ptr);
        ptr += tdigest_size;
        break;
    }
    default:
        break;
    }
    return ptr - dst;
}

void QuantileState::merge(const QuantileState& other) {
    switch (other._type) {
    case EMPTY:
        break;
    case SINGLE: {
        add_value(other._single_data);
        break;
    }
    case EXPLICIT: {
        switch (_type) {
        case EMPTY:
            _type = EXPLICIT;
            _explicit_data = other._explicit_data;
            break;
        case SINGLE:
            _type = EXPLICIT;
            _explicit_data = other._explicit_data;
            add_value(_single_data);
            break;
        case EXPLICIT:
            if (_explicit_data.size() + other._explicit_data.size() > QUANTILE_STATE_EXPLICIT_NUM) {
                _type = TDIGEST;
                _tdigest_ptr = std::make_shared<TDigest>(_compression);
                for (int i = 0; i < _explicit_data.size(); i++) {
                    _tdigest_ptr->add(_explicit_data[i]);
                }
                for (int i = 0; i < other._explicit_data.size(); i++) {
                    _tdigest_ptr->add(other._explicit_data[i]);
                }
            } else {
                _explicit_data.insert(_explicit_data.end(), other._explicit_data.begin(),
                                      other._explicit_data.end());
            }
            break;
        case TDIGEST:
            for (int i = 0; i < other._explicit_data.size(); i++) {
                _tdigest_ptr->add(other._explicit_data[i]);
            }
            break;
        default:
            break;
        }
        break;
    }
    case TDIGEST: {
        switch (_type) {
        case EMPTY:
            _type = TDIGEST;
            _tdigest_ptr = other._tdigest_ptr;
            break;
        case SINGLE:
            _type = TDIGEST;
            _tdigest_ptr = other._tdigest_ptr;
            _tdigest_ptr->add(_single_data);
            break;
        case EXPLICIT:
            _type = TDIGEST;
            _tdigest_ptr = other._tdigest_ptr;
            for (int i = 0; i < _explicit_data.size(); i++) {
                _tdigest_ptr->add(_explicit_data[i]);
            }
            break;
        case TDIGEST:
            _tdigest_ptr->merge(other._tdigest_ptr.get());
            break;
        default:
            break;
        }
        break;
    }
    default:
        return;
    }
}

void QuantileState::add_value(const double& value) {
    switch (_type) {
    case EMPTY:
        _single_data = value;
        _type = SINGLE;
        break;
    case SINGLE:
        _explicit_data.emplace_back(_single_data);
        _explicit_data.emplace_back(value);
        _type = EXPLICIT;
        break;
    case EXPLICIT:
        if (_explicit_data.size() == QUANTILE_STATE_EXPLICIT_NUM) {
            _tdigest_ptr = std::make_shared<TDigest>(_compression);
            for (int i = 0; i < _explicit_data.size(); i++) {
                _tdigest_ptr->add(_explicit_data[i]);
            }
            _explicit_data.clear();
            _explicit_data.shrink_to_fit();
            _type = TDIGEST;

        } else {
            _explicit_data.emplace_back(value);
        }
        break;
    case TDIGEST:
        _tdigest_ptr->add(value);
        break;
    }
}

void QuantileState::clear() {
    _type = EMPTY;
    _tdigest_ptr.reset();
    _explicit_data.clear();
    _explicit_data.shrink_to_fit();
}

} // namespace doris
