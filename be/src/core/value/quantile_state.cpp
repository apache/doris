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
#include "core/value/quantile_state.h"

#include <string.h>

#include <cmath>
#include <mutex>
#include <ostream>
#include <shared_mutex>
#include <utility>

#include "common/logging.h"
#include "util/coding.h"
#include "util/slice.h"
#include "util/tdigest.h"
#include "util/unaligned.h"

#ifdef BE_TEST
#include "cpp/sync_point.h"
#endif

namespace doris {

// Shares a digest across QuantileState copies and detaches it before sample
// writes. Readers use shared locks; compression uses an exclusive lock.
struct QuantileState::TDigestHolder {
    // At C=128 the explicit state converts on sample 129. Keeping the default 8*C
    // unprocessed buffer would retain more centroids than the original 128 doubles.
    explicit TDigestHolder(float compression)
            : digest(compression, compression < QUANTILE_STATE_COMPRESSION_DEFAULT
                                          ? static_cast<Index>(std::ceil(compression))
                                          : 0) {}
    TDigestHolder(const TDigestHolder& other) : digest(other.digest) {}

    std::shared_lock<std::shared_mutex> lock_processed_digest() {
        std::shared_lock read_lock(mutex);
        if (digest.have_unprocessed()) {
            read_lock.unlock();
            {
                std::unique_lock write_lock(mutex);
                if (digest.have_unprocessed()) {
                    digest.compress();
                }
            }
            read_lock.lock();
        }
        return read_lock;
    }

    TDigest digest;
    std::shared_mutex mutex;
};

QuantileState QuantileState::copy_for_result() const {
    if (_type != TDIGEST) {
        return *this;
    }
    QuantileState result(_compression);
    result._type = TDIGEST;
    {
        // Reuse processed centroids on the next row instead of sorting the prefix again.
        auto lock = _tdigest_ptr->lock_processed_digest();
        // Vector copies retain the elements, not the accumulator's spare write capacity.
        result._tdigest_ptr = std::make_shared<TDigestHolder>(*_tdigest_ptr);
    }
    return result;
}

TDigest& QuantileState::_mutable_tdigest() {
    if (_tdigest_ptr.use_count() == 1) {
        return _tdigest_ptr->digest;
    }
    std::shared_ptr<TDigestHolder> detached;
    {
        std::shared_lock lock(_tdigest_ptr->mutex);
#ifdef BE_TEST
        TEST_SYNC_POINT("QuantileState::detach:source_locked");
#endif
        detached = std::make_shared<TDigestHolder>(*_tdigest_ptr);
    }
    _tdigest_ptr = std::move(detached);
    return _tdigest_ptr->digest;
}

void QuantileState::_add_to_digest(TDigest& digest, double value) const {
    const auto sample = static_cast<float>(value);
    if (_compression < QUANTILE_STATE_COMPRESSION_DEFAULT) {
        digest.add_preserving_extrema(sample);
    } else {
        digest.add(sample);
    }
}

QuantileState::QuantileState() : _compression(QUANTILE_STATE_COMPRESSION_DEFAULT) {}

QuantileState::QuantileState(float compression) : _compression(compression) {}

QuantileState::QuantileState(const Slice& slice) {
    if (!deserialize(slice)) {
        _type = EMPTY;
    }
}

size_t QuantileState::get_serialized_size() const {
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
    case TDIGEST: {
        // Compress before sizing so concurrent queries cannot change the size
        // before serialize(); writes through shared copies detach via COW.
        auto lock = _tdigest_ptr->lock_processed_digest();
        size += _tdigest_ptr->digest.serialized_size();
        break;
    }
    }
    return size;
}

void QuantileState::set_compression(float compression) {
    DCHECK(compression >= QUANTILE_STATE_COMPRESSION_MIN &&
           compression <= QUANTILE_STATE_COMPRESSION_MAX);
    this->_compression = compression;
}

bool QuantileState::is_valid(const Slice& slice) {
    if (slice.size < sizeof(float) + 1) {
        return false;
    }
    const uint8_t* ptr = (uint8_t*)slice.data;
    const uint8_t* end = (uint8_t*)slice.data + slice.size;
    float compress_value = unaligned_load<float>(ptr);
    if (!(compress_value >= QUANTILE_STATE_COMPRESSION_MIN &&
          compress_value <= QUANTILE_STATE_COMPRESSION_MAX)) {
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
    size_t n = _explicit_data.size();
    std::vector<double> sorted_data(_explicit_data.begin(), _explicit_data.end());
    std::sort(sorted_data.begin(), sorted_data.end());

    double index = double(n - 1) * percentile;
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
        auto lock = _tdigest_ptr->lock_processed_digest();
        return _tdigest_ptr->digest.quantile_processed(percentile);
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
    _compression = unaligned_load<float>(ptr);
    ptr += sizeof(float);
    // first byte : type
    _type = (QuantileStateType)*ptr++;
    switch (_type) {
    case EMPTY:
        // 1: empty
        break;
    case SINGLE: {
        // 2: single_data value
        _single_data = unaligned_load<double>(ptr);
        ptr += sizeof(double);
        break;
    }
    case EXPLICIT: {
        // 3: number of explicit values
        // make sure that num_explicit is positive
        uint16_t num_explicits = decode_fixed16_le(ptr);
        ptr += sizeof(uint16_t);
        if (_compression >= QUANTILE_STATE_COMPRESSION_DEFAULT) {
            _explicit_data.reserve(std::min(num_explicits * 2, QUANTILE_STATE_EXPLICIT_NUM));
        }
        _explicit_data.resize(num_explicits);
        memcpy(&_explicit_data[0], ptr, num_explicits * sizeof(double));
        ptr += num_explicits * sizeof(double);
        break;
    }
    case TDIGEST: {
        // 4: Tdigest object value
        _tdigest_ptr = std::make_shared<TDigestHolder>(0);
        _tdigest_ptr->digest.unserialize(ptr);
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
    unaligned_store<float>(ptr, _compression);
    ptr += sizeof(float);
    switch (_type) {
    case EMPTY: {
        *ptr++ = EMPTY;
        break;
    }
    case SINGLE: {
        *ptr++ = SINGLE;
        unaligned_store<double>(ptr, _single_data);
        ptr += sizeof(double);
        break;
    }
    case EXPLICIT: {
        *ptr++ = EXPLICIT;
        auto size = (uint16_t)_explicit_data.size();
        unaligned_store<uint16_t>(ptr, size);
        ptr += sizeof(uint16_t);
        memcpy(ptr, &_explicit_data[0], size * sizeof(double));
        ptr += size * sizeof(double);
        break;
    }
    case TDIGEST: {
        *ptr++ = TDIGEST;
        auto lock = _tdigest_ptr->lock_processed_digest();
        size_t tdigest_size = _tdigest_ptr->digest.serialize(ptr);
        ptr += tdigest_size;
        break;
    }
    default:
        break;
    }
    return ptr - dst;
}

size_t QuantileState::_explicit_limit() const {
    return std::min<size_t>(QUANTILE_STATE_EXPLICIT_NUM,
                            static_cast<size_t>(std::ceil(_compression)));
}

void QuantileState::_convert_to_tdigest() {
    DCHECK(_type == SINGLE || _type == EXPLICIT);
    auto holder = std::make_shared<TDigestHolder>(_compression);
    if (_type == SINGLE) {
        _add_to_digest(holder->digest, _single_data);
    } else {
        for (double value : _explicit_data) {
            _add_to_digest(holder->digest, value);
        }
    }
    _tdigest_ptr = std::move(holder);
    // A converted group no longer needs its explicit buffer; clear() alone retains its capacity.
    std::vector<double>().swap(_explicit_data);
    _type = TDIGEST;
}

void QuantileState::_normalize_digest() {
    if (_type != TDIGEST) {
        return;
    }
    if (_tdigest_ptr->digest.compression() == _compression &&
        (_compression >= QUANTILE_STATE_COMPRESSION_DEFAULT ||
         _tdigest_ptr->digest.max_unprocessed() == static_cast<Index>(std::ceil(_compression)))) {
        return;
    }
    // A changed outer C must not leave stale inner metadata (for example, 128+2048 -> 2048).
    // Rebuild from weighted centroids; a prior low-C digest cannot regain lost precision.
    auto rebuilt = std::make_shared<TDigestHolder>(_compression);
    {
        std::shared_lock lock(_tdigest_ptr->mutex);
        rebuilt->digest.merge_bounded(&_tdigest_ptr->digest);
    }
    _tdigest_ptr = std::move(rebuilt);
}

void QuantileState::promote_compression_for_compaction() {
    // A compacted row may have only one low-C input, so the floor applies before any merge too.
    _compression = std::max<float>(_compression, QUANTILE_STATE_COMPRESSION_DEFAULT);
    _normalize_digest();
}

void QuantileState::_merge_explicit(const QuantileState& other) {
    DCHECK(other._type == EXPLICIT);
    if (_type == SINGLE && _compression >= QUANTILE_STATE_COMPRESSION_DEFAULT) {
        // Retain the high-C insertion order: copy the explicit partial, then add the singleton.
        _type = EXPLICIT;
        _explicit_data = other._explicit_data;
        add_value(_single_data);
        return;
    }
    if (_type == EXPLICIT &&
        _explicit_data.size() + other._explicit_data.size() <= _explicit_limit()) {
        _explicit_data.insert(_explicit_data.end(), other._explicit_data.begin(),
                              other._explicit_data.end());
        return;
    }
    if (_type == EXPLICIT) {
        // A historical explicit partial can exceed the newly selected low-C limit.
        _convert_to_tdigest();
    } else if (_type == SINGLE && other._explicit_data.size() + 1 > _explicit_limit()) {
        _convert_to_tdigest();
    }
    if (_type == TDIGEST) {
        auto& digest = _mutable_tdigest();
        for (double value : other._explicit_data) {
            _add_to_digest(digest, value);
        }
        return;
    }
    for (double value : other._explicit_data) {
        add_value(value);
    }
}

void QuantileState::merge(const QuantileState& other) {
    if (this == &other) {
        const QuantileState source(other);
        merge(source);
        return;
    }
    if (other._type == EMPTY) {
        return;
    }
    if (_type == EMPTY) {
        // EMPTY carries no precision choice: the first nonempty input supplies both samples and C.
        *this = other;
        _normalize_digest();
        return;
    }

    // Mixed partials use the higher requested compression in either merge order.
    // A previously compressed low-C input cannot regain the precision it already discarded.
    _compression = std::max(_compression, other._compression);
    _normalize_digest();

    if (other._type == SINGLE) {
        add_value(other._single_data);
    } else if (other._type == EXPLICIT) {
        _merge_explicit(other);
    } else if (other._type == TDIGEST) {
        if (_type != TDIGEST) {
            const auto& source_digest = other._tdigest_ptr->digest;
            const bool can_share = _compression == other._compression &&
                                   source_digest.compression() == _compression &&
                                   (_compression >= QUANTILE_STATE_COMPRESSION_DEFAULT ||
                                    source_digest.max_unprocessed() ==
                                            static_cast<Index>(std::ceil(_compression)));
            if (can_share) {
                // Sharing requires matching inner metadata; a 10000 outer header must not
                // hide a 2048 digest when a singleton meets a lower-C partial.
                _tdigest_ptr = other._tdigest_ptr;
                const auto old_type = _type;
                _type = TDIGEST;
                auto& digest = _mutable_tdigest();
                if (old_type == SINGLE) {
                    _add_to_digest(digest, _single_data);
                } else {
                    for (double value : _explicit_data) {
                        _add_to_digest(digest, value);
                    }
                    std::vector<double>().swap(_explicit_data);
                }
                return;
            }
            _convert_to_tdigest();
        }
        auto& digest = _mutable_tdigest();
        std::shared_lock lock(other._tdigest_ptr->mutex);
#ifdef BE_TEST
        TEST_SYNC_POINT("QuantileState::merge:source_locked");
#endif
        if (_compression < QUANTILE_STATE_COMPRESSION_DEFAULT) {
            // A high-C partial may have tens of thousands of pending centroids;
            // bulk merge would retain that allocation in this low-C result.
            digest.merge_bounded(&other._tdigest_ptr->digest);
        } else {
            digest.merge_preserving_extrema(&other._tdigest_ptr->digest);
        }
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
        // A legacy explicit state may already exceed a newly selected low-C limit.
        if (_explicit_data.size() >= _explicit_limit()) {
            _convert_to_tdigest();
            _add_to_digest(_tdigest_ptr->digest, value);
        } else {
            _explicit_data.emplace_back(value);
        }
        break;
    case TDIGEST:
        _add_to_digest(_mutable_tdigest(), value);
        break;
    }
}

void QuantileState::reset() {
    _type = EMPTY;
    _tdigest_ptr.reset();
    _explicit_data.clear();
}

void QuantileState::clear() {
    reset();
    _explicit_data.shrink_to_fit();
}

} // namespace doris
