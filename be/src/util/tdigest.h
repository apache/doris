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

/*
 * Licensed to Derrick R. Burns under one or more
 * contributor license agreements.  See the NOTICES file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// T-Digest :  Percentile and Quantile Estimation of Big Data
// A new data structure for accurate on-line accumulation of rank-based statistics
// such as quantiles and trimmed means.
// See original paper: "Computing extremely accurate quantiles using t-digest"
// by Ted Dunning and Otmar Ertl for more details
// https://github.com/tdunning/t-digest/blob/07b8f2ca2be8d0a9f04df2feadad5ddc1bb73c88/docs/t-digest-paper/histo.pdf.
// https://github.com/derrickburns/tdigest

#pragma once

#include <pdqsort.h>

#include <algorithm>
#include <atomic>
#include <cfloat>
#include <cmath>
#include <condition_variable>
#include <iostream>
#include <memory>
#include <mutex>
#include <queue>
#include <utility>
#include <vector>

#include "common/factory_creator.h"
#include "common/logging.h"

#ifdef BE_TEST
#include <new>

#include "cpp/sync_point.h"
#endif

namespace doris {

using Value = float;
using Weight = float;
using Index = size_t;

constexpr size_t K_HIGH_WATER = 40000;

class Centroid {
public:
    Centroid() : Centroid(0.0, 0.0) {}

    Centroid(Value mean, Weight weight) : _mean(mean), _weight(weight) {}

    Value mean() const noexcept { return _mean; }

    Weight weight() const noexcept { return _weight; }

    Value& mean() noexcept { return _mean; }

    Weight& weight() noexcept { return _weight; }

    void add(const Centroid& c) {
        DCHECK_GT(c._weight, 0);
        if (_weight != 0.0) {
            _weight += c._weight;
            _mean += c._weight * (c._mean - _mean) / _weight;
        } else {
            _weight = c._weight;
            _mean = c._mean;
        }
    }

private:
    Value _mean = 0;
    Weight _weight = 0;
};

struct CentroidList {
    CentroidList(const std::vector<Centroid>& s) : iter(s.cbegin()), end(s.cend()) {}
    std::vector<Centroid>::const_iterator iter;
    std::vector<Centroid>::const_iterator end;

    bool advance() { return ++iter != end; }
};

class CentroidListComparator {
public:
    CentroidListComparator() = default;

    bool operator()(const CentroidList& left, const CentroidList& right) const {
        return left.iter->mean() > right.iter->mean();
    }
};

using CentroidListQueue =
        std::priority_queue<CentroidList, std::vector<CentroidList>, CentroidListComparator>;

struct CentroidComparator {
    bool operator()(const Centroid& a, const Centroid& b) const { return a.mean() < b.mean(); }
};

class TDigest {
    ENABLE_FACTORY_CREATOR(TDigest);

    class TDigestComparator {
    public:
        TDigestComparator() = default;

        bool operator()(const TDigest* left, const TDigest* right) const {
            return left->total_size() > right->total_size();
        }
    };
    using TDigestQueue =
            std::priority_queue<const TDigest*, std::vector<const TDigest*>, TDigestComparator>;

public:
    TDigest() : TDigest(10000) {}

    explicit TDigest(Value compression) : TDigest(compression, 0) {}

    TDigest(Value compression, Index buffer_size) : TDigest(compression, buffer_size, 0) {}

    TDigest(Value compression, Index unmerged_size, Index merged_size)
            : _data(std::make_shared<Data>(compression, unmerged_size, merged_size)) {}

    TDigest(std::vector<Centroid>&& processed, std::vector<Centroid>&& unprocessed,
            Value compression, Index unmerged_size, Index merged_size)
            : TDigest(compression, unmerged_size, merged_size) {
        _data->_processed = std::move(processed);
        _data->_unprocessed = std::move(unprocessed);

        _data->_processed_weight = weight(_data->_processed);
        _data->_unprocessed_weight = weight(_data->_unprocessed);
        if (_data->_processed.size() > 0) {
            _data->_min = std::min(_data->_min, _data->_processed[0].mean());
            _data->_max = std::max(_data->_max, (_data->_processed.cend() - 1)->mean());
        }
        _update_cumulative();
    }

    static Weight weight(std::vector<Centroid>& centroids) noexcept {
        Weight w = 0.0;
        for (auto centroid : centroids) {
            w += centroid.weight();
        }
        return w;
    }

    // Copies share immutable data until either handle is modified. Distinct handles
    // may be mutated concurrently, and const operations on one handle may run
    // concurrently. Mutating the same handle requires external synchronization.
    TDigest(const TDigest& other) : _data(other._data) {
        _data->_is_shared.store(true, std::memory_order_release);
    }
    TDigest& operator=(const TDigest& other) {
        if (this != &other) {
            *this = TDigest(other);
        }
        return *this;
    }
    TDigest(TDigest&&) noexcept = default;
    TDigest& operator=(TDigest&&) noexcept = default;

    static inline Index processed_size(Index size, Value compression) noexcept {
        return (size == 0) ? static_cast<Index>(2 * std::ceil(compression)) : size;
    }

    static inline Index unprocessed_size(Index size, Value compression) noexcept {
        return (size == 0) ? static_cast<Index>(8 * std::ceil(compression)) : size;
    }

    // merge in another t-digest
    void merge(const TDigest* other) {
        std::vector<const TDigest*> others {other};
        add(others.cbegin(), others.cend());
    }

    const std::vector<Centroid>& processed() const { return _data->_processed; }

    const std::vector<Centroid>& unprocessed() const { return _data->_unprocessed; }

    Index max_unprocessed() const { return _data->_max_unprocessed; }

    Index max_processed() const { return _data->_max_processed; }

    void add(std::vector<const TDigest*> digests) { add(digests.cbegin(), digests.cend()); }

    // merge in a vector of tdigests in the most efficient manner possible
    // in constant space
    // works for any value of K_HIGH_WATER
    void add(std::vector<const TDigest*>::const_iterator iter,
             std::vector<const TDigest*>::const_iterator end) {
        if (iter == end) {
            return;
        }
        if (std::find(iter, end, this) != end) {
            const TDigest snapshot(*this);
            std::vector<const TDigest*> inputs(iter, end);
            std::replace(inputs.begin(), inputs.end(), static_cast<const TDigest*>(this),
                         &snapshot);
            _prepare_for_write();
            _add(inputs.cbegin(), inputs.cend());
            return;
        }
        _prepare_for_write();
        _add(iter, end);
    }

private:
    void _add(std::vector<const TDigest*>::const_iterator iter,
              std::vector<const TDigest*>::const_iterator end) {
        if (iter != end) {
            auto size = std::distance(iter, end);
            TDigestQueue pq(TDigestComparator {});
            for (; iter != end; iter++) {
                pq.push((*iter));
            }
            std::vector<const TDigest*> batch;
            batch.reserve(size);

            size_t total_size = 0;
            while (!pq.empty()) {
                const auto* td = pq.top();
                batch.push_back(td);
                pq.pop();
                total_size += td->total_size();
                if (total_size >= K_HIGH_WATER || pq.empty()) {
                    _merge_processed(batch);
                    _merge_unprocessed(batch);
                    _process_if_necessary();
                    batch.clear();
                    total_size = 0;
                }
            }
            _update_cumulative();
        }
    }

public:
    Weight processed_weight() const { return _data->_processed_weight; }

    Weight unprocessed_weight() const { return _data->_unprocessed_weight; }

    bool have_unprocessed() const { return _data->_unprocessed.size() > 0; }

    size_t total_size() const { return _data->_processed.size() + _data->_unprocessed.size(); }

    long total_weight() const {
        return static_cast<long>(_data->_processed_weight + _data->_unprocessed_weight);
    }

    // return the cdf on the t-digest
    Value cdf(Value x) const { return _processed_digest().cdf_processed(x); }

    bool is_dirty() const {
        return _data->_processed.size() > _data->_max_processed ||
               _data->_unprocessed.size() > _data->_max_unprocessed;
    }

    // return the cdf on the processed values
    Value cdf_processed(Value x) const {
        VLOG_CRITICAL << "cdf value " << x;
        VLOG_CRITICAL << "processed size " << _data->_processed.size();
        if (_data->_processed.size() == 0) {
            // no data to examine
            VLOG_CRITICAL << "no processed values";

            return 0.0;
        } else if (_data->_processed.size() == 1) {
            VLOG_CRITICAL << "one processed value "
                          << " _data->_min " << _data->_min << " _data->_max " << _data->_max;
            // exactly one centroid, should have _data->_max==_data->_min
            auto width = _data->_max - _data->_min;
            if (x < _data->_min) {
                return 0.0;
            } else if (x > _data->_max) {
                return 1.0;
            } else if (x - _data->_min <= width) {
                // _data->_min and _data->_max are too close together to do any viable interpolation
                return 0.5;
            } else {
                // interpolate if somehow we have weight > 0 and _data->_max != _data->_min
                return (x - _data->_min) / (_data->_max - _data->_min);
            }
        } else {
            auto n = _data->_processed.size();
            if (x <= _data->_min) {
                VLOG_CRITICAL << "below _data->_min "
                              << " _data->_min " << _data->_min << " x " << x;
                return 0;
            }

            if (x >= _data->_max) {
                VLOG_CRITICAL << "above _data->_max "
                              << " _data->_max " << _data->_max << " x " << x;
                return 1;
            }

            // check for the left tail
            if (x <= _mean(0)) {
                VLOG_CRITICAL << "left tail "
                              << " _data->_min " << _data->_min << " mean(0) " << _mean(0) << " x "
                              << x;

                // note that this is different than mean(0) > _data->_min ... this guarantees interpolation works
                if (_mean(0) - _data->_min > 0) {
                    return static_cast<Value>((x - _data->_min) / (_mean(0) - _data->_min) *
                                              _weight(0) / _data->_processed_weight / 2.0);
                } else {
                    return 0;
                }
            }

            // and the right tail
            if (x >= _mean(n - 1)) {
                VLOG_CRITICAL << "right tail"
                              << " _data->_max " << _data->_max << " mean(n - 1) " << _mean(n - 1)
                              << " x " << x;

                if (_data->_max - _mean(n - 1) > 0) {
                    return static_cast<Value>(
                            1.0 - (_data->_max - x) / (_data->_max - _mean(n - 1)) *
                                          _weight(n - 1) / _data->_processed_weight / 2.0);
                } else {
                    return 1;
                }
            }

            CentroidComparator cc;
            auto iter = std::upper_bound(_data->_processed.cbegin(), _data->_processed.cend(),
                                         Centroid(x, 0), cc);

            auto i = std::distance(_data->_processed.cbegin(), iter);
            auto z1 = x - (iter - 1)->mean();
            auto z2 = (iter)->mean() - x;
            DCHECK_LE(0.0, z1);
            DCHECK_LE(0.0, z2);
            VLOG_CRITICAL << "middle "
                          << " z1 " << z1 << " z2 " << z2 << " x " << x;

            return _weighted_average(_data->_cumulative[i - 1], z2, _data->_cumulative[i], z1) /
                   _data->_processed_weight;
        }
    }

    // this returns a quantile on the t-digest
    Value quantile(Value q) const { return _processed_digest().quantile_processed(q); }

    void quantiles(const double* quantile_levels, const size_t* permutation, size_t size,
                   double* result) const {
        if (size == 0) {
            return;
        }
        _processed_digest()._quantiles_processed(quantile_levels, permutation, size, result);
    }

private:
    void _quantiles_processed(const double* quantile_levels, const size_t* permutation, size_t size,
                              double* result) const {
        if (_data->_processed.empty()) {
            std::fill(result, result + size, NAN);
            return;
        }

        if (_data->_processed.size() == 1) {
            std::fill(result, result + size, static_cast<double>(_mean(0)));
            return;
        }

        const auto n = _data->_processed.size();
        size_t cumulative_index = 0;
        for (size_t result_index = 0; result_index < size; ++result_index) {
            const size_t level_index = permutation[result_index];
            const auto q = static_cast<Value>(quantile_levels[level_index]);
            DCHECK_GE(q, 0);
            DCHECK_LE(q, 1);

            const auto index = q * _data->_processed_weight;
            if (index <= _weight(0) / 2.0) {
                DCHECK_GT(_weight(0), 0);
                result[level_index] = static_cast<Value>(
                        _data->_min + 2.0 * index / _weight(0) * (_mean(0) - _data->_min));
                continue;
            }

            while (cumulative_index < _data->_cumulative.size() &&
                   _data->_cumulative[cumulative_index] < index) {
                ++cumulative_index;
            }

            if (cumulative_index > 0 && cumulative_index + 1 < _data->_cumulative.size()) {
                auto z1 = index - _data->_cumulative[cumulative_index - 1];
                auto z2 = _data->_cumulative[cumulative_index] - index;
                result[level_index] = static_cast<double>(_weighted_average(
                        _mean(cumulative_index - 1), z2, _mean(cumulative_index), z1));
                continue;
            }

            DCHECK_LE(index, _data->_processed_weight);
            DCHECK_GE(index, _data->_processed_weight - _weight(n - 1) / 2.0);
            auto z1 = static_cast<Value>(index - _data->_processed_weight - _weight(n - 1) / 2.0);
            auto z2 = static_cast<Value>(_weight(n - 1) / 2 - z1);
            result[level_index] =
                    static_cast<double>(_weighted_average(_mean(n - 1), z1, _data->_max, z2));
        }
    }

public:
    // this returns a quantile on the currently processed values without changing the t-digest
    // the value will not represent the unprocessed values
    Value quantile_processed(Value q) const {
        if (q < 0 || q > 1) {
            VLOG_CRITICAL << "q should be in [0,1], got " << q;
            return NAN;
        }

        if (_data->_processed.size() == 0) {
            // no sorted means no data, no way to get a quantile
            return NAN;
        } else if (_data->_processed.size() == 1) {
            // with one data point, all quantiles lead to Rome

            return _mean(0);
        }

        // we know that there are at least two sorted now
        auto n = _data->_processed.size();

        // if values were stored in a sorted array, index would be the offset we are Weighterested in
        const auto index = q * _data->_processed_weight;

        // at the boundaries, we return _data->_min or _data->_max
        if (index <= _weight(0) / 2.0) {
            DCHECK_GT(_weight(0), 0);
            return static_cast<Value>(_data->_min +
                                      2.0 * index / _weight(0) * (_mean(0) - _data->_min));
        }

        auto iter = std::lower_bound(_data->_cumulative.cbegin(), _data->_cumulative.cend(), index);

        if (iter != _data->_cumulative.cend() && iter != _data->_cumulative.cbegin() &&
            iter + 1 != _data->_cumulative.cend()) {
            auto i = std::distance(_data->_cumulative.cbegin(), iter);
            auto z1 = index - *(iter - 1);
            auto z2 = *(iter)-index;
            // VLOG_CRITICAL << "z2 " << z2 << " index " << index << " z1 " << z1;
            return _weighted_average(_mean(i - 1), z2, _mean(i), z1);
        }

        DCHECK_LE(index, _data->_processed_weight);
        DCHECK_GE(index, _data->_processed_weight - _weight(n - 1) / 2.0);

        auto z1 = static_cast<Value>(index - _data->_processed_weight - _weight(n - 1) / 2.0);
        auto z2 = static_cast<Value>(_weight(n - 1) / 2 - z1);
        return _weighted_average(_mean(n - 1), z1, _data->_max, z2);
    }

    Value compression() const { return _data->_compression; }

    void add(Value x) { add(x, 1); }

    void compress() {
        if (total_size() != 0) {
            _prepare_for_write();
            _process();
        }
    }

    // add a single centroid to the unprocessed vector, processing previously unprocessed sorted if our limit has
    // been reached.
    bool add(Value x, Weight w) {
        if (std::isnan(x)) {
            return false;
        }
        _prepare_for_write();
        _data->_unprocessed.emplace_back(x, w);
        _data->_unprocessed_weight += w;
        _process_if_necessary();
        return true;
    }

    void add(std::vector<Centroid>::const_iterator iter,
             std::vector<Centroid>::const_iterator end) {
        const std::vector<Centroid> centroids(iter, end);
        _prepare_for_write();
        iter = centroids.cbegin();
        end = centroids.cend();
        while (iter != end) {
            const size_t diff = std::distance(iter, end);
            const size_t room = _data->_max_unprocessed - _data->_unprocessed.size();
            auto mid = iter + std::min(diff, room);
            while (iter != mid) {
                _data->_unprocessed_weight += iter->weight();
                _data->_unprocessed.push_back(*(iter++));
            }
            if (_data->_unprocessed.size() >= _data->_max_unprocessed) {
                _process();
            }
        }
    }

    uint32_t serialized_size() const {
        return static_cast<uint32_t>(sizeof(uint32_t) + sizeof(Value) * 5 + sizeof(Index) * 2 +
                                     sizeof(uint32_t) * 3 +
                                     _data->_processed.size() * sizeof(Centroid) +
                                     _data->_unprocessed.size() * sizeof(Centroid) +
                                     _data->_cumulative.size() * sizeof(Weight));
    }

    size_t serialize(uint8_t* writer) const {
        uint8_t* dst = writer;
        uint32_t total_size = serialized_size();
        memcpy(writer, &total_size, sizeof(uint32_t));
        writer += sizeof(uint32_t);
        memcpy(writer, &_data->_compression, sizeof(Value));
        writer += sizeof(Value);
        memcpy(writer, &_data->_min, sizeof(Value));
        writer += sizeof(Value);
        memcpy(writer, &_data->_max, sizeof(Value));
        writer += sizeof(Value);
        memcpy(writer, &_data->_max_processed, sizeof(Index));
        writer += sizeof(Index);
        memcpy(writer, &_data->_max_unprocessed, sizeof(Index));
        writer += sizeof(Index);
        memcpy(writer, &_data->_processed_weight, sizeof(Value));
        writer += sizeof(Value);
        memcpy(writer, &_data->_unprocessed_weight, sizeof(Value));
        writer += sizeof(Value);

        auto size = static_cast<uint32_t>(_data->_processed.size());
        memcpy(writer, &size, sizeof(uint32_t));
        writer += sizeof(uint32_t);
        for (int i = 0; i < size; i++) {
            memcpy(writer, &_data->_processed[i], sizeof(Centroid));
            writer += sizeof(Centroid);
        }

        size = static_cast<uint32_t>(_data->_unprocessed.size());
        memcpy(writer, &size, sizeof(uint32_t));
        writer += sizeof(uint32_t);
        //TODO(weixiang): may be once memcpy is enough!
        for (int i = 0; i < size; i++) {
            memcpy(writer, &_data->_unprocessed[i], sizeof(Centroid));
            writer += sizeof(Centroid);
        }

        size = static_cast<uint32_t>(_data->_cumulative.size());
        memcpy(writer, &size, sizeof(uint32_t));
        writer += sizeof(uint32_t);
        for (int i = 0; i < size; i++) {
            memcpy(writer, &_data->_cumulative[i], sizeof(Weight));
            writer += sizeof(Weight);
        }
        return writer - dst;
    }

    void unserialize(const uint8_t* type_reader) {
        if (_data->_is_shared.load(std::memory_order_acquire)) {
            // Deserialization replaces every field, so do not copy the old payload.
            _data = std::make_shared<Data>(0, 0, 0);
        } else {
            _data->_processed_snapshot.reset();
        }
        uint32_t total_length = 0;
        memcpy(&total_length, type_reader, sizeof(uint32_t));
        type_reader += sizeof(uint32_t);
        memcpy(&_data->_compression, type_reader, sizeof(Value));
        type_reader += sizeof(Value);
        memcpy(&_data->_min, type_reader, sizeof(Value));
        type_reader += sizeof(Value);
        memcpy(&_data->_max, type_reader, sizeof(Value));
        type_reader += sizeof(Value);

        memcpy(&_data->_max_processed, type_reader, sizeof(Index));
        type_reader += sizeof(Index);
        memcpy(&_data->_max_unprocessed, type_reader, sizeof(Index));
        type_reader += sizeof(Index);
        memcpy(&_data->_processed_weight, type_reader, sizeof(Value));
        type_reader += sizeof(Value);
        memcpy(&_data->_unprocessed_weight, type_reader, sizeof(Value));
        type_reader += sizeof(Value);

        uint32_t size;
        memcpy(&size, type_reader, sizeof(uint32_t));
        type_reader += sizeof(uint32_t);
        _data->_processed.resize(size);
        for (int i = 0; i < size; i++) {
            memcpy(&_data->_processed[i], type_reader, sizeof(Centroid));
            type_reader += sizeof(Centroid);
        }
        memcpy(&size, type_reader, sizeof(uint32_t));
        type_reader += sizeof(uint32_t);
        _data->_unprocessed.resize(size);
        for (int i = 0; i < size; i++) {
            memcpy(&_data->_unprocessed[i], type_reader, sizeof(Centroid));
            type_reader += sizeof(Centroid);
        }
        memcpy(&size, type_reader, sizeof(uint32_t));
        type_reader += sizeof(uint32_t);
        _data->_cumulative.resize(size);
        for (int i = 0; i < size; i++) {
            memcpy(&_data->_cumulative[i], type_reader, sizeof(Weight));
            type_reader += sizeof(Weight);
        }
    }

private:
    struct Data {
        enum class CopyMode { READ_SNAPSHOT, WRITE };

        Data(Value compression, Index unmerged_size, Index merged_size)
                : _compression(compression),
                  _max_processed(processed_size(merged_size, compression)),
                  _max_unprocessed(unprocessed_size(unmerged_size, compression)) {
            _processed.reserve(_max_processed);
            _unprocessed.reserve(_max_unprocessed + 1);
        }

        // A detached state starts without a read cache. The source cache may be
        // initialized concurrently, so neither copy nor inspect it here.
        Data(const Data& other, CopyMode mode = CopyMode::READ_SNAPSHOT)
                : _compression(other._compression),
                  _min(other._min),
                  _max(other._max),
                  _max_processed(other._max_processed),
                  _max_unprocessed(other._max_unprocessed),
                  _processed_weight(other._processed_weight),
                  _unprocessed_weight(other._unprocessed_weight) {
            // Reserve before copying to avoid allocating and moving the payload
            // twice. Read snapshots do not need spare capacity for future adds.
            const bool for_write = mode == CopyMode::WRITE;
            _processed.reserve(for_write ? std::max(other._processed.capacity(), _max_processed)
                                         : other._processed.size());
            _unprocessed.reserve(
                    for_write ? std::max(other._unprocessed.capacity(), _max_unprocessed + 1)
                              : other._unprocessed.size());
            _cumulative.reserve(for_write ? other._cumulative.capacity()
                                          : other._cumulative.size());
            _processed.assign(other._processed.begin(), other._processed.end());
            _unprocessed.assign(other._unprocessed.begin(), other._unprocessed.end());
            _cumulative.assign(other._cumulative.begin(), other._cumulative.end());
        }

        Value _compression;
        Value _min = std::numeric_limits<Value>::max();
        Value _max = std::numeric_limits<Value>::lowest();
        Index _max_processed;
        Index _max_unprocessed;
        Value _processed_weight = 0.0;
        Value _unprocessed_weight = 0.0;
        std::vector<Centroid> _processed;
        std::vector<Centroid> _unprocessed;
        std::vector<Weight> _cumulative;
        // Once published to another handle, the payload stays immutable even
        // when its reference count returns to one. shared_ptr::use_count() does
        // not synchronize with another thread finishing reads before release.
        std::atomic<bool> _is_shared {false};
        std::mutex _read_mutex;
        std::condition_variable _snapshot_cv;
        // Guarded by _read_mutex. A non-null snapshot represents the ready state.
        bool _building_snapshot = false;
        std::unique_ptr<const TDigest> _processed_snapshot;
    };

    std::shared_ptr<Data> _data;

    explicit TDigest(const Data& data) : _data(std::make_shared<Data>(data)) {}

    void _prepare_for_write() {
        if (_data->_is_shared.load(std::memory_order_acquire)) {
            _data = std::make_shared<Data>(*_data, Data::CopyMode::WRITE);
        } else {
            _data->_processed_snapshot.reset();
        }
    }

    const TDigest& _processed_digest() const {
        if (!have_unprocessed() && !is_dirty()) {
            return *this;
        }
        std::unique_lock<std::mutex> lock(_data->_read_mutex);
#ifdef BE_TEST
        if (_data->_building_snapshot) {
            TEST_SYNC_POINT("TDigest::_processed_digest:wait_snapshot");
        }
#endif
        _data->_snapshot_cv.wait(lock, [this] { return !_data->_building_snapshot; });
        if (_data->_processed_snapshot) {
            return *_data->_processed_snapshot;
        }
        _data->_building_snapshot = true;
        lock.unlock();

        // Only this reader builds a snapshot. Other readers wait with the mutex
        // released, while copies and writers on other handles can still proceed.
        std::unique_ptr<TDigest> snapshot;
        try {
#ifdef BE_TEST
            bool fail_allocation = false;
            TEST_SYNC_POINT_CALLBACK("TDigest::_processed_digest:build_snapshot", &fail_allocation);
            if (fail_allocation) {
                throw std::bad_alloc();
            }
#endif
            snapshot = TDigest::create_unique(*_data);
            snapshot->compress();
        } catch (...) {
            // Allocation or compression can throw. Allow a waiting reader to
            // retry rather than leaving the cache permanently in building state.
            lock.lock();
            _data->_building_snapshot = false;
            lock.unlock();
            _data->_snapshot_cv.notify_all();
            throw;
        }

        lock.lock();
        _data->_processed_snapshot = std::move(snapshot);
        _data->_building_snapshot = false;
        lock.unlock();
        _data->_snapshot_cv.notify_all();
        return *_data->_processed_snapshot;
    }

    // return mean of i-th centroid
    Value _mean(int64_t i) const noexcept { return _data->_processed[i].mean(); }

    // return weight of i-th centroid
    Weight _weight(int64_t i) const noexcept { return _data->_processed[i].weight(); }

    // append all unprocessed centroids into current unprocessed vector
    void _merge_unprocessed(const std::vector<const TDigest*>& tdigests) {
        if (tdigests.size() == 0) {
            return;
        }

        size_t total = _data->_unprocessed.size();
        for (const auto& td : tdigests) {
            total += td->_data->_unprocessed.size();
        }

        _data->_unprocessed.reserve(total);
        for (const auto& td : tdigests) {
            _data->_unprocessed.insert(_data->_unprocessed.end(), td->_data->_unprocessed.cbegin(),
                                       td->_data->_unprocessed.cend());
            _data->_unprocessed_weight += td->_data->_unprocessed_weight;
        }
    }

    // merge all processed centroids together into a single sorted vector
    void _merge_processed(const std::vector<const TDigest*>& tdigests) {
        if (tdigests.size() == 0) {
            return;
        }

        size_t total = 0;
        CentroidListQueue pq(CentroidListComparator {});
        for (const auto& td : tdigests) {
            const auto& sorted = td->_data->_processed;
            auto size = sorted.size();
            if (size > 0) {
                pq.push(CentroidList(sorted));
                total += size;
                _data->_processed_weight += td->_data->_processed_weight;
            }
        }
        if (total == 0) {
            return;
        }

        if (_data->_processed.size() > 0) {
            pq.push(CentroidList(_data->_processed));
            total += _data->_processed.size();
        }

        std::vector<Centroid> sorted;
        VLOG_CRITICAL << "total " << total;
        sorted.reserve(total);

        while (!pq.empty()) {
            auto best = pq.top();
            pq.pop();
            sorted.push_back(*(best.iter));
            if (best.advance()) {
                pq.push(best);
            }
        }
        _data->_processed = std::move(sorted);
        if (_data->_processed.size() > 0) {
            _data->_min = std::min(_data->_min, _data->_processed[0].mean());
            _data->_max = std::max(_data->_max, (_data->_processed.cend() - 1)->mean());
        }
    }

    void _process_if_necessary() {
        if (is_dirty()) {
            _process();
        }
    }

    void _update_cumulative() {
        const auto n = _data->_processed.size();
        _data->_cumulative.clear();
        _data->_cumulative.reserve(n + 1);
        Weight previous = 0.0;
        for (Index i = 0; i < n; i++) {
            Weight current = _weight(i);
            auto half_current = static_cast<Weight>(current / 2.0);
            _data->_cumulative.push_back(previous + half_current);
            previous = previous + current;
        }
        _data->_cumulative.push_back(previous);
    }

    // merges _data->_unprocessed centroids and _data->_processed centroids together and processes them
    // when complete, _data->_unprocessed will be empty and _data->_processed will have at most _data->_max_processed centroids
    void _process() {
        CentroidComparator cc;
        // select percentile_approx(lo_orderkey,0.5) from lineorder;
        // have test pdqsort and RadixSort, find here pdqsort performance is better when data is struct Centroid
        // But when sort plain type like int/float of std::vector<T>, find RadixSort is better
        pdqsort(_data->_unprocessed.begin(), _data->_unprocessed.end(), cc);
        auto count = _data->_unprocessed.size();
        _data->_unprocessed.insert(_data->_unprocessed.end(), _data->_processed.cbegin(),
                                   _data->_processed.cend());
        std::inplace_merge(_data->_unprocessed.begin(), _data->_unprocessed.begin() + count,
                           _data->_unprocessed.end(), cc);

        _data->_processed_weight += _data->_unprocessed_weight;
        _data->_unprocessed_weight = 0;
        _data->_processed.clear();

        _data->_processed.push_back(_data->_unprocessed[0]);
        Weight w_so_far = _data->_unprocessed[0].weight();
        Weight w_limit = _data->_processed_weight * _integrated_q(1.0);

        auto end = _data->_unprocessed.end();
        for (auto iter = _data->_unprocessed.cbegin() + 1; iter < end; iter++) {
            const auto& centroid = *iter;
            Weight projected_w = w_so_far + centroid.weight();
            if (projected_w <= w_limit) {
                w_so_far = projected_w;
                (_data->_processed.end() - 1)->add(centroid);
            } else {
                auto k1 = _integrated_location(w_so_far / _data->_processed_weight);
                w_limit = _data->_processed_weight * _integrated_q(static_cast<Value>(k1 + 1.0));
                w_so_far += centroid.weight();
                _data->_processed.emplace_back(centroid);
            }
        }
        _data->_unprocessed.clear();
        _data->_min = std::min(_data->_min, _data->_processed[0].mean());
        VLOG_CRITICAL << "new _data->_min " << _data->_min;
        _data->_max = std::max(_data->_max, (_data->_processed.cend() - 1)->mean());
        VLOG_CRITICAL << "new _data->_max " << _data->_max;
        _update_cumulative();
    }

    size_t _check_weights(const std::vector<Centroid>& sorted, Value total) {
        size_t bad_weight = 0;
        auto k1 = 0.0;
        auto q = 0.0;
        for (auto iter = sorted.cbegin(); iter != sorted.cend(); iter++) {
            auto w = iter->weight();
            auto dq = w / total;
            auto k2 = _integrated_location(static_cast<Value>(q + dq));
            if (k2 - k1 > 1 && w != 1) {
                VLOG_CRITICAL << "Oversize centroid at " << std::distance(sorted.cbegin(), iter)
                              << " k1 " << k1 << " k2 " << k2 << " dk " << (k2 - k1) << " w " << w
                              << " q " << q;
                bad_weight++;
            }
            if (k2 - k1 > 1.5 && w != 1) {
                VLOG_CRITICAL << "Egregiously Oversize centroid at "
                              << std::distance(sorted.cbegin(), iter) << " k1 " << k1 << " k2 "
                              << k2 << " dk " << (k2 - k1) << " w " << w << " q " << q;
                bad_weight++;
            }
            q += dq;
            k1 = k2;
        }

        return bad_weight;
    }

    /**
    * Converts a quantile into a centroid scale value.  The centroid scale is nomin_ally
    * the number k of the centroid that a quantile point q should belong to.  Due to
    * round-offs, however, we can't align things perfectly without splitting points
    * and sorted.  We don't want to do that, so we have to allow for offsets.
    * In the end, the criterion is that any quantile range that spans a centroid
    * scale range more than one should be split across more than one centroid if
    * possible.  This won't be possible if the quantile range refers to a single point
    * or an already existing centroid.
    * <p/>
    * This mapping is steep near q=0 or q=1 so each centroid there will correspond to
    * less q range.  Near q=0.5, the mapping is flatter so that sorted there will
    * represent a larger chunk of quantiles.
    *
    * @param q The quantile scale value to be mapped.
    * @return The centroid scale value corresponding to q.
    */
    Value _integrated_location(Value q) const {
        return static_cast<Value>(_data->_compression * (std::asin(2.0 * q - 1.0) + M_PI / 2) /
                                  M_PI);
    }

    Value _integrated_q(Value k) const {
        return static_cast<Value>(
                (std::sin(std::min(k, _data->_compression) * M_PI / _data->_compression -
                          M_PI / 2) +
                 1) /
                2);
    }

    /**
     * Same as {@link #_weighted_average_sorted(Value, Value, Value, Value)} but flips
     * the order of the variables if <code>x2</code> is greater than
     * <code>x1</code>.
    */
    static Value _weighted_average(Value x1, Value w1, Value x2, Value w2) {
        return (x1 <= x2) ? _weighted_average_sorted(x1, w1, x2, w2)
                          : _weighted_average_sorted(x2, w2, x1, w1);
    }

    /**
    * Compute the weighted average between <code>x1</code> with a weight of
    * <code>w1</code> and <code>x2</code> with a weight of <code>w2</code>.
    * This expects <code>x1</code> to be less than or equal to <code>x2</code>
    * and is guaranteed to return a number between <code>x1</code> and
    * <code>x2</code>.
    */
    static Value _weighted_average_sorted(Value x1, Value w1, Value x2, Value w2) {
        DCHECK_LE(x1, x2);
        const Value x = (x1 * w1 + x2 * w2) / (w1 + w2);
        return std::max(x1, std::min(x, x2));
    }

    static Value _interpolate(Value x, Value x0, Value x1) { return (x - x0) / (x1 - x0); }

    /**
    * Computes an interpolated value of a quantile that is between two sorted.
    *
    * Index is the quantile desired multiplied by the total number of samples - 1.
    *
    * @param index              Denormalized quantile desired
    * @param previous_index     The denormalized quantile corresponding to the center of the previous centroid.
    * @param next_index         The denormalized quantile corresponding to the center of the following centroid.
    * @param previous_mean      The mean of the previous centroid.
    * @param next_mean          The mean of the following centroid.
    * @return  The interpolated mean.
    */
    static Value _quantile(Value index, Value previous_index, Value next_index, Value previous_mean,
                           Value next_mean) {
        const auto delta = next_index - previous_index;
        const auto previous_weight = (next_index - index) / delta;
        const auto next_weight = (index - previous_index) / delta;
        return previous_mean * previous_weight + next_mean * next_weight;
    }
};
} // namespace doris
