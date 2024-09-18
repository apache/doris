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
#include <cfloat>
#include <cmath>
#include <iostream>
#include <memory>
#include <queue>
#include <utility>
#include <vector>

#include "common/factory_creator.h"
#include "common/logging.h"
#include "udf/udf.h"
#include "util/debug_util.h"
#include "util/radix_sort.h"

namespace doris {

using Value = float;
using Weight = float;
using Index = size_t;

const size_t kHighWater = 40000;

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
    CentroidListComparator() {}

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
    struct TDigestRadixSortTraits {
        using Element = Centroid;
        using Key = Value;
        using CountType = uint32_t;
        using KeyBits = uint32_t;

        static constexpr size_t PART_SIZE_BITS = 8;

        using Transform = RadixSortFloatTransform<KeyBits>;
        using Allocator = RadixSortMallocAllocator;

        static Key& extractKey(Element& elem) { return elem.mean(); }
    };

    class TDigestComparator {
    public:
        TDigestComparator() {}

        bool operator()(const TDigest* left, const TDigest* right) const {
            return left->totalSize() > right->totalSize();
        }
    };
    using TDigestQueue =
            std::priority_queue<const TDigest*, std::vector<const TDigest*>, TDigestComparator>;

public:
    TDigest() : TDigest(10000) {}

    explicit TDigest(Value compression) : TDigest(compression, 0) {}

    TDigest(Value compression, Index bufferSize) : TDigest(compression, bufferSize, 0) {}

    TDigest(Value compression, Index unmergedSize, Index mergedSize)
            : _compression(compression),
              _max_processed(processedSize(mergedSize, compression)),
              _max_unprocessed(unprocessedSize(unmergedSize, compression)) {
        _processed.reserve(_max_processed);
        _unprocessed.reserve(_max_unprocessed + 1);
    }

    TDigest(std::vector<Centroid>&& processed, std::vector<Centroid>&& unprocessed,
            Value compression, Index unmergedSize, Index mergedSize)
            : TDigest(compression, unmergedSize, mergedSize) {
        _processed = std::move(processed);
        _unprocessed = std::move(unprocessed);

        _processed_weight = weight(_processed);
        _unprocessed_weight = weight(_unprocessed);
        if (_processed.size() > 0) {
            _min = std::min(_min, _processed[0].mean());
            _max = std::max(_max, (_processed.cend() - 1)->mean());
        }
        updateCumulative();
    }

    static Weight weight(std::vector<Centroid>& centroids) noexcept {
        Weight w = 0.0;
        for (auto centroid : centroids) {
            w += centroid.weight();
        }
        return w;
    }

    TDigest& operator=(TDigest&& o) {
        _compression = o._compression;
        _max_processed = o._max_processed;
        _max_unprocessed = o._max_unprocessed;
        _processed_weight = o._processed_weight;
        _unprocessed_weight = o._unprocessed_weight;
        _processed = std::move(o._processed);
        _unprocessed = std::move(o._unprocessed);
        _cumulative = std::move(o._cumulative);
        _min = o._min;
        _max = o._max;
        return *this;
    }

    TDigest(TDigest&& o)
            : TDigest(std::move(o._processed), std::move(o._unprocessed), o._compression,
                      o._max_unprocessed, o._max_processed) {}

    static inline Index processedSize(Index size, Value compression) noexcept {
        return (size == 0) ? static_cast<Index>(2 * std::ceil(compression)) : size;
    }

    static inline Index unprocessedSize(Index size, Value compression) noexcept {
        return (size == 0) ? static_cast<Index>(8 * std::ceil(compression)) : size;
    }

    // merge in another t-digest
    void merge(const TDigest* other) {
        std::vector<const TDigest*> others {other};
        add(others.cbegin(), others.cend());
    }

    const std::vector<Centroid>& processed() const { return _processed; }

    const std::vector<Centroid>& unprocessed() const { return _unprocessed; }

    Index maxUnprocessed() const { return _max_unprocessed; }

    Index maxProcessed() const { return _max_processed; }

    void add(std::vector<const TDigest*> digests) { add(digests.cbegin(), digests.cend()); }

    // merge in a vector of tdigests in the most efficient manner possible
    // in constant space
    // works for any value of kHighWater
    void add(std::vector<const TDigest*>::const_iterator iter,
             std::vector<const TDigest*>::const_iterator end) {
        if (iter != end) {
            auto size = std::distance(iter, end);
            TDigestQueue pq(TDigestComparator {});
            for (; iter != end; iter++) {
                pq.push((*iter));
            }
            std::vector<const TDigest*> batch;
            batch.reserve(size);

            size_t totalSize = 0;
            while (!pq.empty()) {
                auto td = pq.top();
                batch.push_back(td);
                pq.pop();
                totalSize += td->totalSize();
                if (totalSize >= kHighWater || pq.empty()) {
                    mergeProcessed(batch);
                    mergeUnprocessed(batch);
                    processIfNecessary();
                    batch.clear();
                    totalSize = 0;
                }
            }
            updateCumulative();
        }
    }

    Weight processedWeight() const { return _processed_weight; }

    Weight unprocessedWeight() const { return _unprocessed_weight; }

    bool haveUnprocessed() const { return _unprocessed.size() > 0; }

    size_t totalSize() const { return _processed.size() + _unprocessed.size(); }

    long totalWeight() const { return static_cast<long>(_processed_weight + _unprocessed_weight); }

    // return the cdf on the t-digest
    Value cdf(Value x) {
        if (haveUnprocessed() || isDirty()) process();
        return cdfProcessed(x);
    }

    bool isDirty() {
        return _processed.size() > _max_processed || _unprocessed.size() > _max_unprocessed;
    }

    // return the cdf on the processed values
    Value cdfProcessed(Value x) const {
        VLOG_CRITICAL << "cdf value " << x;
        VLOG_CRITICAL << "processed size " << _processed.size();
        if (_processed.size() == 0) {
            // no data to examine
            VLOG_CRITICAL << "no processed values";

            return 0.0;
        } else if (_processed.size() == 1) {
            VLOG_CRITICAL << "one processed value "
                          << " _min " << _min << " _max " << _max;
            // exactly one centroid, should have _max==_min
            auto width = _max - _min;
            if (x < _min) {
                return 0.0;
            } else if (x > _max) {
                return 1.0;
            } else if (x - _min <= width) {
                // _min and _max are too close together to do any viable interpolation
                return 0.5;
            } else {
                // interpolate if somehow we have weight > 0 and _max != _min
                return (x - _min) / (_max - _min);
            }
        } else {
            auto n = _processed.size();
            if (x <= _min) {
                VLOG_CRITICAL << "below _min "
                              << " _min " << _min << " x " << x;
                return 0;
            }

            if (x >= _max) {
                VLOG_CRITICAL << "above _max "
                              << " _max " << _max << " x " << x;
                return 1;
            }

            // check for the left tail
            if (x <= mean(0)) {
                VLOG_CRITICAL << "left tail "
                              << " _min " << _min << " mean(0) " << mean(0) << " x " << x;

                // note that this is different than mean(0) > _min ... this guarantees interpolation works
                if (mean(0) - _min > 0) {
                    return (x - _min) / (mean(0) - _min) * weight(0) / _processed_weight / 2.0;
                } else {
                    return 0;
                }
            }

            // and the right tail
            if (x >= mean(n - 1)) {
                VLOG_CRITICAL << "right tail"
                              << " _max " << _max << " mean(n - 1) " << mean(n - 1) << " x " << x;

                if (_max - mean(n - 1) > 0) {
                    return 1.0 - (_max - x) / (_max - mean(n - 1)) * weight(n - 1) /
                                         _processed_weight / 2.0;
                } else {
                    return 1;
                }
            }

            CentroidComparator cc;
            auto iter =
                    std::upper_bound(_processed.cbegin(), _processed.cend(), Centroid(x, 0), cc);

            auto i = std::distance(_processed.cbegin(), iter);
            auto z1 = x - (iter - 1)->mean();
            auto z2 = (iter)->mean() - x;
            DCHECK_LE(0.0, z1);
            DCHECK_LE(0.0, z2);
            VLOG_CRITICAL << "middle "
                          << " z1 " << z1 << " z2 " << z2 << " x " << x;

            return weightedAverage(_cumulative[i - 1], z2, _cumulative[i], z1) / _processed_weight;
        }
    }

    // this returns a quantile on the t-digest
    Value quantile(Value q) {
        if (haveUnprocessed() || isDirty()) process();
        return quantileProcessed(q);
    }

    // this returns a quantile on the currently processed values without changing the t-digest
    // the value will not represent the unprocessed values
    Value quantileProcessed(Value q) const {
        if (q < 0 || q > 1) {
            VLOG_CRITICAL << "q should be in [0,1], got " << q;
            return NAN;
        }

        if (_processed.size() == 0) {
            // no sorted means no data, no way to get a quantile
            return NAN;
        } else if (_processed.size() == 1) {
            // with one data point, all quantiles lead to Rome

            return mean(0);
        }

        // we know that there are at least two sorted now
        auto n = _processed.size();

        // if values were stored in a sorted array, index would be the offset we are Weighterested in
        const auto index = q * _processed_weight;

        // at the boundaries, we return _min or _max
        if (index <= weight(0) / 2.0) {
            DCHECK_GT(weight(0), 0);
            return _min + 2.0 * index / weight(0) * (mean(0) - _min);
        }

        auto iter = std::lower_bound(_cumulative.cbegin(), _cumulative.cend(), index);

        if (iter + 1 != _cumulative.cend()) {
            auto i = std::distance(_cumulative.cbegin(), iter);
            auto z1 = index - *(iter - 1);
            auto z2 = *(iter)-index;
            // VLOG_CRITICAL << "z2 " << z2 << " index " << index << " z1 " << z1;
            return weightedAverage(mean(i - 1), z2, mean(i), z1);
        }

        DCHECK_LE(index, _processed_weight);
        DCHECK_GE(index, _processed_weight - weight(n - 1) / 2.0);

        auto z1 = index - _processed_weight - weight(n - 1) / 2.0;
        auto z2 = weight(n - 1) / 2 - z1;
        return weightedAverage(mean(n - 1), z1, _max, z2);
    }

    Value compression() const { return _compression; }

    void add(Value x) { add(x, 1); }

    void compress() { process(); }

    // add a single centroid to the unprocessed vector, processing previously unprocessed sorted if our limit has
    // been reached.
    bool add(Value x, Weight w) {
        if (std::isnan(x)) {
            return false;
        }
        _unprocessed.push_back(Centroid(x, w));
        _unprocessed_weight += w;
        processIfNecessary();
        return true;
    }

    void add(std::vector<Centroid>::const_iterator iter,
             std::vector<Centroid>::const_iterator end) {
        while (iter != end) {
            const size_t diff = std::distance(iter, end);
            const size_t room = _max_unprocessed - _unprocessed.size();
            auto mid = iter + std::min(diff, room);
            while (iter != mid) _unprocessed.push_back(*(iter++));
            if (_unprocessed.size() >= _max_unprocessed) {
                process();
            }
        }
    }

    uint32_t serialized_size() {
        return sizeof(uint32_t) + sizeof(Value) * 5 + sizeof(Index) * 2 + sizeof(uint32_t) * 3 +
               _processed.size() * sizeof(Centroid) + _unprocessed.size() * sizeof(Centroid) +
               _cumulative.size() * sizeof(Weight);
    }

    size_t serialize(uint8_t* writer) {
        uint8_t* dst = writer;
        uint32_t total_size = serialized_size();
        memcpy(writer, &total_size, sizeof(uint32_t));
        writer += sizeof(uint32_t);
        memcpy(writer, &_compression, sizeof(Value));
        writer += sizeof(Value);
        memcpy(writer, &_min, sizeof(Value));
        writer += sizeof(Value);
        memcpy(writer, &_max, sizeof(Value));
        writer += sizeof(Value);
        memcpy(writer, &_max_processed, sizeof(Index));
        writer += sizeof(Index);
        memcpy(writer, &_max_unprocessed, sizeof(Index));
        writer += sizeof(Index);
        memcpy(writer, &_processed_weight, sizeof(Value));
        writer += sizeof(Value);
        memcpy(writer, &_unprocessed_weight, sizeof(Value));
        writer += sizeof(Value);

        uint32_t size = _processed.size();
        memcpy(writer, &size, sizeof(uint32_t));
        writer += sizeof(uint32_t);
        for (int i = 0; i < size; i++) {
            memcpy(writer, &_processed[i], sizeof(Centroid));
            writer += sizeof(Centroid);
        }

        size = _unprocessed.size();
        memcpy(writer, &size, sizeof(uint32_t));
        writer += sizeof(uint32_t);
        //TODO(weixiang): may be once memcpy is enough!
        for (int i = 0; i < size; i++) {
            memcpy(writer, &_unprocessed[i], sizeof(Centroid));
            writer += sizeof(Centroid);
        }

        size = _cumulative.size();
        memcpy(writer, &size, sizeof(uint32_t));
        writer += sizeof(uint32_t);
        for (int i = 0; i < size; i++) {
            memcpy(writer, &_cumulative[i], sizeof(Weight));
            writer += sizeof(Weight);
        }
        return writer - dst;
    }

    void unserialize(const uint8_t* type_reader) {
        uint32_t total_length = 0;
        memcpy(&total_length, type_reader, sizeof(uint32_t));
        type_reader += sizeof(uint32_t);
        memcpy(&_compression, type_reader, sizeof(Value));
        type_reader += sizeof(Value);
        memcpy(&_min, type_reader, sizeof(Value));
        type_reader += sizeof(Value);
        memcpy(&_max, type_reader, sizeof(Value));
        type_reader += sizeof(Value);

        memcpy(&_max_processed, type_reader, sizeof(Index));
        type_reader += sizeof(Index);
        memcpy(&_max_unprocessed, type_reader, sizeof(Index));
        type_reader += sizeof(Index);
        memcpy(&_processed_weight, type_reader, sizeof(Value));
        type_reader += sizeof(Value);
        memcpy(&_unprocessed_weight, type_reader, sizeof(Value));
        type_reader += sizeof(Value);

        uint32_t size;
        memcpy(&size, type_reader, sizeof(uint32_t));
        type_reader += sizeof(uint32_t);
        _processed.resize(size);
        for (int i = 0; i < size; i++) {
            memcpy(&_processed[i], type_reader, sizeof(Centroid));
            type_reader += sizeof(Centroid);
        }
        memcpy(&size, type_reader, sizeof(uint32_t));
        type_reader += sizeof(uint32_t);
        _unprocessed.resize(size);
        for (int i = 0; i < size; i++) {
            memcpy(&_unprocessed[i], type_reader, sizeof(Centroid));
            type_reader += sizeof(Centroid);
        }
        memcpy(&size, type_reader, sizeof(uint32_t));
        type_reader += sizeof(uint32_t);
        _cumulative.resize(size);
        for (int i = 0; i < size; i++) {
            memcpy(&_cumulative[i], type_reader, sizeof(Weight));
            type_reader += sizeof(Weight);
        }
    }

private:
    Value _compression;

    Value _min = std::numeric_limits<Value>::max();

    Value _max = std::numeric_limits<Value>::min();

    Index _max_processed;

    Index _max_unprocessed;

    Value _processed_weight = 0.0;

    Value _unprocessed_weight = 0.0;

    std::vector<Centroid> _processed;

    std::vector<Centroid> _unprocessed;

    std::vector<Weight> _cumulative;

    // return mean of i-th centroid
    Value mean(int i) const noexcept { return _processed[i].mean(); }

    // return weight of i-th centroid
    Weight weight(int i) const noexcept { return _processed[i].weight(); }

    // append all unprocessed centroids into current unprocessed vector
    void mergeUnprocessed(const std::vector<const TDigest*>& tdigests) {
        if (tdigests.size() == 0) return;

        size_t total = _unprocessed.size();
        for (auto& td : tdigests) {
            total += td->_unprocessed.size();
        }

        _unprocessed.reserve(total);
        for (auto& td : tdigests) {
            _unprocessed.insert(_unprocessed.end(), td->_unprocessed.cbegin(),
                                td->_unprocessed.cend());
            _unprocessed_weight += td->_unprocessed_weight;
        }
    }

    // merge all processed centroids together into a single sorted vector
    void mergeProcessed(const std::vector<const TDigest*>& tdigests) {
        if (tdigests.size() == 0) return;

        size_t total = 0;
        CentroidListQueue pq(CentroidListComparator {});
        for (auto& td : tdigests) {
            auto& sorted = td->_processed;
            auto size = sorted.size();
            if (size > 0) {
                pq.push(CentroidList(sorted));
                total += size;
                _processed_weight += td->_processed_weight;
            }
        }
        if (total == 0) return;

        if (_processed.size() > 0) {
            pq.push(CentroidList(_processed));
            total += _processed.size();
        }

        std::vector<Centroid> sorted;
        VLOG_CRITICAL << "total " << total;
        sorted.reserve(total);

        while (!pq.empty()) {
            auto best = pq.top();
            pq.pop();
            sorted.push_back(*(best.iter));
            if (best.advance()) pq.push(best);
        }
        _processed = std::move(sorted);
        if (_processed.size() > 0) {
            _min = std::min(_min, _processed[0].mean());
            _max = std::max(_max, (_processed.cend() - 1)->mean());
        }
    }

    void processIfNecessary() {
        if (isDirty()) {
            process();
        }
    }

    void updateCumulative() {
        const auto n = _processed.size();
        _cumulative.clear();
        _cumulative.reserve(n + 1);
        auto previous = 0.0;
        for (Index i = 0; i < n; i++) {
            auto current = weight(i);
            auto halfCurrent = current / 2.0;
            _cumulative.push_back(previous + halfCurrent);
            previous = previous + current;
        }
        _cumulative.push_back(previous);
    }

    // merges _unprocessed centroids and _processed centroids together and processes them
    // when complete, _unprocessed will be empty and _processed will have at most _max_processed centroids
    void process() {
        CentroidComparator cc;
        // select percentile_approx(lo_orderkey,0.5) from lineorder;
        // have test pdqsort and RadixSort, find here pdqsort performance is better when data is struct Centroid
        // But when sort plain type like int/float of std::vector<T>, find RadixSort is better
        pdqsort(_unprocessed.begin(), _unprocessed.end(), cc);
        auto count = _unprocessed.size();
        _unprocessed.insert(_unprocessed.end(), _processed.cbegin(), _processed.cend());
        std::inplace_merge(_unprocessed.begin(), _unprocessed.begin() + count, _unprocessed.end(),
                           cc);

        _processed_weight += _unprocessed_weight;
        _unprocessed_weight = 0;
        _processed.clear();

        _processed.push_back(_unprocessed[0]);
        Weight wSoFar = _unprocessed[0].weight();
        Weight wLimit = _processed_weight * integratedQ(1.0);

        auto end = _unprocessed.end();
        for (auto iter = _unprocessed.cbegin() + 1; iter < end; iter++) {
            auto& centroid = *iter;
            Weight projectedW = wSoFar + centroid.weight();
            if (projectedW <= wLimit) {
                wSoFar = projectedW;
                (_processed.end() - 1)->add(centroid);
            } else {
                auto k1 = integratedLocation(wSoFar / _processed_weight);
                wLimit = _processed_weight * integratedQ(k1 + 1.0);
                wSoFar += centroid.weight();
                _processed.emplace_back(centroid);
            }
        }
        _unprocessed.clear();
        _min = std::min(_min, _processed[0].mean());
        VLOG_CRITICAL << "new _min " << _min;
        _max = std::max(_max, (_processed.cend() - 1)->mean());
        VLOG_CRITICAL << "new _max " << _max;
        updateCumulative();
    }

    int checkWeights() { return checkWeights(_processed, _processed_weight); }

    size_t checkWeights(const std::vector<Centroid>& sorted, Value total) {
        size_t badWeight = 0;
        auto k1 = 0.0;
        auto q = 0.0;
        for (auto iter = sorted.cbegin(); iter != sorted.cend(); iter++) {
            auto w = iter->weight();
            auto dq = w / total;
            auto k2 = integratedLocation(q + dq);
            if (k2 - k1 > 1 && w != 1) {
                VLOG_CRITICAL << "Oversize centroid at " << std::distance(sorted.cbegin(), iter)
                              << " k1 " << k1 << " k2 " << k2 << " dk " << (k2 - k1) << " w " << w
                              << " q " << q;
                badWeight++;
            }
            if (k2 - k1 > 1.5 && w != 1) {
                VLOG_CRITICAL << "Egregiously Oversize centroid at "
                              << std::distance(sorted.cbegin(), iter) << " k1 " << k1 << " k2 "
                              << k2 << " dk " << (k2 - k1) << " w " << w << " q " << q;
                badWeight++;
            }
            q += dq;
            k1 = k2;
        }

        return badWeight;
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
    Value integratedLocation(Value q) const {
        return _compression * (std::asin(2.0 * q - 1.0) + M_PI / 2) / M_PI;
    }

    Value integratedQ(Value k) const {
        return (std::sin(std::min(k, _compression) * M_PI / _compression - M_PI / 2) + 1) / 2;
    }

    /**
     * Same as {@link #weightedAverageSorted(Value, Value, Value, Value)} but flips
     * the order of the variables if <code>x2</code> is greater than
     * <code>x1</code>.
    */
    static Value weightedAverage(Value x1, Value w1, Value x2, Value w2) {
        return (x1 <= x2) ? weightedAverageSorted(x1, w1, x2, w2)
                          : weightedAverageSorted(x2, w2, x1, w1);
    }

    /**
    * Compute the weighted average between <code>x1</code> with a weight of
    * <code>w1</code> and <code>x2</code> with a weight of <code>w2</code>.
    * This expects <code>x1</code> to be less than or equal to <code>x2</code>
    * and is guaranteed to return a number between <code>x1</code> and
    * <code>x2</code>.
    */
    static Value weightedAverageSorted(Value x1, Value w1, Value x2, Value w2) {
        DCHECK_LE(x1, x2);
        const Value x = (x1 * w1 + x2 * w2) / (w1 + w2);
        return std::max(x1, std::min(x, x2));
    }

    static Value interpolate(Value x, Value x0, Value x1) { return (x - x0) / (x1 - x0); }

    /**
    * Computes an interpolated value of a quantile that is between two sorted.
    *
    * Index is the quantile desired multiplied by the total number of samples - 1.
    *
    * @param index              Denormalized quantile desired
    * @param previousIndex      The denormalized quantile corresponding to the center of the previous centroid.
    * @param nextIndex          The denormalized quantile corresponding to the center of the following centroid.
    * @param previousMean       The mean of the previous centroid.
    * @param nextMean           The mean of the following centroid.
    * @return  The interpolated mean.
    */
    static Value quantile(Value index, Value previousIndex, Value nextIndex, Value previousMean,
                          Value nextMean) {
        const auto delta = nextIndex - previousIndex;
        const auto previousWeight = (nextIndex - index) / delta;
        const auto nextWeight = (index - previousIndex) / delta;
        return previousMean * previousWeight + nextMean * nextWeight;
    }
};

} // namespace doris
