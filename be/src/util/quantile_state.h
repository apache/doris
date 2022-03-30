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
#ifndef DORIS_BE_SRC_OLAP_QUANTILE_STATE_H
#define DORIS_BE_SRC_OLAP_QUANTILE_STATE_H

#include <memory>
#include <string>
#include <vector>

#include "slice.h"
#include "tdigest.h"

namespace doris {

struct Slice;
class TDigest;

const static int QUANTILE_STATE_EXPLICIT_NUM = 2048;
const static int QUANTILE_STATE_COMPRESSION_MIN = 2048;
const static int QUANTILE_STATE_COMPRESSION_MAX = 10000;

enum QuantileStateType {
    EMPTY = 0,
    SINGLE = 1,   // single element
    EXPLICIT = 2, // more than one elements,stored in vector
    TDIGEST = 3   // TDIGEST object
};

template <typename T>
class QuantileState {
public:
    QuantileState();
    explicit QuantileState(float compression);
    explicit QuantileState(const Slice& slice);
    void set_compression(float compression);
    bool deserialize(const Slice& slice);
    size_t serialize(uint8_t* dst) const;
    void merge(QuantileState<T>& other);
    void add_value(const T& value);
    void clear();
    bool is_valid(const Slice& slice);
    size_t get_serialized_size();
    T get_value_by_percentile(float percentile);
    T get_explicit_value_by_percentile(float percentile);
    ~QuantileState() = default;

private:
    QuantileStateType _type = EMPTY;
    std::unique_ptr<TDigest> _tdigest_ptr;
    T _single_data;
    std::vector<T> _explicit_data;
    float _compression;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_QUANTILE_STATE_H
