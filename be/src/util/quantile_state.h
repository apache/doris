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

#pragma once

#include <stddef.h>
#include <stdint.h>

#include <algorithm>
#include <memory>
#include <vector>

#include "slice.h"

namespace doris {

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

class QuantileState {
public:
    QuantileState();
    explicit QuantileState(float compression);
    explicit QuantileState(const Slice& slice);
    QuantileState& operator=(const QuantileState& other) noexcept = default;
    QuantileState(const QuantileState& other) noexcept = default;
    QuantileState& operator=(QuantileState&& other) noexcept = default;
    QuantileState(QuantileState&& other) noexcept = default;

    void set_compression(float compression);
    bool deserialize(const Slice& slice);
    size_t serialize(uint8_t* dst) const;
    void merge(const QuantileState& other);
    void add_value(const double& value);
    void clear();
    bool is_valid(const Slice& slice);
    size_t get_serialized_size();
    double get_value_by_percentile(float percentile) const;
    double get_explicit_value_by_percentile(float percentile) const;
    ~QuantileState() = default;

private:
    QuantileStateType _type = EMPTY;
    std::shared_ptr<TDigest> _tdigest_ptr;
    double _single_data;
    std::vector<double> _explicit_data;
    float _compression;
};

} // namespace doris
