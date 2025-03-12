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

#include "DataSketches/hll.hpp"

#include "util/slice.h"

namespace doris {

using ds_hll_union = datasketches::hll_union;
using ds_hll_sketch = datasketches::hll_sketch;
using ds_hll_type = datasketches::target_hll_type;

class DSHyperLogLog {
    static const uint8_t DEFAULT_LG_K = 17;
    static const std::string DEFAULT_HLL_TYPE;
    static const std::unordered_map<std::string, ds_hll_type> ds_hll_map;
public:
    explicit DSHyperLogLog(
            uint8_t lg_k = DEFAULT_LG_K, std::string hll_type_str = DEFAULT_HLL_TYPE);

    void update(uint64_t hash_value);

    void merge(const DSHyperLogLog& other);

    int64_t estimate_cardinality() const {
        if (_sketch_union) {
            return (int64_t)_sketch_union->get_estimate();
        } else if (_sketch) {
            return (int64_t)_sketch->get_estimate();
        } else {
            return 0;
        }
    }

    std::string to_string() const {
        std::string str {"ds hll"};
        str.append("\ncardinality:\t");
        str.append(std::to_string(estimate_cardinality()));
        return str;
    }

    void clear() {
        if (_sketch_union) {
            _sketch_union.reset();
        }
        if (_sketch) {
            _sketch.reset();
        }
    }
    size_t max_serialized_size() const;
    ds_hll_sketch* get_sketch() const;
    size_t serialize(uint8_t* dst) const;
    bool deserialize(const Slice& slice);

private:

    void _check_lg_k() {
        if ((_lg_config_k < datasketches::hll_constants::MIN_LOG_K)
            || (_lg_config_k > datasketches::hll_constants::MAX_LOG_K)) {
            throw std::invalid_argument("Invalid value of k: " + std::to_string(_lg_config_k));
        }
    }

    mutable std::unique_ptr<ds_hll_union> _sketch_union; // used for merge
    uint8_t _lg_config_k;
    ds_hll_type _hll_type;
    mutable std::unique_ptr<ds_hll_sketch> _sketch;
};

} // namespace hll end