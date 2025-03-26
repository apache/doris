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
using ds_vector_bytes = datasketches::hll_sketch::vector_bytes;

class DSHyperLogLog {
    static const uint8_t DEFAULT_LG_K = 17;
    static const std::string DEFAULT_HLL_TYPE;
    static const std::unordered_map<std::string, ds_hll_type> ds_hll_map;

public:
    explicit DSHyperLogLog(uint8_t lg_k = DEFAULT_LG_K,
                           std::string hll_type_str = DEFAULT_HLL_TYPE);
    explicit DSHyperLogLog(uint8_t lg_k, ds_hll_type hll_type);

    explicit DSHyperLogLog(const Slice& slice);

    DSHyperLogLog(DSHyperLogLog&& other) noexcept
            : _sketch_union(std::move(other._sketch_union)),
              _lg_config_k(other._lg_config_k),
              _hll_type(other._hll_type),
              _sketch(std::move(other._sketch)) {}

    DSHyperLogLog& operator=(DSHyperLogLog&& other) noexcept {
        if (this != &other) {
            _sketch_union.reset();
            _sketch.reset();
            _sketch_union = std::move(other._sketch_union);
            _sketch = std::move(other._sketch);
            _lg_config_k = other._lg_config_k;
            _hll_type = other._hll_type;
        }
        return *this;
    }

    void update(const void* data, size_t length_bytes);

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

    uint8_t get_lg_config_k() const { return _lg_config_k; }
    ds_hll_type get_hll_type() const { return _hll_type; }

    bool is_empty() const {
        if (_sketch) {
            return _sketch->is_empty();
        }
        if (_sketch_union) {
            return _sketch_union->is_empty();
        }
        return true;
    }

    void clear() {
        if (_sketch_union) {
            _sketch_union.reset();
        }
        if (_sketch) {
            _sketch.reset();
        }
    }
    template <bool create_if_not_exists = false>
    ds_hll_sketch* get_sketch() const;
    size_t serialized_size() const;
    void serialize(ds_vector_bytes& dst) const;

private:
    bool deserialize(const Slice& slice);
    void _check_lg_k() const {
        if ((_lg_config_k < datasketches::hll_constants::MIN_LOG_K) ||
            (_lg_config_k > datasketches::hll_constants::MAX_LOG_K)) {
            throw std::invalid_argument("Invalid value of k: " + std::to_string(_lg_config_k));
        }
    }

    mutable std::unique_ptr<ds_hll_union> _sketch_union; // used for merge
    uint8_t _lg_config_k;
    ds_hll_type _hll_type;
    mutable std::unique_ptr<ds_hll_sketch> _sketch;
};

} // namespace doris