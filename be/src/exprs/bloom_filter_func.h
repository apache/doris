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

#include "common/exception.h"
#include "common/status.h"
#include "exprs/bloom_filter_func_impl.h"
#include "exprs/filter_base.h"
#include "runtime_filter/runtime_filter_definitions.h"
#include "vec/columns/column_dictionary.h"

namespace doris {
#include "common/compile_check_begin.h"
// Only Used In RuntimeFilter
class BloomFilterFuncBase : public FilterBase {
public:
    BloomFilterFuncBase(bool null_aware) : FilterBase(null_aware) {}
    virtual ~BloomFilterFuncBase() = default;

    void init_params(const RuntimeFilterParams* params) {
        _bloom_filter_length = params->bloom_filter_size;

        _build_bf_by_runtime_size = params->build_bf_by_runtime_size;
        _runtime_bloom_filter_min_size = params->runtime_bloom_filter_min_size;
        _runtime_bloom_filter_max_size = params->runtime_bloom_filter_max_size;
        _bloom_filter_size_calculated_by_ndv = params->bloom_filter_size_calculated_by_ndv;
        _limit_length();
    }
    Status init_with_fixed_length(size_t runtime_size) {
        if (_build_bf_by_runtime_size) {
            // Use the same algorithm as org.apache.doris.planner.RuntimeFilter#calculateFilterSize
            // m is the number of bits we would need to get the fpp specified
            double m = -K * (double)runtime_size / std::log(1 - std::pow(FPP, 1.0 / K));

#ifdef __APPLE__
            constexpr double LN2 = 0.693147180559945309417;
#else
            constexpr double LN2 = std::numbers::ln2;
#endif

            // Handle case where ndv == 1 => ceil(log2(m/8)) < 0.
            int log_filter_size = std::max(0, (int)(std::ceil(std::log(m / 8) / LN2)));
            auto be_calculate_size = (((int64_t)1) << log_filter_size);
            // if FE do use ndv stat to predict the bf size, BE only use the row count. FE have more
            // exactly row count stat. which one is min is more correctly.
            if (_bloom_filter_size_calculated_by_ndv) {
                _bloom_filter_length = std::min(be_calculate_size, _bloom_filter_length);
            } else {
                _bloom_filter_length = be_calculate_size;
            }
            _limit_length();
        }

        DCHECK(_bloom_filter_length >= 0);
        DCHECK_EQ((_bloom_filter_length & (_bloom_filter_length - 1)), 0);
        _bloom_filter_alloced = _bloom_filter_length;
        _bloom_filter.reset(BloomFilterAdaptor::create(_null_aware));
        RETURN_IF_ERROR(_bloom_filter->init(_bloom_filter_length));
        return Status::OK();
    }

    bool build_bf_by_runtime_size() const { return _build_bf_by_runtime_size; }

    Status merge(BloomFilterFuncBase* other) {
        if (other == nullptr) {
            return Status::InternalError("bloomfilter_func is nullptr");
        }
        if (other->_bloom_filter == nullptr) {
            return Status::InternalError("other->_bloom_filter is nullptr");
        }

        if (_bloom_filter_alloced == 0) {
            return Status::InternalError("bloom filter is not initialized");
        }
        DCHECK(_bloom_filter);

        if (_bloom_filter_alloced != other->_bloom_filter_alloced) {
            return Status::InternalError(
                    "bloom filter size not the same: already allocated bytes {}, expected "
                    "allocated bytes {}",
                    _bloom_filter_alloced, other->_bloom_filter_alloced);
        }
        _bloom_filter->set_contain_null(other->contain_null());
        return _bloom_filter->merge(other->_bloom_filter.get());
    }

    Status assign(butil::IOBufAsZeroCopyInputStream* data, const size_t data_size,
                  bool contain_null) {
        if (_bloom_filter == nullptr) {
            _bloom_filter.reset(BloomFilterAdaptor::create(_null_aware));
        }
        _bloom_filter->set_contain_null(contain_null);

        _bloom_filter_alloced = data_size;
        return _bloom_filter->init(data, data_size);
    }

    void get_data(char** data, int* len) {
        *data = _bloom_filter->data();
        *len = (int)_bloom_filter->size();
    }

    bool contain_null() const {
        if (!_bloom_filter) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "_bloom_filter is nullptr");
        }
        return _bloom_filter->contain_null();
    }

    size_t get_size() const { return _bloom_filter ? _bloom_filter->size() : 0; }

    void light_copy(BloomFilterFuncBase* bloomfilter_func) {
        auto* other_func = bloomfilter_func;
        _bloom_filter_alloced = other_func->_bloom_filter_alloced;
        _bloom_filter = other_func->_bloom_filter;
    }

    virtual void insert_set(std::shared_ptr<HybridSetBase> set) = 0;

    virtual void insert_fixed_len(const vectorized::ColumnPtr& column, size_t start) = 0;

    virtual void find_fixed_len(const vectorized::ColumnPtr& column, uint8_t* results) = 0;

    virtual uint16_t find_fixed_len_olap_engine(const char* data, const uint8_t* nullmap,
                                                uint16_t* offsets, int number,
                                                bool is_parse_column) = 0;

private:
    static constexpr double FPP = 0.05;
    static constexpr double K = 8; // BUCKET_WORDS
    void _limit_length() {
        if (_runtime_bloom_filter_min_size > 0) {
            _bloom_filter_length = std::max(_bloom_filter_length, _runtime_bloom_filter_min_size);
        }
        if (_runtime_bloom_filter_max_size > 0) {
            _bloom_filter_length = std::min(_bloom_filter_length, _runtime_bloom_filter_max_size);
        }
    }

protected:
    int64_t _bloom_filter_alloced = 0;
    std::shared_ptr<BloomFilterAdaptor> _bloom_filter;
    int64_t _bloom_filter_length;
    int64_t _runtime_bloom_filter_min_size;
    int64_t _runtime_bloom_filter_max_size;
    bool _build_bf_by_runtime_size = false;
    bool _bloom_filter_size_calculated_by_ndv = false;
};

template <PrimitiveType type>
class BloomFilterFunc final : public BloomFilterFuncBase {
public:
    BloomFilterFunc(bool null_aware) : BloomFilterFuncBase(null_aware) {}
    void insert_set(std::shared_ptr<HybridSetBase> set) override {
        OpV2::insert_set(*_bloom_filter, set);
        _bloom_filter->set_contain_null(set->contain_null());
    }

    void insert_fixed_len(const vectorized::ColumnPtr& column, size_t start) override {
        DCHECK(_bloom_filter != nullptr);
        OpV2::insert_batch(*_bloom_filter, column, start);
    }

    void find_fixed_len(const vectorized::ColumnPtr& column, uint8_t* results) override {
        OpV2::find_batch(*_bloom_filter, column, results);
    }

    template <bool is_nullable>
    uint16_t find_dict_olap_engine(const vectorized::ColumnDictI32* column, const uint8_t* nullmap,
                                   uint16_t* offsets, int number) {
        uint16_t new_size = 0;
        for (uint16_t i = 0; i < number; i++) {
            uint16_t idx = offsets[i];
            offsets[new_size] = idx;
            if constexpr (is_nullable) {
                new_size += nullmap[idx] && _bloom_filter->contain_null();
                new_size += !nullmap[idx] && _bloom_filter->test(column->get_hash_value(idx));
            } else {
                new_size += _bloom_filter->test(column->get_hash_value(idx));
            }
        }
        return new_size;
    }

    uint16_t find_fixed_len_olap_engine(const char* data, const uint8_t* nullmap, uint16_t* offsets,
                                        int number, bool is_parse_column) override {
        return OpV2::find_batch_olap_engine(*_bloom_filter, data, nullmap, offsets, number,
                                            is_parse_column);
    }

private:
    using OpV2 = typename BloomFilterTypeTraits<fixed_len_to_uint32_v2, type>::FindOp;
};
#include "common/compile_check_end.h"
} // namespace doris
