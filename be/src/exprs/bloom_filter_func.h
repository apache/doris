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

#include "exprs/block_bloom_filter.hpp"
#include "exprs/runtime_filter.h"
#include "olap/rowset/segment_v2/bloom_filter.h" // IWYU pragma: keep
#include "vec/columns/column_dictionary.h"
#include "vec/common/string_ref.h"

namespace doris {

class BloomFilterAdaptor {
public:
    BloomFilterAdaptor(bool null_aware = false) : _null_aware(null_aware) {
        _bloom_filter = std::make_shared<doris::BlockBloomFilter>();
    }

    static int64_t optimal_bit_num(int64_t expect_num, double fpp) {
        return doris::segment_v2::BloomFilter::optimal_bit_num(expect_num, fpp) / 8;
    }

    static BloomFilterAdaptor* create(bool null_aware) {
        return new BloomFilterAdaptor(null_aware);
    }

    Status merge(BloomFilterAdaptor* other) { return _bloom_filter->merge(*other->_bloom_filter); }

    Status init(int len) {
        int log_space = (int)log2(len);
        return _bloom_filter->init(log_space, /*hash_seed*/ 0);
    }

    Status init(butil::IOBufAsZeroCopyInputStream* data, const size_t data_size) {
        int log_space = (int)log2(data_size);
        return _bloom_filter->init_from_directory(log_space, data, data_size, false, 0);
    }

    char* data() { return (char*)_bloom_filter->directory().data; }

    size_t size() { return _bloom_filter->directory().size; }

    template <typename T>
    bool test(T data) const {
        return _bloom_filter->find(data);
    }

    // test_element/find_element only used on vectorized engine
    template <typename T>
    bool test_element(T element) const {
        if constexpr (std::is_same_v<T, StringRef>) {
            return _bloom_filter->find(element);
        } else {
            return _bloom_filter->find(HashUtil::fixed_len_to_uint32(element));
        }
    }

    template <typename T>
    void add_element(T element) {
        if constexpr (std::is_same_v<T, StringRef>) {
            _bloom_filter->insert(element);
        } else {
            _bloom_filter->insert(HashUtil::fixed_len_to_uint32(element));
        }
    }

    void set_contain_null() { _contain_null = true; }

    void set_contain_null_and_null_aware() {
        _contain_null = true;
        _null_aware = true;
    }

    bool contain_null() const { return _null_aware && _contain_null; }

private:
    bool _null_aware = false;
    bool _contain_null = false;
    std::shared_ptr<doris::BlockBloomFilter> _bloom_filter;
};

// Only Used In RuntimeFilter
class BloomFilterFuncBase : public RuntimeFilterFuncBase {
public:
    virtual ~BloomFilterFuncBase() = default;

    void init_params(const RuntimeFilterParams* params) {
        _bloom_filter_length = params->bloom_filter_size;

        _build_bf_exactly = params->build_bf_exactly;
        _runtime_bloom_filter_min_size = params->runtime_bloom_filter_min_size;
        _runtime_bloom_filter_max_size = params->runtime_bloom_filter_max_size;
        _null_aware = params->null_aware;
        _bloom_filter_size_calculated_by_ndv = params->bloom_filter_size_calculated_by_ndv;
        _limit_length();
    }

    Status init_with_fixed_length() { return init_with_fixed_length(_bloom_filter_length); }

    bool get_build_bf_cardinality() const { return _build_bf_exactly; }

    Status init_with_cardinality(const size_t build_bf_cardinality) {
        if (_build_bf_exactly) {
            // Use the same algorithm as org.apache.doris.planner.RuntimeFilter#calculateFilterSize
            constexpr double fpp = 0.05;
            constexpr double k = 8; // BUCKET_WORDS
            // m is the number of bits we would need to get the fpp specified
            double m = -k * build_bf_cardinality / std::log(1 - std::pow(fpp, 1.0 / k));

            // Handle case where ndv == 1 => ceil(log2(m/8)) < 0.
            int log_filter_size = std::max(0, (int)(std::ceil(std::log(m / 8) / std::log(2))));
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
        return init_with_fixed_length(_bloom_filter_length);
    }

    Status init_with_fixed_length(int64_t bloom_filter_length) {
        if (_inited) {
            return Status::OK();
        }
        DCHECK(bloom_filter_length >= 0);
        DCHECK_EQ((bloom_filter_length & (bloom_filter_length - 1)), 0);
        _bloom_filter_alloced = bloom_filter_length;
        _bloom_filter.reset(BloomFilterAdaptor::create(_null_aware));
        RETURN_IF_ERROR(_bloom_filter->init(bloom_filter_length));
        _inited = true;
        return Status::OK();
    }

    Status merge(BloomFilterFuncBase* bloomfilter_func) {
        // If `_inited` is false, there is no memory allocated in bloom filter and this is the first
        // call for `merge` function. So we just reuse this bloom filter, and we don't need to
        // allocate memory again.
        if (!_inited) {
            auto* other_func = static_cast<BloomFilterFuncBase*>(bloomfilter_func);
            DCHECK(_bloom_filter == nullptr);
            DCHECK(bloomfilter_func != nullptr);
            _bloom_filter = bloomfilter_func->_bloom_filter;
            _bloom_filter_alloced = other_func->_bloom_filter_alloced;
            _inited = true;
            return Status::OK();
        }
        DCHECK(bloomfilter_func != nullptr);
        auto* other_func = static_cast<BloomFilterFuncBase*>(bloomfilter_func);
        if (_bloom_filter_alloced != other_func->_bloom_filter_alloced) {
            return Status::InternalError(
                    "bloom filter size not the same: already allocated bytes {}, expected "
                    "allocated bytes {}",
                    _bloom_filter_alloced, other_func->_bloom_filter_alloced);
        }
        if (other_func->_bloom_filter->contain_null()) {
            _bloom_filter->set_contain_null_and_null_aware();
        }
        return _bloom_filter->merge(other_func->_bloom_filter.get());
    }

    Status assign(butil::IOBufAsZeroCopyInputStream* data, const size_t data_size,
                  bool contain_null) {
        if (_bloom_filter == nullptr) {
            _null_aware = contain_null;
            _bloom_filter.reset(BloomFilterAdaptor::create(_null_aware));
        }
        if (contain_null) {
            _bloom_filter->set_contain_null();
        }

        _bloom_filter_alloced = data_size;
        _inited = true;
        return _bloom_filter->init(data, data_size);
    }

    void get_data(char** data, int* len) {
        *data = _bloom_filter->data();
        *len = _bloom_filter->size();
    }

    bool contain_null() const {
        DCHECK(_bloom_filter);
        return _bloom_filter->contain_null();
    }

    void set_contain_null_and_null_aware() { _bloom_filter->set_contain_null_and_null_aware(); }

    size_t get_size() const { return _bloom_filter ? _bloom_filter->size() : 0; }

    void light_copy(BloomFilterFuncBase* bloomfilter_func) {
        auto* other_func = static_cast<BloomFilterFuncBase*>(bloomfilter_func);
        _bloom_filter_alloced = other_func->_bloom_filter_alloced;
        _bloom_filter = other_func->_bloom_filter;
        _inited = other_func->_inited;
    }

    virtual void insert(const void* data) = 0;

    virtual void insert_fixed_len(const vectorized::ColumnPtr& column, size_t start) = 0;

    virtual void find_fixed_len(const vectorized::ColumnPtr& column, uint8_t* results) = 0;

    virtual uint16_t find_fixed_len_olap_engine(const char* data, const uint8* nullmap,
                                                uint16_t* offsets, int number,
                                                bool is_parse_column) = 0;

private:
    void _limit_length() {
        if (_runtime_bloom_filter_min_size > 0) {
            _bloom_filter_length = std::max(_bloom_filter_length, _runtime_bloom_filter_min_size);
        }
        if (_runtime_bloom_filter_max_size > 0) {
            _bloom_filter_length = std::min(_bloom_filter_length, _runtime_bloom_filter_max_size);
        }
    }

protected:
    // bloom filter size
    int32_t _bloom_filter_alloced;
    std::shared_ptr<BloomFilterAdaptor> _bloom_filter;
    bool _inited = false;
    int64_t _bloom_filter_length;
    int64_t _runtime_bloom_filter_min_size;
    int64_t _runtime_bloom_filter_max_size;
    bool _build_bf_exactly = false;
    bool _bloom_filter_size_calculated_by_ndv = false;
};

template <typename T, bool need_trim = false>
uint16_t find_batch_olap(const BloomFilterAdaptor& bloom_filter, const char* data,
                         const uint8* nullmap, uint16_t* offsets, int number,
                         const bool is_parse_column) {
    auto get_element = [](const char* input_data, int idx) {
        if constexpr (std::is_same_v<T, StringRef> && need_trim) {
            const auto value = ((const StringRef*)(input_data))[idx];
            int64_t size = value.size;
            const char* data = value.data;
            // CHAR type may pad the tail with \0, need to trim
            while (size > 0 && data[size - 1] == '\0') {
                size--;
            }
            return StringRef(value.data, size);
        } else {
            return ((const T*)(input_data))[idx];
        }
    };

    uint16_t new_size = 0;
    if (is_parse_column) {
        if (nullmap == nullptr) {
            for (int i = 0; i < number; i++) {
                uint16_t idx = offsets[i];
                if (!bloom_filter.test_element(get_element(data, idx))) {
                    continue;
                }
                offsets[new_size++] = idx;
            }
        } else {
            for (int i = 0; i < number; i++) {
                uint16_t idx = offsets[i];
                if (nullmap[idx]) {
                    if (!bloom_filter.contain_null()) {
                        continue;
                    }
                } else {
                    if (!bloom_filter.test_element(get_element(data, idx))) {
                        continue;
                    }
                }
                offsets[new_size++] = idx;
            }
        }
    } else {
        if (nullmap == nullptr) {
            for (int i = 0; i < number; i++) {
                if (!bloom_filter.test_element(get_element(data, i))) {
                    continue;
                }
                offsets[new_size++] = i;
            }
        } else {
            for (int i = 0; i < number; i++) {
                if (nullmap[i]) {
                    if (!bloom_filter.contain_null()) {
                        continue;
                    }
                } else {
                    if (!bloom_filter.test_element(get_element(data, i))) {
                        continue;
                    }
                }
                offsets[new_size++] = i;
            }
        }
    }
    return new_size;
}

template <class T>
struct CommonFindOp {
    uint16_t find_batch_olap_engine(const BloomFilterAdaptor& bloom_filter, const char* data,
                                    const uint8* nullmap, uint16_t* offsets, int number,
                                    const bool is_parse_column) {
        return find_batch_olap<T>(bloom_filter, data, nullmap, offsets, number, is_parse_column);
    }

    void insert_batch(BloomFilterAdaptor& bloom_filter, const vectorized::ColumnPtr& column,
                      size_t start) const {
        const auto size = column->size();
        if (column->is_nullable()) {
            const auto* nullable = assert_cast<const vectorized::ColumnNullable*>(column.get());
            const auto& col = nullable->get_nested_column();
            const auto& nullmap =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable->get_null_map_column())
                            .get_data();

            const T* data = (T*)col.get_raw_data().data;
            for (size_t i = start; i < size; i++) {
                if (!nullmap[i]) {
                    bloom_filter.add_element(*(data + i));
                } else {
                    bloom_filter.set_contain_null();
                }
            }
        } else {
            const T* data = (T*)column->get_raw_data().data;
            for (size_t i = start; i < size; i++) {
                bloom_filter.add_element(*(data + i));
            }
        }
    }

    void find_batch(const BloomFilterAdaptor& bloom_filter, const vectorized::ColumnPtr& column,
                    uint8_t* results) const {
        const T* __restrict data = nullptr;
        const uint8_t* __restrict nullmap = nullptr;
        if (column->is_nullable()) {
            const auto* nullable = assert_cast<const vectorized::ColumnNullable*>(column.get());
            if (nullable->has_null()) {
                nullmap =
                        assert_cast<const vectorized::ColumnUInt8&>(nullable->get_null_map_column())
                                .get_data()
                                .data();
            }
            data = (T*)nullable->get_nested_column().get_raw_data().data;
        } else {
            data = (T*)column->get_raw_data().data;
        }

        const auto size = column->size();
        if (nullmap) {
            for (size_t i = 0; i < size; i++) {
                if (!nullmap[i]) {
                    results[i] = bloom_filter.test_element(data[i]);
                } else {
                    results[i] = bloom_filter.contain_null();
                }
            }
        } else {
            for (size_t i = 0; i < size; i++) {
                results[i] = bloom_filter.test_element(data[i]);
            }
        }
    }

    void insert(BloomFilterAdaptor& bloom_filter, const void* data) const {
        bloom_filter.add_element(*(T*)data);
    }
};

struct StringFindOp : CommonFindOp<StringRef> {
    static void insert_batch(BloomFilterAdaptor& bloom_filter, const vectorized::ColumnPtr& column,
                             size_t start) {
        auto _insert_batch_col_str = [&](const auto& col, const uint8_t* __restrict nullmap,
                                         size_t start, size_t size) {
            for (size_t i = start; i < size; i++) {
                if (nullmap == nullptr || !nullmap[i]) {
                    bloom_filter.add_element(col.get_data_at(i));
                } else {
                    bloom_filter.set_contain_null();
                }
            }
        };

        if (column->is_nullable()) {
            const auto* nullable = assert_cast<const vectorized::ColumnNullable*>(column.get());
            const auto& nullmap =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable->get_null_map_column())
                            .get_data();
            if (nullable->get_nested_column().is_column_string64()) {
                _insert_batch_col_str(assert_cast<const vectorized::ColumnString64&>(
                                              nullable->get_nested_column()),
                                      nullmap.data(), start, nullmap.size());
            } else {
                _insert_batch_col_str(
                        assert_cast<const vectorized::ColumnString&>(nullable->get_nested_column()),
                        nullmap.data(), start, nullmap.size());
            }
        } else {
            if (column->is_column_string64()) {
                _insert_batch_col_str(assert_cast<const vectorized::ColumnString64&>(*column),
                                      nullptr, start, column->size());
            } else {
                _insert_batch_col_str(assert_cast<const vectorized::ColumnString&>(*column),
                                      nullptr, start, column->size());
            }
        }
    }

    static void find_batch(const BloomFilterAdaptor& bloom_filter,
                           const vectorized::ColumnPtr& column, uint8_t* results) {
        if (column->is_nullable()) {
            const auto* nullable = assert_cast<const vectorized::ColumnNullable*>(column.get());
            const auto& col =
                    assert_cast<const vectorized::ColumnString&>(nullable->get_nested_column());
            const auto& nullmap =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable->get_null_map_column())
                            .get_data();

            if (nullable->has_null()) {
                for (size_t i = 0; i < col.size(); i++) {
                    if (!nullmap[i]) {
                        results[i] = bloom_filter.test_element(col.get_data_at(i));
                    } else {
                        results[i] = bloom_filter.contain_null();
                    }
                }
            } else {
                for (size_t i = 0; i < col.size(); i++) {
                    results[i] = bloom_filter.test_element(col.get_data_at(i));
                }
            }
        } else {
            const auto& col = assert_cast<const vectorized::ColumnString*>(column.get());
            for (size_t i = 0; i < col->size(); i++) {
                results[i] = bloom_filter.test_element(col->get_data_at(i));
            }
        }
    }
};

// We do not need to judge whether data is empty, because null will not appear
// when filer used by the storage engine
struct FixedStringFindOp : public StringFindOp {
    static uint16_t find_batch_olap_engine(const BloomFilterAdaptor& bloom_filter, const char* data,
                                           const uint8* nullmap, uint16_t* offsets, int number,
                                           const bool is_parse_column) {
        return find_batch_olap<StringRef, true>(bloom_filter, data, nullmap, offsets, number,
                                                is_parse_column);
    }
};

template <PrimitiveType type>
struct BloomFilterTypeTraits {
    using T = typename PrimitiveTypeTraits<type>::CppType;
    using FindOp = CommonFindOp<T>;
};

template <>
struct BloomFilterTypeTraits<TYPE_CHAR> {
    using FindOp = FixedStringFindOp;
};

template <>
struct BloomFilterTypeTraits<TYPE_VARCHAR> {
    using FindOp = StringFindOp;
};

template <>
struct BloomFilterTypeTraits<TYPE_STRING> {
    using FindOp = StringFindOp;
};

template <PrimitiveType type>
class BloomFilterFunc final : public BloomFilterFuncBase {
public:
    BloomFilterFunc() : BloomFilterFuncBase() {}

    ~BloomFilterFunc() override = default;

    void insert(const void* data) override {
        DCHECK(_bloom_filter != nullptr);
        dummy.insert(*_bloom_filter, data);
    }

    void insert_fixed_len(const vectorized::ColumnPtr& column, size_t start) override {
        DCHECK(_bloom_filter != nullptr);
        dummy.insert_batch(*_bloom_filter, column, start);
    }

    void find_fixed_len(const vectorized::ColumnPtr& column, uint8_t* results) override {
        dummy.find_batch(*_bloom_filter, column, results);
    }

    template <bool is_nullable>
    uint16_t find_dict_olap_engine(const vectorized::ColumnDictI32* column, const uint8* nullmap,
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

    uint16_t find_fixed_len_olap_engine(const char* data, const uint8* nullmap, uint16_t* offsets,
                                        int number, bool is_parse_column) override {
        return dummy.find_batch_olap_engine(*_bloom_filter, data, nullmap, offsets, number,
                                            is_parse_column);
    }

private:
    typename BloomFilterTypeTraits<type>::FindOp dummy;
};

} // namespace doris
