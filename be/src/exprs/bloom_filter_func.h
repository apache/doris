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

namespace doris {

class BloomFilterAdaptor {
public:
    BloomFilterAdaptor() { _bloom_filter = std::make_shared<doris::BlockBloomFilter>(); }
    static int64_t optimal_bit_num(int64_t expect_num, double fpp) {
        return doris::segment_v2::BloomFilter::optimal_bit_num(expect_num, fpp) / 8;
    }

    static BloomFilterAdaptor* create() { return new BloomFilterAdaptor(); }

    Status merge(BloomFilterAdaptor* other) { return _bloom_filter->merge(*other->_bloom_filter); }

    Status init(int len) {
        int log_space = log2(len);
        return _bloom_filter->init(log_space, /*hash_seed*/ 0);
    }

    Status init(butil::IOBufAsZeroCopyInputStream* data, const size_t data_size) {
        int log_space = log2(data_size);
        return _bloom_filter->init_from_directory(log_space, data, data_size, false, 0);
    }

    char* data() { return (char*)_bloom_filter->directory().data; }

    size_t size() { return _bloom_filter->directory().size; }

    template <typename T>
    bool test(T data) const {
        return _bloom_filter->find(data);
    }

    void add_bytes(const char* data, size_t len) { _bloom_filter->insert(Slice(data, len)); }

    // test_element/find_element only used on vectorized engine
    template <typename T>
    bool test_element(T element) const {
        if constexpr (std::is_same_v<T, Slice>) {
            return _bloom_filter->find(element);
        } else {
            return _bloom_filter->find(HashUtil::fixed_len_to_uint32(element));
        }
    }

    template <typename T>
    void add_element(T element) {
        if constexpr (std::is_same_v<T, Slice>) {
            _bloom_filter->insert(element);
        } else {
            _bloom_filter->insert(HashUtil::fixed_len_to_uint32(element));
        }
    }

private:
    std::shared_ptr<doris::BlockBloomFilter> _bloom_filter;
};

// Only Used In RuntimeFilter
class BloomFilterFuncBase : public FilterFuncBase {
public:
    virtual ~BloomFilterFuncBase() = default;

    Status init(int64_t expect_num, double fpp) {
        size_t filter_size = BloomFilterAdaptor::optimal_bit_num(expect_num, fpp);
        return init_with_fixed_length(filter_size);
    }

    void set_length(int64_t bloom_filter_length) { _bloom_filter_length = bloom_filter_length; }

    void set_build_bf_exactly(bool build_bf_exactly) { _build_bf_exactly = build_bf_exactly; }

    Status init_with_fixed_length() {
        //        if (_build_bf_exactly) {
        //            return Status::OK();
        //        }
        return init_with_fixed_length(_bloom_filter_length);
    }

    Status init_with_cardinality(const size_t build_bf_cardinality) {
        //        if (_build_bf_exactly) {
        //            // Use the same algorithm as org.apache.doris.planner.RuntimeFilter#calculateFilterSize
        //            constexpr double fpp = 0.05;
        //            constexpr double k = 8; // BUCKET_WORDS
        //            // m is the number of bits we would need to get the fpp specified
        //            double m = -k * build_bf_cardinality / std::log(1 - std::pow(fpp, 1.0 / k));
        //
        //            // Handle case where ndv == 1 => ceil(log2(m/8)) < 0.
        //            int log_filter_size = std::max(0, (int)(std::ceil(std::log(m / 8) / std::log(2))));
        //            return init_with_fixed_length(((int64_t)1) << log_filter_size);
        //        }
        return Status::OK();
    }

    Status init_with_fixed_length(int64_t bloom_filter_length) {
        if (_inited) {
            return Status::OK();
        }
        // TODO: really need the lock?
        std::lock_guard<std::mutex> l(_lock);
        if (_inited) {
            return Status::OK();
        }
        DCHECK(bloom_filter_length >= 0);
        DCHECK_EQ((bloom_filter_length & (bloom_filter_length - 1)), 0);
        _bloom_filter_alloced = bloom_filter_length;
        _bloom_filter.reset(BloomFilterAdaptor::create());
        RETURN_IF_ERROR(_bloom_filter->init(bloom_filter_length));
        _inited = true;
        return Status::OK();
    }

    Status merge(BloomFilterFuncBase* bloomfilter_func) {
        // If `_inited` is false, there is no memory allocated in bloom filter and this is the first
        // call for `merge` function. So we just reuse this bloom filter, and we don't need to
        // allocate memory again.
        if (_inited) {
            DCHECK(bloomfilter_func != nullptr);
            auto* other_func = static_cast<BloomFilterFuncBase*>(bloomfilter_func);
            if (_bloom_filter_alloced != other_func->_bloom_filter_alloced) {
                return Status::InvalidArgument(
                        "bloom filter size not the same: already allocated bytes {}, expected "
                        "allocated bytes {}",
                        _bloom_filter_alloced, other_func->_bloom_filter_alloced);
            }
            return _bloom_filter->merge(other_func->_bloom_filter.get());
        }
        {
            std::lock_guard<std::mutex> l(_lock);
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
                return Status::InvalidArgument(
                        "bloom filter size not the same: already allocated bytes {}, expected "
                        "allocated bytes {}",
                        _bloom_filter_alloced, other_func->_bloom_filter_alloced);
            }
            return _bloom_filter->merge(other_func->_bloom_filter.get());
        }
    }

    Status assign(butil::IOBufAsZeroCopyInputStream* data, const size_t data_size) {
        if (_bloom_filter == nullptr) {
            _bloom_filter.reset(BloomFilterAdaptor::create());
        }

        _bloom_filter_alloced = data_size;
        return _bloom_filter->init(data, data_size);
    }

    Status get_data(char** data, int* len) {
        *data = _bloom_filter->data();
        *len = _bloom_filter->size();
        return Status::OK();
    }

    size_t get_size() const { return _bloom_filter ? _bloom_filter->size() : 0; }

    void light_copy(BloomFilterFuncBase* bloomfilter_func) {
        auto* other_func = static_cast<BloomFilterFuncBase*>(bloomfilter_func);
        _bloom_filter_alloced = other_func->_bloom_filter_alloced;
        _bloom_filter = other_func->_bloom_filter;
        _inited = other_func->_inited;
    }

    virtual void insert(const void* data) = 0;

    virtual bool find(const void* data) const = 0;

    virtual bool find_olap_engine(const void* data) const = 0;

    virtual bool find_uint32_t(uint32_t data) const = 0;

    virtual void insert_fixed_len(const vectorized::ColumnPtr& column, size_t start) = 0;

    virtual void find_fixed_len(const vectorized::ColumnPtr& column, uint8_t* results) = 0;

protected:
    // bloom filter size
    int32_t _bloom_filter_alloced;
    std::shared_ptr<BloomFilterAdaptor> _bloom_filter;
    bool _inited {};
    std::mutex _lock;
    int64_t _bloom_filter_length;
    bool _build_bf_exactly = false;
};

template <class T>
struct CommonFindOp {
    void insert_batch(BloomFilterAdaptor& bloom_filter, const vectorized::ColumnPtr& column,
                      size_t start) const {
        if (column->is_nullable()) {
            const auto* nullable = assert_cast<const vectorized::ColumnNullable*>(column.get());
            const auto& col = nullable->get_nested_column();
            const auto& nullmap =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable->get_null_map_column())
                            .get_data();

            const T* data = (T*)col.get_raw_data().data;
            for (size_t i = start; i < column->size(); i++) {
                if (!nullmap[i]) {
                    bloom_filter.add_element(*(data + i));
                }
            }
        } else {
            const T* data = (T*)column->get_raw_data().data;
            for (size_t i = start; i < column->size(); i++) {
                bloom_filter.add_element(*(data + i));
            }
        }
    }

    void find_batch(const BloomFilterAdaptor& bloom_filter, const vectorized::ColumnPtr& column,
                    uint8_t* results) const {
        if (column->is_nullable()) {
            const auto* nullable = assert_cast<const vectorized::ColumnNullable*>(column.get());
            const auto& nullmap =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable->get_null_map_column())
                            .get_data();

            const T* data = (T*)nullable->get_nested_column().get_raw_data().data;
            for (size_t i = 0; i < column->size(); i++) {
                if (!nullmap[i]) {
                    results[i] = bloom_filter.test_element(data[i]);
                } else {
                    results[i] = false;
                }
            }
        } else {
            const T* data = (T*)column->get_raw_data().data;
            for (size_t i = 0; i < column->size(); i++) {
                results[i] = bloom_filter.test_element(data[i]);
            }
        }
    }

    void insert(BloomFilterAdaptor& bloom_filter, const void* data) const {
        bloom_filter.add_bytes((char*)data, sizeof(T));
    }
    bool find(const BloomFilterAdaptor& bloom_filter, const void* data) const {
        return bloom_filter.test_element(((T*)data)[0]);
    }
    bool find_olap_engine(const BloomFilterAdaptor& bloom_filter, const void* data) const {
        return find(bloom_filter, data);
    }
    bool find(const BloomFilterAdaptor& bloom_filter, uint32_t data) const {
        return bloom_filter.test(data);
    }
};

struct StringFindOp {
    static void insert_batch(BloomFilterAdaptor& bloom_filter, const vectorized::ColumnPtr& column,
                             size_t start) {
        if (column->is_nullable()) {
            const auto* nullable = assert_cast<const vectorized::ColumnNullable*>(column.get());
            const auto& col =
                    assert_cast<const vectorized::ColumnString&>(nullable->get_nested_column());
            const auto& nullmap =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable->get_null_map_column())
                            .get_data();

            for (size_t i = start; i < column->size(); i++) {
                if (!nullmap[i]) {
                    bloom_filter.add_element(col.get_data_at(i));
                }
            }
        } else {
            const auto& col = assert_cast<const vectorized::ColumnString*>(column.get());
            for (size_t i = start; i < column->size(); i++) {
                bloom_filter.add_element(col->get_data_at(i));
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

            for (size_t i = 0; i < column->size(); i++) {
                if (!nullmap[i]) {
                    results[i] = bloom_filter.test_element(col.get_data_at(i));
                } else {
                    results[i] = false;
                }
            }
        } else {
            const auto& col = assert_cast<const vectorized::ColumnString*>(column.get());
            for (size_t i = 0; i < column->size(); i++) {
                results[i] = bloom_filter.test_element(col->get_data_at(i));
            }
        }
    }

    static void insert(BloomFilterAdaptor& bloom_filter, const void* data) {
        const auto* value = reinterpret_cast<const StringRef*>(data);
        if (value) {
            bloom_filter.add_bytes(value->data, value->size);
        }
    }

    static bool find(const BloomFilterAdaptor& bloom_filter, const void* data) {
        const auto* value = reinterpret_cast<const StringRef*>(data);
        if (value == nullptr) {
            return false;
        }
        return bloom_filter.test(Slice(value->data, value->size));
    }

    static bool find_olap_engine(const BloomFilterAdaptor& bloom_filter, const void* data) {
        return StringFindOp::find(bloom_filter, data);
    }

    static bool find(const BloomFilterAdaptor& bloom_filter, uint32_t data) {
        return bloom_filter.test(data);
    }
};

// We do not need to judge whether data is empty, because null will not appear
// when filer used by the storage engine
struct FixedStringFindOp : public StringFindOp {
    static bool find_olap_engine(const BloomFilterAdaptor& bloom_filter, const void* input_data) {
        const auto* value = reinterpret_cast<const StringRef*>(input_data);
        int64_t size = value->size;
        const char* data = value->data;
        // CHAR type may pad the tail with \0, need to trim
        while (size > 0 && data[size - 1] == '\0') {
            size--;
        }
        return bloom_filter.test(Slice(value->data, size));
    }
};

struct DateTimeFindOp : public CommonFindOp<VecDateTimeValue> {
    static bool find_olap_engine(const BloomFilterAdaptor& bloom_filter, const void* data) {
        VecDateTimeValue value;
        value.from_olap_datetime(*reinterpret_cast<const uint64_t*>(data));
        return bloom_filter.test(Slice((char*)&value, sizeof(VecDateTimeValue)));
    }
};

// avoid violating C/C++ aliasing rules.
// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=101684

struct DateFindOp : public CommonFindOp<VecDateTimeValue> {
    static bool find_olap_engine(const BloomFilterAdaptor& bloom_filter, const void* data) {
        uint24_t date = *static_cast<const uint24_t*>(data);
        uint64_t value = uint32_t(date);

        VecDateTimeValue date_value;
        date_value.from_olap_date(value);

        return bloom_filter.test(Slice((char*)&date_value, sizeof(VecDateTimeValue)));
    }
};

struct DecimalV2FindOp : public CommonFindOp<DecimalV2Value> {
    static bool find_olap_engine(const BloomFilterAdaptor& bloom_filter, const void* data) {
        auto packed_decimal = *static_cast<const decimal12_t*>(data);
        DecimalV2Value value;
        int64_t int_value = packed_decimal.integer;
        int32_t frac_value = packed_decimal.fraction;
        value.from_olap_decimal(int_value, frac_value);

        constexpr int decimal_value_sz = sizeof(DecimalV2Value);
        char data_bytes[decimal_value_sz];
        memcpy(&data_bytes, &value, decimal_value_sz);
        return bloom_filter.test(Slice(data_bytes, decimal_value_sz));
    }
};

template <PrimitiveType type>
struct BloomFilterTypeTraits {
    using T = typename PrimitiveTypeTraits<type>::CppType;
    using FindOp = CommonFindOp<T>;
};

template <>
struct BloomFilterTypeTraits<TYPE_DATE> {
    using FindOp = DateFindOp;
};

template <>
struct BloomFilterTypeTraits<TYPE_DATETIME> {
    using FindOp = DateTimeFindOp;
};

template <>
struct BloomFilterTypeTraits<TYPE_DECIMALV2> {
    using FindOp = DecimalV2FindOp;
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

    bool find(const void* data) const override {
        DCHECK(_bloom_filter != nullptr);
        return dummy.find(*_bloom_filter, data);
    }

    bool find_olap_engine(const void* data) const override {
        return dummy.find_olap_engine(*_bloom_filter, data);
    }

    bool find_uint32_t(uint32_t data) const override { return dummy.find(*_bloom_filter, data); }

private:
    typename BloomFilterTypeTraits<type>::FindOp dummy;
};

} // namespace doris
