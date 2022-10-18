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

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <memory>
#include <string>
#include <type_traits>

#include "common/object_pool.h"
#include "exprs/block_bloom_filter.hpp"
#include "exprs/predicate.h"
#include "olap/decimal12.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/uint24.h"
#include "util/hash_util.hpp"

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

    Status init(const char* data, int len) {
        int log_space = log2(len);
        return _bloom_filter->init_from_directory(log_space, Slice(data, len), false, 0);
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
class BloomFilterFuncBase {
public:
    BloomFilterFuncBase() : _inited(false) {}

    virtual ~BloomFilterFuncBase() = default;

    Status init(int64_t expect_num, double fpp) {
        size_t filter_size = BloomFilterAdaptor::optimal_bit_num(expect_num, fpp);
        return init_with_fixed_length(filter_size);
    }

    Status init_with_fixed_length(int64_t bloom_filter_length) {
        if (_inited) {
            return Status::OK();
        }
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
        auto other_func = static_cast<BloomFilterFuncBase*>(bloomfilter_func);
        if (bloomfilter_func == nullptr) {
            _bloom_filter.reset(BloomFilterAdaptor::create());
        }
        if (_bloom_filter_alloced != other_func->_bloom_filter_alloced) {
            LOG(WARNING) << "bloom filter size not the same: already allocated bytes = "
                         << _bloom_filter_alloced
                         << ", expected allocated bytes = " << other_func->_bloom_filter_alloced;
            return Status::InvalidArgument("bloom filter size invalid");
        }
        return _bloom_filter->merge(other_func->_bloom_filter.get());
    }

    Status assign(const char* data, int len) {
        if (_bloom_filter == nullptr) {
            _bloom_filter.reset(BloomFilterAdaptor::create());
        }

        _bloom_filter_alloced = len;
        return _bloom_filter->init(data, len);
    }

    Status get_data(char** data, int* len) {
        *data = _bloom_filter->data();
        *len = _bloom_filter->size();
        return Status::OK();
    }

    void light_copy(BloomFilterFuncBase* bloomfilter_func) {
        auto other_func = static_cast<BloomFilterFuncBase*>(bloomfilter_func);
        _bloom_filter_alloced = other_func->_bloom_filter_alloced;
        _bloom_filter = other_func->_bloom_filter;
        _inited = other_func->_inited;
    }

    virtual void insert(const void* data) = 0;

    virtual bool find(const void* data) const = 0;

    virtual bool find_olap_engine(const void* data) const = 0;

    virtual bool find_uint32_t(uint32_t data) const = 0;

    virtual void insert_fixed_len(const char* data, const int* offsets, int number) = 0;

    virtual uint16_t find_fixed_len_olap_engine(const char* data, const uint8* nullmap,
                                                uint16_t* offsets, int number) = 0;

    virtual void find_fixed_len(const char* data, const uint8* nullmap, int number,
                                uint8* results) = 0;

protected:
    // bloom filter size
    int32_t _bloom_filter_alloced;
    std::shared_ptr<BloomFilterAdaptor> _bloom_filter;
    bool _inited;
    std::mutex _lock;
};

template <class T>
struct CommonFindOp {
    // test_batch/find_batch/find_batch_olap_engine only used on vectorized engine
    void insert_batch(BloomFilterAdaptor& bloom_filter, const char* data, const int* offsets,
                      int number) const {
        for (int i = 0; i < number; i++) {
            bloom_filter.add_element(*((T*)data + offsets[i]));
        }
    }

    uint16_t find_batch_olap_engine(const BloomFilterAdaptor& bloom_filter, const char* data,
                                    const uint8* nullmap, uint16_t* offsets, int number) const {
        uint16_t new_size = 0;
        for (int i = 0; i < number; i++) {
            uint16_t idx = offsets[i];
            if (nullmap != nullptr && nullmap[idx]) {
                continue;
            }
            if (!bloom_filter.test_element(*((T*)data + idx))) {
                continue;
            }
            offsets[new_size++] = idx;
        }
        return new_size;
    }

    void find_batch(const BloomFilterAdaptor& bloom_filter, const char* data, const uint8* nullmap,
                    int number, uint8* results) const {
        for (int i = 0; i < number; i++) {
            results[i] = false;
            if (nullmap != nullptr && nullmap[i]) {
                continue;
            }
            if (!bloom_filter.test_element(*((T*)data + i))) {
                continue;
            }
            results[i] = true;
        }
    }

    void insert(BloomFilterAdaptor& bloom_filter, const void* data) const {
        bloom_filter.add_bytes((char*)data, sizeof(T));
    }
    bool find(const BloomFilterAdaptor& bloom_filter, const void* data) const {
        return bloom_filter.test(Slice((char*)data, sizeof(T)));
    }
    bool find_olap_engine(const BloomFilterAdaptor& bloom_filter, const void* data) const {
        return find(bloom_filter, data);
    }
    bool find(const BloomFilterAdaptor& bloom_filter, uint32_t data) const {
        return bloom_filter.test(data);
    }
};

struct StringFindOp {
    void insert_batch(BloomFilterAdaptor& bloom_filter, const char* data, const int* offsets,
                      int number) const {
        LOG(FATAL) << "StringFindOp does not support insert_batch";
    }

    uint16_t find_batch_olap_engine(const BloomFilterAdaptor& bloom_filter, const char* data,
                                    const uint8* nullmap, uint16_t* offsets, int number) const {
        LOG(FATAL) << "StringFindOp does not support find_batch_olap_engine";
        return 0;
    }

    void find_batch(const BloomFilterAdaptor& bloom_filter, const char* data, const uint8* nullmap,
                    int number, uint8* results) const {
        LOG(FATAL) << "StringFindOp does not support find_batch";
    }

    void insert(BloomFilterAdaptor& bloom_filter, const void* data) const {
        const auto* value = reinterpret_cast<const StringValue*>(data);
        if (value) {
            bloom_filter.add_bytes(value->ptr, value->len);
        }
    }
    bool find(const BloomFilterAdaptor& bloom_filter, const void* data) const {
        const auto* value = reinterpret_cast<const StringValue*>(data);
        if (value == nullptr) {
            return false;
        }
        return bloom_filter.test(Slice(value->ptr, value->len));
    }
    bool find_olap_engine(const BloomFilterAdaptor& bloom_filter, const void* data) const {
        return StringFindOp::find(bloom_filter, data);
    }
    bool find(const BloomFilterAdaptor& bloom_filter, uint32_t data) const {
        return bloom_filter.test(data);
    }
};

// We do not need to judge whether data is empty, because null will not appear
// when filer used by the storage engine
struct FixedStringFindOp : public StringFindOp {
    bool find_olap_engine(const BloomFilterAdaptor& bloom_filter, const void* input_data) const {
        const auto* value = reinterpret_cast<const StringValue*>(input_data);
        int64_t size = value->len;
        char* data = value->ptr;
        while (size > 0 && data[size - 1] == '\0') {
            size--;
        }
        return bloom_filter.test(Slice(value->ptr, size));
    }
};

struct DateTimeFindOp : public CommonFindOp<DateTimeValue> {
    bool find_olap_engine(const BloomFilterAdaptor& bloom_filter, const void* data) const {
        DateTimeValue value;
        value.from_olap_datetime(*reinterpret_cast<const uint64_t*>(data));
        return bloom_filter.test(Slice((char*)&value, sizeof(DateTimeValue)));
    }
};

// avoid violating C/C++ aliasing rules.
// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=101684

struct DateFindOp : public CommonFindOp<DateTimeValue> {
    bool find_olap_engine(const BloomFilterAdaptor& bloom_filter, const void* data) const {
        uint24_t date = *static_cast<const uint24_t*>(data);
        uint64_t value = uint32_t(date);

        DateTimeValue date_value;
        date_value.from_olap_date(value);
        date_value.to_datetime();

        char data_bytes[sizeof(date_value)];
        memcpy(&data_bytes, &date_value, sizeof(date_value));
        return bloom_filter.test(Slice(data_bytes, sizeof(DateTimeValue)));
    }
};

struct DecimalV2FindOp : public CommonFindOp<DecimalV2Value> {
    bool find_olap_engine(const BloomFilterAdaptor& bloom_filter, const void* data) const {
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

    void insert_fixed_len(const char* data, const int* offsets, int number) override {
        DCHECK(_bloom_filter != nullptr);
        dummy.insert_batch(*_bloom_filter, data, offsets, number);
    }

    uint16_t find_fixed_len_olap_engine(const char* data, const uint8* nullmap, uint16_t* offsets,
                                        int number) override {
        return dummy.find_batch_olap_engine(*_bloom_filter, data, nullmap, offsets, number);
    }

    void find_fixed_len(const char* data, const uint8* nullmap, int number,
                        uint8* results) override {
        dummy.find_batch(*_bloom_filter, data, nullmap, number, results);
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

// BloomFilterPredicate only used in runtime filter
class BloomFilterPredicate : public Predicate {
public:
    ~BloomFilterPredicate() override;
    BloomFilterPredicate(const TExprNode& node);
    BloomFilterPredicate(const BloomFilterPredicate& other);
    Expr* clone(ObjectPool* pool) const override {
        return pool->add(new BloomFilterPredicate(*this));
    }
    using Predicate::prepare;
    Status prepare(RuntimeState* state, std::shared_ptr<BloomFilterFuncBase> bloomfilterfunc);

    std::shared_ptr<BloomFilterFuncBase> get_bloom_filter_func() { return _filter; }

    BooleanVal get_boolean_val(ExprContext* context, TupleRow* row) override;

    Status open(RuntimeState* state, ExprContext* context,
                FunctionContext::FunctionStateScope scope) override;

protected:
    friend class Expr;
    std::string debug_string() const override;

private:
    bool _is_prepare;
    // if we set always = true, we will skip bloom filter
    bool _always_true;
    /// TODO: statistic filter rate in the profile
    std::atomic<int64_t> _filtered_rows;
    std::atomic<int64_t> _scan_rows;

    std::shared_ptr<BloomFilterFuncBase> _filter;
    bool _has_calculate_filter = false;
    // if filter rate less than this, bloom filter will set always true
    constexpr static double _expect_filter_rate = 0.4;
};
} // namespace doris
