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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_BLOOM_PREDICATE_H
#define DORIS_BE_SRC_QUERY_EXPRS_BLOOM_PREDICATE_H
#include <algorithm>
#include <memory>
#include <string>

#include "common/object_pool.h"
#include "exprs/expr_context.h"
#include "exprs/predicate.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"

namespace doris {
/// only used in Runtime Filter
class BloomFilterFuncBase {
public:
    BloomFilterFuncBase(MemTracker* tracker) : _tracker(tracker), _inited(false) {};

    virtual ~BloomFilterFuncBase() {
        if (_tracker != nullptr) {
            _tracker->Release(_bloom_filter_alloced);
        }
    }

    // init a bloom filter with expect element num
    virtual Status init(int64_t expect_num = 4096, double fpp = 0.05) {
        DCHECK(!_inited);
        DCHECK(expect_num >= 0); // we need alloc 'optimal_bit_num(expect_num,fpp) / 8' bytes
        _bloom_filter_alloced =
                doris::segment_v2::BloomFilter::optimal_bit_num(expect_num, fpp) / 8;

        std::unique_ptr<doris::segment_v2::BloomFilter> bloom_filter;
        Status st = doris::segment_v2::BloomFilter::create(
                doris::segment_v2::BloomFilterAlgorithmPB::BLOCK_BLOOM_FILTER, &bloom_filter);
        // status is always true if we use valid BloomFilterAlgorithmPB
        DCHECK(st.ok());
        RETURN_IF_ERROR(st);
        st = bloom_filter->init(_bloom_filter_alloced,
                                doris::segment_v2::HashStrategyPB::HASH_MURMUR3_X64_64);
        // status is always true if we use HASH_MURMUR3_X64_64
        DCHECK(st.ok());
        _bloom_filter.reset(bloom_filter.release());
        _tracker->Consume(_bloom_filter_alloced);
        _inited = true;
        return st;
    }

    virtual Status init_with_fixed_length(int64_t bloom_filter_length) {
        DCHECK(!_inited);
        DCHECK(bloom_filter_length >= 0);

        std::unique_ptr<doris::segment_v2::BloomFilter> bloom_filter;
        _bloom_filter_alloced = bloom_filter_length;
        Status st = doris::segment_v2::BloomFilter::create(
                doris::segment_v2::BloomFilterAlgorithmPB::BLOCK_BLOOM_FILTER, &bloom_filter);
        DCHECK(st.ok());
        st = bloom_filter->init(_bloom_filter_alloced,
                                doris::segment_v2::HashStrategyPB::HASH_MURMUR3_X64_64);
        DCHECK(st.ok());
        _tracker->Consume(_bloom_filter_alloced);
        _bloom_filter.reset(bloom_filter.release());
        _inited = true;
        return st;
    }

    virtual void insert(const void* data) = 0;

    virtual bool find(const void* data) = 0;

    // Because the data structures of the execution layer and the storage layer are inconsistent,
    // we need to provide additional interfaces for the storage layer to call
    virtual bool find_olap_engine(const void* data) { return this->find(data); }

    Status merge(BloomFilterFuncBase* bloomfilter_func) {
        DCHECK(_inited);
        if (_bloom_filter == nullptr) {
            std::unique_ptr<doris::segment_v2::BloomFilter> bloom_filter;
            RETURN_IF_ERROR(doris::segment_v2::BloomFilter::create(
                    doris::segment_v2::BloomFilterAlgorithmPB::BLOCK_BLOOM_FILTER, &bloom_filter));
            _bloom_filter.reset(bloom_filter.release());
        }
        if (_bloom_filter_alloced != bloomfilter_func->_bloom_filter_alloced) {
            LOG(WARNING) << "bloom filter size not the same";
            return Status::InvalidArgument("bloom filter size invalid");
        }
        return _bloom_filter->merge(bloomfilter_func->_bloom_filter.get());
    }

    Status assign(const char* data, int len) {
        if (_bloom_filter == nullptr) {
            std::unique_ptr<doris::segment_v2::BloomFilter> bloom_filter;
            RETURN_IF_ERROR(doris::segment_v2::BloomFilter::create(
                    doris::segment_v2::BloomFilterAlgorithmPB::BLOCK_BLOOM_FILTER, &bloom_filter));
            _bloom_filter.reset(bloom_filter.release());
        }
        _bloom_filter_alloced = len - 1;
        _tracker->Consume(_bloom_filter_alloced);
        return _bloom_filter->init(data, len,
                                   doris::segment_v2::HashStrategyPB::HASH_MURMUR3_X64_64);
    }
    /// create a bloom filter function
    /// tracker shouldn't be nullptr
    static BloomFilterFuncBase* create_bloom_filter(MemTracker* tracker, PrimitiveType type);

    Status get_data(char** data, int* len);

    MemTracker* tracker() { return _tracker; }

    void light_copy(BloomFilterFuncBase* other) {
        _tracker = nullptr;
        _bloom_filter_alloced = other->_bloom_filter_alloced;
        _bloom_filter = other->_bloom_filter;
        _inited = other->_inited;
    }

protected:
    MemTracker* _tracker;
    // bloom filter size
    int32_t _bloom_filter_alloced;
    std::shared_ptr<doris::segment_v2::BloomFilter> _bloom_filter;
    bool _inited;
};

template <class T>
class BloomFilterFunc : public BloomFilterFuncBase {
public:
    BloomFilterFunc(MemTracker* tracker) : BloomFilterFuncBase(tracker) {}

    ~BloomFilterFunc() = default;

    virtual void insert(const void* data) {
        DCHECK(_bloom_filter != nullptr);
        _bloom_filter->add_bytes((char*)data, sizeof(T));
    }

    virtual bool find(const void* data) {
        DCHECK(_bloom_filter != nullptr);
        return _bloom_filter->test_bytes((char*)data, sizeof(T));
    }
};

template <>
class BloomFilterFunc<StringValue> : public BloomFilterFuncBase {
public:
    BloomFilterFunc(MemTracker* tracker) : BloomFilterFuncBase(tracker) {}

    ~BloomFilterFunc() = default;

    virtual void insert(const void* data) {
        DCHECK(_bloom_filter != nullptr);
        const auto* value = reinterpret_cast<const StringValue*>(data);
        _bloom_filter->add_bytes(value->ptr, value->len);
    }

    virtual bool find(const void* data) {
        DCHECK(_bloom_filter != nullptr);
        const auto* value = reinterpret_cast<const StringValue*>(data);
        return _bloom_filter->test_bytes(value->ptr, value->len);
    }
};

class FixedCharBloomFilterFunc : public BloomFilterFunc<StringValue> {
public:
    FixedCharBloomFilterFunc(MemTracker* tracker) : BloomFilterFunc<StringValue>(tracker) {}

    ~FixedCharBloomFilterFunc() = default;

    virtual bool find(const void* data) {
        DCHECK(_bloom_filter != nullptr);
        const auto* value = reinterpret_cast<const StringValue*>(data);
        auto end_ptr = value->ptr + value->len - 1;
        while (end_ptr > value->ptr && *end_ptr == '\0') --end_ptr;
        return _bloom_filter->test_bytes(value->ptr, end_ptr - value->ptr + 1);
    }
};

class DateTimeBloomFilterFunc : public BloomFilterFunc<DateTimeValue> {
public:
    DateTimeBloomFilterFunc(MemTracker* tracker) : BloomFilterFunc<DateTimeValue>(tracker) {}

    virtual bool find_olap_engine(const void* data) {
        DateTimeValue value;
        value.from_olap_datetime(*reinterpret_cast<const uint64_t*>(data));
        return _bloom_filter->test_bytes((char*)&value, sizeof(DateTimeValue));
    }
};

class DateBloomFilterFunc : public BloomFilterFunc<DateTimeValue> {
public:
    DateBloomFilterFunc(MemTracker* tracker) : BloomFilterFunc<DateTimeValue>(tracker) {}

    virtual bool find_olap_engine(const void* data) {
        uint64_t value = 0;
        value = *(unsigned char*)((char*)data + 2);
        value <<= 8;
        value |= *(unsigned char*)((char*)data + 1);
        value <<= 8;
        value |= *(unsigned char*)((char*)data);
        DateTimeValue date_value;
        date_value.from_olap_date(value);
        date_value.to_datetime();
        return _bloom_filter->test_bytes((char*)&date_value, sizeof(DateTimeValue));
    }
};

class DecimalFilterFunc : public BloomFilterFunc<DecimalValue> {
public:
    DecimalFilterFunc(MemTracker* tracker) : BloomFilterFunc<DecimalValue>(tracker) {}

    virtual bool find_olap_engine(const void* data) {
        int64_t int_value = *(int64_t*)(data);
        int32_t frac_value = *(int32_t*)((char*)data + sizeof(int64_t));
        DecimalValue value(int_value, frac_value);
        return _bloom_filter->test_bytes((char*)&value, sizeof(DecimalValue));
    }
};

class DecimalV2FilterFunc : public BloomFilterFunc<DecimalV2Value> {
public:
    DecimalV2FilterFunc(MemTracker* tracker) : BloomFilterFunc<DecimalV2Value>(tracker) {}

    virtual bool find_olap_engine(const void* data) {
        DecimalV2Value value;
        int64_t int_value = *(int64_t*)(data);
        int32_t frac_value = *(int32_t*)((char*)data + sizeof(int64_t));
        value.from_olap_decimal(int_value, frac_value);
        return _bloom_filter->test_bytes((char*)&value, sizeof(DecimalV2Value));
    }
};

// BloomFilterPredicate only used in runtime filter
class BloomFilterPredicate : public Predicate {
public:
    virtual ~BloomFilterPredicate();
    BloomFilterPredicate(const TExprNode& node);
    BloomFilterPredicate(const BloomFilterPredicate& other);
    virtual Expr* clone(ObjectPool* pool) const override {
        return pool->add(new BloomFilterPredicate(*this));
    }
    Status prepare(RuntimeState* state, BloomFilterFuncBase* bloomfilterfunc);

    std::shared_ptr<BloomFilterFuncBase> get_bloom_filter_func() { return _filter; }

    virtual BooleanVal get_boolean_val(ExprContext* context, TupleRow* row) override;

    virtual Status open(RuntimeState* state, ExprContext* context,
                        FunctionContext::FunctionStateScope scope) override;

protected:
    friend class Expr;
    virtual std::string debug_string() const override;

private:
    bool _is_prepare;
    // if we set always = true, we will skip bloom filter
    bool _always_true;
    /// TODO: statistic filter rate in the profile
    std::atomic<int64_t> _filtered_rows;
    std::atomic<int64_t> _scan_rows;

    std::shared_ptr<BloomFilterFuncBase> _filter;
    bool _has_calculate_filter = false;
    // loop size must be power of 2
    constexpr static int64_t _loop_size = 8192;
    // if filter rate less than this, bloom filter will set always true
    constexpr static double _expect_filter_rate = 0.2;
};
} // namespace doris
#endif
