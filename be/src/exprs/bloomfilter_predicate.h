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
#include <cmath>
#include <memory>
#include <string>

#include "common/object_pool.h"
#include "exprs/block_bloom_filter.hpp"
#include "exprs/predicate.h"
#include "olap/bloom_filter.hpp"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"

namespace doris {
namespace detail {
class BlockBloomFilterAdaptor {
public:
    BlockBloomFilterAdaptor() { _bloom_filter = std::make_shared<doris::BlockBloomFilter>(); }
    static int64_t optimal_bit_num(int64_t expect_num, double fpp) {
        return doris::segment_v2::BloomFilter::optimal_bit_num(expect_num, fpp) / 8;
    }

    static BlockBloomFilterAdaptor* create() { return new BlockBloomFilterAdaptor(); }

    Status merge(BlockBloomFilterAdaptor* other) {
        return _bloom_filter->merge(*other->_bloom_filter);
    }

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

    bool test_bytes(const char* data, size_t len) const {
        return _bloom_filter->find(Slice(data, len));
    }

    void add_bytes(const char* data, size_t len) { _bloom_filter->insert(Slice(data, len)); }

private:
    std::shared_ptr<doris::BlockBloomFilter> _bloom_filter;
};

} // namespace detail
using CurrentBloomFilterAdaptor = detail::BlockBloomFilterAdaptor;
// Only Used In RuntimeFilter
class IBloomFilterFuncBase {
public:
    virtual ~IBloomFilterFuncBase() {}
    virtual Status init(int64_t expect_num, double fpp) = 0;
    virtual Status init_with_fixed_length(int64_t bloom_filter_length) = 0;

    virtual void insert(const void* data) = 0;
    virtual bool find(const void* data) const = 0;
    virtual bool find_olap_engine(const void* data) const = 0;

    virtual Status merge(IBloomFilterFuncBase* bloomfilter_func) = 0;
    virtual Status assign(const char* data, int len) = 0;

    virtual Status get_data(char** data, int* len) = 0;
    virtual MemTracker* tracker() = 0;
    virtual void light_copy(IBloomFilterFuncBase* other) = 0;

    static IBloomFilterFuncBase* create_bloom_filter(MemTracker* tracker, PrimitiveType type);
};

template <class BloomFilterAdaptor>
class BloomFilterFuncBase : public IBloomFilterFuncBase {
public:
    BloomFilterFuncBase(MemTracker* tracker) : _tracker(tracker), _inited(false) {}

    virtual ~BloomFilterFuncBase() {
        if (_tracker != nullptr) {
            _tracker->Release(_bloom_filter_alloced);
        }
    }

    Status init(int64_t expect_num, double fpp) override {
        size_t filter_size = BloomFilterAdaptor::optimal_bit_num(expect_num, fpp);
        return init_with_fixed_length(filter_size);
    }

    Status init_with_fixed_length(int64_t bloom_filter_length) override {
        DCHECK(!_inited);
        DCHECK(bloom_filter_length >= 0);
        DCHECK_EQ((bloom_filter_length & (bloom_filter_length - 1)), 0);
        _bloom_filter_alloced = bloom_filter_length;
        _bloom_filter.reset(BloomFilterAdaptor::create());
        RETURN_IF_ERROR(_bloom_filter->init(bloom_filter_length));
        _tracker->Consume(_bloom_filter_alloced);
        _inited = true;
        return Status::OK();
    }

    Status merge(IBloomFilterFuncBase* bloomfilter_func) override {
        auto other_func = static_cast<BloomFilterFuncBase*>(bloomfilter_func);
        if (bloomfilter_func == nullptr) {
            _bloom_filter.reset(BloomFilterAdaptor::create());
        }
        if (_bloom_filter_alloced != other_func->_bloom_filter_alloced) {
            LOG(WARNING) << "bloom filter size not the same";
            return Status::InvalidArgument("bloom filter size invalid");
        }
        return _bloom_filter->merge(other_func->_bloom_filter.get());
    }

    Status assign(const char* data, int len) override {
        if (_bloom_filter == nullptr) {
            _bloom_filter.reset(BloomFilterAdaptor::create());
        }

        _bloom_filter_alloced = len;
        _tracker->Consume(_bloom_filter_alloced);
        return _bloom_filter->init(data, len);
    }

    Status get_data(char** data, int* len) override {
        *data = _bloom_filter->data();
        *len = _bloom_filter->size();
        return Status::OK();
    }

    MemTracker* tracker() override { return _tracker; }

    void light_copy(IBloomFilterFuncBase* bloomfilter_func) override {
        auto other_func = static_cast<BloomFilterFuncBase*>(bloomfilter_func);
        _tracker = nullptr;
        _bloom_filter_alloced = other_func->_bloom_filter_alloced;
        _bloom_filter = other_func->_bloom_filter;
        _inited = other_func->_inited;
    }

protected:
    MemTracker* _tracker;
    // bloom filter size
    int32_t _bloom_filter_alloced;
    std::shared_ptr<BloomFilterAdaptor> _bloom_filter;
    bool _inited;
};

template <class T, class BloomFilterAdaptor>
struct CommonFindOp {
    ALWAYS_INLINE void insert(BloomFilterAdaptor& bloom_filter, const void* data) const {
        bloom_filter.add_bytes((char*)data, sizeof(T));
    }
    ALWAYS_INLINE bool find(const BloomFilterAdaptor& bloom_filter, const void* data) const {
        return bloom_filter.test_bytes((char*)data, sizeof(T));
    }
    ALWAYS_INLINE bool find_olap_engine(const BloomFilterAdaptor& bloom_filter,
                                        const void* data) const {
        return this->find(bloom_filter, data);
    }
};

template <class BloomFilterAdaptor>
struct StringFindOp {
    ALWAYS_INLINE void insert(BloomFilterAdaptor& bloom_filter, const void* data) const {
        const auto* value = reinterpret_cast<const StringValue*>(data);
        bloom_filter.add_bytes(value->ptr, value->len);
    }
    ALWAYS_INLINE bool find(const BloomFilterAdaptor& bloom_filter, const void* data) const {
        const auto* value = reinterpret_cast<const StringValue*>(data);
        return bloom_filter.test_bytes(value->ptr, value->len);
    }
    ALWAYS_INLINE bool find_olap_engine(const BloomFilterAdaptor& bloom_filter,
                                        const void* data) const {
        return StringFindOp::find(bloom_filter, data);
    }
};

template <class BloomFilterAdaptor>
struct FixedStringFindOp : public StringFindOp<BloomFilterAdaptor> {
    ALWAYS_INLINE bool find_olap_engine(const BloomFilterAdaptor& bloom_filter,
                                        const void* data) const {
        const auto* value = reinterpret_cast<const StringValue*>(data);
        auto end_ptr = value->ptr + value->len - 1;
        while (end_ptr > value->ptr && *end_ptr == '\0') --end_ptr;
        return bloom_filter.test_bytes(value->ptr, end_ptr - value->ptr + 1);
    }
};

template <class BloomFilterAdaptor>
struct DateTimeFindOp : public CommonFindOp<DateTimeValue, BloomFilterAdaptor> {
    bool find_olap_engine(const BloomFilterAdaptor& bloom_filter, const void* data) const {
        DateTimeValue value;
        value.from_olap_datetime(*reinterpret_cast<const uint64_t*>(data));
        return bloom_filter.test_bytes((char*)&value, sizeof(DateTimeValue));
    }
};

template <class BloomFilterAdaptor>
struct DateFindOp : public CommonFindOp<DateTimeValue, BloomFilterAdaptor> {
    bool find_olap_engine(const BloomFilterAdaptor& bloom_filter, const void* data) const {
        uint64_t value = 0;
        value = *(unsigned char*)((char*)data + 2);
        value <<= 8;
        value |= *(unsigned char*)((char*)data + 1);
        value <<= 8;
        value |= *(unsigned char*)((char*)data);
        DateTimeValue date_value;
        date_value.from_olap_date(value);
        date_value.to_datetime();
        return bloom_filter.test_bytes((char*)&date_value, sizeof(DateTimeValue));
    }
};

template <class BloomFilterAdaptor>
struct DecimalV2FindOp : public CommonFindOp<DateTimeValue, BloomFilterAdaptor> {
    bool find_olap_engine(const BloomFilterAdaptor& bloom_filter, const void* data) const {
        DecimalV2Value value;
        int64_t int_value = *(int64_t*)(data);
        int32_t frac_value = *(int32_t*)((char*)data + sizeof(int64_t));
        value.from_olap_decimal(int_value, frac_value);
        return bloom_filter.test_bytes((char*)&value, sizeof(DecimalV2Value));
    }
};

template <PrimitiveType type, class BloomFilterAdaptor>
struct BloomFilterTypeTraits {
    using T = typename PrimitiveTypeTraits<type>::CppType;
    using FindOp = CommonFindOp<T, BloomFilterAdaptor>;
};

template <class BloomFilterAdaptor>
struct BloomFilterTypeTraits<TYPE_DATE, BloomFilterAdaptor> {
    using FindOp = DateFindOp<BloomFilterAdaptor>;
};

template <class BloomFilterAdaptor>
struct BloomFilterTypeTraits<TYPE_DATETIME, BloomFilterAdaptor> {
    using FindOp = DateTimeFindOp<BloomFilterAdaptor>;
};

template <class BloomFilterAdaptor>
struct BloomFilterTypeTraits<TYPE_DECIMALV2, BloomFilterAdaptor> {
    using FindOp = DecimalV2FindOp<BloomFilterAdaptor>;
};

template <class BloomFilterAdaptor>
struct BloomFilterTypeTraits<TYPE_CHAR, BloomFilterAdaptor> {
    using FindOp = FixedStringFindOp<BloomFilterAdaptor>;
};

template <class BloomFilterAdaptor>
struct BloomFilterTypeTraits<TYPE_VARCHAR, BloomFilterAdaptor> {
    using FindOp = StringFindOp<BloomFilterAdaptor>;
};

template <PrimitiveType type, class BloomFilterAdaptor>
class BloomFilterFunc final : public BloomFilterFuncBase<BloomFilterAdaptor> {
public:
    BloomFilterFunc(MemTracker* tracker) : BloomFilterFuncBase<BloomFilterAdaptor>(tracker) {}

    ~BloomFilterFunc() = default;

    void insert(const void* data) {
        DCHECK(this->_bloom_filter != nullptr);
        dummy.insert(*this->_bloom_filter, data);
    }

    bool find(const void* data) const override {
        DCHECK(this->_bloom_filter != nullptr);
        return dummy.find(*this->_bloom_filter, data);
    }

    bool find_olap_engine(const void* data) const override {
        return dummy.find_olap_engine(*this->_bloom_filter, data);
    }

private:
    typename BloomFilterTypeTraits<type, BloomFilterAdaptor>::FindOp dummy;
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
    Status prepare(RuntimeState* state, IBloomFilterFuncBase* bloomfilterfunc);

    std::shared_ptr<IBloomFilterFuncBase> get_bloom_filter_func() { return _filter; }

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

    std::shared_ptr<IBloomFilterFuncBase> _filter;
    bool _has_calculate_filter = false;
    // loop size must be power of 2
    constexpr static int64_t _loop_size = 8192;
    // if filter rate less than this, bloom filter will set always true
    constexpr static double _expect_filter_rate = 0.2;
};
} // namespace doris
#endif
