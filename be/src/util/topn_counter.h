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

#ifndef DORIS_BE_SRC_UTI_TOPN_COUNTER_H
#define DORIS_BE_SRC_UTI_TOPN_COUNTER_H

#include <list>
#include <unordered_map>

#include "common/logging.h"
#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/large_int_value.h"
#include "udf/udf.h"

namespace doris {

static const uint32_t DEFAULT_SPACE_EXPAND_RATE = 50;

struct Slice;

class Counter {
public:
    Counter() = default;

    Counter(const std::string& item, uint64_t count) : _item(item), _count(count) {}

    uint64_t get_count() const { return _count; }

    const std::string& get_item() const { return _item; }

    void add_count(uint64_t count) { _count += count; }

    bool operator==(const Counter& other) {
        if (_item.compare(other._item) != 0) {
            return false;
        }
        if (_count != other._count) {
            return false;
        }
        return true;
    }

private:
    std::string _item;
    uint64_t _count;
};

// Refer to TopNCounter.java in https://github.com/apache/kylin
// Based on the Space-Saving algorithm and the Stream-Summary data structure as described in:
// Efficient Computation of Frequent and Top-k Elements in Data Streams by Metwally, Agrawal, and Abbadi
class TopNCounter {
public:
    TopNCounter(uint32_t space_expand_rate = DEFAULT_SPACE_EXPAND_RATE)
            : _top_num(0),
              _space_expand_rate(space_expand_rate),
              _capacity(0),
              _ordered(false),
              _counter_map(new std::unordered_map<std::string, Counter>(_capacity)),
              _counter_vec(new std::vector<Counter>(_capacity)) {}

    TopNCounter(const Slice& src)
            : _top_num(0),
              _space_expand_rate(0),
              _capacity(0),
              _ordered(false),
              _counter_map(new std::unordered_map<std::string, Counter>(_capacity)),
              _counter_vec(new std::vector<Counter>(_capacity)) {
        bool res = deserialize(src);
        DCHECK(res);
    }

    ~TopNCounter() {
        delete _counter_map;
        delete _counter_vec;
    }

    template <typename T>
    void add_item(const T& item) {
        add_item(item, 1);
    }

    void add_item(const BooleanVal& item, uint64_t incrementCount) {
        add_item_numeric(item, incrementCount);
    }
    void add_item(const TinyIntVal& item, uint64_t incrementCount) {
        add_item_numeric(item, incrementCount);
    }
    void add_item(const SmallIntVal& item, uint64_t incrementCount) {
        add_item_numeric(item, incrementCount);
    }
    void add_item(const IntVal& item, uint64_t incrementCount) {
        add_item_numeric(item, incrementCount);
    }
    void add_item(const BigIntVal& item, uint64_t incrementCount) {
        add_item_numeric(item, incrementCount);
    }
    void add_item(const FloatVal& item, uint64_t incrementCount) {
        add_item_numeric(item, incrementCount);
    }
    void add_item(const DoubleVal& item, uint64_t incrementCount) {
        add_item_numeric(item, incrementCount);
    }
    void add_item(const StringVal& item, uint64_t incrementCount) {
        add_item(std::string((char*)item.ptr, item.len), incrementCount);
    }
    void add_item(const DateTimeVal& item, uint64_t incrementCount) {
        char str[MAX_DTVALUE_STR_LEN];
        DateTimeValue::from_datetime_val(item).to_string(str);
        add_item(std::string(str), incrementCount);
    }
    void add_item(const LargeIntVal& item, uint64_t incrementCount) {
        add_item(LargeIntValue::to_string(item.val), incrementCount);
    }
    void add_item(const DecimalV2Val& item, uint64_t incrementCount) {
        add_item(DecimalV2Value::from_decimal_val(item).to_string(), incrementCount);
    }

    template <typename T>
    void add_item_numeric(const T& item, uint64_t incrementCount) {
        add_item(std::to_string(item.val), incrementCount);
    }

    void add_item(const std::string& item, uint64_t incrementCount);

    void serialize(std::string* buffer);

    bool deserialize(const Slice& src);

    void merge(doris::TopNCounter&& other);

    // Sort counter by count value and record it in _counter_vec
    void sort_retain(uint32_t capacity);

    void sort_retain(uint32_t capacity, std::vector<Counter>* sort_vec);

    void finalize(std::string&);

    void set_top_num(uint32_t top_num) {
        _top_num = top_num;
        _capacity = top_num * _space_expand_rate;
    }

private:
    uint32_t _top_num;
    uint32_t _space_expand_rate;
    uint64_t _capacity;
    bool _ordered;
    std::unordered_map<std::string, Counter>* _counter_map;
    std::vector<Counter>* _counter_vec;
};

class TopNComparator {
public:
    bool operator()(const Counter& s1, const Counter& s2) {
        return s1.get_count() > s2.get_count();
    }
};
} // namespace doris

#endif //DORIS_BE_SRC_UTI_TOPN_COUNTER_H
