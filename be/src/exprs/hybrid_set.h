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

#ifndef DORIS_BE_SRC_QUERY_EXPRS_HYBRID_SET_H
#define DORIS_BE_SRC_QUERY_EXPRS_HYBRID_SET_H

#include <parallel_hashmap/phmap.h>

#include <cstring>

#include "common/object_pool.h"
#include "common/status.h"
#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"

namespace doris {

class HybridSetBase {
public:
    HybridSetBase() = default;
    virtual ~HybridSetBase() = default;
    virtual void insert(const void* data) = 0;
    // use in vectorize execute engine
    virtual void insert(void* data, size_t) = 0;

    virtual void insert(HybridSetBase* set) = 0;

    virtual int size() = 0;
    virtual bool find(void* data) = 0;
    // use in vectorize execute engine
    virtual bool find(void* data, size_t) = 0;
    class IteratorBase {
    public:
        IteratorBase() {}
        virtual ~IteratorBase() {}
        virtual const void* get_value() = 0;
        virtual bool has_next() const = 0;
        virtual void next() = 0;
    };

    virtual IteratorBase* begin() = 0;
};

template <class T>
class HybridSet : public HybridSetBase {
public:
    HybridSet() = default;

    ~HybridSet() override = default;

    void insert(const void* data) override {
        if (data == nullptr) return;

        if (sizeof(T) >= 16) {
            // for largeint, it will core dump with no memcpy
            T value;
            memcpy(&value, data, sizeof(T));
            _set.insert(value);
        } else {
            _set.insert(*reinterpret_cast<const T*>(data));
        }
    }
    void insert(void* data, size_t) override { insert(data); }

    void insert(HybridSetBase* set) override {
        HybridSet<T>* hybrid_set = reinterpret_cast<HybridSet<T>*>(set);
        _set.insert(hybrid_set->_set.begin(), hybrid_set->_set.end());
    }

    int size() override { return _set.size(); }

    bool find(void* data) override {
        auto it = _set.find(*reinterpret_cast<T*>(data));
        return !(it == _set.end());
    }

    bool find(void* data, size_t) override { return find(data); }

    template <class _iT>
    class Iterator : public IteratorBase {
    public:
        Iterator(typename phmap::flat_hash_set<_iT>::iterator begin,
                 typename phmap::flat_hash_set<_iT>::iterator end)
                : _begin(begin), _end(end) {}
        ~Iterator() override = default;
        virtual bool has_next() const override { return !(_begin == _end); }
        virtual const void* get_value() override { return _begin.operator->(); }
        virtual void next() override { ++_begin; }

    private:
        typename phmap::flat_hash_set<_iT>::iterator _begin;
        typename phmap::flat_hash_set<_iT>::iterator _end;
    };

    IteratorBase* begin() override {
        return _pool.add(new (std::nothrow) Iterator<T>(_set.begin(), _set.end()));
    }

private:
    phmap::flat_hash_set<T> _set;
    ObjectPool _pool;
};

class StringValueSet : public HybridSetBase {
public:
    StringValueSet() = default;

    ~StringValueSet() override = default;

    void insert(const void* data) override {
        if (data == nullptr) return;

        const auto* value = reinterpret_cast<const StringValue*>(data);
        std::string str_value(value->ptr, value->len);
        _set.insert(str_value);
    }
    void insert(void* data, size_t size) override {
        std::string str_value(reinterpret_cast<char*>(data), size);
        _set.insert(str_value);
    }

    void insert(HybridSetBase* set) override {
        StringValueSet* string_set = reinterpret_cast<StringValueSet*>(set);
        _set.insert(string_set->_set.begin(), string_set->_set.end());
    }

    int size() override { return _set.size(); }

    bool find(void* data) override {
        auto* value = reinterpret_cast<StringValue*>(data);
        std::string_view str_value(const_cast<const char*>(value->ptr), value->len);
        auto it = _set.find(str_value);

        return !(it == _set.end());
    }

    bool find(void* data, size_t size) override {
        std::string str_value(reinterpret_cast<char*>(data), size);
        auto it = _set.find(str_value);
        return !(it == _set.end());
    }

    class Iterator : public IteratorBase {
    public:
        Iterator(phmap::flat_hash_set<std::string>::iterator begin,
                 phmap::flat_hash_set<std::string>::iterator end)
                : _begin(begin), _end(end) {}
        ~Iterator() override = default;
        virtual bool has_next() const override { return !(_begin == _end); }
        virtual const void* get_value() override {
            _value.ptr = const_cast<char*>(_begin->data());
            _value.len = _begin->length();
            return &_value;
        }
        virtual void next() override { ++_begin; }

    private:
        typename phmap::flat_hash_set<std::string>::iterator _begin;
        typename phmap::flat_hash_set<std::string>::iterator _end;
        StringValue _value;
    };

    IteratorBase* begin() override {
        return _pool.add(new (std::nothrow) Iterator(_set.begin(), _set.end()));
    }

private:
    phmap::flat_hash_set<std::string> _set;
    ObjectPool _pool;
};

} // namespace doris

#endif // DORIS_BE_SRC_QUERY_EXPRS_HYBRID_SET_H
