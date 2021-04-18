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

#include <cstring>
#include <unordered_set>

#include "common/object_pool.h"
#include "common/status.h"
#include "runtime/datetime_value.h"
#include "runtime/decimal_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"

namespace doris {

class HybridSetBase {
public:
    HybridSetBase() {}
    virtual ~HybridSetBase() {}
    virtual void insert(void* data) = 0;

    virtual void insert(HybridSetBase* set) = 0;

    virtual int size() = 0;
    virtual bool find(void* data) = 0;

    static HybridSetBase* create_set(PrimitiveType type);
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
    HybridSet() {}

    virtual ~HybridSet() {}

    virtual void insert(void* data) {
        if (sizeof(T) >= 16) {
            // for largeint, it will core dump with no memcpy
            T value;
            memcpy(&value, data, sizeof(T));
            _set.insert(value);
        } else {
            _set.insert(*reinterpret_cast<T*>(data));
        }
    }

    virtual void insert(HybridSetBase* set) {
        HybridSet<T>* hybrid_set = reinterpret_cast<HybridSet<T>*>(set);
        _set.insert(hybrid_set->_set.begin(), hybrid_set->_set.end());
    }

    virtual int size() { return _set.size(); }
    virtual bool find(void* data) {
        typename std::unordered_set<T>::const_iterator it = _set.find(*reinterpret_cast<T*>(data));

        if (it == _set.end()) {
            return false;
        } else {
            return true;
        }

        return false;
    }

    template <class _iT>
    class Iterator : public IteratorBase {
    public:
        Iterator(typename std::unordered_set<_iT>::iterator begin,
                 typename std::unordered_set<_iT>::iterator end)
                : _begin(begin), _end(end) {}
        virtual ~Iterator() {}
        virtual bool has_next() const { return !(_begin == _end); }
        virtual const void* get_value() { return _begin.operator->(); }
        virtual void next() { ++_begin; }

    private:
        typename std::unordered_set<_iT>::iterator _begin;
        typename std::unordered_set<_iT>::iterator _end;
    };

    IteratorBase* begin() {
        return _pool.add(new (std::nothrow) Iterator<T>(_set.begin(), _set.end()));
    }

private:
    std::unordered_set<T> _set;
    ObjectPool _pool;
};

class StringValueSet : public HybridSetBase {
public:
    StringValueSet() {}

    virtual ~StringValueSet() {}

    virtual void insert(void* data) {
        StringValue* value = reinterpret_cast<StringValue*>(data);
        std::string str_value(value->ptr, value->len);
        _set.insert(str_value);
    }

    void insert(HybridSetBase* set) {
        StringValueSet* string_set = reinterpret_cast<StringValueSet*>(set);
        _set.insert(string_set->_set.begin(), string_set->_set.end());
    }

    virtual int size() { return _set.size(); }
    virtual bool find(void* data) {
        StringValue* value = reinterpret_cast<StringValue*>(data);
        std::string str_value(value->ptr, value->len);
        typename std::unordered_set<std::string>::iterator it = _set.find(str_value);

        if (it == _set.end()) {
            return false;
        } else {
            return true;
        }

        return false;
    }

    class Iterator : public IteratorBase {
    public:
        Iterator(std::unordered_set<std::string>::iterator begin,
                 std::unordered_set<std::string>::iterator end)
                : _begin(begin), _end(end) {}
        virtual ~Iterator() {}
        virtual bool has_next() const { return !(_begin == _end); }
        virtual const void* get_value() {
            _value.ptr = const_cast<char*>(_begin->data());
            _value.len = _begin->length();
            return &_value;
        }
        virtual void next() { ++_begin; }

    private:
        typename std::unordered_set<std::string>::iterator _begin;
        typename std::unordered_set<std::string>::iterator _end;
        StringValue _value;
    };

    IteratorBase* begin() {
        return _pool.add(new (std::nothrow) Iterator(_set.begin(), _set.end()));
    }

private:
    std::unordered_set<std::string> _set;
    ObjectPool _pool;
};

} // namespace doris

#endif // DORIS_BE_SRC_QUERY_EXPRS_HYBRID_SET_H
