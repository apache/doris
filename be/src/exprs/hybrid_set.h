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

#include <parallel_hashmap/phmap.h>

#include <cstring>

#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/expr.h"
#include "runtime/datetime_value.h"
#include "runtime/decimalv2_value.h"
#include "runtime/large_int_value.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"
#include "vec/exprs/vliteral.h"

namespace doris {

class HybridSetBase {
public:
    HybridSetBase() = default;
    virtual ~HybridSetBase() = default;
    virtual void insert(const void* data) = 0;
    // use in vectorize execute engine
    virtual void insert(void* data, size_t) = 0;

    virtual void insert_fixed_len(const char* data, const int* offsets, int number) = 0;

    virtual void insert(HybridSetBase* set) = 0;

    virtual int size() = 0;
    virtual bool find(void* data) = 0;
    // use in vectorize execute engine
    virtual bool find(void* data, size_t) = 0;

    virtual Status to_vexpr_list(doris::ObjectPool* pool,
                                 std::vector<doris::vectorized::VExpr*>* vexpr_list, int precision,
                                 int scale) = 0;

    virtual bool is_date_v2() { return false; }
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

template <PrimitiveType T, bool is_vec = false>
class HybridSet : public HybridSetBase {
public:
    using CppType = std::conditional_t<is_vec, typename VecPrimitiveTypeTraits<T>::CppType,
                                       typename PrimitiveTypeTraits<T>::CppType>;

    HybridSet() = default;

    ~HybridSet() override = default;

    bool is_date_v2() override { return T == TYPE_DATEV2; }

    Status to_vexpr_list(doris::ObjectPool* pool,
                         std::vector<doris::vectorized::VExpr*>* vexpr_list, int precision,
                         int scale) override {
        HybridSetBase::IteratorBase* it = begin();
        DCHECK(it != nullptr);
        while (it->has_next()) {
            TExprNode node;
            const void* v = it->get_value();
            create_texpr_literal_node<T>(v, &node, precision, scale);
            vexpr_list->push_back(pool->add(new doris::vectorized::VLiteral(node)));
            it->next();
        }
        return Status::OK();
    };

    void insert(const void* data) override {
        if (data == nullptr) return;

        if constexpr (sizeof(CppType) >= 16) {
            // for large int, it will core dump with no memcpy
            CppType value;
            memcpy(&value, data, sizeof(CppType));
            _set.insert(value);
        } else {
            _set.insert(*reinterpret_cast<const CppType*>(data));
        }
    }
    void insert(void* data, size_t) override { insert(data); }

    void insert_fixed_len(const char* data, const int* offsets, int number) override {
        for (int i = 0; i < number; i++) {
            _set.insert(*((CppType*)data + offsets[i]));
        }
    }

    void insert(HybridSetBase* set) override {
        HybridSet<T, is_vec>* hybrid_set = reinterpret_cast<HybridSet<T, is_vec>*>(set);
        _set.insert(hybrid_set->_set.begin(), hybrid_set->_set.end());
    }

    int size() override { return _set.size(); }

    bool find(void* data) override {
        auto it = _set.find(*reinterpret_cast<CppType*>(data));
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
        return _pool.add(new (std::nothrow) Iterator<CppType>(_set.begin(), _set.end()));
    }

private:
    phmap::flat_hash_set<CppType> _set;
    ObjectPool _pool;
};

class StringSet : public HybridSetBase {
public:
    StringSet() = default;

    ~StringSet() override = default;

    Status to_vexpr_list(doris::ObjectPool* pool,
                         std::vector<doris::vectorized::VExpr*>* vexpr_list, int precision,
                         int scale) override {
        HybridSetBase::IteratorBase* it = begin();
        DCHECK(it != nullptr);
        while (it->has_next()) {
            TExprNode node;
            const void* v = it->get_value();
            create_texpr_literal_node<TYPE_STRING>(v, &node);
            vexpr_list->push_back(pool->add(new doris::vectorized::VLiteral(node)));
            it->next();
        }
        return Status::OK();
    };

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

    void insert_fixed_len(const char* data, const int* offsets, int number) override {
        LOG(FATAL) << "string set not support insert_fixed_len";
    }

    void insert(HybridSetBase* set) override {
        StringSet* string_set = reinterpret_cast<StringSet*>(set);
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

// note: Two difference from StringSet
// 1 StringValue has better comparison performance than std::string
// 2 std::string keeps its own memory, bug StringValue just keeps ptr and len, so you the caller should manage memory of StringValue
class StringValueSet : public HybridSetBase {
public:
    StringValueSet() = default;

    ~StringValueSet() override = default;

    Status to_vexpr_list(doris::ObjectPool* pool,
                         std::vector<doris::vectorized::VExpr*>* vexpr_list, int precision,
                         int scale) override {
        HybridSetBase::IteratorBase* it = begin();
        DCHECK(it != nullptr);
        while (it->has_next()) {
            TExprNode node;
            const void* v = it->get_value();
            create_texpr_literal_node<TYPE_STRING>(v, &node);
            vexpr_list->push_back(pool->add(new doris::vectorized::VLiteral(node)));
            it->next();
        }
        return Status::OK();
    };

    void insert(const void* data) override {
        if (data == nullptr) return;

        const auto* value = reinterpret_cast<const StringValue*>(data);
        StringValue sv(value->ptr, value->len);
        _set.insert(sv);
    }

    void insert(void* data, size_t size) override {
        StringValue sv(reinterpret_cast<char*>(data), size);
        _set.insert(sv);
    }

    void insert_fixed_len(const char* data, const int* offsets, int number) override {
        LOG(FATAL) << "string set not support insert_fixed_len";
    }

    void insert(HybridSetBase* set) override {
        StringValueSet* string_set = reinterpret_cast<StringValueSet*>(set);
        _set.insert(string_set->_set.begin(), string_set->_set.end());
    }

    int size() override { return _set.size(); }

    bool find(void* data) override {
        auto* value = reinterpret_cast<StringValue*>(data);
        auto it = _set.find(*value);

        return !(it == _set.end());
    }

    bool find(void* data, size_t size) override {
        // std::string str_value(reinterpret_cast<char*>(data), size);
        StringValue sv(reinterpret_cast<char*>(data), size);
        auto it = _set.find(sv);
        return !(it == _set.end());
    }

    class Iterator : public IteratorBase {
    public:
        Iterator(phmap::flat_hash_set<StringValue>::iterator begin,
                 phmap::flat_hash_set<StringValue>::iterator end)
                : _begin(begin), _end(end) {}
        ~Iterator() override = default;
        virtual bool has_next() const override { return !(_begin == _end); }
        virtual const void* get_value() override {
            _value.ptr = const_cast<char*>(_begin->ptr);
            _value.len = _begin->len;
            return &_value;
        }
        virtual void next() override { ++_begin; }

    private:
        typename phmap::flat_hash_set<StringValue>::iterator _begin;
        typename phmap::flat_hash_set<StringValue>::iterator _end;
        StringValue _value;
    };

    IteratorBase* begin() override {
        return _pool.add(new (std::nothrow) Iterator(_set.begin(), _set.end()));
    }

private:
    phmap::flat_hash_set<StringValue> _set;
    ObjectPool _pool;
};

} // namespace doris
