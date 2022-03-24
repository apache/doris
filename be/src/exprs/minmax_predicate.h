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

#include "common/object_pool.h"
#include "runtime/primitive_type.h"
#include "runtime/type_limit.h"

namespace doris {
// only used in Runtime Filter
class MinMaxFuncBase {
public:
    virtual void insert(const void* data) = 0;
    virtual bool find(void* data) = 0;
    virtual bool is_empty() = 0;
    virtual void* get_max() = 0;
    virtual void* get_min() = 0;
    // assign minmax data
    virtual Status assign(void* min_data, void* max_data) = 0;
    // merge from other minmax_func
    virtual Status merge(MinMaxFuncBase* minmax_func, ObjectPool* pool) = 0;
    virtual ~MinMaxFuncBase() = default;
};

template <class T>
class MinMaxNumFunc : public MinMaxFuncBase {
public:
    MinMaxNumFunc() = default;
    ~MinMaxNumFunc() = default;

    void insert(const void* data) override {
        if (data == nullptr) {
            return;
        }

        T val_data;
        if constexpr (sizeof(T) >= sizeof(int128_t)) {
            // use dereference operator on unalign address maybe lead segmentation fault
            memcpy(&val_data, data, sizeof(T));
        } else {
            val_data = *reinterpret_cast<const T*>(data);
        }

        if (_empty) {
            _min = val_data;
            _max = val_data;
            _empty = false;
            return;
        }
        if (val_data < _min) {
            _min = val_data;
        } else if (val_data > _max) {
            _max = val_data;
        }
    }

    bool find(void* data) override {
        if (data == nullptr) {
            return false;
        }

        T val_data = *reinterpret_cast<T*>(data);
        return val_data >= _min && val_data <= _max;
    }

    Status merge(MinMaxFuncBase* minmax_func, ObjectPool* pool) override {
        if constexpr (std::is_same_v<T, StringValue>) {
            MinMaxNumFunc<T>* other_minmax = static_cast<MinMaxNumFunc<T>*>(minmax_func);

            if (other_minmax->_min < _min) {
                auto& other_min = other_minmax->_min;
                auto str = pool->add(new std::string(other_min.ptr, other_min.len));
                _min.ptr = str->data();
                _min.len = str->length();
            }
            if (other_minmax->_max > _max) {
                auto& other_max = other_minmax->_max;
                auto str = pool->add(new std::string(other_max.ptr, other_max.len));
                _max.ptr = str->data();
                _max.len = str->length();
            }
        } else {
            MinMaxNumFunc<T>* other_minmax = static_cast<MinMaxNumFunc<T>*>(minmax_func);
            if (other_minmax->_min < _min) {
                _min = other_minmax->_min;
            }
            if (other_minmax->_max > _max) {
                _max = other_minmax->_max;
            }
        }

        return Status::OK();
    }

    bool is_empty() override { return _empty; }

    void* get_max() override { return &_max; }

    void* get_min() override { return &_min; }

    Status assign(void* min_data, void* max_data) override {
        _min = *(T*)min_data;
        _max = *(T*)max_data;
        return Status::OK();
    }

private:
    T _max = type_limit<T>::min();
    T _min = type_limit<T>::max();
    // we use _empty to avoid compare twice
    bool _empty = true;
};

} // namespace doris
