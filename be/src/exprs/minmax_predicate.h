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
#include "runtime/type_limit.h"

namespace doris {
// only used in Runtime Filter
class MinMaxFuncBase {
public:
    virtual void insert(const void* data) = 0;
    virtual void insert_fixed_len(const char* data, const int* offsets, int number) = 0;
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
    ~MinMaxNumFunc() override = default;

    void insert(const void* data) override {
        if (data == nullptr) {
            return;
        }

        T val_data = *reinterpret_cast<const T*>(data);

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

    void insert_fixed_len(const char* data, const int* offsets, int number) override {
        if (!number) {
            return;
        }
        if (_empty) {
            _min = *((T*)data + offsets[0]);
            _max = *((T*)data + offsets[0]);
        }
        for (int i = _empty; i < number; i++) {
            _min = std::min(_min, *((T*)data + offsets[i]));
            _max = std::max(_max, *((T*)data + offsets[i]));
        }
        _empty = false;
    }

    bool find(void* data) override {
        if (data == nullptr) {
            return false;
        }

        T val_data = *reinterpret_cast<T*>(data);
        return val_data >= _min && val_data <= _max;
    }

    Status merge(MinMaxFuncBase* minmax_func, ObjectPool* pool) override {
        if constexpr (std::is_same_v<T, StringRef>) {
            MinMaxNumFunc<T>* other_minmax = static_cast<MinMaxNumFunc<T>*>(minmax_func);

            if (other_minmax->_min < _min) {
                auto& other_min = other_minmax->_min;
                auto str = pool->add(new std::string(other_min.data, other_min.size));
                _min.data = str->data();
                _min.size = str->length();
            }
            if (other_minmax->_max > _max) {
                auto& other_max = other_minmax->_max;
                auto str = pool->add(new std::string(other_max.data, other_max.size));
                _max.data = str->data();
                _max.size = str->length();
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

protected:
    T _max = type_limit<T>::min();
    T _min = type_limit<T>::max();
    // we use _empty to avoid compare twice
    bool _empty = true;
};

template <class T>
class MinNumFunc : public MinMaxNumFunc<T> {
public:
    MinNumFunc() = default;
    ~MinNumFunc() override = default;

    void insert(const void* data) override {
        if (data == nullptr) {
            return;
        }

        T val_data = *reinterpret_cast<const T*>(data);

        if (this->_empty) {
            this->_min = val_data;
            this->_empty = false;
            return;
        }
        if (val_data < this->_min) {
            this->_min = val_data;
        }
    }

    void insert_fixed_len(const char* data, const int* offsets, int number) override {
        if (!number) {
            return;
        }
        if (this->_empty) {
            this->_min = *((T*)data + offsets[0]);
        }
        for (int i = this->_empty; i < number; i++) {
            this->_min = std::min(this->_min, *((T*)data + offsets[i]));
        }
        this->_empty = false;
    }

    bool find(void* data) override {
        if (data == nullptr) {
            return false;
        }

        T val_data = *reinterpret_cast<T*>(data);
        return val_data >= this->_min;
    }

    Status merge(MinMaxFuncBase* minmax_func, ObjectPool* pool) override {
        if constexpr (std::is_same_v<T, StringRef>) {
            MinNumFunc<T>* other_minmax = static_cast<MinNumFunc<T>*>(minmax_func);
            if (other_minmax->_min < this->_min) {
                auto& other_min = other_minmax->_min;
                auto str = pool->add(new std::string(other_min.data, other_min.size));
                this->_min.data = str->data();
                this->_min.size = str->length();
            }
        } else {
            MinNumFunc<T>* other_minmax = static_cast<MinNumFunc<T>*>(minmax_func);
            if (other_minmax->_min < this->_min) {
                this->_min = other_minmax->_min;
            }
        }

        return Status::OK();
    }

    //min filter the max is useless, so return nullptr directly
    void* get_max() override { return nullptr; }

    Status assign(void* min_data, void* max_data) override {
        this->_min = *(T*)min_data;
        return Status::OK();
    }
};

template <class T>
class MaxNumFunc : public MinMaxNumFunc<T> {
public:
    MaxNumFunc() = default;
    ~MaxNumFunc() override = default;

    void insert(const void* data) override {
        if (data == nullptr) {
            return;
        }

        T val_data = *reinterpret_cast<const T*>(data);

        if (this->_empty) {
            this->_max = val_data;
            this->_empty = false;
            return;
        }
        if (val_data > this->_max) {
            this->_max = val_data;
        }
    }

    void insert_fixed_len(const char* data, const int* offsets, int number) override {
        if (!number) {
            return;
        }
        if (this->_empty) {
            this->_max = *((T*)data + offsets[0]);
        }
        for (int i = this->_empty; i < number; i++) {
            this->_max = std::max(this->_max, *((T*)data + offsets[i]));
        }
        this->_empty = false;
    }

    bool find(void* data) override {
        if (data == nullptr) {
            return false;
        }

        T val_data = *reinterpret_cast<T*>(data);
        return val_data <= this->_max;
    }

    Status merge(MinMaxFuncBase* minmax_func, ObjectPool* pool) override {
        if constexpr (std::is_same_v<T, StringRef>) {
            MinMaxNumFunc<T>* other_minmax = static_cast<MinMaxNumFunc<T>*>(minmax_func);

            if (other_minmax->_max > this->_max) {
                auto& other_max = other_minmax->_max;
                auto str = pool->add(new std::string(other_max.data, other_max.size));
                this->_max.data = str->data();
                this->_max.size = str->length();
            }
        } else {
            MinMaxNumFunc<T>* other_minmax = static_cast<MinMaxNumFunc<T>*>(minmax_func);
            if (other_minmax->_max > this->_max) {
                this->_max = other_minmax->_max;
            }
        }

        return Status::OK();
    }

    //max filter the min is useless, so return nullptr directly
    void* get_min() override { return nullptr; }

    Status assign(void* min_data, void* max_data) override {
        this->_max = *(T*)max_data;
        return Status::OK();
    }
};

} // namespace doris
