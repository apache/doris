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

#include <type_traits>

#include "common/object_pool.h"
#include "runtime/type_limit.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/common/assert_cast.h"

namespace doris {
// only used in Runtime Filter
class MinMaxFuncBase {
public:
    virtual void insert(const void* data) = 0;
    virtual void insert_fixed_len(const vectorized::ColumnPtr& column, size_t start) = 0;
    virtual bool find(void* data) = 0;
    virtual void* get_max() = 0;
    virtual void* get_min() = 0;
    // assign minmax data
    virtual Status assign(void* min_data, void* max_data) = 0;
    // merge from other minmax_func
    virtual Status merge(MinMaxFuncBase* minmax_func, ObjectPool* pool) = 0;
    virtual ~MinMaxFuncBase() = default;
};

template <class T, bool NeedMax = true, bool NeedMin = true>
class MinMaxNumFunc : public MinMaxFuncBase {
public:
    MinMaxNumFunc() = default;
    ~MinMaxNumFunc() override = default;

    void insert(const void* data) override {
        if (data == nullptr) {
            return;
        }

        T val_data = *reinterpret_cast<const T*>(data);

        if constexpr (NeedMin) {
            if (val_data < _min) {
                _min = val_data;
            }
        }

        if constexpr (NeedMax) {
            if (val_data > _max) {
                _max = val_data;
            }
        }
    }

    void insert_fixed_len(const vectorized::ColumnPtr& column, size_t start) override {
        if (column->empty()) {
            return;
        }
        if (column->is_nullable()) {
            const auto* nullable = assert_cast<const vectorized::ColumnNullable*>(column.get());
            const auto& col = nullable->get_nested_column();
            const auto& nullmap =
                    assert_cast<const vectorized::ColumnUInt8&>(nullable->get_null_map_column())
                            .get_data();

            if constexpr (std::is_same_v<T, StringRef>) {
                const auto& column_string = assert_cast<const vectorized::ColumnString&>(col);
                for (size_t i = start; i < column->size(); i++) {
                    if (!nullmap[i]) {
                        if constexpr (NeedMin) {
                            _min = std::min(_min, column_string.get_data_at(i));
                        }
                        if constexpr (NeedMax) {
                            _max = std::max(_max, column_string.get_data_at(i));
                        }
                    }
                }
            } else {
                const T* data = (T*)col.get_raw_data().data;
                for (size_t i = start; i < column->size(); i++) {
                    if (!nullmap[i]) {
                        if constexpr (NeedMin) {
                            _min = std::min(_min, *(data + i));
                        }
                        if constexpr (NeedMax) {
                            _max = std::max(_max, *(data + i));
                        }
                    }
                }
            }
        } else {
            if constexpr (std::is_same_v<T, StringRef>) {
                const auto& column_string = assert_cast<const vectorized::ColumnString&>(*column);
                for (size_t i = start; i < column->size(); i++) {
                    if constexpr (NeedMin) {
                        _min = std::min(_min, column_string.get_data_at(i));
                    }
                    if constexpr (NeedMax) {
                        _max = std::max(_max, column_string.get_data_at(i));
                    }
                }
            } else {
                const T* data = (T*)column->get_raw_data().data;
                for (size_t i = start; i < column->size(); i++) {
                    if constexpr (NeedMin) {
                        _min = std::min(_min, *(data + i));
                    }
                    if constexpr (NeedMax) {
                        _max = std::max(_max, *(data + i));
                    }
                }
            }
        }
    }

    bool find(void* data) override {
        if (data == nullptr) {
            return false;
        }

        T val_data = *reinterpret_cast<T*>(data);
        if constexpr (NeedMin) {
            if (val_data < _min) {
                return false;
            }
        }
        if constexpr (NeedMax) {
            if (val_data > _max) {
                return false;
            }
        }
        return true;
    }

    Status merge(MinMaxFuncBase* minmax_func, ObjectPool* pool) override {
        if constexpr (std::is_same_v<T, StringRef>) {
            auto* other_minmax = static_cast<MinMaxNumFunc<T>*>(minmax_func);
            if constexpr (NeedMin) {
                if (other_minmax->_min < _min) {
                    auto& other_min = other_minmax->_min;
                    auto* str = pool->add(new std::string(other_min.data, other_min.size));
                    _min.data = str->data();
                    _min.size = str->length();
                }
            }
            if constexpr (NeedMax) {
                if (other_minmax->_max > _max) {
                    auto& other_max = other_minmax->_max;
                    auto* str = pool->add(new std::string(other_max.data, other_max.size));
                    _max.data = str->data();
                    _max.size = str->length();
                }
            }
        } else {
            auto* other_minmax = static_cast<MinMaxNumFunc<T>*>(minmax_func);
            if constexpr (NeedMin) {
                if (other_minmax->_min < _min) {
                    _min = other_minmax->_min;
                }
            }
            if constexpr (NeedMax) {
                if (other_minmax->_max > _max) {
                    _max = other_minmax->_max;
                }
            }
        }

        return Status::OK();
    }

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
};

template <class T>
using MinNumFunc = MinMaxNumFunc<T, false, true>;

template <class T>
using MaxNumFunc = MinMaxNumFunc<T, true, false>;

} // namespace doris
