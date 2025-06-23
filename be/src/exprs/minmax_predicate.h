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

#include <gen_cpp/internal_service.pb.h>

#include "exprs/filter_base.h"
#include "runtime/type_limit.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"

namespace doris {

// only used in Runtime Filter
class MinMaxFuncBase : public FilterBase {
public:
    MinMaxFuncBase(bool null_aware) : FilterBase(null_aware) {}
    virtual void insert_fixed_len(const vectorized::ColumnPtr& column, size_t start) = 0;
    virtual void* get_max() = 0;
    virtual void* get_min() = 0;
    // assign minmax data
    virtual Status assign(void* min_data, void* max_data) = 0;
    // merge from other minmax_func
    virtual Status merge(MinMaxFuncBase* minmax_func) = 0;
    virtual ~MinMaxFuncBase() = default;

    virtual void to_pb(PMinMaxFilter* filter) = 0;
};

template <class T, bool NeedMax = true, bool NeedMin = true>
class MinMaxNumFunc : public MinMaxFuncBase {
public:
    static constexpr bool IsStringValue = std::is_same_v<T, std::string>;
    MinMaxNumFunc(bool null_aware) : MinMaxFuncBase(null_aware) {}
    ~MinMaxNumFunc() override = default;

    void insert_fixed_len(const vectorized::ColumnPtr& column, size_t start) override {
        if (column->is_nullable()) {
            const auto* nullable = assert_cast<const vectorized::ColumnNullable*>(column.get());
            const auto& col = nullable->get_nested_column_ptr();
            const auto& nullmap = nullable->get_null_map_data();
            if (nullable->has_null()) {
                _update_batch(col, nullmap, start);
                _contain_null = true;
            } else {
                _update_batch(col, start);
            }
        } else {
            _update_batch(column, start);
        }
    }

    Status merge(MinMaxFuncBase* minmax_func) override {
        auto* other_minmax = static_cast<MinMaxNumFunc<T>*>(minmax_func);
        if constexpr (NeedMin) {
            if constexpr (IsStringValue) {
                if (other_minmax->_min < _min || !_min_value_set) {
                    _min = other_minmax->_min;
                    _min_value_set = true;
                }
            } else if (other_minmax->_min < _min) {
                _min = other_minmax->_min;
            }
        }
        if constexpr (NeedMax) {
            if (other_minmax->_max > _max) {
                _max = other_minmax->_max;
            }
        }

        _contain_null |= minmax_func->contain_null();
        return Status::OK();
    }

    void* get_max() override { return &_max; }

    void* get_min() override { return &_min; }

    Status assign(void* min_data, void* max_data) override {
        if constexpr (IsStringValue) {
            _min_value_set = true;
        }
        _min = *(T*)min_data;
        _max = *(T*)max_data;
        return Status::OK();
    }

    void to_pb(PMinMaxFilter* filter) override { _set_pb(filter, get_convertor<T>()); }

private:
    void _set_pb(PMinMaxFilter* filter, auto f) {
        if constexpr (NeedMin) {
            f(filter->mutable_min_val(), _min);
        }
        if constexpr (NeedMax) {
            f(filter->mutable_max_val(), _max);
        }
    }

    void _update_batch_string(const auto& column_string, const uint8_t* __restrict nullmap,
                              size_t start, size_t size) {
        for (size_t i = start; i < size; i++) {
            if (nullmap == nullptr || !nullmap[i]) {
                if constexpr (NeedMin) {
                    if (column_string.get_data_at(i) < StringRef(_min) || !_min_value_set) {
                        _min = column_string.get_data_at(i).to_string();
                        _min_value_set = true;
                    }
                }
                if constexpr (NeedMax) {
                    if (column_string.get_data_at(i) > StringRef(_max)) {
                        _max = column_string.get_data_at(i).to_string();
                    }
                }
            }
        }
    }

    void _update_batch(const vectorized::ColumnPtr& column, size_t start) {
        const auto size = column->size();
        if constexpr (std::is_same_v<T, std::string>) {
            if (column->is_column_string64()) {
                _update_batch_string(assert_cast<const vectorized::ColumnString64&>(*column),
                                     nullptr, start, size);
            } else {
                _update_batch_string(assert_cast<const vectorized::ColumnString&>(*column), nullptr,
                                     start, size);
            }
        } else {
            const T* data = (T*)column->get_raw_data().data;
            for (size_t i = start; i < size; i++) {
                if constexpr (NeedMin) {
                    _min = std::min(_min, *(data + i));
                }
                if constexpr (NeedMax) {
                    _max = std::max(_max, *(data + i));
                }
            }
        }
    }

    void _update_batch(const vectorized::ColumnPtr& column, const vectorized::NullMap& nullmap,
                       size_t start) {
        const auto size = column->size();
        if constexpr (std::is_same_v<T, std::string>) {
            if (column->is_column_string64()) {
                _update_batch_string(assert_cast<const vectorized::ColumnString64&>(*column),
                                     nullmap.data(), start, size);
            } else {
                _update_batch_string(assert_cast<const vectorized::ColumnString&>(*column),
                                     nullmap.data(), start, size);
            }
        } else {
            const T* data = (T*)column->get_raw_data().data;
            for (size_t i = start; i < size; i++) {
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
    }

    T _max = type_limit<T>::min();
    T _min = type_limit<T>::max();

    bool _min_value_set = false;
};

template <class T>
using MinNumFunc = MinMaxNumFunc<T, false, true>;

template <class T>
using MaxNumFunc = MinMaxNumFunc<T, true, false>;

} // namespace doris
