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

#include "core/column/column_nullable.h"

namespace doris {

template <bool arg_is_nullable>
struct Value {
public:
    bool is_null() const {
        if (_ptr == nullptr) {
            return true;
        }
        if constexpr (arg_is_nullable) {
            return assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(_ptr)
                    ->is_null_at(_offset);
        }
        return false;
    }

    template <typename ColVecType>
    void insert_into(IColumn& to) const {
        if constexpr (arg_is_nullable) {
            const auto* col = assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(_ptr);
            assert_cast<ColVecType&, TypeCheckOnRelease::DISABLE>(to).insert_from(
                    col->get_nested_column(), _offset);
        } else {
            assert_cast<ColVecType&, TypeCheckOnRelease::DISABLE>(to).insert_from(*_ptr, _offset);
        }
    }

    // Non-template version: virtual dispatch on IColumn::insert_from.
    // Used by window path where devirtualization is not beneficial.
    void insert_into(IColumn& to) const {
        if constexpr (arg_is_nullable) {
            const auto* col = assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(_ptr);
            to.insert_from(col->get_nested_column(), _offset);
        } else {
            to.insert_from(*_ptr, _offset);
        }
    }

    void set_value(const IColumn* column, size_t row) {
        _ptr = column;
        _offset = row;
    }

    void reset() {
        _ptr = nullptr;
        _offset = 0;
    }

protected:
    const IColumn* _ptr = nullptr;
    size_t _offset = 0;
};

template <bool arg_is_nullable>
struct CopiedValue : public Value<arg_is_nullable> {
public:
    template <typename ColVecType>
    void insert_into(IColumn& to) const {
        assert_cast<ColVecType&, TypeCheckOnRelease::DISABLE>(to).insert(_copied_value);
    }

    bool is_null() const { return this->_ptr == nullptr; }

    template <typename ColVecType>
    void set_value(const IColumn* column, size_t row) {
        // here _ptr, maybe null at row, so call reset to set nullptr
        // But we will use is_null() check first, others have set _ptr column to a meaningless address
        // because the address have meaningless, only need it to check is nullptr
        this->_ptr = (IColumn*)0x00000001;
        if constexpr (arg_is_nullable) {
            const auto* col =
                    assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(column);
            if (col->is_null_at(row)) {
                this->reset();
                return;
            } else {
                auto& nested_col = assert_cast<const ColVecType&, TypeCheckOnRelease::DISABLE>(
                        col->get_nested_column());
                nested_col.get(row, _copied_value);
            }
        } else {
            column->get(row, _copied_value);
        }
    }

private:
    Field _copied_value;
};

template <typename ColVecType, bool result_is_nullable, bool arg_is_nullable, bool is_copy>
struct ReaderFirstAndLastData {
public:
    using StoreType =
            std::conditional_t<is_copy, CopiedValue<arg_is_nullable>, Value<arg_is_nullable>>;
    static constexpr bool nullable = arg_is_nullable;
    static constexpr bool result_nullable = result_is_nullable;

    void reset() {
        _data_value.reset();
        _has_value = false;
    }

    void insert_result_into(IColumn& to) const {
        if constexpr (result_is_nullable) {
            if (_data_value.is_null()) { //_ptr == nullptr || null data at row
                auto& col = assert_cast<ColumnNullable&>(to);
                col.insert_default();
            } else {
                auto& col = assert_cast<ColumnNullable&>(to);
                col.get_null_map_data().push_back(0);
                if constexpr (!std::is_same_v<ColVecType, void>) {
                    _data_value.template insert_into<ColVecType>(col.get_nested_column());
                } else {
                    _data_value.insert_into(col.get_nested_column());
                }
            }
        } else {
            if constexpr (!std::is_same_v<ColVecType, void>) {
                _data_value.template insert_into<ColVecType>(to);
            } else {
                _data_value.insert_into(to);
            }
        }
    }

    // here not check the columns[0] is null at the row,
    // but it is need to check in other
    void set_value(const IColumn** columns, size_t pos) {
        if constexpr (is_copy) {
            _data_value.template set_value<ColVecType>(columns[0], pos);
        } else {
            _data_value.set_value(columns[0], pos);
        }
        _has_value = true;
    }

    bool has_set_value() { return _has_value; }

    bool is_null() { return _data_value.is_null(); }

protected:
    StoreType _data_value;
    bool _has_value = false;
};

} // namespace doris
