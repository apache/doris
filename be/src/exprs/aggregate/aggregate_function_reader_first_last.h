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

#include "core/column/column_nullable.h"
#include "exprs/aggregate/single_value_data.h"

namespace doris {

template <bool result_is_nullable, bool arg_is_nullable>
struct ReaderFirstAndLastData {
public:
    static constexpr bool nullable = arg_is_nullable;
    static constexpr bool result_nullable = result_is_nullable;

    void reset() {
        _data_value.reset();
        _has_value = false;
    }

    void insert_result_into(IColumn& to) const {
        if constexpr (result_is_nullable) {
            if (!_data_value.has()) {
                auto& col = assert_cast<ColumnNullable&, TypeCheckOnRelease::DISABLE>(to);
                col.insert_default();
            } else {
                auto& col = assert_cast<ColumnNullable&, TypeCheckOnRelease::DISABLE>(to);
                col.get_null_map_data().push_back(0);
                _data_value.insert_result_into(col.get_nested_column());
            }
        } else {
            _data_value.insert_result_into(to);
        }
    }

    void set_value(const IColumn** columns, size_t pos) {
        if constexpr (arg_is_nullable) {
            const auto& nullable_column =
                    assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*columns[0]);
            if (nullable_column.is_null_at(pos)) {
                _data_value.reset();
            } else {
                _data_value.set(nullable_column.get_nested_column(), pos);
            }
        } else {
            _data_value.set(*columns[0], pos);
        }
        _has_value = true;
    }

    bool has_set_value() const { return _has_value; }

    bool is_null() const { return !_data_value.has(); }

protected:
    SingleValueDataColumn _data_value;
    bool _has_value = false;
};

} // namespace doris
