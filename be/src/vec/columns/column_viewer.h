
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

#include "runtime/define_primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nullable.h"
#include "vec/core/field.h"
namespace doris::vectorized {

// ColumnViewer does not own any data. When using it, ensure that ColumnViewer's lifetime does not exceed that of the passed-in ColumnPtr
// Primarily used in non-templated scenarios, can automatically handle const and nullable columns
// Only supports primitive types (supported types are similar to those supported by dispatch_switch_scalar: SCALAR = INT | FLOAT | DECIMAL | DATETIME | IP)
template <PrimitiveType PType>
class ColumnViewer {
public:
    using value_type = typename PrimitiveTypeTraits<PType>::ColumnItemType;
    using ColumnType = typename PrimitiveTypeTraits<PType>::ColumnType;

    explicit ColumnViewer(const ColumnPtr& column)
            : _const_mask(const_mask(column)), _size(column->size()) {
        auto [not_const_column, is_const] = unpack_if_const(column);
        if (not_const_column->is_nullable()) {
            const auto& nullable_column = assert_cast<const ColumnNullable&>(*not_const_column);
            _null_map = &nullable_column.get_null_map_data();
            auto nested_column = nullable_column.get_nested_column_ptr();
            _data = assert_cast<const ColumnType&>(*nested_column).get_data().data();
        } else {
            _null_map = nullptr;
            _data = assert_cast<const ColumnType&>(*not_const_column).get_data().data();
        }
    }

    const value_type& value(const size_t idx) const { return _data[idx & _const_mask]; }

    bool is_null(const size_t idx) const {
        return _null_map ? (*_null_map)[idx & _const_mask] : false;
    }

    size_t size() const { return _size; }

private:
    static size_t const_mask(const ColumnPtr& column) {
        if (is_column_const(*column)) {
            return 0;
        } else {
            return -1;
        }
    }

    const size_t _const_mask;
    const size_t _size;
    // if _null_map == nullptr, means no null column
    const NullMap* _null_map;
    // raw pointer
    const value_type* _data;
};

} // namespace doris::vectorized
