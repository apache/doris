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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/If.cpp
// and modified by Doris

#include <glog/logging.h>
#include <stddef.h>

#include <boost/iterator/iterator_facade.hpp>

#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

template <typename Type>
struct NumIfImpl {
private:
    using ArrayCond = PaddedPODArray<UInt8>;
    using Array = PaddedPODArray<Type>;
    using ColVecResult = ColumnVector<Type>;
    using ColVecT = ColumnVector<Type>;

    // res[i] = cond[i] ? a[i] : b[i];
    template <bool is_const_a, bool is_const_b>
    static ColumnPtr native_execute(const ArrayCond& cond, const Array& a, const Array& b) {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container& res = col_res->get_data();
        for (size_t i = 0; i < size; ++i) {
            res[i] = cond[i] ? a[index_check_const<is_const_a>(i)]
                             : b[index_check_const<is_const_b>(i)];
        }
        return col_res;
    }

public:
    static const Array& get_data_from_column_const(const ColumnConst* column) {
        return assert_cast<const ColVecT&>(column->get_data_column()).get_data();
    }

    static ColumnPtr execute_if(const ArrayCond& cond, const ColumnPtr& then_col,
                                const ColumnPtr& else_col) {
        if (const auto* col_then = check_and_get_column<ColVecT>(then_col.get())) {
            if (const auto* col_else = check_and_get_column<ColVecT>(else_col.get())) {
                return native_execute<false, false>(cond, col_then->get_data(),
                                                    col_else->get_data());
            } else if (const auto* col_const_else =
                               check_and_get_column_const<ColVecT>(else_col.get())) {
                return native_execute<false, true>(cond, col_then->get_data(),
                                                   get_data_from_column_const(col_const_else));
            }
        } else if (const auto* col_const_then =
                           check_and_get_column_const<ColVecT>(then_col.get())) {
            if (const auto* col_else = check_and_get_column<ColVecT>(else_col.get())) {
                return native_execute<true, false>(cond, get_data_from_column_const(col_const_then),
                                                   col_else->get_data());
            } else if (const auto* col_const_else =
                               check_and_get_column_const<ColVecT>(else_col.get())) {
                return native_execute<true, true>(cond, get_data_from_column_const(col_const_then),
                                                  get_data_from_column_const(col_const_else));
            }
        }
        return nullptr;
    }
};

} // namespace doris::vectorized
