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

    static ColumnPtr vector_vector(const ArrayCond& cond, const Array& a, const Array& b) {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container& res = col_res->get_data();

        for (size_t i = 0; i < size; ++i) {
            res[i] = cond[i] ? a[i] : b[i];
        }
        return col_res;
    }

    static ColumnPtr vector_constant(const ArrayCond& cond, const Array& a, Type b) {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container& res = col_res->get_data();

        for (size_t i = 0; i < size; ++i) {
            res[i] = cond[i] ? a[i] : b;
        }
        return col_res;
    }

    static ColumnPtr constant_vector(const ArrayCond& cond, Type a, const Array& b) {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container& res = col_res->get_data();

        for (size_t i = 0; i < size; ++i) {
            res[i] = cond[i] ? a : b[i];
        }
        return col_res;
    }

    static ColumnPtr constant_constant(const ArrayCond& cond, Type a, Type b) {
        size_t size = cond.size();
        auto col_res = ColVecResult::create(size);
        typename ColVecResult::Container& res = col_res->get_data();

        for (size_t i = 0; i < size; ++i) {
            res[i] = cond[i] ? a : b;
        }
        return col_res;
    }

public:
    static ColumnPtr execute_if(const ArrayCond& cond, const ColumnPtr& then_col,
                                const ColumnPtr& else_col) {
        if (const auto* col_then = check_and_get_column<ColVecT>(then_col.get())) {
            if (const auto* col_else = check_and_get_column<ColVecT>(else_col.get())) {
                return vector_vector(cond, col_then->get_data(), col_else->get_data());
            } else if (const auto* col_const_else =
                               check_and_get_column_const<ColVecT>(else_col.get())) {
                return vector_constant(cond, col_then->get_data(),
                                       col_const_else->template get_value<Type>());
            }
        } else if (const auto* col_const_then =
                           check_and_get_column_const<ColVecT>(then_col.get())) {
            if (const auto* col_else = check_and_get_column<ColVecT>(else_col.get())) {
                return constant_vector(cond, col_const_then->template get_value<Type>(),
                                       col_else->get_data());
            } else if (const auto* col_const_else =
                               check_and_get_column_const<ColVecT>(else_col.get())) {
                return constant_constant(cond, col_const_then->template get_value<Type>(),
                                         col_const_else->template get_value<Type>());
            }
        }
        return nullptr;
    }
};

} // namespace doris::vectorized
