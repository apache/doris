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

#include "vec/functions/array/function_array_binary.h"
#include "vec/functions/array/function_array_set.h"

#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

struct NameArrayIntersect {
    static constexpr auto name = "array_intersect";
};

template <typename ColumnType>
struct IntersectAction {
    using ElementType = typename ColumnType::value_type;
    using ElementNativeType = typename NativeType<ElementType>::Type;
    using Set = HashSetWithStackMemory<ElementNativeType, DefaultHash<ElementNativeType>, 4>;
    static constexpr auto apply_left_first = false;
    Set set;
    Set result_set;
    bool null_flag = false;
    bool result_null_flag = false;

    bool apply_null_left() {
        if (!result_null_flag) {
            result_null_flag = true;
            if (null_flag) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    bool apply_left(const ElementType* elem) {
        if (set.find(*elem) && !result_set.find(*elem)) {
            result_set.insert(*elem);
            return true;
        }
        return false;
    }

    bool apply_null_right() {
        if (!null_flag) {
            null_flag = true;
        }
        return false;
    }

    bool apply_right(const ElementType* elem) {
        if (!set.find(*elem)) {
            set.insert(*elem);
        }
        return false;
    }
};

template <>
struct IntersectAction<ColumnString> {
    using Set = HashSetWithStackMemory<StringRef, DefaultHash<StringRef>, 4>;
    static constexpr auto apply_left_first = false;
    Set set;
    Set result_set;
    bool null_flag = false;

    bool apply_null_left() {
        if (!null_flag) {
            null_flag = true;
            return true;
        } else {
            return false;
        }
    }

    bool apply_left(const StringRef& elem) {
        if (set.find(elem) && !result_set.find(elem)) {
            result_set.insert(elem);
            return true;
        }
        return false;
    }

    bool apply_null_right() {
        if (!null_flag) {
            null_flag = true;
        }
        return false;
    }

    bool apply_right(const StringRef& elem) {
        if (!set.find(elem)) {
            set.insert(elem);
        }
        return false;
    }
};

using FunctionArrayIntersect =
        FunctionArrayBinary<ArraySetImpl<SetOperation::INTERSECT>, NameArrayIntersect>;

void register_function_array_intersect(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionArrayIntersect>();
}

} // namespace doris::vectorized

