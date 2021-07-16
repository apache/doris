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

#include "vec/columns/column.h"

namespace doris::vectorized {

/** Allows to access internal array of ColumnVector or ColumnFixedString without cast to concrete type.
  * We will inherit ColumnVector and ColumnFixedString from this class instead of IColumn.
  * Assumes data layout of ColumnVector, ColumnFixedString and PODArray.
  *
  * Why it is needed?
  *
  * There are some algorithms that specialize on the size of data type but doesn't care about concrete type.
  * The same specialization may work for UInt64, Int64, Float64, FixedString(8), if it only does byte moving and hashing.
  * To avoid code bloat and compile time increase, we can use single template instantiation for these cases
  *  and just static_cast pointer to some single column type (e. g. ColumnUInt64) assuming that all types have identical memory layout.
  *
  * But this static_cast (downcast to unrelated type) is illegal according to the C++ standard and UBSan warns about it.
  * To allow functional tests to work under UBSan we have to separate some base class that will present the memory layout in explicit way,
  *  and we will do static_cast to this class.
  */
class ColumnVectorHelper : public IColumn {
public:
    template <size_t ELEMENT_SIZE>
    const char* get_raw_data_begin() const {
        return reinterpret_cast<const PODArrayBase<ELEMENT_SIZE, 4096, Allocator<false>, 15, 16>*>(
                       reinterpret_cast<const char*>(this) + sizeof(*this))
                ->raw_data();
    }

    template <size_t ELEMENT_SIZE>
    void insert_raw_data(const char* ptr) {
        return reinterpret_cast<PODArrayBase<ELEMENT_SIZE, 4096, Allocator<false>, 15, 16>*>(
                       reinterpret_cast<char*>(this) + sizeof(*this))
                ->push_back_raw(ptr);
    }
};

} // namespace doris::vectorized
