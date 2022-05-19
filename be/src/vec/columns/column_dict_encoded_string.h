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

#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"

using DictId = int32_t;

namespace doris::vectorized {

template <typename T>
class ColumnDictEncodedString final : public ColumnVector<T> {
private:
    friend class COWHelper<IColumn, ColumnDictEncodedString<T>>;
    ColumnDictEncodedString() {}
    ColumnDictEncodedString(DictId dict_id) : _dict_id(dict_id) {}

public:
    template <typename... Args>
    static MutableColumnPtr create(Args&&... args) {
        return COWHelper<IColumn, ColumnDictEncodedString<T>>::create(std::forward<Args>(args)...);
    }

    void set_dict_id(DictId dict_id) { _dict_id = dict_id; }

    DictId get_dict_id() const { return _dict_id; }

private:
    DictId _dict_id = -1;
};

using ColumnDictEncodedStringUInt8 = ColumnDictEncodedString<UInt8>;
using ColumnDictEncodedStringUInt16 = ColumnDictEncodedString<UInt16>;
using ColumnDictEncodedStringUInt32 = ColumnDictEncodedString<UInt32>;

} // namespace doris::vectorized