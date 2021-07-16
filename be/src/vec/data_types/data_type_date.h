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

#include "vec/data_types/data_type_number_base.h"

namespace doris::vectorized {

class DataTypeDate final : public DataTypeNumberBase<Int128> {
public:
    TypeIndex get_type_id() const override { return TypeIndex::Date; }
    const char* get_family_name() const override { return "Date"; }

    bool can_be_used_as_version() const override { return true; }
    bool can_be_inside_nullable() const override { return true; }

    bool equals(const IDataType& rhs) const override;
    std::string to_string(const IColumn& column, size_t row_num) const;
    void to_string(const IColumn &column, size_t row_num, BufferWritable &ostr) const override;

    static void cast_to_date(Int128& x);
};

} // namespace doris::vectorized
