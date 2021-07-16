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

#include "vec/data_types/data_type_nothing.h"

#include "gen_cpp/data.pb.h"
#include "vec/columns/column_nothing.h"
#include "vec/common/typeid_cast.h"

namespace doris::vectorized {

MutableColumnPtr DataTypeNothing::create_column() const {
    return ColumnNothing::create(0);
}

size_t DataTypeNothing::serialize(const IColumn&, PColumn* pcolumn) const {
    return 0;
}

void DataTypeNothing::deserialize(const PColumn& pcolumn, IColumn* column) const {}
bool DataTypeNothing::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

} // namespace doris::vectorized
