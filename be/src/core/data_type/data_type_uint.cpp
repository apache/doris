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

#include "core/data_type/data_type_uint.h"

#include <typeinfo>

#include "common/exception.h"
#include "core/column/column_vector.h"

namespace doris {

template <PrimitiveType T>
const std::string DataTypeUInt<T>::get_family_name() const {
    return type_to_string(T);
}

template <PrimitiveType T>
Field DataTypeUInt<T>::get_field(const TExprNode&) const {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "SQL literals are not supported for storage-only type {}", get_name());
}

template <PrimitiveType T>
bool DataTypeUInt<T>::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

template <PrimitiveType T>
int64_t DataTypeUInt<T>::get_uncompressed_serialized_bytes(const IColumn&, int) const {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "Block serialization is not supported for storage-only type {}", get_name());
}

template <PrimitiveType T>
char* DataTypeUInt<T>::serialize(const IColumn&, char*, int) const {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "Block serialization is not supported for storage-only type {}", get_name());
}

template <PrimitiveType T>
const char* DataTypeUInt<T>::deserialize(const char*, MutableColumnPtr*, int) const {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "Block deserialization is not supported for storage-only type {}", get_name());
}

template <PrimitiveType T>
MutableColumnPtr DataTypeUInt<T>::create_column() const {
    return ColumnType::create();
}

template <PrimitiveType T>
Status DataTypeUInt<T>::check_column(const IColumn& column) const {
    return check_column_non_nested_type<ColumnType>(column);
}

template <PrimitiveType T>
DataTypeSerDeSPtr DataTypeUInt<T>::get_serde(int) const {
    throw Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                    "SerDe is not supported for storage-only type {}", get_name());
}

template class DataTypeUInt<TYPE_UINT32>;
template class DataTypeUInt<TYPE_UINT64>;

} // namespace doris
