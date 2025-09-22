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

#include "data_type_number.h"

#include <fmt/format.h>

#include "util/mysql_global.h"
#include "util/to_string.h"
#include "vec/functions/cast/cast_to_string.h"

namespace doris::vectorized {

/// TODO: Currently, only integers have been considered; other types will be added later.
template <PrimitiveType T>
size_t DataTypeNumber<T>::number_length() const {
    // The maximum number of decimal digits for an integer represented by n bytes:
    // 1. Each byte = 8 bits, so n bytes = 8n bits.
    // 2. Maximum value of an 8n-bit integer = 2^(8n) - 1.
    // 3. Number of decimal digits d = floor(log10(2^(8n) - 1)) + 1.
    // 4. Approximation: log10(2^(8n)) ≈ 8n * log10(2).
    // 5. log10(2) ≈ 0.30103, so 8n * log10(2) ≈ 2.40824n.
    // 6. Therefore, d ≈ floor(2.408 * n) + 1.
    return size_t(2.408 * sizeof(typename PrimitiveTypeTraits<T>::ColumnItemType)) + 1;
}

template <PrimitiveType T>
void DataTypeNumber<T>::push_number(
        ColumnString::Chars& chars,
        const typename PrimitiveTypeTraits<T>::ColumnItemType& num) const {
    CastToString::push_number(num, chars);
}

template class DataTypeNumber<TYPE_BOOLEAN>;
template class DataTypeNumber<TYPE_TINYINT>;
template class DataTypeNumber<TYPE_SMALLINT>;
template class DataTypeNumber<TYPE_INT>;
template class DataTypeNumber<TYPE_BIGINT>;
template class DataTypeNumber<TYPE_LARGEINT>;
template class DataTypeNumber<TYPE_FLOAT>;
template class DataTypeNumber<TYPE_DOUBLE>;

} // namespace doris::vectorized
