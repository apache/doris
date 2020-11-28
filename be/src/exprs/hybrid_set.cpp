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

#include "exprs/hybrid_set.h"

namespace doris {

HybridSetBase* HybridSetBase::create_set(PrimitiveType type) {
    switch (type) {
    case TYPE_BOOLEAN:
        return new (std::nothrow) HybridSet<bool>();

    case TYPE_TINYINT:
        return new (std::nothrow) HybridSet<int8_t>();

    case TYPE_SMALLINT:
        return new (std::nothrow) HybridSet<int16_t>();

    case TYPE_INT:
        return new (std::nothrow) HybridSet<int32_t>();

    case TYPE_BIGINT:
        return new (std::nothrow) HybridSet<int64_t>();

    case TYPE_FLOAT:
        return new (std::nothrow) HybridSet<float>();

    case TYPE_DOUBLE:
        return new (std::nothrow) HybridSet<double>();

    case TYPE_DATE:
    case TYPE_DATETIME:
        return new (std::nothrow) HybridSet<DateTimeValue>();

    case TYPE_DECIMAL:
        return new (std::nothrow) HybridSet<DecimalValue>();

    case TYPE_DECIMALV2:
        return new (std::nothrow) HybridSet<DecimalV2Value>();

    case TYPE_LARGEINT:
        return new (std::nothrow) HybridSet<__int128>();

    case TYPE_CHAR:
    case TYPE_VARCHAR:
        return new (std::nothrow) StringValueSet();

    default:
        return NULL;
    }

    return NULL;
}

} // namespace doris

/* vim: set ts=4 sw=4 sts=4 tw=100 */
