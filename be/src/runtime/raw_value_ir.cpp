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

#include "runtime/raw_value.h"
#include "runtime/string_value.hpp"
#include "util/types.h"

namespace doris {

int RawValue::compare(const void* v1, const void* v2, const TypeDescriptor& type) {
    const StringValue* string_value1;
    const StringValue* string_value2;
    const DateTimeValue* ts_value1;
    const DateTimeValue* ts_value2;
    float f1 = 0;
    float f2 = 0;
    double d1 = 0;
    double d2 = 0;
    int32_t i1;
    int32_t i2;
    int64_t b1;
    int64_t b2;

    if (nullptr == v1 && nullptr == v2) {
        return 0;
    } else if (nullptr == v1 && nullptr != v2) {
        return -1;
    } else if (nullptr != v1 && nullptr == v2) {
        return 1;
    }

    switch (type.type) {
    case TYPE_NULL:
        return 0;

    case TYPE_BOOLEAN:
        return *reinterpret_cast<const bool*>(v1) - *reinterpret_cast<const bool*>(v2);

    case TYPE_TINYINT:
        return *reinterpret_cast<const int8_t*>(v1) - *reinterpret_cast<const int8_t*>(v2);

    case TYPE_SMALLINT:
        return *reinterpret_cast<const int16_t*>(v1) - *reinterpret_cast<const int16_t*>(v2);

    case TYPE_INT:
        i1 = *reinterpret_cast<const int32_t*>(v1);
        i2 = *reinterpret_cast<const int32_t*>(v2);
        return i1 > i2 ? 1 : (i1 < i2 ? -1 : 0);

    case TYPE_BIGINT:
        b1 = *reinterpret_cast<const int64_t*>(v1);
        b2 = *reinterpret_cast<const int64_t*>(v2);
        return b1 > b2 ? 1 : (b1 < b2 ? -1 : 0);

    case TYPE_FLOAT:
        // TODO: can this be faster? (just returning the difference has underflow problems)
        f1 = *reinterpret_cast<const float*>(v1);
        f2 = *reinterpret_cast<const float*>(v2);
        return f1 > f2 ? 1 : (f1 < f2 ? -1 : 0);

    case TYPE_DOUBLE:
        // TODO: can this be faster?
        d1 = *reinterpret_cast<const double*>(v1);
        d2 = *reinterpret_cast<const double*>(v2);
        return d1 > d2 ? 1 : (d1 < d2 ? -1 : 0);

    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_HLL:
    case TYPE_STRING:
        string_value1 = reinterpret_cast<const StringValue*>(v1);
        string_value2 = reinterpret_cast<const StringValue*>(v2);
        return string_value1->compare(*string_value2);

    case TYPE_DATE:
    case TYPE_DATETIME:
        ts_value1 = reinterpret_cast<const DateTimeValue*>(v1);
        ts_value2 = reinterpret_cast<const DateTimeValue*>(v2);
        return *ts_value1 > *ts_value2 ? 1 : (*ts_value1 < *ts_value2 ? -1 : 0);

    case TYPE_DECIMALV2: {
        DecimalV2Value decimal_value1(reinterpret_cast<const PackedInt128*>(v1)->value);
        DecimalV2Value decimal_value2(reinterpret_cast<const PackedInt128*>(v2)->value);
        return (decimal_value1 > decimal_value2) ? 1 : (decimal_value1 < decimal_value2 ? -1 : 0);
    }

    case TYPE_LARGEINT: {
        __int128 large_int_value1 = reinterpret_cast<const PackedInt128*>(v1)->value;
        __int128 large_int_value2 = reinterpret_cast<const PackedInt128*>(v2)->value;
        return large_int_value1 > large_int_value2 ? 1
                                                   : (large_int_value1 < large_int_value2 ? -1 : 0);
    }

    default:
        DCHECK(false) << "invalid type: " << type.type;
        return 0;
    };
}

} // namespace doris
