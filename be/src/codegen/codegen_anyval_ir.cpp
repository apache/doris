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

#ifdef IR_COMPILE

#include "runtime/string_value.hpp"
#include "runtime/datetime_value.h"
#include "runtime/decimal_value.h"
#include "udf/udf.h"

namespace doris {
// Note: we explicitly pass by reference because passing by value has special ABI rules

// Used by CodegenAnyVal::Eq()

bool string_val_eq(const StringVal& x, const StringVal& y) {
    return x == y;
}

bool datetime_val_eq(const DateTimeVal& x, const DateTimeVal& y) {
    return x == y;
}

bool decimal_val_eq(const DecimalVal& x, const DecimalVal& y) {
    return x == y;
}

// Used by CodegenAnyVal::EqToNativePtr()

bool string_value_eq(const StringVal& x, const StringValue& y) {
    StringValue sv = StringValue::from_string_val(x);
    return sv.eq(y);
}

bool datetime_value_eq(const DateTimeVal& x, const DateTimeValue& y) {
    DateTimeValue tv = DateTimeValue::from_datetime_val(x);
    return tv == y;
}

bool decimal_value_eq(const DecimalVal& x, const DecimalValue& y) {
    DecimalValue tv = DecimalValue::from_decimal_val(x);
    return tv == y;
}
}
#else
#error "This file should only be used for cross compiling to IR."
#endif
