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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/registerFunctionsComparison.cpp
// and modified by Doris

#include "vec/io/io_helper.h"

namespace doris::vectorized {
bool read_date_text_impl(VecDateTimeValue& x, const StringRef& buf) {
    auto ans = x.from_date_str(buf.data, buf.size);
    x.cast_to_date();
    return ans;
}

bool read_datetime_text_impl(VecDateTimeValue& x, const StringRef& buf) {
    auto ans = x.from_date_str(buf.data, buf.size);
    x.to_datetime();
    return ans;
}

bool read_date_v2_text_impl(DateV2Value<DateV2ValueType>& x, const StringRef& buf) {
    return x.from_date_str(buf.data, (int)buf.size, config::allow_zero_date);
}

bool read_date_v2_text_impl(DateV2Value<DateV2ValueType>& x, const StringRef& buf,
                            const cctz::time_zone& local_time_zone) {
    return x.from_date_str(buf.data, buf.size, local_time_zone, config::allow_zero_date);
}

bool read_datetime_v2_text_impl(DateV2Value<DateTimeV2ValueType>& x, const StringRef& buf,
                                UInt32 scale) {
    return x.from_date_str(buf.data, (int)buf.size, scale, config::allow_zero_date);
}

bool read_datetime_v2_text_impl(DateV2Value<DateTimeV2ValueType>& x, const StringRef& buf,
                                const cctz::time_zone& local_time_zone, UInt32 scale) {
    return x.from_date_str(buf.data, buf.size, local_time_zone, scale, config::allow_zero_date);
}

} // namespace doris::vectorized
