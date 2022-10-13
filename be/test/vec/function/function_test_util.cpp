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

#include "vec/function/function_test_util.h"

namespace doris::vectorized {
int64_t str_to_data_time(std::string datetime_str, bool data_time) {
    VecDateTimeValue v;
    v.from_date_str(datetime_str.c_str(), datetime_str.size());
    if (data_time) { //bool data_time only to simplifly means data_time or data to cast, just use in time-functions uint test
        v.to_datetime();
    } else {
        v.cast_to_date();
    }
    return binary_cast<VecDateTimeValue, Int64>(v);
}
} // namespace doris::vectorized
