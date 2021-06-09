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
//

#pragma once

#include <re2/re2.h>

#include "cctz/time_zone.h"

namespace doris {

class TimezoneUtils {
public:
    static bool find_cctz_time_zone(const std::string& timezone, cctz::time_zone& ctz);

public:
    static const std::string default_time_zone;

private:
    // RE2 obj is thread safe
    static RE2 time_zone_offset_format_reg;
};
} // namespace doris
