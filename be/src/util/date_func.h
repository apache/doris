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

#include <stdint.h>
#include <time.h>

#include <string>

#include "olap/field.h"

namespace doris {

uint64_t timestamp_from_datetime(const std::string& datetime_str);
uint24_t timestamp_from_date(const std::string& date_str);
int32_t time_to_buffer_from_double(double time, char* buffer);
uint32_t timestamp_from_date_v2(const std::string& date_str);
uint64_t timestamp_from_datetime_v2(const std::string& date_str);

} // namespace doris
