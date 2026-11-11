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

#include <string_view>

namespace doris::spill_storage_type {
inline constexpr char LOCAL[] = "local";
inline constexpr char REMOTE[] = "remote";
// Keep existing BE configurations valid; new configurations should use REMOTE.
inline constexpr char S3[] = "s3";

inline constexpr bool is_remote(std::string_view type) {
    return type == REMOTE || type == S3;
}
} // namespace doris::spill_storage_type
