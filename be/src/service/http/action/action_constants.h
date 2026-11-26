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

#include <cstddef>
#include <string>

namespace doris {

// Constants shared by the http action handlers. Each of these used to be
// copy-pasted as a file-scope constant in many action .cpp files; single
// definitions let those sources live together in one unity TU.
inline const std::string HEADER_JSON = "application/json";
inline const std::string TABLET_ID = "tablet_id";
inline const std::string SCHEMA_HASH = "schema_hash";
inline const std::string OP = "op";
inline const std::string PATH = "path";
inline const std::string TOKEN_PARAMETER = "token";
inline constexpr size_t MEBIBYTE = 1024 * 1024;

} // namespace doris
