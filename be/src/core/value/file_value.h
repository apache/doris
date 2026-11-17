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

#include <optional>
#include <string>
#include <string_view>

namespace doris {

bool is_valid_file_content_type(std::string_view content_type);

// Takes a filesystem name, not an encoded URI. The caller owns URI parsing.
std::string_view infer_file_content_type_from_name(std::string_view name);

// S3 HEAD and LIST use the same FILE checksum: remove one enclosing quote pair,
// preserve the remaining opaque bytes, and return NULL for missing or empty ETags.
std::optional<std::string> file_checksum_from_etag(std::optional<std::string_view> etag);

} // namespace doris
