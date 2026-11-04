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

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "common/status.h"

namespace doris {

struct File;

// Validate the values of a non-NULL FILE without changing any bytes.
// The caller must supply six children with the canonical FILE field types.
// SQL NULL FILE values are handled by the caller, outside this function.
Status validate_file(const File& value);

// Borrow metadata from FILE columns. Inline bytes are opaque to validation;
// only their SQL NULL state participates in the rules.
Status validate_file(std::optional<std::string_view> uri, std::optional<int64_t> offset,
                     std::optional<int64_t> size, std::optional<std::string_view> content_type,
                     std::optional<std::string_view> checksum, bool inline_is_null);

bool is_valid_file_content_type(std::string_view content_type);

// Takes a filesystem name, not an encoded URI. The caller owns URI parsing.
std::string_view infer_file_content_type_from_name(std::string_view name);

// S3 HEAD and LIST use the same FILE checksum: remove one enclosing quote pair,
// preserve the remaining opaque bytes, and return NULL for missing or empty ETags.
std::optional<std::string> file_checksum_from_etag(std::optional<std::string_view> etag);

} // namespace doris
