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
#include <string_view>

#include "common/status.h"
#include "io/obj_storage/obj_storage_config.h"

namespace doris::io {

// Credential rules that depend on time or on the credential's own encoding. Structural rules
// (required fields, one credential per config) belong to AzureConfigValidator.
//
// AzureClientProvider runs this whenever a client is requested, i.e. when a reader or writer
// is opened, but never per range read: the service enforces expiry and permissions on every
// request anyway, so the local check only turns an expired credential into a clear error
// before any request is sent.
class AzureCredentialValidator {
public:
    // Returns an error if a SAS has a known expiry at or before now_ms. When both the upstream
    // expiry and the token's "se" field are known, the earlier one applies. For example, with
    // expires_at_ms = 10:00 and se=09:30, the SAS is rejected from 09:30 on.
    static Status validate(const AzureAuthConfig& auth, int64_t now_ms);

    // Expiry ("se") of a SAS query string in Unix milliseconds, or nullopt if the token has no
    // "se" field. Accepts the ISO 8601 UTC forms Azure issues, URL-encoded or not:
    // "2026-10-15", "2026-10-15T08:30Z", "2026-10-15T08:30:00Z", "2026-10-15T08:30:00.1234567Z".
    static Result<std::optional<int64_t>> sas_expiry_ms(std::string_view token);
};

} // namespace doris::io
