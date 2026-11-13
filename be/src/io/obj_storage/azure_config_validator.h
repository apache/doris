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

#include "common/status.h"
#include "io/obj_storage/obj_storage_config.h"

namespace doris::io {

// Rules of an AzureClientConfig decoded from TAzureClientConfig: the storage target (endpoint,
// container, the account name against a standard Azure endpoint), a SharedKey that is never
// anonymous, and the required fields of the selected method, which are the common rules in
// azure_auth.h (validate_azure_auth). The SAS expiry is checked by AzureClientProvider when a
// client is requested (validate_azure_auth_expiry).
class AzureConfigValidator {
public:
    // Example: SharedKey with account "acct" and endpoint https://other.blob.core.windows.net
    // is rejected, because the request would be signed for one account and sent to another.
    // A custom endpoint (e.g. Azurite at http://127.0.0.1:10000/acct) carries no account in its
    // host and is accepted with whatever account is configured.
    static Status validate(const AzureClientConfig& config);
};

} // namespace doris::io
