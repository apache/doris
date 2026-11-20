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

namespace cpp doris
namespace java org.apache.doris.thrift

// Reusable native Azure Blob authentication parameters. Scan/sink protocols may compose
// these types when their connectors adopt native Azure authentication.
struct TAzureSharedKey {
    1: optional string account_key
}

struct TAzureSas {
    // SAS query string without the leading '?'.
    1: optional string token
    // Known expiry in Unix milliseconds. The shared validator also reads the token
    // expiry ("se") and uses the earlier of the two.
    2: optional i64 expires_at_ms
}

struct TAzureClientSecret {
    1: optional string tenant_id
    2: optional string client_id
    3: optional string client_secret
    // Root of the Entra ID authority, e.g. https://login.microsoftonline.com/. The SDK requests access
    // tokens from this host. Unset means the Azure public cloud.
    4: optional string authority_host
}

union TAzureCredential {
    1: TAzureSharedKey shared_key
    2: TAzureSas sas
    3: TAzureClientSecret client_secret
}

struct TAzureClientConfig {
    1: optional string endpoint
    // Required by SharedKey, which signs requests with the account name.
    2: optional string account_name
    // The caller resolves the target container before creating an object-storage resource.
    3: optional string container
    4: optional TAzureCredential credential
}
