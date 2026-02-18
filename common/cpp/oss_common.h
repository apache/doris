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

#include <string>

namespace doris {

// Alibaba Cloud OSS Credentials Provider Type
enum class OSSCredProviderType {
    SIMPLE = 0,           // Static AK/SK credentials
    INSTANCE_PROFILE = 1, // ECS instance profile (metadata service)
};

// Normalize OSS endpoint by ensuring it has a scheme (http:// or https://)
// Defaults to HTTPS if no scheme is specified for security and KMS bucket compatibility
inline std::string normalize_oss_endpoint(const std::string& endpoint) {
    if (endpoint.empty()) {
        return endpoint;
    }

    // Already has scheme, return as-is
    if (endpoint.starts_with("https://") || endpoint.starts_with("http://")) {
        return endpoint;
    }

    // Default to HTTPS for security and KMS bucket compatibility
    return "https://" + endpoint;
}

} // namespace doris
