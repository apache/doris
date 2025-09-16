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

#include "aws_common.h"

#include <glog/logging.h>

namespace doris {

CredProviderType cred_provider_type_from_pb(cloud::CredProviderTypePB cred_provider_type) {
    switch (cred_provider_type) {
    case cloud::CredProviderTypePB::DEFAULT:
        return CredProviderType::Default;
    case cloud::CredProviderTypePB::SIMPLE:
        return CredProviderType::Simple;
    case cloud::CredProviderTypePB::INSTANCE_PROFILE:
        return CredProviderType::InstanceProfile;
    default:
        __builtin_unreachable();
        LOG(WARNING) << "Invalid CredProviderTypePB value: " << cred_provider_type
                     << ", use default instead.";
        return CredProviderType::Default;
    }
}

std::string get_valid_ca_cert_path(const std::vector<std::string>& ca_cert_file_paths) {
    for (const auto& path : ca_cert_file_paths) {
        if (std::filesystem::exists(path)) {
            return path;
        }
    }
    return "";
}
}