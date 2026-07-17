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

#include <aws/core/client/ClientConfiguration.h>
#include <glog/logging.h>

#include "util.h"

namespace doris {

CredProviderType cred_provider_type_from_pb(cloud::CredProviderTypePB cred_provider_type) {
    switch (cred_provider_type) {
    case cloud::CredProviderTypePB::DEFAULT:
        return CredProviderType::Default;
    case cloud::CredProviderTypePB::SIMPLE:
        return CredProviderType::Simple;
    case cloud::CredProviderTypePB::INSTANCE_PROFILE:
        return CredProviderType::InstanceProfile;
    case cloud::CredProviderTypePB::ENV:
        return CredProviderType::Env;
    case cloud::CredProviderTypePB::SYSTEM_PROPERTIES:
        return CredProviderType::SystemProperties;
    case cloud::CredProviderTypePB::WEB_IDENTITY:
        return CredProviderType::WebIdentity;
    case cloud::CredProviderTypePB::CONTAINER:
        return CredProviderType::Container;
    case cloud::CredProviderTypePB::ANONYMOUS:
        return CredProviderType::Anonymous;
    default:
        __builtin_unreachable();
        LOG(WARNING) << "Invalid CredProviderTypePB value: " << cred_provider_type
                     << ", use default instead.";
        return CredProviderType::Default;
    }
}

CredProviderType cred_provider_type_from_string(const std::string& type) {
    if (type.empty() || type == "DEFAULT") {
        return CredProviderType::Default;
    }
    if (type == "SIMPLE") {
        return CredProviderType::Simple;
    }
    if (type == "INSTANCE_PROFILE") {
        return CredProviderType::InstanceProfile;
    }
    if (type == "ENV") {
        return CredProviderType::Env;
    }
    if (type == "SYSTEM_PROPERTIES") {
        return CredProviderType::SystemProperties;
    }
    if (type == "WEB_IDENTITY") {
        return CredProviderType::WebIdentity;
    }
    if (type == "CONTAINER") {
        return CredProviderType::Container;
    }
    if (type == "ANONYMOUS") {
        return CredProviderType::Anonymous;
    }
    LOG(WARNING) << "Unknown credentials provider type: " << type << ", use default instead.";
    return CredProviderType::Default;
}

std::string get_valid_ca_cert_path(const std::vector<std::string>& ca_cert_file_paths) {
    for (const auto& path : ca_cert_file_paths) {
        if (std::filesystem::exists(path)) {
            return path;
        }
    }
    return "";
}

void set_s3_client_http_scheme(Aws::Client::ClientConfiguration& client_config,
                               const std::string& scheme) {
    client_config.endpointOverride = strip_uri_scheme(client_config.endpointOverride);
    client_config.scheme =
            scheme == "http" ? Aws::Http::Scheme::HTTP : Aws::Http::Scheme::HTTPS;
}
}
