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

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/GeneralHTTPCredentialsProvider.h>
#include <unistd.h>

#include <array>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "cpp/aws_common.h"

// Test support for the container credentials provider built by
// create_container_credentials_provider(). It lives in common/cpp because both test trees need it:
// the BE unit tests cover the S3 client factory and the Kafka MSK IAM signer, the cloud unit tests
// cover the recycler, and all three reach the same factory. Header-only, so it never enters the
// CommonCPP library
namespace doris {

// Clears the four AWS_CONTAINER_* variables on construction and restores them on destruction.
class ContainerCredentialsEnvGuard {
public:
    ContainerCredentialsEnvGuard() {
        for (const char* name : kNames) {
            const char* value = std::getenv(name);
            _saved.emplace_back(value == nullptr ? std::nullopt
                                                 : std::optional<std::string>(value));
            unsetenv(name);
        }
    }

    ~ContainerCredentialsEnvGuard() {
        for (size_t i = 0; i < kNames.size(); ++i) {
            if (_saved[i].has_value()) {
                setenv(kNames[i], _saved[i]->c_str(), 1);
            } else {
                unsetenv(kNames[i]);
            }
        }
        for (const auto& path : _token_files) {
            std::remove(path.c_str());
        }
    }

    ContainerCredentialsEnvGuard(const ContainerCredentialsEnvGuard&) = delete;
    ContainerCredentialsEnvGuard& operator=(const ContainerCredentialsEnvGuard&) = delete;

    // Presents the environment the way EKS Pod Identity does: a full URL, a token that exists only
    // as a file, and no relative URI or inline token at all.
    void set_pod_identity(const std::string& full_uri, const std::string& token_file) {
        unsetenv(AWS_CONTAINER_CREDENTIALS_RELATIVE_URI);
        unsetenv(AWS_CONTAINER_AUTHORIZATION_TOKEN);
        setenv(AWS_CONTAINER_CREDENTIALS_FULL_URI, full_uri.c_str(), 1);
        setenv(AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE, token_file.c_str(), 1);
    }

    // Presents the environment the way ECS does: a relative path against the agent's own address,
    // and no full URI or token file.
    void set_ecs_task_role(const std::string& relative_uri) {
        unsetenv(AWS_CONTAINER_CREDENTIALS_FULL_URI);
        unsetenv(AWS_CONTAINER_AUTHORIZATION_TOKEN);
        unsetenv(AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE);
        setenv(AWS_CONTAINER_CREDENTIALS_RELATIVE_URI, relative_uri.c_str(), 1);
    }

    void set_inline_token_endpoint(const std::string& full_uri, const std::string& token) {
        unsetenv(AWS_CONTAINER_CREDENTIALS_RELATIVE_URI);
        unsetenv(AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE);
        setenv(AWS_CONTAINER_CREDENTIALS_FULL_URI, full_uri.c_str(), 1);
        setenv(AWS_CONTAINER_AUTHORIZATION_TOKEN, token.c_str(), 1);
    }

    // Writes a token file and takes responsibility for deleting it. Call it again with the same path
    // to rotate the token the way the kubelet does.
    void write_token_file(const std::string& path, const std::string& contents) {
        std::ofstream out(path, std::ios::trunc);
        out << contents;
        out.flush();
        _token_files.push_back(path);
    }

    // A token file path unique to this test and this process
    std::string token_file_path(const std::string& name) const {
        return "/tmp/doris_container_credentials_token_" + name + "_" + std::to_string(getpid());
    }

private:
    static constexpr std::array<const char*, 4> kNames {
            AWS_CONTAINER_CREDENTIALS_RELATIVE_URI, AWS_CONTAINER_CREDENTIALS_FULL_URI,
            AWS_CONTAINER_AUTHORIZATION_TOKEN, AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE};

    std::vector<std::optional<std::string>> _saved;
    std::vector<std::string> _token_files;
};

// Returns the provider only if it is a container credentials provider that can actually reach an
// endpoint, and nullptr otherwise.
inline std::shared_ptr<Aws::Auth::GeneralHTTPCredentialsProvider> as_valid_http_provider(
        const std::shared_ptr<Aws::Auth::AWSCredentialsProvider>& provider) {
    auto http_provider =
            std::dynamic_pointer_cast<Aws::Auth::GeneralHTTPCredentialsProvider>(provider);
    if (http_provider != nullptr && !http_provider->IsValid()) {
        return nullptr;
    }
    return http_provider;
}

} // namespace doris
