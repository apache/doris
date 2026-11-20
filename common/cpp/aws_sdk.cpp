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

#include "cpp/aws_sdk.h"

#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>

#include "cpp/aws_logger.h"

namespace doris {

class AwsSdkLifetime {
public:
    explicit AwsSdkLifetime(int log_level) {
        auto level = static_cast<Aws::Utils::Logging::LogLevel>(log_level);
        _options.loggingOptions.logLevel = level;
        _options.loggingOptions.logger_create_fn = [level] {
            return std::make_shared<DorisAWSLogger>(level);
        };
        Aws::InitAPI(_options);
    }
    ~AwsSdkLifetime() { Aws::ShutdownAPI(_options); }

private:
    Aws::SDKOptions _options;
};

std::shared_ptr<AwsSdkLifetime> acquire_aws_sdk(int log_level) {
    static auto lifetime = std::make_shared<AwsSdkLifetime>(log_level);
    return lifetime;
}

void ensure_aws_sdk_initialized(int log_level) {
    acquire_aws_sdk(log_level);
}

const Aws::Client::ClientConfiguration& default_aws_client_configuration() {
    static const auto lifetime = acquire_aws_sdk();
    static const Aws::Client::ClientConfiguration configuration;
    return configuration;
}

unsigned default_aws_max_connections() {
    return default_aws_client_configuration().maxConnections;
}

} // namespace doris
