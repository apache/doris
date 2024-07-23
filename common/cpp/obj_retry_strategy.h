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

#include <aws/core/client/AWSError.h>
#include <aws/core/client/DefaultRetryStrategy.h>

#include <azure/core/http/policies/policy.hpp>

namespace doris {
class S3CustomRetryStrategy final : public Aws::Client::DefaultRetryStrategy {
public:
    S3CustomRetryStrategy(int maxRetries);
    ~S3CustomRetryStrategy() override;

    bool ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
                     long attemptedRetries) const override;
};

class AzureRetryRecordPolicy final : public Azure::Core::Http::Policies::HttpPolicy {
public:
    AzureRetryRecordPolicy(int retry_cnt);
    ~AzureRetryRecordPolicy() override;
    std::unique_ptr<HttpPolicy> Clone() const override;
    std::unique_ptr<Azure::Core::Http::RawResponse> Send(
            Azure::Core::Http::Request& request,
            Azure::Core::Http::Policies::NextHttpPolicy nextPolicy,
            Azure::Core::Context const& context) const override;

private:
    mutable int retry_cnt;
};
} // namespace doris