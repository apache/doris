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

#include "obj_retry_strategy.h"

#include <bvar/reducer.h>

namespace doris {

bvar::Adder<int64_t> s3_too_many_request_retry_cnt("s3_too_many_request_retry_cnt");

S3CustomRetryStrategy::S3CustomRetryStrategy(int maxRetries) : DefaultRetryStrategy(maxRetries) {}

S3CustomRetryStrategy::~S3CustomRetryStrategy() = default;

bool S3CustomRetryStrategy::ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
                                        long attemptedRetries) const {
    if (attemptedRetries < m_maxRetries &&
        error.GetResponseCode() == Aws::Http::HttpResponseCode::TOO_MANY_REQUESTS) {
        s3_too_many_request_retry_cnt << 1;
        return true;
    }
    return Aws::Client::DefaultRetryStrategy::ShouldRetry(error, attemptedRetries);
}

AzureRetryRecordPolicy::AzureRetryRecordPolicy(int retry_cnt) : retry_cnt(retry_cnt) {}

AzureRetryRecordPolicy::~AzureRetryRecordPolicy() = default;

std::unique_ptr<Azure::Core::Http::RawResponse> AzureRetryRecordPolicy::Send(
        Azure::Core::Http::Request& request, Azure::Core::Http::Policies::NextHttpPolicy nextPolicy,
        Azure::Core::Context const& context) const {
    auto resp = nextPolicy.Send(request, context);
    if (retry_cnt != 0 &&
        resp->GetStatusCode() == Azure::Core::Http::HttpStatusCode::TooManyRequests) {
        retry_cnt--;
        s3_too_many_request_retry_cnt << 1;
    }
    return resp;
}

std::unique_ptr<AzureRetryRecordPolicy::HttpPolicy> AzureRetryRecordPolicy::Clone() const {
    auto ret = std::make_unique<AzureRetryRecordPolicy>(*this);
    ret->retry_cnt = 0;
    return ret;
}
} // namespace doris