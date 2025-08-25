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

#include <aws/core/http/HttpResponse.h>
#include <bvar/reducer.h>
#include <glog/logging.h>

namespace doris {

bvar::Adder<int64_t> object_request_retry_count("object_request_retry_count");

S3CustomRetryStrategy::S3CustomRetryStrategy(int maxRetries) : DefaultRetryStrategy(maxRetries) {}

S3CustomRetryStrategy::~S3CustomRetryStrategy() = default;

bool S3CustomRetryStrategy::ShouldRetry(const Aws::Client::AWSError<Aws::Client::CoreErrors>& error,
                                        long attemptedRetries) const {
    if (attemptedRetries >= m_maxRetries) {
        return false;
    }

    if (Aws::Http::IsRetryableHttpResponseCode(error.GetResponseCode()) || error.ShouldRetry()) {
        object_request_retry_count << 1;
        LOG(INFO) << "retry due to error: " << error << ", attempt: " << attemptedRetries + 1 << "/"
                  << m_maxRetries;
        return true;
    }

    return false;
}
#ifdef USE_AZURE

std::unique_ptr<Azure::Core::Http::RawResponse> AzureRetryRecordPolicy::Send(
        Azure::Core::Http::Request& request, Azure::Core::Http::Policies::NextHttpPolicy nextPolicy,
        Azure::Core::Context const& context) const {
    // https://learn.microsoft.com/en-us/azure/developer/cpp/sdk/fundamentals/http-pipelines-and-retries

    std::unique_ptr<Azure::Core::Http::RawResponse> response = nextPolicy.Send(request, context);
    int32_t retry_count =
            Azure::Core::Http::Policies::_internal::RetryPolicy::GetRetryCount(context);

    if (static_cast<int>(response->GetStatusCode()) > 299 ||
        static_cast<int>(response->GetStatusCode()) < 200) {
        if (retry_count > 0) {
            object_request_retry_count << 1;
        }

        // If the response is not successful, we log the retry attempt and status code.
        LOG(INFO) << "azure retry retry_count: " << retry_count
                  << ", status code: " << static_cast<int>(response->GetStatusCode())
                  << ", reason: " << response->GetReasonPhrase();
    }

    return response;
}

std::unique_ptr<AzureRetryRecordPolicy::HttpPolicy> AzureRetryRecordPolicy::Clone() const {
    return std::make_unique<AzureRetryRecordPolicy>(*this);
}
#endif
} // namespace doris