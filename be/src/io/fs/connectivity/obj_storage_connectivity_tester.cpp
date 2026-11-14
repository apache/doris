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

#include "io/fs/connectivity/obj_storage_connectivity_tester.h"

#include "io/object_storage/obj_storage_client_holder.h"
#include "io/object_storage/obj_storage_config_parser.h"
#include "io/object_storage/obj_storage_rate_limiter_manager.h"
#include "util/s3_uri.h"

namespace doris::io {

Status ObjStorageConnectivityTester::test(const std::map<std::string, std::string>& properties) {
    auto it = properties.find(TEST_LOCATION);
    if (it == properties.end()) {
        return Status::InvalidArgument("Missing '{}' property in S3 connectivity test request",
                                       TEST_LOCATION);
    }
    S3URI s3_uri(it->second);
    RETURN_IF_ERROR(s3_uri.parse());

    std::string bucket = s3_uri.get_bucket();
    if (bucket.empty()) {
        return Status::InvalidArgument("Failed to extract bucket from location: {}", it->second);
    }

    auto storage_config = DORIS_TRY(ObjStorageConfigParser::from_properties(properties, s3_uri));

    ObjStorageClientHolder holder(
            std::move(storage_config.client),
            ObjStorageRateLimiterManager::policy_for(storage_config.is_internal_bucket));
    RETURN_IF_ERROR(holder.init());

    auto resp = holder.get()->head_object({.bucket = bucket, .key = ""});
    if (resp.resp.status.code != ErrorCode::OK && resp.resp.status.code != ErrorCode::NOT_FOUND) {
        return Status::IOError("S3 connectivity test failed for bucket '{}': {}", bucket,
                               resp.resp.status.msg);
    }

    return Status::OK();
}
} // namespace doris::io
