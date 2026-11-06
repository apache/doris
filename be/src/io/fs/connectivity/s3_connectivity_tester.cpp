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

#include "io/fs/connectivity/s3_connectivity_tester.h"

#include "io/fs/s3_file_system.h"
#include "io/obj_storage/obj_client_factory.h"
#include "io/obj_storage/obj_storage_config_resolver.h"
#include "io/obj_storage/obj_storage_uri.h"

namespace doris::io {

Status S3ConnectivityTester::test(const std::map<std::string, std::string>& properties) {
    auto it = properties.find(TEST_LOCATION);
    if (it == properties.end()) {
        return Status::InvalidArgument("Missing '{}' property in S3 connectivity test request",
                                       TEST_LOCATION);
    }
    auto location = DORIS_TRY(ObjStorageUri::parse_s3(it->second));

    std::string bucket = location.bucket;
    if (bucket.empty()) {
        return Status::InvalidArgument("Failed to extract bucket from location: {}", it->second);
    }

    auto config = DORIS_TRY(ObjStorageConfigResolver::from_properties(properties, location));

    auto obj_client = DORIS_TRY(ObjClientFactory::instance().get_or_create(config.client));

    auto resp = obj_client->head_object({.bucket = bucket, .key = ""});
    if (resp.resp.status.code != ErrorCode::OK && resp.resp.status.code != ErrorCode::NOT_FOUND) {
        return Status::IOError("S3 connectivity test failed for bucket '{}': {}", bucket,
                               resp.resp.status.msg);
    }

    return Status::OK();
}
} // namespace doris::io
