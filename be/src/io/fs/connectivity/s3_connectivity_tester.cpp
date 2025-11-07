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
#include "util/s3_uri.h"

namespace doris::io {
#include "common/compile_check_begin.h"

Status S3ConnectivityTester::test(const std::map<std::string, std::string>& properties) {
    auto it = properties.find(TEST_LOCATION);
    S3URI s3_uri(it->second);
    RETURN_IF_ERROR(s3_uri.parse());

    std::string bucket = s3_uri.get_bucket();
    if (bucket.empty()) {
        return Status::InvalidArgument("Failed to extract bucket from location: {}", it->second);
    }

    S3Conf s3_conf;
    RETURN_IF_ERROR(S3ClientFactory::convert_properties_to_s3_conf(properties, s3_uri, &s3_conf));

    auto obj_client = S3ClientFactory::instance().create(s3_conf.client_conf);
    if (!obj_client) {
        return Status::InternalError("Failed to create S3 client");
    }

    auto resp = obj_client->head_object({.bucket = bucket, .key = ""});
    if (resp.resp.status.code != ErrorCode::OK && resp.resp.status.code != ErrorCode::NOT_FOUND) {
        return Status::IOError("S3 connectivity test failed for bucket '{}': {}", bucket,
                               resp.resp.status.msg);
    }

    return Status::OK();
}
#include "common/compile_check_end.h"
} // namespace doris::io
