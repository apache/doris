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

#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <fmt/format.h>
#include <stdint.h>

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "gutil/hash/hash.h"

namespace Aws {
namespace S3 {
class S3Client;
} // namespace S3
} // namespace Aws
namespace bvar {
template <typename T>
class Adder;
}

namespace doris {

namespace s3_bvar {
extern bvar::Adder<uint64_t> s3_get_total;
extern bvar::Adder<uint64_t> s3_put_total;
extern bvar::Adder<uint64_t> s3_delete_total;
extern bvar::Adder<uint64_t> s3_head_total;
extern bvar::Adder<uint64_t> s3_multi_part_upload_total;
extern bvar::Adder<uint64_t> s3_list_total;
extern bvar::Adder<uint64_t> s3_list_object_versions_total;
extern bvar::Adder<uint64_t> s3_get_bucket_version_total;
extern bvar::Adder<uint64_t> s3_copy_object_total;
}; // namespace s3_bvar

class S3URI;

const static std::string S3_AK = "s3.access_key";
const static std::string S3_SK = "s3.secret_key";
const static std::string S3_ENDPOINT = "s3.endpoint";
const static std::string S3_REGION = "s3.region";
const static std::string S3_TOKEN = "s3.session_token";
const static std::string S3_MAX_CONN_SIZE = "s3.connection.maximum";
const static std::string S3_REQUEST_TIMEOUT_MS = "s3.connection.request.timeout";
const static std::string S3_CONN_TIMEOUT_MS = "s3.connection.timeout";

struct S3Conf {
    std::string ak;
    std::string sk;
    std::string endpoint;
    std::string region;
    std::string bucket;
    std::string prefix;
    int max_connections = -1;
    int request_timeout_ms = -1;
    int connect_timeout_ms = -1;
    bool use_virtual_addressing = true;

    std::string to_string() const {
        return fmt::format(
                "(ak={}, sk=*, endpoint={}, region={}, bucket={}, prefix={}, max_connections={}, "
                "request_timeout_ms={}, connect_timeout_ms={}, use_virtual_addressing={})",
                ak, endpoint, region, bucket, prefix, max_connections, request_timeout_ms,
                connect_timeout_ms, use_virtual_addressing);
    }

    uint64_t get_hash() const {
        uint64_t hash_code = 0;
        hash_code += Fingerprint(ak);
        hash_code += Fingerprint(sk);
        hash_code += Fingerprint(endpoint);
        hash_code += Fingerprint(region);
        hash_code += Fingerprint(bucket);
        hash_code += Fingerprint(prefix);
        hash_code += Fingerprint(max_connections);
        hash_code += Fingerprint(request_timeout_ms);
        hash_code += Fingerprint(connect_timeout_ms);
        hash_code += Fingerprint(use_virtual_addressing);
        return hash_code;
    }
};

class S3ClientFactory {
public:
    ~S3ClientFactory();

    static S3ClientFactory& instance();

    std::shared_ptr<Aws::S3::S3Client> create(const S3Conf& s3_conf);

    static bool is_s3_conf_valid(const std::map<std::string, std::string>& prop);

    static bool is_s3_conf_valid(const S3Conf& s3_conf);

    static Status convert_properties_to_s3_conf(const std::map<std::string, std::string>& prop,
                                                const S3URI& s3_uri, S3Conf* s3_conf);

    static Aws::Client::ClientConfiguration& getClientConfiguration() {
        // The default constructor of ClientConfiguration will do some http call
        // such as Aws::Internal::GetEC2MetadataClient and other init operation,
        // which is unnecessary.
        // So here we use a static instance, and deep copy every time
        // to avoid unnecessary operations.
        static Aws::Client::ClientConfiguration instance;
        return instance;
    }

private:
    S3ClientFactory();

    Aws::SDKOptions _aws_options;
    std::mutex _lock;
    std::unordered_map<uint64_t, std::shared_ptr<Aws::S3::S3Client>> _cache;
};

} // end namespace doris
