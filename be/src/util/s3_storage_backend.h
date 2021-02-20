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
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <util/storage_backend.h>
#include <util/string_util.h>

namespace doris {

class S3StorageBackend : public StorageBackend {
public:
    S3StorageBackend(const std::map<std::string, std::string>& prop);
    ~S3StorageBackend() {}
    Status download(const std::string& remote, const std::string& local);
    Status upload(const std::string& local, const std::string& remote);
    Status upload_with_checksum(const std::string& local, const std::string& remote,
                                const std::string& checksum);
    Status list(const std::string& remote_path, std::map<std::string, FileStat>* files);
    Status rename(const std::string& orig_name, const std::string& new_name);
    Status direct_upload(const std::string& remote, const std::string& content);
    Status rm(const std::string& remote);
    Status copy(const std::string& src, const std::string& dst);
    Status mkdir(const std::string& path);
    Status exist(const std::string& path);

private:
    template <typename AwsOutcome>
    std::string error_msg(const AwsOutcome& outcome);
    static const std::string _S3_AK;
    static const std::string _S3_SK;
    static const std::string _S3_ENDPOINT;
    static const std::string _S3_REGION;
    const StringCaseMap<std::string> _properties;
    Aws::Auth::AWSCredentials _aws_cred;
    Aws::Client::ClientConfiguration _aws_config;
    std::unique_ptr<Aws::S3::S3Client> _client;
};

} // end namespace doris
