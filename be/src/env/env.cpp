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

#include "env/env.h"
#include "env/env_posix.h"
#include "env/env_remote.h"

namespace doris {

// Default Posix Env
Env *Env::Default() {
    static PosixEnv default_env;
    return &default_env;
}

Env* Env::get_env(TStorageMedium::type storage_medium) {
    switch (storage_medium) {
        case TStorageMedium::S3:
            static RemoteEnv remote_env;
            return &remote_env;
        case TStorageMedium::SSD:
        case TStorageMedium::HDD:
        default:
            return Default();
    }
}

bool Env::init() {
    std::map<std::string, std::string> storage_prop;
    storage_prop[S3_AK] = doris::config::s3_ak;
    storage_prop[S3_SK] = doris::config::s3_sk;
    storage_prop[S3_ENDPOINT] = doris::config::s3_endpoint;
    storage_prop[S3_REGION] = doris::config::s3_region;
    storage_prop[S3_MAX_CONN_SIZE] = std::string(doris::config::s3_max_conn);
    storage_prop[S3_REQUEST_TIMEOUT_MS] = std::string(doris::config::s3_request_timeout_ms);
    storage_prop[S3_CONN_TIMEOUT_MS] = std::string(doris::config::s3_conn_timeout_ms);

    return reinterpret_cast<RemoteEnv>(get_env(TStorageMedium::S3))->init(storage_prop);
}

} // end namespace doris