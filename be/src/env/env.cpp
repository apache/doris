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

#include "common/config.h"
#include "env/env.h"
#include "env/env_posix.h"
#include "env/env_remote.h"
#include "env/env_remote_mgr.h"

namespace doris {

std::shared_ptr<PosixEnv> Env::_posix_env(new PosixEnv());
std::shared_ptr<RemoteEnvMgr> Env::_remote_env_mgr(new RemoteEnvMgr());

// Default Posix Env
Env *Env::Default() {
    return _posix_env.get();
}

std::shared_ptr<Env> Env::get_env(TStorageMedium::type storage_medium) {
    switch (storage_medium) {
        case TStorageMedium::S3:
        {
            if (doris::config::default_remote_storage_s3_ak.empty() || doris::config::default_remote_storage_s3_sk.empty()
                || doris::config::default_remote_storage_s3_endpoint.empty() || doris::config::default_remote_storage_s3_region.empty()) {
                return nullptr;
            }
            TStorageParam storage_param;
            storage_param.storage_medium = TStorageMedium::S3;
            storage_param.s3_storage_param.s3_endpoint = doris::config::default_remote_storage_s3_endpoint;
            storage_param.s3_storage_param.s3_region = doris::config::default_remote_storage_s3_region;
            storage_param.s3_storage_param.s3_ak = doris::config::default_remote_storage_s3_ak;
            storage_param.s3_storage_param.s3_sk = doris::config::default_remote_storage_s3_sk;
            storage_param.s3_storage_param.s3_max_conn = doris::config::default_remote_storage_s3_max_conn;
            storage_param.s3_storage_param.s3_request_timeout_ms = doris::config::default_remote_storage_s3_request_timeout_ms;
            storage_param.s3_storage_param.s3_conn_timeout_ms = doris::config::default_remote_storage_s3_conn_timeout_ms;
            return _remote_env_mgr->get_remote_env(storage_param);
        }
        case TStorageMedium::SSD:
        case TStorageMedium::HDD:
        default:
            return _posix_env;
    }
}


} // end namespace doris