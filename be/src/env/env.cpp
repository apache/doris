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

std::shared_ptr<PosixEnv> Env::_posix_env(new PosixEnv());
std::shared_ptr<RemoteEnv> Env::_remote_env(new RemoteEnv());

// Default Posix Env
Env *Env::Default() {
    return _posix_env.get();
}

Env* Env::get_env(TStorageMedium::type storage_medium) {
    switch (storage_medium) {
        case TStorageMedium::S3:
            return _remote_env.get();
        case TStorageMedium::SSD:
        case TStorageMedium::HDD:
        default:
            return Default();
    }
}

Status Env::init() {
    RETURN_IF_ERROR(_posix_env->init_conf());
    RETURN_IF_ERROR(_remote_env->init_conf());
    LOG(INFO) << "Env init successfully.";
    return Status::OK();
}

} // end namespace doris