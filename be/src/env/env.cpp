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

} // end namespace doris