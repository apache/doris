//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors

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