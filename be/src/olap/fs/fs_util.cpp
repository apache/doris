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

#include "olap/fs/fs_util.h"

#include "common/status.h"
#include "env/env.h"
#include "env/env_remote.h"
#include "olap/fs/file_block_manager.h"
#include "olap/fs/remote_block_manager.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"

namespace doris {
namespace fs {
namespace fs_util {

BlockManager* block_manager(TStorageMedium::type storage_medium) {
    fs::BlockManagerOptions bm_opts;
    bm_opts.read_only = false;
    switch (storage_medium) {
        case TStorageMedium::S3:
            bm_opts.read_only = true;
            static RemoteBlockManager remote_block_mgr(
                    Env::Default(), dynamic_cast<RemoteEnv*>(Env::get_env(storage_medium)), bm_opts);
            return &remote_block_mgr;
        case TStorageMedium::SSD:
        case TStorageMedium::HDD:
        default:
            static FileBlockManager block_mgr(Env::Default(), std::move(bm_opts));
            return &block_mgr;
    }
}

StorageMediumPB get_storage_medium_pb(TStorageMedium::type t_storage_medium) {
    switch (t_storage_medium) {
        case TStorageMedium::S3:
            return StorageMediumPB::S3;
        case TStorageMedium::SSD:
            return StorageMediumPB::SSD;
        case TStorageMedium::HDD:
        default:
            return StorageMediumPB::HDD;
    }
}

} // namespace fs_util
} // namespace fs
} // namespace doris
