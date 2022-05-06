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
#include "olap/fs/file_block_manager.h"
#include "olap/fs/remote_block_manager.h"
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"
#include "util/storage_backend.h"
#include "util/storage_backend_mgr.h"

namespace doris {
namespace fs {
namespace fs_util {

BlockManager* block_manager(const FilePathDesc& path_desc) {
    fs::BlockManagerOptions bm_opts;
    bm_opts.read_only = false;
    if (path_desc.is_remote()) {
        bm_opts.read_only = true;
        std::shared_ptr<StorageBackend> storage_backend =
                StorageBackendMgr::instance()->get_storage_backend(path_desc.storage_name);
        if (storage_backend == nullptr) {
            LOG(WARNING) << "storage_backend is invalid: " << path_desc.debug_string();
            return nullptr;
        }
        static RemoteBlockManager remote_block_mgr(Env::Default(), storage_backend, bm_opts);
        return &remote_block_mgr;
    } else {
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

TStorageMedium::type get_t_storage_medium(StorageMediumPB storage_medium) {
    switch (storage_medium) {
    case StorageMediumPB::S3:
        return TStorageMedium::S3;
    case StorageMediumPB::SSD:
        return TStorageMedium::SSD;
    case StorageMediumPB::HDD:
    default:
        return TStorageMedium::HDD;
    }
}

StorageParamPB get_storage_param_pb(const TStorageParam& t_storage_param) {
    StorageParamPB storage_param;
    storage_param.set_storage_medium(get_storage_medium_pb(t_storage_param.storage_medium));
    storage_param.set_storage_name(t_storage_param.storage_name);
    switch (t_storage_param.storage_medium) {
    case TStorageMedium::S3: {
        S3StorageParamPB* s3_param = storage_param.mutable_s3_storage_param();
        s3_param->set_s3_endpoint(t_storage_param.s3_storage_param.s3_endpoint);
        s3_param->set_s3_region(t_storage_param.s3_storage_param.s3_region);
        s3_param->set_s3_ak(t_storage_param.s3_storage_param.s3_ak);
        s3_param->set_s3_sk(t_storage_param.s3_storage_param.s3_sk);
        s3_param->set_s3_max_conn(t_storage_param.s3_storage_param.s3_max_conn);
        s3_param->set_s3_request_timeout_ms(t_storage_param.s3_storage_param.s3_request_timeout_ms);
        s3_param->set_s3_conn_timeout_ms(t_storage_param.s3_storage_param.s3_conn_timeout_ms);
        s3_param->set_root_path(t_storage_param.s3_storage_param.root_path);
        return storage_param;
    }
    case TStorageMedium::SSD:
    case TStorageMedium::HDD:
    default:
        return storage_param;
    }
}

} // namespace fs_util
} // namespace fs
} // namespace doris
