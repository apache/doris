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

#include "cloud/cloud_internal_service.h"

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet_mgr.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"

namespace doris {

CloudInternalServiceImpl::CloudInternalServiceImpl(CloudStorageEngine& engine, ExecEnv* exec_env)
        : PInternalService(exec_env), _engine(engine) {}

CloudInternalServiceImpl::~CloudInternalServiceImpl() = default;

void CloudInternalServiceImpl::alter_vault_sync(google::protobuf::RpcController* controller,
                                                const doris::PAlterVaultSyncRequest* request,
                                                PAlterVaultSyncResponse* response,
                                                google::protobuf::Closure* done) {
    LOG(INFO) << "alter be to sync vault info from Meta Service";
    // If the vaults containing hdfs vault then it would try to create hdfs connection using jni
    // which would acuiqre one thread local jniEnv. But bthread context can't guarantee that the brpc
    // worker thread wouldn't do bthread switch between worker threads.
    bool ret = _heavy_work_pool.try_offer([this, done]() {
        brpc::ClosureGuard closure_guard(done);
        _engine.sync_storage_vault();
    });
    if (!ret) {
        brpc::ClosureGuard closure_guard(done);
        LOG(WARNING) << "fail to offer alter_vault_sync request to the work pool, pool="
                     << _heavy_work_pool.get_info();
    }
}

FileCacheType cache_type_to_pb(io::FileCacheType type) {
    switch (type) {
    case io::FileCacheType::TTL:
        return FileCacheType::TTL;
    case io::FileCacheType::INDEX:
        return FileCacheType::INDEX;
    case io::FileCacheType::NORMAL:
        return FileCacheType::NORMAL;
    default:
        DCHECK(false);
    }
    return FileCacheType::NORMAL;
}

void CloudInternalServiceImpl::get_file_cache_meta_by_tablet_id(
        google::protobuf::RpcController* controller [[maybe_unused]],
        const PGetFileCacheMetaRequest* request, PGetFileCacheMetaResponse* response,
        google::protobuf::Closure* done) {
    brpc::ClosureGuard closure_guard(done);
    if (!config::enable_file_cache) {
        LOG_WARNING("try to access tablet file cache meta, but file cache not enabled");
        return;
    }
    for (const auto& tablet_id : request->tablet_ids()) {
        auto res = _engine.tablet_mgr().get_tablet(tablet_id);
        if (!res.has_value()) {
            LOG(ERROR) << "failed to get tablet: " << tablet_id
                       << " err msg: " << res.error().msg();
            return;
        }
        CloudTabletSPtr tablet = std::move(res.value());
        auto rowsets = tablet->get_snapshot_rowset();
        std::for_each(rowsets.cbegin(), rowsets.cend(), [&](const RowsetSharedPtr& rowset) {
            std::string rowset_id = rowset->rowset_id().to_string();
            for (int64_t segment_id = 0; segment_id < rowset->num_segments(); segment_id++) {
                std::string file_name = fmt::format("{}_{}.dat", rowset_id, segment_id);
                auto cache_key = io::BlockFileCache::hash(file_name);
                auto* cache = io::FileCacheFactory::instance()->get_by_path(cache_key);

                auto segments_meta = cache->get_hot_blocks_meta(cache_key);
                for (const auto& tuple : segments_meta) {
                    FileCacheBlockMeta* meta = response->add_file_cache_block_metas();
                    meta->set_tablet_id(tablet_id);
                    meta->set_rowset_id(rowset_id);
                    meta->set_segment_id(segment_id);
                    meta->set_file_name(file_name);
                    meta->set_offset(std::get<0>(tuple));
                    meta->set_size(std::get<1>(tuple));
                    meta->set_cache_type(cache_type_to_pb(std::get<2>(tuple)));
                    meta->set_expiration_time(std::get<3>(tuple));
                }
            }
        });
    }
}

} // namespace doris
