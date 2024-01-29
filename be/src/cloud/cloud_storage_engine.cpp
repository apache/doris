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

#include "cloud/cloud_storage_engine.h"

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "cloud/config.h"
#include "io/fs/s3_file_system.h"
#include "olap/memtable_flush_executor.h"
#include "olap/storage_policy.h"
#include "runtime/memory/cache_manager.h"

namespace doris {

using namespace std::literals;

CloudStorageEngine::CloudStorageEngine(const UniqueId& backend_uid)
        : BaseStorageEngine(Type::CLOUD, backend_uid),
          _meta_mgr(std::make_unique<cloud::CloudMetaMgr>()),
          _tablet_mgr(std::make_unique<CloudTabletMgr>(*this)) {}

CloudStorageEngine::~CloudStorageEngine() = default;

Status CloudStorageEngine::open() {
    std::vector<std::tuple<std::string, S3Conf>> s3_infos;
    do {
        auto st = _meta_mgr->get_s3_info(&s3_infos);
        if (st.ok()) {
            break;
        }

        LOG(WARNING) << "failed to get s3 info, retry after 5s, err=" << st;
        std::this_thread::sleep_for(5s);
    } while (true);

    CHECK(!s3_infos.empty()) << "no s3 infos";

    for (auto& [id, s3_conf] : s3_infos) {
        LOG(INFO) << "get s3 info: " << s3_conf.to_string() << " resource_id=" << id;
        std::shared_ptr<io::S3FileSystem> s3_fs;
        RETURN_IF_ERROR(io::S3FileSystem::create(std::move(s3_conf), id, &s3_fs));
        RETURN_IF_ERROR(s3_fs->connect());
        put_storage_resource(std::atol(id.c_str()), {s3_fs, 0});
    }

    set_latest_fs(get_filesystem(std::get<0>(s3_infos.back())));

    // TODO(plat1ko): DeleteBitmapTxnManager

    _memtable_flush_executor = std::make_unique<MemTableFlushExecutor>();
    // TODO(plat1ko): Use file cache disks number?
    _memtable_flush_executor->init(1);

    return Status::OK();
}

void CloudStorageEngine::stop() {
    if (_stopped) {
        return;
    }

    _stopped = true;
    _stop_background_threads_latch.count_down();

    for (auto&& t : _bg_threads) {
        if (t) {
            t->join();
        }
    }
}

bool CloudStorageEngine::stopped() {
    return _stopped;
}

Result<BaseTabletSPtr> CloudStorageEngine::get_tablet(int64_t tablet_id) {
    return _tablet_mgr->get_tablet(tablet_id, false).transform([](auto&& t) {
        return static_pointer_cast<BaseTablet>(std::move(t));
    });
}

Status CloudStorageEngine::start_bg_threads() {
    RETURN_IF_ERROR(Thread::create(
            "CloudStorageEngine", "refresh_s3_info_thread",
            [this]() { this->_refresh_s3_info_thread_callback(); }, &_bg_threads.emplace_back()));
    LOG(INFO) << "refresh s3 info thread started";

    RETURN_IF_ERROR(Thread::create(
            "CloudStorageEngine", "vacuum_stale_rowsets_thread",
            [this]() { this->_vacuum_stale_rowsets_thread_callback(); },
            &_bg_threads.emplace_back()));
    LOG(INFO) << "vacuum stale rowsets thread started";

    RETURN_IF_ERROR(Thread::create(
            "CloudStorageEngine", "sync_tablets_thread",
            [this]() { this->_sync_tablets_thread_callback(); }, &_bg_threads.emplace_back()));
    LOG(INFO) << "sync tablets thread started";

    // TODO(plat1ko): lease_compaction_thread

    // TODO(plat1ko): check_bucket_enable_versioning_thread

    return Status::OK();
}

void CloudStorageEngine::_refresh_s3_info_thread_callback() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::refresh_s3_info_interval_s))) {
        std::vector<std::tuple<std::string, S3Conf>> s3_infos;
        auto st = _meta_mgr->get_s3_info(&s3_infos);
        if (!st.ok()) {
            LOG(WARNING) << "failed to refresh object store info. err=" << st;
            continue;
        }

        CHECK(!s3_infos.empty()) << "no s3 infos";
        for (auto& [id, s3_conf] : s3_infos) {
            auto fs = get_filesystem(id);
            if (fs == nullptr) {
                LOG(INFO) << "get new s3 info: " << s3_conf.to_string() << " resource_id=" << id;
                std::shared_ptr<io::S3FileSystem> s3_fs;
                auto st = io::S3FileSystem::create(std::move(s3_conf), id, &s3_fs);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to create s3 fs. id=" << id;
                    continue;
                }

                st = s3_fs->connect();
                if (!st.ok()) {
                    LOG(WARNING) << "failed to connect s3 fs. id=" << id;
                    continue;
                }

                put_storage_resource(std::atol(id.c_str()), {s3_fs, 0});
            } else {
                auto s3_fs = std::reinterpret_pointer_cast<io::S3FileSystem>(fs);
                if (s3_fs->s3_conf().ak != s3_conf.ak || s3_fs->s3_conf().sk != s3_conf.sk ||
                    s3_fs->s3_conf().sse_enabled != s3_conf.sse_enabled) {
                    auto cur_s3_conf = s3_fs->s3_conf();
                    LOG(INFO) << "update s3 info, old: " << cur_s3_conf.to_string()
                              << " new: " << s3_conf.to_string() << " resource_id=" << id;
                    cur_s3_conf.ak = s3_conf.ak;
                    cur_s3_conf.sk = s3_conf.sk;
                    cur_s3_conf.sse_enabled = s3_conf.sse_enabled;
                    s3_fs->set_conf(std::move(cur_s3_conf));
                    st = s3_fs->connect();
                    if (!st.ok()) {
                        LOG(WARNING) << "failed to connect s3 fs. id=" << id;
                    }
                }
            }
        }

        if (auto& id = std::get<0>(s3_infos.back()); latest_fs()->id() != id) {
            set_latest_fs(get_filesystem(id));
        }
    }
}

void CloudStorageEngine::_vacuum_stale_rowsets_thread_callback() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::vacuum_stale_rowsets_interval_s))) {
        _tablet_mgr->vacuum_stale_rowsets();
    }
}

void CloudStorageEngine::_sync_tablets_thread_callback() {
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::seconds(config::schedule_sync_tablets_interval_s))) {
        _tablet_mgr->sync_tablets();
    }
}

} // namespace doris
