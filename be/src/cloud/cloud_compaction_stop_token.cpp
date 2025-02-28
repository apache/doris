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

#include "cloud/cloud_compaction_stop_token.h"

#include "cloud/cloud_meta_mgr.h"
#include "cloud/config.h"
#include "common/logging.h"
#include "gen_cpp/cloud.pb.h"

namespace doris {

CloudCompactionStopToken::CloudCompactionStopToken(CloudStorageEngine& engine,
                                                   CloudTabletSPtr tablet, int64_t initiator)
        : _engine {engine}, _tablet {std::move(tablet)}, _initiator(initiator) {
    auto uuid = UUIDGenerator::instance()->next_uuid();
    std::stringstream ss;
    ss << uuid;
    _uuid = ss.str();
}

void CloudCompactionStopToken::do_lease() {
    cloud::TabletJobInfoPB job;
    auto* idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto* compaction_job = job.add_compaction();
    compaction_job->set_id(_uuid);
    using namespace std::chrono;
    int64_t lease_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count() +
                         (config::lease_compaction_interval_seconds * 4);
    compaction_job->set_lease(lease_time);
    auto st = _engine.meta_mgr().lease_tablet_job(job);
    if (!st.ok()) {
        LOG_WARNING("failed to lease compaction stop token")
                .tag("job_id", _uuid)
                .tag("delete_bitmap_lock_initiator", _initiator)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
    }
}

Status CloudCompactionStopToken::do_register() {
    int64_t base_compaction_cnt = 0;
    int64_t cumulative_compaction_cnt = 0;
    {
        std::lock_guard lock {_tablet->get_header_lock()};
        base_compaction_cnt = _tablet->base_compaction_cnt();
        cumulative_compaction_cnt = _tablet->cumulative_compaction_cnt();
    }
    cloud::TabletJobInfoPB job;
    auto* idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto* compaction_job = job.add_compaction();
    compaction_job->set_id(_uuid);
    compaction_job->set_delete_bitmap_lock_initiator(_initiator);
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(cloud::TabletCompactionJobPB::STOP_TOKEN);
    // required by MS to check if it's a valid compaction job
    compaction_job->set_base_compaction_cnt(base_compaction_cnt);
    compaction_job->set_cumulative_compaction_cnt(cumulative_compaction_cnt);
    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    compaction_job->set_expiration(now + config::compaction_timeout_seconds);
    compaction_job->set_lease(now + (config::lease_compaction_interval_seconds * 4));
    cloud::StartTabletJobResponse resp;
    auto st = _engine.meta_mgr().prepare_tablet_job(job, &resp);
    if (!st.ok()) {
        LOG_WARNING("failed to register compaction stop token")
                .tag("job_id", _uuid)
                .tag("delete_bitmap_lock_initiator", _initiator)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
    }
    return st;
}

Status CloudCompactionStopToken::do_unregister() {
    cloud::TabletJobInfoPB job;
    auto* idx = job.mutable_idx();
    idx->set_tablet_id(_tablet->tablet_id());
    idx->set_table_id(_tablet->table_id());
    idx->set_index_id(_tablet->index_id());
    idx->set_partition_id(_tablet->partition_id());
    auto* compaction_job = job.add_compaction();
    compaction_job->set_id(_uuid);
    compaction_job->set_delete_bitmap_lock_initiator(_initiator);
    compaction_job->set_initiator(BackendOptions::get_localhost() + ':' +
                                  std::to_string(config::heartbeat_service_port));
    compaction_job->set_type(cloud::TabletCompactionJobPB::STOP_TOKEN);
    auto st = _engine.meta_mgr().abort_tablet_job(job);
    if (!st.ok()) {
        LOG_WARNING("failed to unregister compaction stop token")
                .tag("job_id", _uuid)
                .tag("delete_bitmap_lock_initiator", _initiator)
                .tag("tablet_id", _tablet->tablet_id())
                .error(st);
    }
    return st;
}

int64_t CloudCompactionStopToken::initiator() const {
    return _initiator;
}
} // namespace doris
