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

#include "cloud/cloud_rowset_builder.h"

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "olap/storage_policy.h"

namespace doris {
using namespace ErrorCode;

CloudRowsetBuilder::CloudRowsetBuilder(CloudStorageEngine& engine, const WriteRequest& req,
                                       RuntimeProfile* profile)
        : BaseRowsetBuilder(req, profile), _engine(engine) {}

CloudRowsetBuilder::~CloudRowsetBuilder() = default;

Status CloudRowsetBuilder::init() {
    _tablet = DORIS_TRY(_engine.get_tablet(_req.tablet_id));

    std::shared_ptr<MowContext> mow_context;
    if (_tablet->enable_unique_key_merge_on_write()) {
        auto st = std::static_pointer_cast<CloudTablet>(_tablet)->sync_rowsets();
        // sync_rowsets will return INVALID_TABLET_STATE when tablet is under alter
        if (!st.ok() && !st.is<ErrorCode::INVALID_TABLET_STATE>()) {
            return st;
        }
        RETURN_IF_ERROR(init_mow_context(mow_context));
    }
    RETURN_IF_ERROR(check_tablet_version_count());

    using namespace std::chrono;
    std::static_pointer_cast<CloudTablet>(_tablet)->last_load_time_ms =
            duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    // build tablet schema in request level
    _build_current_tablet_schema(_req.index_id, _req.table_schema_param.get(),
                                 *_tablet->tablet_schema());

    RowsetWriterContext context;
    context.txn_id = _req.txn_id;
    context.txn_expiration = _req.txn_expiration;
    context.load_id = _req.load_id;
    context.rowset_state = PREPARED;
    context.segments_overlap = OVERLAPPING;
    context.tablet_schema = _tablet_schema;
    context.newest_write_timestamp = UnixSeconds();
    context.tablet_id = _req.tablet_id;
    context.index_id = _req.index_id;
    context.tablet = _tablet;
    context.write_type = DataWriteType::TYPE_DIRECT;
    context.mow_context = mow_context;
    context.write_file_cache = _req.write_file_cache;
    context.partial_update_info = _partial_update_info;
    context.file_cache_ttl_sec = _tablet->ttl_seconds();
    context.storage_resource = _engine.get_storage_resource(_req.storage_vault_id);
    if (!context.storage_resource) {
        return Status::InternalError("vault id not found, maybe not sync, vault id {}",
                                     _req.storage_vault_id);
    }

    _rowset_writer = DORIS_TRY(_tablet->create_rowset_writer(context, false));

    _calc_delete_bitmap_token = _engine.calc_delete_bitmap_executor()->create_token();

    RETURN_IF_ERROR(_engine.meta_mgr().prepare_rowset(*_rowset_writer->rowset_meta()));

    _is_init = true;
    return Status::OK();
}

Status CloudRowsetBuilder::check_tablet_version_count() {
    int version_count = cloud_tablet()->fetch_add_approximate_num_rowsets(0);
    // TODO(plat1ko): load backoff algorithm
    if (version_count > config::max_tablet_version_num) {
        return Status::Error<TOO_MANY_VERSION>(
                "failed to init rowset builder. version count: {}, exceed limit: {}, "
                "tablet: {}",
                version_count, config::max_tablet_version_num, _tablet->tablet_id());
    }
    return Status::OK();
}

void CloudRowsetBuilder::update_tablet_stats() {
    auto* tablet = cloud_tablet();
    DCHECK(tablet);
    DCHECK(_rowset);
    tablet->fetch_add_approximate_num_rowsets(1);
    tablet->fetch_add_approximate_num_segments(_rowset->num_segments());
    tablet->fetch_add_approximate_num_rows(_rowset->num_rows());
    tablet->fetch_add_approximate_data_size(_rowset->data_disk_size());
    tablet->fetch_add_approximate_cumu_num_rowsets(1);
    tablet->fetch_add_approximate_cumu_num_deltas(_rowset->num_segments());
    tablet->write_count.fetch_add(1, std::memory_order_relaxed);
}

CloudTablet* CloudRowsetBuilder::cloud_tablet() {
    return static_cast<CloudTablet*>(_tablet.get());
}

const RowsetMetaSharedPtr& CloudRowsetBuilder::rowset_meta() {
    return _rowset_writer->rowset_meta();
}

Status CloudRowsetBuilder::set_txn_related_delete_bitmap() {
    if (_tablet->enable_unique_key_merge_on_write()) {
        if (config::enable_merge_on_write_correctness_check && _rowset->num_rows() != 0) {
            auto st = _tablet->check_delete_bitmap_correctness(
                    _delete_bitmap, _rowset->end_version() - 1, _req.txn_id, _rowset_ids);
            if (!st.ok()) {
                LOG(WARNING) << fmt::format(
                        "[tablet_id:{}][txn_id:{}][load_id:{}][partition_id:{}] "
                        "delete bitmap correctness check failed in commit phase!",
                        _req.tablet_id, _req.txn_id, UniqueId(_req.load_id).to_string(),
                        _req.partition_id);
                return st;
            }
        }
        _engine.txn_delete_bitmap_cache().set_tablet_txn_info(
                _req.txn_id, _tablet->tablet_id(), _delete_bitmap, _rowset_ids, _rowset,
                _req.txn_expiration, _partial_update_info);
    }
    return Status::OK();
}
} // namespace doris
