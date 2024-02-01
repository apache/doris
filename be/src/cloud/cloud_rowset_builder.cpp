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

namespace doris {
using namespace ErrorCode;

CloudRowsetBuilder::CloudRowsetBuilder(CloudStorageEngine& engine, const WriteRequest& req,
                                       RuntimeProfile* profile)
        : BaseRowsetBuilder(req, profile), _engine(engine) {}

CloudRowsetBuilder::~CloudRowsetBuilder() = default;

Status CloudRowsetBuilder::init() {
    _tablet = DORIS_TRY(_engine.get_tablet(_req.tablet_id));

    // TODO(plat1ko): get rowset ids snapshot to calculate delete bitmap

    RETURN_IF_ERROR(check_tablet_version_count());

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
    context.original_tablet_schema = _tablet_schema;
    context.newest_write_timestamp = UnixSeconds();
    context.tablet_id = _req.tablet_id;
    context.index_id = _req.index_id;
    context.tablet = _tablet;
    context.write_type = DataWriteType::TYPE_DIRECT;
    // TODO(plat1ko):
    // context.mow_context = mow_context;
    context.write_file_cache = _req.write_file_cache;
    context.partial_update_info = _partial_update_info;
    // New loaded data is always written to latest shared storage
    context.fs = _engine.latest_fs();
    context.rowset_dir = _tablet->tablet_path();
    _rowset_writer = DORIS_TRY(_tablet->create_rowset_writer(context, false));

    // TODO(plat1ko):
    //_calc_delete_bitmap_token = _engine.calc_delete_bitmap_executor()->create_token();

    RETURN_IF_ERROR(_engine.meta_mgr().prepare_rowset(*_rowset_writer->rowset_meta(), true));

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
}

CloudTablet* CloudRowsetBuilder::cloud_tablet() {
    return static_cast<CloudTablet*>(_tablet.get());
}

const RowsetMetaSharedPtr& CloudRowsetBuilder::rowset_meta() {
    return _rowset_writer->rowset_meta();
}

} // namespace doris
