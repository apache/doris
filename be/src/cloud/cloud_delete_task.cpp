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

#include "cloud/cloud_delete_task.h"

#include <gen_cpp/AgentService_types.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "cloud/cloud_tablet_mgr.h"
#include "common/logging.h"
#include "olap/delete_handler.h"

namespace doris {
using namespace ErrorCode;

Status CloudDeleteTask::execute(CloudStorageEngine& engine, const TPushReq& request) {
    VLOG_DEBUG << "begin to process delete data. request=" << ThriftDebugString(request);

    if (!request.__isset.transaction_id) {
        return Status::InvalidArgument("transaction_id is not set");
    }

    auto tablet = DORIS_TRY(engine.tablet_mgr().get_tablet(request.tablet_id));

    if (!request.__isset.schema_version) {
        return Status::InternalError("No valid schema version in request, tablet_id={}",
                                     tablet->tablet_id());
    }

    using namespace std::chrono;
    tablet->last_load_time_ms =
            duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    // check if version number exceed limit
    if (tablet->fetch_add_approximate_num_rowsets(0) > config::max_tablet_version_num) {
        LOG_WARNING("tablet exceeds max version num limit")
                .tag("limit", config::max_tablet_version_num)
                .tag("tablet_id", tablet->tablet_id());
        return Status::Error<TOO_MANY_VERSION>("too many versions, versions={} tablet={}",
                                               config::max_tablet_version_num, tablet->tablet_id());
    }

    // check delete condition if push for delete
    DeletePredicatePB del_pred;
    auto tablet_schema = std::make_shared<TabletSchema>();
    // FIXME(plat1ko): Rewrite columns updating logic
    tablet_schema->update_tablet_columns(*tablet->tablet_schema(), request.columns_desc);
    tablet_schema->set_schema_version(request.schema_version);
    RETURN_IF_ERROR(DeleteHandler::generate_delete_predicate(*tablet_schema,
                                                             request.delete_conditions, &del_pred));

    PUniqueId load_id;
    load_id.set_hi(0);
    load_id.set_lo(0);
    RowsetWriterContext context;
    context.storage_resource = engine.get_storage_resource(request.storage_vault_id);
    if (!context.storage_resource) {
        return Status::InternalError("vault id not found, maybe not sync, vault id {}",
                                     request.storage_vault_id);
    }

    context.txn_id = request.transaction_id;
    context.load_id = load_id;
    context.rowset_state = PREPARED;
    context.segments_overlap = OVERLAP_UNKNOWN;
    context.tablet_schema = tablet_schema;
    // ATTN: `request.timeout` is always 0 in current version, so we MUST ensure that the retention time of the
    //  recycler is much longer than the duration of the push task
    context.txn_expiration = ::time(nullptr) + request.timeout;
    auto rowset_writer = DORIS_TRY(tablet->create_rowset_writer(context, false));

    RowsetSharedPtr rowset;
    RETURN_IF_ERROR(rowset_writer->build(rowset));
    rowset->rowset_meta()->set_delete_predicate(std::move(del_pred));

    auto st = engine.meta_mgr().commit_rowset(*rowset->rowset_meta());

    // Update tablet stats
    tablet->fetch_add_approximate_num_rowsets(1);
    tablet->fetch_add_approximate_cumu_num_rowsets(1);

    // TODO(liaoxin) delete operator don't send calculate delete bitmap task from fe,
    //  then we don't need to set_txn_related_delete_bitmap here.
    if (tablet->enable_unique_key_merge_on_write()) {
        DeleteBitmapPtr delete_bitmap = std::make_shared<DeleteBitmap>(tablet->tablet_id());
        RowsetIdUnorderedSet rowset_ids;
        engine.txn_delete_bitmap_cache().set_tablet_txn_info(
                request.transaction_id, tablet->tablet_id(), delete_bitmap, rowset_ids, rowset,
                request.timeout, nullptr);
    }

    return Status::OK();
}

} // namespace doris
