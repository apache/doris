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

#include "cloud/cloud_snapshot_mgr.h"

#include <gen_cpp/olap_file.pb.h>

#include <fmt/format.h>
#include <mutex>
#include <map>
#include <unordered_map>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet_mgr.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "io/fs/local_file_system.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/pb_helper.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/storage_policy.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_cache.h"
#include "olap/utils.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/slice.h"
#include "util/uid_util.h"

namespace doris {
using namespace ErrorCode;

CloudSnapshotMgr::CloudSnapshotMgr(CloudStorageEngine& engine) : _engine(engine) {
    _mem_tracker =
            MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::OTHER, "CloudSnapshotMgr");
}

Status CloudSnapshotMgr::make_snapshot(int64_t target_tablet_id, StorageResource& storage_resource,
                                       std::unordered_map<std::string, std::string>& file_mapping,
                                       bool is_restore, const Slice* slice) {
    SCOPED_ATTACH_TASK(_mem_tracker);
    if (is_restore && slice == nullptr) {
        return Status::Error<INVALID_ARGUMENT>("slice cannot be null in restore.");
    }

    CloudTabletSPtr target_tablet = DORIS_TRY(_engine.tablet_mgr().get_tablet(target_tablet_id));
    if (target_tablet == nullptr) {
        return Status::Error<TABLE_NOT_FOUND>("failed to get tablet. tablet={}", target_tablet_id);
    }

    TabletMeta tablet_meta;
    if (is_restore) {
        // 1. deserialize tablet meta from memory
        RETURN_IF_ERROR(tablet_meta.create_from_buffer((const uint8_t*)slice->data, slice->size));
        TabletMetaPB tablet_meta_pb;
        tablet_meta.to_meta_pb(&tablet_meta_pb);

        tablet_meta_pb.clear_rs_metas();
        tablet_meta_pb.clear_stale_rs_metas();
        for (auto& rs : tablet_meta.all_rs_metas()) {
            rs->to_rowset_pb(tablet_meta_pb.add_rs_metas());
        }
        for (auto& rs : tablet_meta.all_stale_rs_metas()) {
            rs->to_rowset_pb(tablet_meta_pb.add_stale_rs_metas());
        }

        TabletMetaPB new_tablet_meta_pb;
        // 2. convert rowsets
        RETURN_IF_ERROR(convert_rowsets(&new_tablet_meta_pb, tablet_meta_pb, target_tablet_id,
                                        target_tablet, storage_resource, file_mapping));
        std::string meta_binary;
        if (!new_tablet_meta_pb.SerializeToString(&meta_binary)) {
            return Status::InternalError("Failed to serialize TabletMetaPB.");
        }
        {
            std::lock_guard<std::mutex> lock(mutex);
            tablet_meta_map[target_tablet_id] = meta_binary;
        }
        return Status::OK();
    }

    // backup not implemented

    LOG(INFO) << "success to make snapshot. [tablet_id=" << target_tablet_id << "]";
    return Status::OK();
}

Status CloudSnapshotMgr::release_snapshot(int64_t tablet_id) {
    SCOPED_ATTACH_TASK(_mem_tracker);
    std::lock_guard<std::mutex> lock(mutex);
    tablet_meta_map.erase(tablet_id);
    LOG(INFO) << "success to release snapshot path. [tablet_id=" << tablet_id << "]";
    return Status::OK();
}

Status CloudSnapshotMgr::convert_rowsets(TabletMetaPB* out, const TabletMetaPB& in, int64_t tablet_id,
                                         CloudTabletSPtr& target_tablet, StorageResource& storage_resource,
                                         std::unordered_map<std::string, std::string>& file_mapping) {
    SCOPED_ATTACH_TASK(_mem_tracker);
    // deep copy
    *out = in;

    out->clear_rs_metas();
    out->clear_inc_rs_metas();
    out->clear_stale_rs_metas();
    // modify tablet id
    out->set_tablet_id(tablet_id);
    *out->mutable_tablet_uid() = TabletUid::gen_uid().to_proto();
    out->set_table_id(target_tablet->table_id());
    out->set_partition_id(target_tablet->partition_id());

    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(in.schema());

    std::unordered_map<Version, RowsetMetaPB*, HashOfVersion> rs_version_map;
    for (auto&& rowset_meta_pb : in.rs_metas()) {
        RowsetMetaPB* new_rowset_meta_pb = out->add_rs_metas();
        RETURN_IF_ERROR(_create_rowset_meta(new_rowset_meta_pb, rowset_meta_pb, tablet_id, target_tablet,
                                            storage_resource, tablet_schema, file_mapping));
        Version rowset_version = {rowset_meta_pb.start_version(), rowset_meta_pb.end_version()};
        rs_version_map[rowset_version] = new_rowset_meta_pb;
    }

    for (auto&& stale_rowset_pb : in.stale_rs_metas()) {
        Version rowset_version = {stale_rowset_pb.start_version(), stale_rowset_pb.end_version()};
        auto exist_rs = rs_version_map.find(rowset_version);
        if (exist_rs != rs_version_map.end()) {
            continue;
        }
        RowsetMetaPB* new_rowset_meta_pb = out->add_stale_rs_metas();
        RETURN_IF_ERROR(_create_rowset_meta(new_rowset_meta_pb, stale_rowset_pb, tablet_id, target_tablet,
                                            storage_resource, tablet_schema, file_mapping));
    }
    return Status::OK();
}

// TODO(xy): we should commit tablet meta in single request
Status CloudSnapshotMgr::commit_snapshot(int64_t tablet_id) {
    SCOPED_ATTACH_TASK(_mem_tracker);
    std::string meta_binary_to_commit;
    {
        std::lock_guard<std::mutex> lock(mutex);
        auto it = tablet_meta_map.find(tablet_id);
        if (it == tablet_meta_map.end()) {
            return Status::NotFound("No tablet meta found for tablet_id: " + std::to_string(tablet_id));
        }
        meta_binary_to_commit = std::move(it->second);
        tablet_meta_map.erase(it);
    }
    TabletMetaPB tablet_meta_pb;
    if (!tablet_meta_pb.ParseFromString(meta_binary_to_commit)) {
        return Status::Error<ErrorCode::PARSE_PROTOBUF_ERROR>(
                "fail to parse file content to protobuf object. file={}.");
    }
    for (auto&& rowset_meta_pb : tablet_meta_pb.rs_metas()) {
        RowsetMeta rowset_meta;
        rowset_meta.init_from_pb(rowset_meta_pb);
        RETURN_IF_ERROR(_engine.meta_mgr().commit_rowset(rowset_meta));
    }
    for (auto&& stale_rowset_pb : tablet_meta_pb.stale_rs_metas()) {
        RowsetMeta rowset_meta;
        rowset_meta.init_from_pb(stale_rowset_pb);
        RETURN_IF_ERROR(_engine.meta_mgr().commit_rowset(rowset_meta));
    }
    return Status::OK();
}

Status CloudSnapshotMgr::_create_rowset_meta(RowsetMetaPB* new_rowset_meta_pb, const RowsetMetaPB& source_meta_pb,
                                             int64_t target_tablet_id, CloudTabletSPtr& target_tablet,
                                             StorageResource& storage_resource, TabletSchemaSPtr tablet_schema,
                                             std::unordered_map<std::string, std::string>& file_mapping) {
    RowsetId dst_rs_id = _engine.next_rowset_id();
    RowsetWriterContext context;
    context.rowset_id = dst_rs_id;
    context.tablet_id = target_tablet_id;
    context.partition_id = target_tablet->partition_id();
    context.index_id = target_tablet->index_id();
    context.txn_id = boost::uuids::hash_value(UUIDGenerator::instance()->next_uuid()) &
                         std::numeric_limits<int64_t>::max();
    context.txn_expiration = 0;
    context.rowset_state = PREPARED;
    context.storage_resource = storage_resource;
    context.tablet = target_tablet;
    context.version = context.version = {source_meta_pb.start_version(), source_meta_pb.end_version()};
    context.segments_overlap = source_meta_pb.segments_overlap_pb();
    context.tablet_schema_hash = source_meta_pb.tablet_schema_hash();
    if (source_meta_pb.has_tablet_schema()) {
        context.tablet_schema = std::make_shared<TabletSchema>();
        context.tablet_schema->init_from_pb(source_meta_pb.tablet_schema());
    } else {
        context.tablet_schema = tablet_schema;
    }
    context.newest_write_timestamp = source_meta_pb.newest_write_timestamp();

    auto rs_writer = DORIS_TRY(RowsetFactory::create_rowset_writer(_engine, context, false));
    // TODO(xy): we should prepare the tablet meta in single request
    // prepare rowsets
    RETURN_IF_ERROR(_engine.meta_mgr().prepare_rowset(*rs_writer->rowset_meta()));
    rs_writer->rowset_meta()->to_rowset_pb(new_rowset_meta_pb);

    // build file mapping
    RowsetId src_rs_id;
    if (source_meta_pb.rowset_id() > 0) {
        src_rs_id.init(source_meta_pb.rowset_id());
    } else {
        src_rs_id.init(source_meta_pb.rowset_id_v2());
    }

    for (int i = 0; i < source_meta_pb.num_segments(); ++i) {
        std::string src_segment_file = fmt::format("{}_{}.dat", src_rs_id.to_string(), i);
        std::string dst_segment_file = fmt::format("{}_{}.dat", dst_rs_id.to_string(), i);
        file_mapping[src_segment_file] = dst_segment_file;
        if (context.tablet_schema->get_inverted_index_storage_format() == InvertedIndexStorageFormatPB::V1) {
            for (const auto& index : context.tablet_schema->inverted_indexes()) {
                auto index_id = index->index_id();
                std::string src_index_file =
                        InvertedIndexDescriptor::get_index_file_path_v1(
                                InvertedIndexDescriptor::get_index_file_path_prefix(src_segment_file),
                                index_id, index->get_index_suffix());
                std::string dst_index_file =
                        InvertedIndexDescriptor::get_index_file_path_v1(
                                InvertedIndexDescriptor::get_index_file_path_prefix(dst_segment_file),
                                index_id, index->get_index_suffix());
                file_mapping[src_index_file] = dst_index_file;
            }
        } else {
            if (context.tablet_schema->has_inverted_index()) {
                std::string src_index_file =
                        InvertedIndexDescriptor::get_index_file_path_v2(
                                InvertedIndexDescriptor::get_index_file_path_prefix(src_segment_file));
                std::string dst_index_file =
                        InvertedIndexDescriptor::get_index_file_path_v2(
                                InvertedIndexDescriptor::get_index_file_path_prefix(dst_segment_file));
                file_mapping[src_index_file] = dst_index_file;
            }
        }
    }

    // build rowset meta
    new_rowset_meta_pb->set_num_rows(source_meta_pb.num_rows());
    new_rowset_meta_pb->set_total_disk_size(source_meta_pb.total_disk_size());
    new_rowset_meta_pb->set_data_disk_size(source_meta_pb.data_disk_size());
    new_rowset_meta_pb->set_index_disk_size(source_meta_pb.index_disk_size());
    new_rowset_meta_pb->set_empty(source_meta_pb.num_rows() == 0);
    new_rowset_meta_pb->set_creation_time(time(nullptr));
    new_rowset_meta_pb->set_num_segments(source_meta_pb.num_segments());
    new_rowset_meta_pb->set_rowset_state(source_meta_pb.rowset_state());

    new_rowset_meta_pb->clear_segments_key_bounds();
    for (const auto& key_bound : source_meta_pb.segments_key_bounds()) {
        *new_rowset_meta_pb->add_segments_key_bounds() = key_bound;
    }
    if (source_meta_pb.has_delete_predicate()) {
        DeletePredicatePB* new_delete_condition = new_rowset_meta_pb->mutable_delete_predicate();
        *new_delete_condition = source_meta_pb.delete_predicate();
    }

    return Status::OK();
}

} // namespace doris
