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

#include "olap/snapshot_manager.h"

#include <fmt/format.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Types_constants.h>
#include <gen_cpp/olap_file.pb.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <ctime>
#include <filesystem>
#include <list>
#include <map>
#include <new>
#include <ostream>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <utility>

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
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_schema_cache.h"
#include "olap/utils.h"
#include "runtime/thread_context.h"
#include "util/uid_util.h"

using std::nothrow;
using std::string;
using std::stringstream;
using std::vector;

namespace doris {
using namespace ErrorCode;

SnapshotManager* SnapshotManager::_s_instance = nullptr;
std::mutex SnapshotManager::_mlock;

SnapshotManager* SnapshotManager::instance() {
    if (_s_instance == nullptr) {
        std::lock_guard<std::mutex> lock(_mlock);
        if (_s_instance == nullptr) {
            _s_instance = new SnapshotManager();
        }
    }
    return _s_instance;
}

Status SnapshotManager::make_snapshot(const TSnapshotRequest& request, string* snapshot_path,
                                      bool* allow_incremental_clone) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    Status res = Status::OK();
    if (snapshot_path == nullptr) {
        return Status::Error<INVALID_ARGUMENT>("output parameter cannot be null");
    }

    TabletSharedPtr ref_tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(request.tablet_id);
    if (ref_tablet == nullptr) {
        return Status::Error<TABLE_NOT_FOUND>("failed to get tablet. tablet={}", request.tablet_id);
    }

    res = _create_snapshot_files(ref_tablet, request, snapshot_path, allow_incremental_clone);

    if (!res.ok()) {
        LOG(WARNING) << "failed to make snapshot. res=" << res << " tablet=" << request.tablet_id;
        return res;
    }

    LOG(INFO) << "success to make snapshot. path=['" << *snapshot_path << "']";
    return res;
}

Status SnapshotManager::release_snapshot(const string& snapshot_path) {
    // If the requested snapshot_path is located in the root/snapshot folder, it is considered legal and can be deleted.
    // Otherwise, it is considered an illegal request and returns an error result.
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    auto stores = StorageEngine::instance()->get_stores();
    for (auto store : stores) {
        std::string abs_path;
        RETURN_IF_ERROR(io::global_local_filesystem()->canonicalize(store->path(), &abs_path));
        if (snapshot_path.compare(0, abs_path.size(), abs_path) == 0 &&
            snapshot_path.compare(abs_path.size() + 1, SNAPSHOT_PREFIX.size(), SNAPSHOT_PREFIX) ==
                    0) {
            RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(snapshot_path));
            LOG(INFO) << "success to release snapshot path. [path='" << snapshot_path << "']";
            return Status::OK();
        }
    }

    return Status::Error<CE_CMD_PARAMS_ERROR>("released snapshot path illegal. [path='{}']",
                                              snapshot_path);
}

Result<std::vector<PendingRowsetGuard>> SnapshotManager::convert_rowset_ids(
        const std::string& clone_dir, int64_t tablet_id, int64_t replica_id, int64_t partition_id,
        int32_t schema_hash) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker);
    std::vector<PendingRowsetGuard> guards;
    // check clone dir existed
    bool exists = true;
    RETURN_IF_ERROR_RESULT(io::global_local_filesystem()->exists(clone_dir, &exists));
    if (!exists) {
        return unexpected(Status::Error<DIR_NOT_EXIST>(
                "clone dir not existed when convert rowsetids. clone_dir={}", clone_dir));
    }

    // load original tablet meta
    auto cloned_meta_file = fmt::format("{}/{}.hdr", clone_dir, tablet_id);
    TabletMeta cloned_tablet_meta;
    RETURN_IF_ERROR_RESULT(cloned_tablet_meta.create_from_file(cloned_meta_file));
    TabletMetaPB cloned_tablet_meta_pb;
    cloned_tablet_meta.to_meta_pb(&cloned_tablet_meta_pb);

    TabletMetaPB new_tablet_meta_pb;
    new_tablet_meta_pb = cloned_tablet_meta_pb;
    new_tablet_meta_pb.clear_rs_metas();
    // inc_rs_meta is deprecated since 0.13.
    // keep this just for safety
    new_tablet_meta_pb.clear_inc_rs_metas();
    new_tablet_meta_pb.clear_stale_rs_metas();
    // should modify tablet id and schema hash because in restore process the tablet id is not
    // equal to tablet id in meta
    new_tablet_meta_pb.set_tablet_id(tablet_id);
    *new_tablet_meta_pb.mutable_tablet_uid() = TabletUid::gen_uid().to_proto();
    new_tablet_meta_pb.set_replica_id(replica_id);
    if (partition_id != -1) {
        new_tablet_meta_pb.set_partition_id(partition_id);
    }
    new_tablet_meta_pb.set_schema_hash(schema_hash);
    TabletSchemaSPtr tablet_schema;
    tablet_schema =
            TabletSchemaCache::instance()->insert(new_tablet_meta_pb.schema().SerializeAsString());

    std::unordered_map<Version, RowsetMetaPB*, HashOfVersion> rs_version_map;
    std::unordered_map<RowsetId, RowsetId, HashOfRowsetId> rowset_id_mapping;
    guards.reserve(cloned_tablet_meta_pb.rs_metas_size() +
                   cloned_tablet_meta_pb.stale_rs_metas_size());
    for (auto&& visible_rowset : cloned_tablet_meta_pb.rs_metas()) {
        RowsetMetaPB* rowset_meta = new_tablet_meta_pb.add_rs_metas();
        if (!visible_rowset.has_resource_id()) {
            // src be local rowset
            RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
            guards.push_back(StorageEngine::instance()->pending_local_rowsets().add(rowset_id));
            RETURN_IF_ERROR_RESULT(_rename_rowset_id(visible_rowset, clone_dir, tablet_schema,
                                                     rowset_id, rowset_meta));
            RowsetId src_rs_id;
            if (visible_rowset.rowset_id() > 0) {
                src_rs_id.init(visible_rowset.rowset_id());
            } else {
                src_rs_id.init(visible_rowset.rowset_id_v2());
            }
            rowset_id_mapping[src_rs_id] = rowset_id;
        } else {
            // remote rowset
            *rowset_meta = visible_rowset;
        }
        Version rowset_version = {visible_rowset.start_version(), visible_rowset.end_version()};
        rs_version_map[rowset_version] = rowset_meta;
    }

    for (auto&& stale_rowset : cloned_tablet_meta_pb.stale_rs_metas()) {
        Version rowset_version = {stale_rowset.start_version(), stale_rowset.end_version()};
        auto exist_rs = rs_version_map.find(rowset_version);
        if (exist_rs != rs_version_map.end()) {
            continue;
        }
        RowsetMetaPB* rowset_meta = new_tablet_meta_pb.add_stale_rs_metas();

        if (!stale_rowset.has_resource_id()) {
            // src be local rowset
            RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
            guards.push_back(StorageEngine::instance()->pending_local_rowsets().add(rowset_id));
            RETURN_IF_ERROR_RESULT(_rename_rowset_id(stale_rowset, clone_dir, tablet_schema,
                                                     rowset_id, rowset_meta));
            RowsetId src_rs_id;
            if (stale_rowset.rowset_id() > 0) {
                src_rs_id.init(stale_rowset.rowset_id());
            } else {
                src_rs_id.init(stale_rowset.rowset_id_v2());
            }
            rowset_id_mapping[src_rs_id] = rowset_id;
        } else {
            // remote rowset
            *rowset_meta = stale_rowset;
        }
    }

    if (!rowset_id_mapping.empty() && cloned_tablet_meta_pb.has_delete_bitmap()) {
        const auto& cloned_del_bitmap_pb = cloned_tablet_meta_pb.delete_bitmap();
        DeleteBitmapPB* new_del_bitmap_pb = new_tablet_meta_pb.mutable_delete_bitmap();
        int rst_ids_size = cloned_del_bitmap_pb.rowset_ids_size();
        for (size_t i = 0; i < rst_ids_size; ++i) {
            RowsetId rst_id;
            rst_id.init(cloned_del_bitmap_pb.rowset_ids(i));
            // It should not happen, if we can't convert some rowid in delete bitmap, the
            // data might be inconsist.
            CHECK(rowset_id_mapping.find(rst_id) != rowset_id_mapping.end())
                    << "can't find rowset_id " << rst_id.to_string() << " in convert_rowset_ids";
            new_del_bitmap_pb->set_rowset_ids(i, rowset_id_mapping[rst_id].to_string());
        }
    }

    RETURN_IF_ERROR_RESULT(TabletMeta::save(cloned_meta_file, new_tablet_meta_pb));

    return guards;
}

Status SnapshotManager::_rename_rowset_id(const RowsetMetaPB& rs_meta_pb,
                                          const std::string& new_tablet_path,
                                          TabletSchemaSPtr tablet_schema, const RowsetId& rowset_id,
                                          RowsetMetaPB* new_rs_meta_pb) {
    Status res = Status::OK();
    RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
    rowset_meta->init_from_pb(rs_meta_pb);
    RowsetSharedPtr org_rowset;
    RETURN_IF_ERROR(
            RowsetFactory::create_rowset(tablet_schema, new_tablet_path, rowset_meta, &org_rowset));
    // do not use cache to load index
    // because the index file may conflict
    // and the cached fd may be invalid
    RETURN_IF_ERROR(org_rowset->load(false));
    RowsetMetaSharedPtr org_rowset_meta = org_rowset->rowset_meta();
    RowsetWriterContext context;
    context.rowset_id = rowset_id;
    context.tablet_id = org_rowset_meta->tablet_id();
    context.partition_id = org_rowset_meta->partition_id();
    context.tablet_schema_hash = org_rowset_meta->tablet_schema_hash();
    context.rowset_type = org_rowset_meta->rowset_type();
    context.rowset_dir = new_tablet_path;
    context.tablet_schema =
            org_rowset_meta->tablet_schema() ? org_rowset_meta->tablet_schema() : tablet_schema;
    context.rowset_state = org_rowset_meta->rowset_state();
    context.version = org_rowset_meta->version();
    context.newest_write_timestamp = org_rowset_meta->newest_write_timestamp();
    // keep segments_overlap same as origin rowset
    context.segments_overlap = rowset_meta->segments_overlap();

    std::unique_ptr<RowsetWriter> rs_writer;
    RETURN_IF_ERROR(RowsetFactory::create_rowset_writer(context, false, &rs_writer));

    res = rs_writer->add_rowset(org_rowset);
    if (!res.ok()) {
        LOG(WARNING) << "failed to add rowset "
                     << " id = " << org_rowset->rowset_id() << " to rowset " << rowset_id;
        return res;
    }
    RowsetSharedPtr new_rowset;
    RETURN_NOT_OK_STATUS_WITH_WARN(rs_writer->build(new_rowset),
                                   "failed to build rowset when rename rowset id");
    RETURN_IF_ERROR(new_rowset->load(false));
    new_rowset->rowset_meta()->to_rowset_pb(new_rs_meta_pb);
    RETURN_IF_ERROR(org_rowset->remove());
    return Status::OK();
}

// get snapshot path: curtime.seq.timeout
// eg: 20190819221234.3.86400
Status SnapshotManager::_calc_snapshot_id_path(const TabletSharedPtr& tablet, int64_t timeout_s,
                                               std::string* out_path) {
    Status res = Status::OK();
    if (out_path == nullptr) {
        return Status::Error<INVALID_ARGUMENT>("output parameter cannot be null");
    }

    // get current timestamp string
    string time_str;
    if ((res = gen_timestamp_string(&time_str)) != Status::OK()) {
        LOG(WARNING) << "failed to generate time_string when move file to trash."
                     << "err code=" << res;
        return res;
    }

    std::unique_lock<std::mutex> auto_lock(_snapshot_mutex);
    uint64_t sid = _snapshot_base_id++;
    *out_path = fmt::format("{}/{}/{}.{}.{}", tablet->data_dir()->path(), SNAPSHOT_PREFIX, time_str,
                            sid, timeout_s);
    return res;
}

// prefix: /path/to/data/DATA_PREFIX/shard_id
// return: /path/to/data/DATA_PREFIX/shard_id/tablet_id/schema_hash
std::string SnapshotManager::get_schema_hash_full_path(const TabletSharedPtr& ref_tablet,
                                                       const std::string& prefix) {
    return fmt::format("{}/{}/{}", prefix, ref_tablet->tablet_id(), ref_tablet->schema_hash());
}

std::string SnapshotManager::_get_header_full_path(const TabletSharedPtr& ref_tablet,
                                                   const std::string& schema_hash_path) const {
    return fmt::format("{}/{}.hdr", schema_hash_path, ref_tablet->tablet_id());
}

std::string SnapshotManager::_get_json_header_full_path(const TabletSharedPtr& ref_tablet,
                                                        const std::string& schema_hash_path) const {
    return fmt::format("{}/{}.hdr.json", schema_hash_path, ref_tablet->tablet_id());
}

Status SnapshotManager::_link_index_and_data_files(
        const std::string& schema_hash_path, const TabletSharedPtr& ref_tablet,
        const std::vector<RowsetSharedPtr>& consistent_rowsets) {
    Status res = Status::OK();
    for (auto& rs : consistent_rowsets) {
        RETURN_IF_ERROR(rs->link_files_to(schema_hash_path, rs->rowset_id()));
    }
    return res;
}

// `rs_metas` MUST already be sorted by `RowsetMeta::comparator`
Status check_version_continuity(const std::vector<RowsetSharedPtr>& rowsets) {
    if (rowsets.size() < 2) {
        return Status::OK();
    }
    auto prev = rowsets.begin();
    for (auto it = rowsets.begin() + 1; it != rowsets.end(); ++it) {
        if ((*prev)->end_version() + 1 != (*it)->start_version()) {
            return Status::InternalError("versions are not continuity: prev={} cur={}",
                                         (*prev)->version().to_string(),
                                         (*it)->version().to_string());
        }
        prev = it;
    }
    return Status::OK();
}

Status SnapshotManager::_create_snapshot_files(const TabletSharedPtr& ref_tablet,
                                               const TSnapshotRequest& request,
                                               string* snapshot_path,
                                               bool* allow_incremental_clone) {
    int32_t snapshot_version = request.preferred_snapshot_version;
    LOG(INFO) << "receive a make snapshot request"
              << ", request detail is " << apache::thrift::ThriftDebugString(request)
              << ", snapshot_version is " << snapshot_version;
    Status res = Status::OK();
    if (snapshot_path == nullptr) {
        return Status::Error<INVALID_ARGUMENT>("output parameter cannot be null");
    }

    // snapshot_id_path:
    //      /data/shard_id/tablet_id/snapshot/time_str/id.timeout/
    int64_t timeout_s = config::snapshot_expire_time_sec;
    if (request.__isset.timeout) {
        timeout_s = request.timeout;
    }
    std::string snapshot_id_path;
    res = _calc_snapshot_id_path(ref_tablet, timeout_s, &snapshot_id_path);
    if (!res.ok()) {
        LOG(WARNING) << "failed to calc snapshot_id_path, ref tablet="
                     << ref_tablet->data_dir()->path();
        return res;
    }

    bool is_copy_binlog = request.__isset.is_copy_binlog ? request.is_copy_binlog : false;

    // schema_full_path_desc.filepath:
    //      /snapshot_id_path/tablet_id/schema_hash/
    auto schema_full_path = get_schema_hash_full_path(ref_tablet, snapshot_id_path);
    // header_path:
    //      /schema_full_path/tablet_id.hdr
    auto header_path = _get_header_full_path(ref_tablet, schema_full_path);
    //      /schema_full_path/tablet_id.hdr.json
    auto json_header_path = _get_json_header_full_path(ref_tablet, schema_full_path);
    bool exists = true;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(schema_full_path, &exists));
    if (exists) {
        VLOG_TRACE << "remove the old schema_full_path." << schema_full_path;
        RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(schema_full_path));
    }

    RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(schema_full_path));
    string snapshot_id;
    RETURN_IF_ERROR(io::global_local_filesystem()->canonicalize(snapshot_id_path, &snapshot_id));

    do {
        TabletMetaSharedPtr new_tablet_meta(new (nothrow) TabletMeta());
        if (new_tablet_meta == nullptr) {
            res = Status::Error<MEM_ALLOC_FAILED>("fail to malloc TabletMeta.");
            break;
        }
        std::vector<RowsetSharedPtr> consistent_rowsets;
        DeleteBitmap delete_bitmap_snapshot(new_tablet_meta->tablet_id());

        /// If set missing_version, try to get all missing version.
        /// If some of them not exist in tablet, we will fall back to
        /// make the full snapshot of the tablet.
        {
            std::shared_lock rdlock(ref_tablet->get_header_lock());
            if (ref_tablet->tablet_state() == TABLET_SHUTDOWN) {
                return Status::Aborted("tablet has shutdown");
            }
            bool is_single_rowset_clone =
                    (request.__isset.start_version && request.__isset.end_version);
            if (is_single_rowset_clone) {
                LOG(INFO) << "handle compaction clone make snapshot, tablet_id: "
                          << ref_tablet->tablet_id();
                Version version(request.start_version, request.end_version);
                const RowsetSharedPtr rowset = ref_tablet->get_rowset_by_version(version, false);
                if (rowset && rowset->is_local()) {
                    consistent_rowsets.push_back(rowset);
                } else {
                    LOG(WARNING) << "failed to find local version when do compaction snapshot. "
                                 << " tablet=" << request.tablet_id
                                 << " schema_hash=" << request.schema_hash
                                 << " version=" << version;
                    res = Status::InternalError(
                            "failed to find version when do compaction snapshot");
                    break;
                }
            }
            // be would definitely set it as true no matter has missed version or not
            // but it would take no effets on the following range loop
            if (!is_single_rowset_clone && request.__isset.missing_version) {
                for (int64_t missed_version : request.missing_version) {
                    Version version = {missed_version, missed_version};
                    // find rowset in both rs_meta and stale_rs_meta
                    const RowsetSharedPtr rowset = ref_tablet->get_rowset_by_version(version, true);
                    if (rowset != nullptr) {
                        if (!rowset->is_local()) {
                            // MUST make full snapshot to ensure `cooldown_meta_id` is consistent with the cooldowned rowsets after clone.
                            res = Status::Error<ErrorCode::INTERNAL_ERROR>(
                                    "missed version is a cooldowned rowset, must make full "
                                    "snapshot. missed_version={}, tablet_id={}",
                                    missed_version, ref_tablet->tablet_id());
                            break;
                        }
                        consistent_rowsets.push_back(rowset);
                    } else {
                        res = Status::InternalError(
                                "failed to find missed version when snapshot. tablet={}, "
                                "schema_hash={}, version={}",
                                request.tablet_id, request.schema_hash, version.to_string());
                        break;
                    }
                }
            }

            // be would definitely set it as true no matter has missed version or not, we could
            // just check whether the missed version is empty or not
            int64_t version = -1;
            if (!is_single_rowset_clone && (!res.ok() || request.missing_version.empty())) {
                if (!request.__isset.missing_version &&
                    ref_tablet->tablet_meta()->cooldown_meta_id().initialized()) {
                    LOG(WARNING) << "currently not support backup tablet with cooldowned remote "
                                    "data. tablet="
                                 << request.tablet_id;
                    return Status::NotSupported(
                            "currently not support backup tablet with cooldowned remote data");
                }
                /// not all missing versions are found, fall back to full snapshot.
                res = Status::OK();         // reset res
                consistent_rowsets.clear(); // reset vector

                // get latest version
                const RowsetSharedPtr last_version = ref_tablet->rowset_with_max_version();
                if (last_version == nullptr) {
                    res = Status::InternalError("tablet has not any version. path={}",
                                                ref_tablet->tablet_id());
                    break;
                }
                // get snapshot version, use request.version if specified
                version = last_version->end_version();
                if (request.__isset.version) {
                    if (last_version->end_version() < request.version) {
                        res = Status::Error<INVALID_ARGUMENT>(
                                "invalid make snapshot request. version={}, req_version={}",
                                last_version->version().to_string(), request.version);
                        break;
                    }
                    version = request.version;
                }
                if (ref_tablet->tablet_meta()->cooldown_meta_id().initialized()) {
                    // Tablet has cooldowned data, MUST pick consistent rowsets with continuous cooldowned version
                    // Get max cooldowned version
                    int64_t max_cooldowned_version = -1;
                    for (auto& [v, rs] : ref_tablet->rowset_map()) {
                        if (rs->is_local()) {
                            continue;
                        }
                        consistent_rowsets.push_back(rs);
                        max_cooldowned_version = std::max(max_cooldowned_version, v.second);
                    }
                    DCHECK_GE(max_cooldowned_version, 1) << "tablet_id=" << ref_tablet->tablet_id();
                    std::sort(consistent_rowsets.begin(), consistent_rowsets.end(),
                              Rowset::comparator);
                    res = check_version_continuity(consistent_rowsets);
                    if (res.ok() && max_cooldowned_version < version) {
                        // Pick consistent rowsets of remaining required version
                        res = ref_tablet->capture_consistent_rowsets(
                                {max_cooldowned_version + 1, version}, &consistent_rowsets);
                    }
                } else {
                    // get shortest version path
                    res = ref_tablet->capture_consistent_rowsets(Version(0, version),
                                                                 &consistent_rowsets);
                }
                if (!res.ok()) {
                    LOG(WARNING) << "fail to select versions to span. res=" << res;
                    break;
                }
                *allow_incremental_clone = false;
            } else {
                version = ref_tablet->max_version_unlocked().second;
                *allow_incremental_clone = true;
            }

            // copy the tablet meta to new_tablet_meta inside header lock
            CHECK(res.ok()) << res;
            ref_tablet->generate_tablet_meta_copy_unlocked(new_tablet_meta);
            // The delete bitmap update operation and the add_inc_rowset operation is not atomic,
            // so delete bitmap may contains some data generated by invisible rowset, we should
            // get rid of these useless bitmaps when doing snapshot.
            if (ref_tablet->keys_type() == UNIQUE_KEYS &&
                ref_tablet->enable_unique_key_merge_on_write()) {
                delete_bitmap_snapshot =
                        ref_tablet->tablet_meta()->delete_bitmap().snapshot(version);
            }
        }

        std::vector<RowsetMetaSharedPtr> rs_metas;
        for (auto& rs : consistent_rowsets) {
            if (rs->is_local()) {
                // local rowset
                res = rs->link_files_to(schema_full_path, rs->rowset_id());
                if (!res.ok()) {
                    break;
                }
            }
            rs_metas.push_back(rs->rowset_meta());
            VLOG_NOTICE << "add rowset meta to clone list. "
                        << " start version " << rs->rowset_meta()->start_version()
                        << " end version " << rs->rowset_meta()->end_version() << " empty "
                        << rs->rowset_meta()->empty();
        }
        if (!res.ok()) {
            LOG(WARNING) << "fail to create hard link. [path=" << snapshot_id_path << "]";
            break;
        }

        // The inc_rs_metas is deprecated since Doris version 0.13.
        // Clear it for safety reason.
        // Whether it is incremental or full snapshot, rowset information is stored in rs_meta.
        new_tablet_meta->revise_rs_metas(std::move(rs_metas));
        if (ref_tablet->keys_type() == UNIQUE_KEYS &&
            ref_tablet->enable_unique_key_merge_on_write()) {
            new_tablet_meta->revise_delete_bitmap_unlocked(delete_bitmap_snapshot);
        }

        if (snapshot_version == g_Types_constants.TSNAPSHOT_REQ_VERSION2) {
            res = new_tablet_meta->save(header_path);
            if (res.ok() && request.__isset.is_copy_tablet_task && request.is_copy_tablet_task) {
                res = new_tablet_meta->save_as_json(json_header_path, ref_tablet->data_dir());
            }
        } else {
            res = Status::Error<INVALID_SNAPSHOT_VERSION>(
                    "snapshot_version not equal to g_Types_constants.TSNAPSHOT_REQ_VERSION2");
        }

        if (!res.ok()) {
            LOG(WARNING) << "convert rowset failed, res:" << res
                         << ", tablet:" << new_tablet_meta->tablet_id()
                         << ", schema hash:" << new_tablet_meta->schema_hash()
                         << ", snapshot_version:" << snapshot_version
                         << ", is incremental:" << request.__isset.missing_version;
            break;
        }

    } while (false);

    // link all binlog files to snapshot path
    do {
        if (!res.ok()) {
            break;
        }

        if (!is_copy_binlog) {
            break;
        }

        RowsetBinlogMetasPB rowset_binlog_metas_pb;
        if (request.__isset.missing_version) {
            res = ref_tablet->get_rowset_binlog_metas(request.missing_version,
                                                      &rowset_binlog_metas_pb);
        } else {
            std::vector<TVersion> missing_versions;
            res = ref_tablet->get_rowset_binlog_metas(missing_versions, &rowset_binlog_metas_pb);
        }
        if (!res.ok()) {
            break;
        }
        if (rowset_binlog_metas_pb.rowset_binlog_metas_size() == 0) {
            break;
        }

        // write to pb file
        auto rowset_binlog_metas_pb_filename =
                fmt::format("{}/rowset_binlog_metas.pb", schema_full_path);
        res = write_pb(rowset_binlog_metas_pb_filename, rowset_binlog_metas_pb);
        if (!res.ok()) {
            break;
        }

        for (auto& rowset_binlog_meta : rowset_binlog_metas_pb.rowset_binlog_metas()) {
            std::string segment_file_path;
            auto num_segments = rowset_binlog_meta.num_segments();
            std::string_view rowset_id = rowset_binlog_meta.rowset_id();

            for (int64_t segment_index = 0; segment_index < num_segments; ++segment_index) {
                segment_file_path = ref_tablet->get_segment_filepath(rowset_id, segment_index);
                auto snapshot_segment_file_path =
                        fmt::format("{}/{}_{}.binlog", schema_full_path, rowset_id, segment_index);

                res = io::global_local_filesystem()->link_file(segment_file_path,
                                                               snapshot_segment_file_path);
                if (!res.ok()) {
                    LOG(WARNING) << "fail to link binlog file. [src=" << segment_file_path
                                 << ", dest=" << snapshot_segment_file_path << "]";
                    break;
                }
            }

            if (!res.ok()) {
                break;
            }
        }
    } while (false);

    if (!res.ok()) {
        LOG(WARNING) << "fail to make snapshot, try to delete the snapshot path. path="
                     << snapshot_id_path.c_str();

        bool exists = true;
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(snapshot_id_path, &exists));
        if (exists) {
            VLOG_NOTICE << "remove snapshot path. [path=" << snapshot_id_path << "]";
            RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(snapshot_id_path));
        }
    } else {
        *snapshot_path = snapshot_id;
    }

    return res;
}

} // namespace doris
