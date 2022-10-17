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

#include <ctype.h>
#include <errno.h>
#include <stdio.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <filesystem>
#include <iterator>
#include <map>
#include <set>

#include "common/status.h"
#include "env/env.h"
#include "gen_cpp/Types_constants.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/storage_engine.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "runtime/thread_context.h"

using std::filesystem::path;
using std::map;
using std::nothrow;
using std::set;
using std::string;
using std::stringstream;
using std::vector;
using std::list;

namespace doris {

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
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    Status res = Status::OK();
    if (snapshot_path == nullptr) {
        LOG(WARNING) << "output parameter cannot be null";
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
    }

    TabletSharedPtr ref_tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(request.tablet_id);
    if (ref_tablet == nullptr) {
        LOG(WARNING) << "failed to get tablet. tablet=" << request.tablet_id;
        return Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
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
    // 如果请求的snapshot_path位于root/snapshot文件夹下，则认为是合法的，可以删除
    // 否则认为是非法请求，返回错误结果
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    auto stores = StorageEngine::instance()->get_stores();
    for (auto store : stores) {
        std::string abs_path;
        RETURN_WITH_WARN_IF_ERROR(Env::Default()->canonicalize(store->path(), &abs_path),
                                  Status::OLAPInternalError(OLAP_ERR_DIR_NOT_EXIST),
                                  "canonical path " + store->path() + "failed");

        if (snapshot_path.compare(0, abs_path.size(), abs_path) == 0 &&
            snapshot_path.compare(abs_path.size() + 1, SNAPSHOT_PREFIX.size(), SNAPSHOT_PREFIX) ==
                    0) {
            Env::Default()->delete_dir(snapshot_path);
            LOG(INFO) << "success to release snapshot path. [path='" << snapshot_path << "']";

            return Status::OK();
        }
    }

    LOG(WARNING) << "released snapshot path illegal. [path='" << snapshot_path << "']";
    return Status::OLAPInternalError(OLAP_ERR_CE_CMD_PARAMS_ERROR);
}

Status SnapshotManager::convert_rowset_ids(const std::string& clone_dir, int64_t tablet_id,
                                           int64_t replica_id, const int32_t& schema_hash) {
    SCOPED_CONSUME_MEM_TRACKER(_mem_tracker.get());
    Status res = Status::OK();
    // check clone dir existed
    if (!FileUtils::check_exist(clone_dir)) {
        res = Status::OLAPInternalError(OLAP_ERR_DIR_NOT_EXIST);
        LOG(WARNING) << "clone dir not existed when convert rowsetids. clone_dir=" << clone_dir;
        return res;
    }

    // load original tablet meta
    auto cloned_meta_file = fmt::format("{}/{}.hdr", clone_dir, tablet_id);
    TabletMeta cloned_tablet_meta;
    if ((res = cloned_tablet_meta.create_from_file(cloned_meta_file)) != Status::OK()) {
        LOG(WARNING) << "fail to load original tablet meta after clone. "
                     << ", cloned_meta_file=" << cloned_meta_file;
        return res;
    }
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
    new_tablet_meta_pb.set_replica_id(replica_id);
    new_tablet_meta_pb.set_schema_hash(schema_hash);
    TabletSchemaSPtr tablet_schema;
    tablet_schema =
            TabletSchemaCache::instance()->insert(new_tablet_meta_pb.schema().SerializeAsString());

    std::unordered_map<Version, RowsetMetaPB*, HashOfVersion> rs_version_map;
    std::unordered_map<RowsetId, RowsetId, HashOfRowsetId> rowset_id_mapping;
    for (auto& visible_rowset : cloned_tablet_meta_pb.rs_metas()) {
        RowsetMetaPB* rowset_meta = new_tablet_meta_pb.add_rs_metas();

        if (!visible_rowset.has_resource_id()) {
            // src be local rowset
            RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
            RETURN_NOT_OK(_rename_rowset_id(visible_rowset, clone_dir, tablet_schema, rowset_id,
                                            rowset_meta));
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
        // FIXME(cyx): Redundant?
        rowset_meta->set_tablet_id(tablet_id);
        rowset_meta->set_tablet_schema_hash(schema_hash);
        Version rowset_version = {visible_rowset.start_version(), visible_rowset.end_version()};
        rs_version_map[rowset_version] = rowset_meta;
    }

    for (auto& stale_rowset : cloned_tablet_meta_pb.stale_rs_metas()) {
        Version rowset_version = {stale_rowset.start_version(), stale_rowset.end_version()};
        auto exist_rs = rs_version_map.find(rowset_version);
        if (exist_rs != rs_version_map.end()) {
            continue;
        }
        RowsetMetaPB* rowset_meta = new_tablet_meta_pb.add_stale_rs_metas();

        if (!stale_rowset.has_resource_id()) {
            // src be local rowset
            RowsetId rowset_id = StorageEngine::instance()->next_rowset_id();
            RETURN_NOT_OK(_rename_rowset_id(stale_rowset, clone_dir, tablet_schema, rowset_id,
                                            rowset_meta));
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
        // FIXME(cyx): Redundant?
        rowset_meta->set_tablet_id(tablet_id);
        rowset_meta->set_tablet_schema_hash(schema_hash);
    }

    if (!rowset_id_mapping.empty() && cloned_tablet_meta_pb.has_delete_bitmap()) {
        auto& cloned_del_bitmap_pb = cloned_tablet_meta_pb.delete_bitmap();
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

    res = TabletMeta::save(cloned_meta_file, new_tablet_meta_pb);
    if (!res.ok()) {
        LOG(WARNING) << "fail to save converted tablet meta to dir='" << clone_dir;
        return res;
    }

    return Status::OK();
}

Status SnapshotManager::_rename_rowset_id(const RowsetMetaPB& rs_meta_pb,
                                          const std::string& new_tablet_path,
                                          TabletSchemaSPtr tablet_schema, const RowsetId& rowset_id,
                                          RowsetMetaPB* new_rs_meta_pb) {
    Status res = Status::OK();
    RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
    rowset_meta->init_from_pb(rs_meta_pb);
    RowsetSharedPtr org_rowset;
    RETURN_NOT_OK(
            RowsetFactory::create_rowset(tablet_schema, new_tablet_path, rowset_meta, &org_rowset));
    // do not use cache to load index
    // because the index file may conflict
    // and the cached fd may be invalid
    RETURN_NOT_OK(org_rowset->load(false));
    RowsetMetaSharedPtr org_rowset_meta = org_rowset->rowset_meta();
    RowsetWriterContext context;
    context.rowset_id = rowset_id;
    context.tablet_id = org_rowset_meta->tablet_id();
    context.partition_id = org_rowset_meta->partition_id();
    context.tablet_schema_hash = org_rowset_meta->tablet_schema_hash();
    context.rowset_type = org_rowset_meta->rowset_type();
    context.tablet_path = new_tablet_path;
    context.tablet_schema =
            org_rowset_meta->tablet_schema() ? org_rowset_meta->tablet_schema() : tablet_schema;
    context.rowset_state = org_rowset_meta->rowset_state();
    context.version = org_rowset_meta->version();
    context.oldest_write_timestamp = org_rowset_meta->oldest_write_timestamp();
    context.newest_write_timestamp = org_rowset_meta->newest_write_timestamp();
    // keep segments_overlap same as origin rowset
    context.segments_overlap = rowset_meta->segments_overlap();

    std::unique_ptr<RowsetWriter> rs_writer;
    RETURN_NOT_OK(RowsetFactory::create_rowset_writer(context, &rs_writer));

    res = rs_writer->add_rowset(org_rowset);
    if (!res.ok()) {
        LOG(WARNING) << "failed to add rowset "
                     << " id = " << org_rowset->rowset_id() << " to rowset " << rowset_id;
        return res;
    }
    RowsetSharedPtr new_rowset = rs_writer->build();
    if (new_rowset == nullptr) {
        LOG(WARNING) << "failed to build rowset when rename rowset id";
        return Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
    }
    RETURN_NOT_OK(new_rowset->load(false));
    new_rowset->rowset_meta()->to_rowset_pb(new_rs_meta_pb);
    org_rowset->remove();
    return Status::OK();
}

// get snapshot path: curtime.seq.timeout
// eg: 20190819221234.3.86400
Status SnapshotManager::_calc_snapshot_id_path(const TabletSharedPtr& tablet, int64_t timeout_s,
                                               std::string* out_path) {
    Status res = Status::OK();
    if (out_path == nullptr) {
        LOG(WARNING) << "output parameter cannot be null";
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
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
        RETURN_NOT_OK(rs->link_files_to(schema_hash_path, rs->rowset_id()));
    }
    return res;
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
        LOG(WARNING) << "output parameter cannot be null";
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
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

    // schema_full_path_desc.filepath:
    //      /snapshot_id_path/tablet_id/schema_hash/
    auto schema_full_path = get_schema_hash_full_path(ref_tablet, snapshot_id_path);
    // header_path:
    //      /schema_full_path/tablet_id.hdr
    auto header_path = _get_header_full_path(ref_tablet, schema_full_path);
    //      /schema_full_path/tablet_id.hdr.json
    auto json_header_path = _get_json_header_full_path(ref_tablet, schema_full_path);
    if (FileUtils::check_exist(schema_full_path)) {
        VLOG_TRACE << "remove the old schema_full_path.";
        FileUtils::remove_all(schema_full_path);
    }

    RETURN_WITH_WARN_IF_ERROR(FileUtils::create_dir(schema_full_path),
                              Status::OLAPInternalError(OLAP_ERR_CANNOT_CREATE_DIR),
                              "create path " + schema_full_path + " failed");

    string snapshot_id;
    RETURN_WITH_WARN_IF_ERROR(FileUtils::canonicalize(snapshot_id_path, &snapshot_id),
                              Status::OLAPInternalError(OLAP_ERR_CANNOT_CREATE_DIR),
                              "canonicalize path " + snapshot_id_path + " failed");

    do {
        TabletMetaSharedPtr new_tablet_meta(new (nothrow) TabletMeta());
        if (new_tablet_meta == nullptr) {
            LOG(WARNING) << "fail to malloc TabletMeta.";
            res = Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
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
            if (request.__isset.missing_version) {
                for (int64_t missed_version : request.missing_version) {
                    Version version = {missed_version, missed_version};
                    // find rowset in both rs_meta and stale_rs_meta
                    const RowsetSharedPtr rowset = ref_tablet->get_rowset_by_version(version, true);
                    if (rowset != nullptr) {
                        consistent_rowsets.push_back(rowset);
                    } else {
                        res = Status::InternalError(
                                "failed to find missed version when snapshot. tablet={}, "
                                "schema_hash={}, version={}",
                                request.tablet_id, request.schema_hash, version.to_string());
                        break;
                    }
                }

                // Take a full snapshot, will revise according to missed rowset later.
                if (ref_tablet->keys_type() == UNIQUE_KEYS &&
                    ref_tablet->enable_unique_key_merge_on_write()) {
                    delete_bitmap_snapshot = ref_tablet->tablet_meta()->delete_bitmap().snapshot(
                            ref_tablet->max_version().second);
                }
            }

            int64_t version = -1;
            if (!res.ok() || !request.__isset.missing_version) {
                /// not all missing versions are found, fall back to full snapshot.
                res = Status::OK();         // reset res
                consistent_rowsets.clear(); // reset vector

                // get latest version
                const RowsetSharedPtr last_version = ref_tablet->rowset_with_max_version();
                if (last_version == nullptr) {
                    res = Status::InternalError("tablet has not any version. path={}",
                                                ref_tablet->full_name());
                    break;
                }
                // get snapshot version, use request.version if specified
                version = last_version->end_version();
                if (request.__isset.version) {
                    if (last_version->end_version() < request.version) {
                        LOG(WARNING) << "invalid make snapshot request. "
                                     << " version=" << last_version->end_version()
                                     << " req_version=" << request.version;
                        res = Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
                        break;
                    }
                    version = request.version;
                }
                // get shortest version path
                // it very important!!!!
                // it means 0-version has to be a readable version graph
                res = ref_tablet->capture_consistent_rowsets(Version(0, version),
                                                             &consistent_rowsets);
                if (!res.ok()) {
                    LOG(WARNING) << "fail to select versions to span. res=" << res;
                    break;
                }
                *allow_incremental_clone = false;
            } else {
                version = ref_tablet->max_version().second;
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
        {
            std::unique_lock wlock(ref_tablet->get_header_lock());
            if (ref_tablet->tablet_state() == TABLET_SHUTDOWN) {
                return Status::Aborted("tablet has shutdown");
            }
            ref_tablet->update_self_owned_remote_rowsets(consistent_rowsets);
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
            res = Status::OLAPInternalError(OLAP_ERR_INVALID_SNAPSHOT_VERSION);
        }

        if (!res.ok()) {
            LOG(WARNING) << "convert rowset failed, res:" << res
                         << ", tablet:" << new_tablet_meta->tablet_id()
                         << ", schema hash:" << new_tablet_meta->schema_hash()
                         << ", snapshot_version:" << snapshot_version
                         << ", is incremental:" << request.__isset.missing_version;
            break;
        }

    } while (0);

    if (!res.ok()) {
        LOG(WARNING) << "fail to make snapshot, try to delete the snapshot path. path="
                     << snapshot_id_path.c_str();

        if (FileUtils::check_exist(snapshot_id_path)) {
            VLOG_NOTICE << "remove snapshot path. [path=" << snapshot_id_path << "]";
            FileUtils::remove_all(snapshot_id_path);
        }
    } else {
        *snapshot_path = snapshot_id;
    }

    return res;
}

} // namespace doris
