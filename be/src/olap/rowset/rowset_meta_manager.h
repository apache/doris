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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ROWSET_META_MANAGER_H
#define DORIS_BE_SRC_OLAP_ROWSET_ROWSET_META_MANAGER_H

#include <gen_cpp/olap_file.pb.h>

#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_meta.h"

namespace doris {
class OlapMeta;
class RowsetMetaPB;
class PartialUpdateInfoPB;
} // namespace doris

namespace doris {
namespace {
const std::string ROWSET_PREFIX = "rst_";

constexpr std::string_view PARTIAL_UPDATE_INFO_PREFIX = "pui_";

} // namespace

// Helper class for managing rowset meta of one root path.
class RowsetMetaManager {
public:
    static bool check_rowset_meta(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id);
    static Status exists(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id);

    static Status get_rowset_meta(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id,
                                  RowsetMetaSharedPtr rowset_meta);

    static Status get_json_rowset_meta(OlapMeta* meta, TabletUid tablet_uid,
                                       const RowsetId& rowset_id, std::string* json_rowset_meta);

    // TODO(Drogon): refactor save && _save_with_binlog to one, adapt to ut temperately
    static Status save(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id,
                       const RowsetMetaPB& rowset_meta_pb, bool enable_binlog);

    static std::vector<std::string> get_binlog_filenames(OlapMeta* meta, TabletUid tablet_uid,
                                                         std::string_view binlog_version,
                                                         int64_t segment_idx);
    static std::pair<std::string, int64_t> get_binlog_info(OlapMeta* meta, TabletUid tablet_uid,
                                                           std::string_view binlog_version);
    static std::string get_rowset_binlog_meta(OlapMeta* meta, TabletUid tablet_uid,
                                              std::string_view binlog_version,
                                              std::string_view rowset_id);
    static Status get_rowset_binlog_metas(OlapMeta* meta, const TabletUid tablet_uid,
                                          const std::vector<int64_t>& binlog_versions,
                                          RowsetBinlogMetasPB* metas_pb);
    static Status remove_binlog(OlapMeta* meta, const std::string& suffix);
    static Status ingest_binlog_metas(OlapMeta* meta, TabletUid tablet_uid,
                                      RowsetBinlogMetasPB* metas_pb);
    static Status traverse_rowset_metas(OlapMeta* meta,
                                        std::function<bool(const TabletUid&, const RowsetId&,
                                                           std::string_view)> const& collector);
    static Status traverse_binlog_metas(
            OlapMeta* meta,
            std::function<bool(std::string_view, std::string_view, bool)> const& func);

    static Status remove(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id);

    static Status load_json_rowset_meta(OlapMeta* meta, const std::string& rowset_meta_path);

    static Status save_partial_update_info(OlapMeta* meta, int64_t tablet_id, int64_t partition_id,
                                           int64_t txn_id,
                                           const PartialUpdateInfoPB& partial_update_info_pb);
    static Status try_get_partial_update_info(OlapMeta* meta, int64_t tablet_id,
                                              int64_t partition_id, int64_t txn_id,
                                              PartialUpdateInfoPB* partial_update_info_pb);
    static Status traverse_partial_update_info(
            OlapMeta* meta,
            std::function<bool(int64_t, int64_t, int64_t, std::string_view)> const& func);
    static Status remove_partial_update_info(OlapMeta* meta, int64_t tablet_id,
                                             int64_t partition_id, int64_t txn_id);
    static Status remove_partial_update_infos(
            OlapMeta* meta, const std::vector<std::tuple<int64_t, int64_t, int64_t>>& keys);
    static Status remove_tablet_related_partial_update_info(OlapMeta* meta, int64_t tablet_id);

private:
    static Status _save(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id,
                        const RowsetMetaPB& rowset_meta_pb);
    static Status _save_with_binlog(OlapMeta* meta, TabletUid tablet_uid, const RowsetId& rowset_id,
                                    const RowsetMetaPB& rowset_meta_pb);
    static Status _get_rowset_binlog_metas(OlapMeta* meta, const TabletUid tablet_uid,
                                           const std::vector<int64_t>& binlog_versions,
                                           RowsetBinlogMetasPB* metas_pb);
    static Status _get_all_rowset_binlog_metas(OlapMeta* meta, const TabletUid tablet_uid,
                                               RowsetBinlogMetasPB* metas_pb);
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_META_MANAGER_H
