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

#include "olap/base_tablet.h"

#include "gutil/strings/substitute.h"
#include "olap/data_dir.h"
#include "util/doris_metrics.h"
#include "util/path_util.h"
#include "util/storage_backend_mgr.h"

namespace doris {

extern MetricPrototype METRIC_query_scan_bytes;
extern MetricPrototype METRIC_query_scan_rows;
extern MetricPrototype METRIC_query_scan_count;

BaseTablet::BaseTablet(TabletMetaSharedPtr tablet_meta, const StorageParamPB& storage_param,
                       DataDir* data_dir)
        : _state(tablet_meta->tablet_state()),
          _tablet_meta(tablet_meta),
          _storage_param(storage_param),
          _schema(tablet_meta->tablet_schema()),
          _data_dir(data_dir) {
    _gen_tablet_path();

    std::stringstream ss;
    ss << _tablet_meta->tablet_id() << "." << _tablet_meta->schema_hash() << "."
       << _tablet_meta->tablet_uid().to_string();
    _full_name = ss.str();

    _metric_entity = DorisMetrics::instance()->metric_registry()->register_entity(
            strings::Substitute("Tablet.$0", tablet_id()),
            {{"tablet_id", std::to_string(tablet_id())}}, MetricEntityType::kTablet);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, query_scan_bytes);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, query_scan_rows);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, query_scan_count);
}

BaseTablet::~BaseTablet() {
    DorisMetrics::instance()->metric_registry()->deregister_entity(_metric_entity);
}

Status BaseTablet::set_tablet_state(TabletState state) {
    if (_tablet_meta->tablet_state() == TABLET_SHUTDOWN && state != TABLET_SHUTDOWN) {
        LOG(WARNING) << "could not change tablet state from shutdown to " << state;
        return Status::OLAPInternalError(OLAP_ERR_META_INVALID_ARGUMENT);
    }
    _tablet_meta->set_tablet_state(state);
    _state = state;
    return Status::OK();
}

void BaseTablet::_gen_tablet_path() {
    if (_data_dir != nullptr && _tablet_meta != nullptr) {
        FilePathDesc root_path_desc;
        root_path_desc.filepath = _data_dir->path_desc().filepath;
        root_path_desc.storage_medium =
                fs::fs_util::get_t_storage_medium(_storage_param.storage_medium());
        if (_data_dir->is_remote()) {
            root_path_desc.storage_name = _storage_param.storage_name();
            root_path_desc.remote_path =
                    StorageBackendMgr::get_root_path_from_param(_storage_param);
        }
        FilePathDescStream desc_s;
        desc_s << root_path_desc << DATA_PREFIX;
        FilePathDesc path_desc = path_util::join_path_desc_segments(
                desc_s.path_desc(), std::to_string(_tablet_meta->shard_id()));
        path_desc = path_util::join_path_desc_segments(path_desc,
                                                       std::to_string(_tablet_meta->tablet_id()));
        _tablet_path_desc = path_util::join_path_desc_segments(
                path_desc, std::to_string(_tablet_meta->schema_hash()));
        if (_tablet_path_desc.is_remote()) {
            _tablet_path_desc.remote_path += "/" + _tablet_meta->tablet_uid().to_string();
        }
    }
}

} /* namespace doris */
