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

#include "olap/data_dir.h"
#include "util/path_util.h"

namespace doris {

BaseTablet::BaseTablet(TabletMetaSharedPtr tablet_meta, DataDir* data_dir)
        : _state(tablet_meta->tablet_state()),
          _tablet_meta(tablet_meta),
          _schema(tablet_meta->tablet_schema()),
          _data_dir(data_dir),
          _is_bad(false) {
    _gen_tablet_path();
}

BaseTablet::~BaseTablet() {}


OLAPStatus BaseTablet::init() {
    return _init_once.call([this] { return _init_once_action(); });
}

// should save tablet meta to remote meta store
// if it's a primary replica
void BaseTablet::save_meta() {
    auto res = _tablet_meta->save_meta(_data_dir);
    CHECK_EQ(res, OLAP_SUCCESS) << "fail to save tablet_meta. res=" << res
                                << ", root=" << _data_dir->path();
    // User could directly update tablet schema by _tablet_meta,
    // So we need to refetch schema again
    _schema = _tablet_meta->tablet_schema();
    // TODO: update _mem_schema too?
}


OLAPStatus BaseTablet::set_tablet_state(TabletState state) {
    if (_tablet_meta->tablet_state() == TABLET_SHUTDOWN && state != TABLET_SHUTDOWN) {
        LOG(WARNING) << "could not change tablet state from shutdown to " << state;
        return OLAP_ERR_META_INVALID_ARGUMENT;
    }
    _tablet_meta->set_tablet_state(state);
    _state = state;
    return OLAP_SUCCESS;
}

void BaseTablet::_gen_tablet_path() {
    if (_data_dir != nullptr) {
        std::string path = _data_dir->path() + DATA_PREFIX;
        path = path_util::join_path_segments(path, std::to_string(_tablet_meta->shard_id()));
        path = path_util::join_path_segments(path, std::to_string(_tablet_meta->tablet_id()));
        path = path_util::join_path_segments(path, std::to_string(_tablet_meta->schema_hash()));
        _tablet_path = path;
    }
}

OLAPStatus BaseTablet::set_partition_id(int64_t partition_id) {
    return _tablet_meta->set_partition_id(partition_id);
}

TabletInfo BaseTablet::get_tablet_info() const {
    return TabletInfo(tablet_id(), schema_hash(), tablet_uid());
}


} /* namespace doris */
