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
          _data_dir(data_dir) {
    _gen_tablet_path();
}

BaseTablet::~BaseTablet() {}

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

} /* namespace doris */
