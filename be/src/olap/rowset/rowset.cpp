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

#include "olap/rowset/rowset.h"

namespace doris {

Rowset::Rowset(const TabletSchema *schema,
               std::string rowset_path,
               DataDir *data_dir,
               RowsetMetaSharedPtr rowset_meta)
        : _schema(schema),
         _rowset_path(std::move(rowset_path)),
         _data_dir(data_dir),
         _rowset_meta(std::move(rowset_meta)) {

    _is_pending = !_rowset_meta->has_version();
    if (_is_pending) {
        _is_cumulative = false;
    } else {
        Version version = _rowset_meta->version();
        _is_cumulative = version.first != version.second;
    }
}

void Rowset::make_visible(Version version, VersionHash version_hash) {
    _is_pending = false;
    _rowset_meta->set_version(version);
    _rowset_meta->set_version_hash(version_hash);
    _rowset_meta->set_rowset_state(VISIBLE);

    if (_rowset_meta->has_delete_predicate()) {
        _rowset_meta->mutable_delete_predicate()->set_version(version.first);
        return;
    }
    make_visible_extra(version, version_hash);
}

} // namespace doris

