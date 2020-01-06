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
               RowsetMetaSharedPtr rowset_meta)
        : _schema(schema),
         _rowset_path(std::move(rowset_path)),
         _rowset_meta(std::move(rowset_meta)),
         _refs_by_reader(0),
         _rowset_state(ROWSET_CREATED) {

    _is_pending = !_rowset_meta->has_version();
    if (_is_pending) {
        _is_cumulative = false;
    } else {
        Version version = _rowset_meta->version();
        _is_cumulative = version.first != version.second;
    }
}

OLAPStatus Rowset::load(bool use_cache) {
    // if the state is ROWSET_CLOSING, it means close() is called
    // and the rowset is already loaded, and the resource is not closed yet.
    if (_rowset_state == ROWSET_LOADED || _rowset_state == ROWSET_CLOSING) {
        return OLAP_SUCCESS;
    }
    if (_rowset_state == ROWSET_DELETE) {
        LOG(WARNING) << "can not load rowset with state ROWSET_DELETE";
        return OLAP_ERR_ROWSET_LOAD_FAILED;
    }
    return do_load(use_cache);
}

OLAPStatus Rowset::create_reader(std::shared_ptr<RowsetReader>* result) {
    if (_rowset_state == ROWSET_DELETE) {
        LOG(WARNING) << "rowset state is ROWSET_DELETE, can not create reader";
        return OLAP_ERR_ROWSET_CREATE_READER;
    }
    MutexLock _load_lock(&_lock);
    RETURN_NOT_OK(load());
    return do_create_reader(result);
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

