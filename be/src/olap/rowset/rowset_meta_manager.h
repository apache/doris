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

#include <string>

#include "olap/rowset/rowset_meta.h"
#include "olap/olap_meta.h"
#include "olap/new_status.h"

using std::string;

namespace doris {

// Helper class for managing rowset meta of one root path.
class RowsetMetaManager {
public:
    static bool check_rowset_meta(OlapMeta* meta, TabletUid tablet_uid, int64_t rowset_id);

    static OLAPStatus get_rowset_meta(OlapMeta* meta, TabletUid tablet_uid, int64_t rowset_id, RowsetMetaSharedPtr rowset_meta);

    static OLAPStatus get_json_rowset_meta(OlapMeta* meta, TabletUid tablet_uid, int64_t rowset_id, std::string* json_rowset_meta);

    static OLAPStatus save(OlapMeta* meta, TabletUid tablet_uid, int64_t rowset_id, RowsetMeta* rowset_meta);

    static OLAPStatus save(OlapMeta* meta, TabletUid tablet_uid, int64_t rowset_id, const string& meta_binary);

    static OLAPStatus remove(OlapMeta* meta, TabletUid tablet_uid, int64_t rowset_id);

    static OLAPStatus traverse_rowset_metas(OlapMeta* meta,
            std::function<bool(const TabletUid&, uint64_t, const std::string&)> const& func);

    static OLAPStatus load_json_rowset_meta(OlapMeta* meta, const std::string& rowset_meta_path);
};

}

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_META_MANAGER_H
