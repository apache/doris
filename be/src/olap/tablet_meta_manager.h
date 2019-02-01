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

#ifndef DORIS_BE_SRC_OLAP_TABLET_META_MANAGER_H
#define DORIS_BE_SRC_OLAP_TABLET_META_MANAGER_H

#include <string>

#include "olap/tablet_meta.h"
#include "olap/olap_define.h"
#include "olap/data_dir.h"

namespace doris {

// Helper Class for managing tablet headers of one root path.
class TabletMetaManager {
public:
    static OLAPStatus get_header(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash, TabletMeta* tablet_meta);

    static OLAPStatus get_json_header(DataDir* store, TTabletId tablet_id,
            TSchemaHash schema_hash, std::string* json_header);

    static OLAPStatus save(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash, const TabletMeta* tablet_meta);
    static OLAPStatus save(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash, const std::string& meta_binary);

    static OLAPStatus remove(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash);

    static OLAPStatus traverse_headers(OlapMeta* meta,
            std::function<bool(long, long, const std::string&)> const& func);

    static OLAPStatus load_json_header(DataDir* store, const std::string& header_path);

    static OLAPStatus dump_header(DataDir* store, TTabletId tablet_id,
            TSchemaHash schema_hash, const std::string& path);
};

}

#endif // DORIS_BE_SRC_OLAP_TABLET_META_MANAGER_H
