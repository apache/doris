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

#ifndef DORIS_BE_SRC_OLAP_OLAP_HEADER_MANAGER_H
#define DORIS_BE_SRC_OLAP_OLAP_HEADER_MANAGER_H

#include <string>

#include "olap/olap_header.h"
#include "olap/olap_define.h"
#include "olap/store.h"

namespace doris {

// Helper Class for managing olap table headers of one root path.
class OlapHeaderManager {
public:
    static OLAPStatus get_header(OlapStore* store, TTabletId tablet_id, TSchemaHash schema_hash, OLAPHeader* header);

    static OLAPStatus get_json_header(OlapStore* store, TTabletId tablet_id,
            TSchemaHash schema_hash, std::string* json_header);

    static OLAPStatus save(OlapStore* store, TTabletId tablet_id, TSchemaHash schema_hash, const OLAPHeader* header);

    static OLAPStatus remove(OlapStore* store, TTabletId tablet_id, TSchemaHash schema_hash);

    static OLAPStatus traverse_headers(OlapMeta* meta,
            std::function<bool(long, long, const std::string&)> const& func);

    static OLAPStatus get_header_converted(OlapStore* store, bool& flag);

    static OLAPStatus set_converted_flag(OlapStore* store);

    static OLAPStatus load_json_header(OlapStore* store, const std::string& header_path);

    static OLAPStatus dump_header(OlapStore* store, TTabletId tablet_id,
            TSchemaHash schema_hash, const std::string& path);
};

}

#endif // DORIS_BE_SRC_OLAP_OLAP_HEADER_MANAGER_H
