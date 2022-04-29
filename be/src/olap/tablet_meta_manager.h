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

#pragma once

#include <string>

#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/tablet_meta.h"

namespace doris {

const std::string OLD_HEADER_PREFIX = "hdr_";

const std::string HEADER_PREFIX = "tabletmeta_";

// Helper Class for managing tablet headers of one root path.
class TabletMetaManager {
public:
    static Status get_meta(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                           TabletMetaSharedPtr tablet_meta);

    static Status get_json_meta(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                                std::string* json_meta);

    static Status save(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                       TabletMetaSharedPtr tablet_meta,
                       const string& header_prefix = "tabletmeta_");
    static Status save(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                       const std::string& meta_binary, const string& header_prefix = "tabletmeta_");

    static Status remove(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                         const string& header_prefix = "tabletmeta_");

    static Status traverse_headers(OlapMeta* meta,
                                   std::function<bool(long, long, const std::string&)> const& func,
                                   const string& header_prefix = "tabletmeta_");

    static Status load_json_meta(DataDir* store, const std::string& meta_path);
};

} // namespace doris
