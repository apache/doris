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

#include <gen_cpp/Types_types.h>

#include <functional>
#include <string>

#include "common/status.h"
#include "gutil/stringprintf.h"
#include "olap/tablet_meta.h"

namespace doris {
class DataDir;
class OlapMeta;

const std::string OLD_HEADER_PREFIX = "hdr_";

const std::string HEADER_PREFIX = "tabletmeta_";

const std::string PENDING_PUBLISH_INFO = "ppi_";

const std::string DELETE_BITMAP = "dlb_";

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

    static Status save_pending_publish_info(DataDir* store, TTabletId tablet_id,
                                            int64_t publish_version,
                                            const std::string& meta_binary);

    static Status remove_pending_publish_info(DataDir* store, TTabletId tablet_id,
                                              int64_t publish_version);

    static Status traverse_pending_publish(
            OlapMeta* meta, std::function<bool(int64_t, int64_t, const std::string&)> const& func);

    static Status save_delete_bitmap(DataDir* store, TTabletId tablet_id,
                                     DeleteBitmapPtr delete_bitmap, int64_t version);

    static Status traverse_delete_bitmap(
            OlapMeta* meta, std::function<bool(int64_t, int64_t, const std::string&)> const& func);

    static std::string encode_delete_bitmap_key(TTabletId tablet_id, int64_t version);
    static std::string encode_delete_bitmap_key(TTabletId tablet_id);

    static void decode_delete_bitmap_key(const string& enc_key, TTabletId* tablet_id,
                                         int64_t* version);
    static Status remove_old_version_delete_bitmap(DataDir* store, TTabletId tablet_id,
                                                   int64_t version);
};

} // namespace doris
