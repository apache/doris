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

constexpr std::string_view OLD_HEADER_PREFIX = "hdr_";

constexpr std::string_view HEADER_PREFIX = "tabletmeta_";

constexpr std::string_view PENDING_PUBLISH_INFO = "ppi_";

constexpr std::string_view DELETE_BITMAP = "dlb_";

// Helper Class for managing tablet headers of one root path.
class TabletMetaManager {
public:
    static Status get_meta(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                           TabletMetaSharedPtr tablet_meta);

    static Status get_json_meta(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                                std::string* json_meta);

    static Status save(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                       TabletMetaSharedPtr tablet_meta,
                       std::string_view header_prefix = HEADER_PREFIX);
    static Status save(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                       const std::string& meta_binary,
                       std::string_view header_prefix = HEADER_PREFIX);

    static Status remove(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash,
                         std::string_view header_prefix = HEADER_PREFIX);

    static Status traverse_headers(OlapMeta* meta,
                                   std::function<bool(long, long, std::string_view)> const& func,
                                   std::string_view header_prefix = HEADER_PREFIX);

    static Status load_json_meta(DataDir* store, const std::string& meta_path);

    static Status save_pending_publish_info(DataDir* store, TTabletId tablet_id,
                                            int64_t publish_version,
                                            const std::string& meta_binary);

    static Status remove_pending_publish_info(DataDir* store, TTabletId tablet_id,
                                              int64_t publish_version);

    static Status traverse_pending_publish(
            OlapMeta* meta, std::function<bool(int64_t, int64_t, std::string_view)> const& func);

    static Status save_delete_bitmap(DataDir* store, TTabletId tablet_id,
                                     DeleteBitmapPtr delete_bitmap, int64_t version);

    static Status traverse_delete_bitmap(
            OlapMeta* meta, std::function<bool(int64_t, int64_t, std::string_view)> const& func);

    static std::string encode_delete_bitmap_key(TTabletId tablet_id, int64_t version);
    static std::string encode_delete_bitmap_key(TTabletId tablet_id);

    static void decode_delete_bitmap_key(std::string_view enc_key, TTabletId* tablet_id,
                                         int64_t* version);
    static Status remove_old_version_delete_bitmap(DataDir* store, TTabletId tablet_id,
                                                   int64_t version);
};

} // namespace doris
