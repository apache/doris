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

#include <fmt/format.h>

#include <string>
#include <string_view>

#include "olap/olap_common.h"

namespace doris {
constexpr std::string_view kBinlogPrefix = "binglog_";
constexpr std::string_view kBinlogMetaPrefix = "binlog_meta_";

inline auto make_binlog_meta_key(std::string_view tablet, int64_t version,
                                 std::string_view rowset) {
    return fmt::format("{}meta_{}_{:020d}_{}", kBinlogPrefix, tablet, version, rowset);
}

inline auto make_binlog_meta_key(std::string_view tablet, std::string_view version_str,
                                 std::string_view rowset) {
    // TODO(Drogon): use fmt::format not convert to version_num, only string with length prefix '0'
    int64_t version = std::atoll(version_str.data());
    return make_binlog_meta_key(tablet, version, rowset);
}

inline auto make_binlog_meta_key(const TabletUid& tablet_uid, int64_t version,
                                 const RowsetId& rowset_id) {
    return make_binlog_meta_key(tablet_uid.to_string(), version, rowset_id.to_string());
}

inline auto make_binlog_data_key(std::string_view tablet, int64_t version,
                                 std::string_view rowset) {
    return fmt::format("{}data_{}_{:020d}_{}", kBinlogPrefix, tablet, version, rowset);
}

inline auto make_binlog_data_key(std::string_view tablet, std::string_view version,
                                 std::string_view rowset) {
    return fmt::format("{}data_{}_{:0>20}_{}", kBinlogPrefix, tablet, version, rowset);
}

inline auto make_binlog_data_key(const TabletUid& tablet_uid, int64_t version,
                                 const RowsetId& rowset_id) {
    return make_binlog_data_key(tablet_uid.to_string(), version, rowset_id.to_string());
}

inline auto make_binlog_filename_key(const TabletUid& tablet_uid, std::string_view version) {
    return fmt::format("{}meta_{}_{:0>20}_", kBinlogPrefix, tablet_uid.to_string(), version);
}

inline auto make_binlog_meta_key_prefix(int64_t tablet_id) {
    return fmt::format("{}meta_{}_", kBinlogPrefix, tablet_id);
}

inline bool starts_with_binlog_meta(std::string_view str) {
    auto prefix = kBinlogMetaPrefix;
    if (prefix.length() > str.length()) {
        return false;
    }

    return str.compare(0, prefix.length(), prefix) == 0;
}

inline std::string get_binlog_data_key_from_meta_key(std::string_view meta_key) {
    // like "binglog_meta_6943f1585fe834b5-e542c2b83a21d0b7" => "binglog_data-6943f1585fe834b5-e542c2b83a21d0b7"
    return fmt::format("{}data_{}", kBinlogPrefix, meta_key.substr(kBinlogMetaPrefix.length()));
}
} // namespace doris
