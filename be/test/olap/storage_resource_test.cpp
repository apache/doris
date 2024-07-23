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

#include <gtest/gtest.h>

#include "gen_cpp/cloud.pb.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/storage_policy.h"

namespace doris {

TEST(StorageResourceTest, RemotePath) {
    StorageResource storage_resource(nullptr); // path v0
    EXPECT_EQ(storage_resource.remote_tablet_path(10005), "data/10005");

    constexpr std::string_view rowset_id_str = "0200000000001cc2224124562e7dfd4834d031b13c0210be";
    EXPECT_EQ(storage_resource.remote_segment_path(10005, rowset_id_str, 5),
              "data/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be_5.dat");
    RowsetMeta rs_meta;
    rs_meta.set_tablet_id(10005);
    RowsetId rowset_id;
    rowset_id.init(rowset_id_str);
    rs_meta.set_rowset_id(rowset_id);
    EXPECT_EQ(storage_resource.remote_segment_path(rs_meta, 5),
              "data/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be_5.dat");

    EXPECT_EQ(storage_resource.cooldown_tablet_meta_path(10005, 10006, 13),
              "data/10005/10006.13.meta");

    cloud::StorageVaultPB storage_vault_pb;
    storage_resource = StorageResource(nullptr, storage_vault_pb.path_format()); // path v0
    EXPECT_EQ(storage_resource.remote_tablet_path(10005), "data/10005");
    EXPECT_EQ(storage_resource.remote_segment_path(10005, rowset_id_str, 5),
              "data/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be_5.dat");
    EXPECT_EQ(storage_resource.remote_segment_path(rs_meta, 5),
              "data/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be_5.dat");

    auto* path_format = storage_vault_pb.mutable_path_format();
    path_format->set_path_version(1);
    path_format->set_shard_num(1000);
    storage_resource = StorageResource(nullptr, storage_vault_pb.path_format()); // path v1
    EXPECT_EQ(storage_resource.remote_tablet_path(10005), "data/611/10005");
    EXPECT_EQ(storage_resource.remote_segment_path(10005, rowset_id_str, 5),
              "data/611/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be/5.dat");
    EXPECT_EQ(storage_resource.remote_segment_path(rs_meta, 5),
              "data/611/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be/5.dat");
    EXPECT_EQ(storage_resource.cooldown_tablet_meta_path(10005, 10006, 13),
              "data/611/10005/10006.13.meta");

    path_format->set_path_version(2);
    ASSERT_DEATH(StorageResource(nullptr, storage_vault_pb.path_format()), "unknown");
}

} // namespace doris
