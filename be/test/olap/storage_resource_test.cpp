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
#include "io/fs/s3_file_system.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/storage_policy.h"

namespace doris {

TEST(StorageResourceTest, RemotePath) {
    S3Conf s3_conf {.bucket = "bucket",
                    .prefix = "prefix",
                    .client_conf = {
                            .endpoint = "endpoint",
                            .region = "region",
                            .ak = "ak",
                            .sk = "sk",
                            .token = "",
                            .bucket = "",
                            .role_arn = "",
                            .external_id = "",
                    }};
    auto res = io::S3FileSystem::create(std::move(s3_conf), io::FileSystem::TMP_FS_ID);
    ASSERT_TRUE(res.has_value()) << res.error();

    StorageResource storage_resource(res.value()); // path v0
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
    storage_resource = StorageResource(res.value(), storage_vault_pb.path_format()); // path v0
    EXPECT_EQ(storage_resource.remote_tablet_path(10005), "data/10005");
    EXPECT_EQ(storage_resource.remote_segment_path(10005, rowset_id_str, 5),
              "data/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be_5.dat");
    EXPECT_EQ(storage_resource.remote_segment_path(rs_meta, 5),
              "data/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be_5.dat");

    auto* path_format = storage_vault_pb.mutable_path_format();
    path_format->set_path_version(1);
    path_format->set_shard_num(1000);
    storage_resource = StorageResource(res.value(), storage_vault_pb.path_format()); // path v1
    EXPECT_EQ(storage_resource.remote_tablet_path(10005), "data/611/10005");
    EXPECT_EQ(storage_resource.remote_segment_path(10005, rowset_id_str, 5),
              "data/611/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be/5.dat");
    EXPECT_EQ(storage_resource.remote_segment_path(rs_meta, 5),
              "data/611/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be/5.dat");
    EXPECT_EQ(storage_resource.cooldown_tablet_meta_path(10005, 10006, 13),
              "data/611/10005/10006.13.meta");

    path_format->set_path_version(2);
    ASSERT_DEATH(StorageResource(res.value(), storage_vault_pb.path_format()), "unknown");
}

TEST(StorageResourceTest, ParseTabletIdFromPath) {
    // Test Version 0 format: data/{tablet_id}/{rowset_id}_{seg_id}.dat
    // see function StorageResource::remote_segment_path
    // fmt::format("{}/{}/{}_{}.dat", DATA_PREFIX, tablet_id, rowset_id, seg_id);
    EXPECT_EQ(
            StorageResource::parse_tablet_id_from_path(
                    "prefix_xxx/data/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be_5.dat"),
            10005);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("//data/12345/rowset_001_0.dat"), 12345);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/999999/rowset_abc_10.dat"), 999999);

    // Test Version 0 format with .idx files (v1 format)
    // see function StorageResource::remote_idx_v1_path
    // fmt::format("{}/{}/{}_{}_{}{}.idx", DATA_PREFIX, rowset.tablet_id(), rowset.rowset_id().to_string(), seg_id, index_id, suffix);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "//data/10005/0200000000001cc2224124562e7_6_6666_suffix.idx"),
              10005);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "bucket_xxx/data/12345/rowsetid_1_666_suffix.idx"),
              12345);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/999999/rowsetid_10_8888_suffix.idx"),
              999999);

    // Test Version 0 format with .idx files (v2 format)
    // see function StorageResource::remote_idx_v2_path
    // fmt::format("{}/{}/{}_{}.idx", DATA_PREFIX, rowset.tablet_id(), rowset.rowset_id().to_string(), seg_id);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "s3://prefix_bucket/data/10005/0200000000001cc2224124562e7_5.idx"),
              10005);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("/data/12345/rowset001_0.idx"), 12345);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/999999/rowsetabc_10.idx"), 999999);

    // Test Version 1 format: data/{shard}/{tablet_id}/{rowset_id}/{seg_id}.dat
    // see function StorageResource::remote_segment_path
    // fmt::format("{}/{}/{}/{}/{}.dat", DATA_PREFIX, shard_fn(rowset.tablet_id()), rowset.tablet_id(), rowset.rowset_id().to_string(), seg_id);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "prefix_xxxx/data/611/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be/"
                      "5.dat"),
              10005);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/0/12345/rowset_001/0.dat"), 12345);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("s3:///data/999/999999/rowset_abc/10.dat"),
              999999);

    // Test Version 1 format with .idx files (v1 format)
    // see function StorageResource::remote_idx_v1_path
    // fmt::format("{}/{}/{}/{}/{}_{}{}.idx", DATA_PREFIX, shard_fn(rowset.tablet_id()), rowset.tablet_id(), rowset.rowset_id().to_string(), seg_id, index_id, suffix);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "s3:///data/611/10005/0200000000001cc2224124562e7dfd4834d031b13c0210be/"
                      "5_6666_suffix.idx"),
              10005);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "prefix_bucket/data/0/12345/rowsetid/1_666_suffix.idx"),
              12345);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "data/999/999999/rowsetid/10_8888_suffix.idx"),
              999999);

    // Test Version 1 format with .idx files (v2 format)
    // see function StorageResource::remote_idx_v2_path
    // fmt::format("{}/{}/{}/{}/{}.idx", DATA_PREFIX, shard_fn(rowset.tablet_id()), rowset.tablet_id(), rowset.rowset_id().to_string(), seg_id);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "s3://prefix_bucket/data/611/10005/"
                      "0200000000001cc2224124562e7dfd4834d031b13c0210be/5.idx"),
              10005);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("/data/0/12345/rowset001/0.idx"), 12345);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/999/999999/rowsetabc/10.idx"),
              999999);

    // Test edge cases
    // fmt::format("{}/{}/{}_{}.dat", DATA_PREFIX, tablet_id, rowset_id, seg_id);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("prefix_bucket/data/0/rowset001_0.dat"),
              0);
    // fmt::format("{}/{}/{}/{}/{}.dat", DATA_PREFIX, shard_fn(rowset.tablet_id()), rowset.tablet_id(), rowset.rowset_id().to_string(), seg_id);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("/data/0/0/rowset001/0.dat"), 0);

    // Test invalid cases
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(""), std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("invalid_path"), std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/"), std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("/data/abc/rowset_001_0.dat"),
              std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "s3://prefix_bucket/data/0/abc/rowset_001/0.dat"),
              std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/10005/rowset_001_0.txt"),
              std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/10005/rowset_001_0"), std::nullopt);

    // Test paths with different slash counts (should return nullopt)
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/10005/rowset_001/extra/0.dat"),
              std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("/data/10005/rowset_001/extra/0.idx"),
              std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path(
                      "prefix_bucket/data/10005/rowset_001/extra/0.dat"),
              std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data/10005.dat"), std::nullopt);

    // Test paths without data prefix
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("10005/rowset_001_0.dat"), std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("0/12345/rowset_001/0.dat"), std::nullopt);

    // Test paths with leading slash after data prefix
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data//10005/rowset_001_0.dat"),
              std::nullopt);
    EXPECT_EQ(StorageResource::parse_tablet_id_from_path("data//0/12345/rowset_001/0.dat"),
              std::nullopt);
}

} // namespace doris
