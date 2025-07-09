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

#include "meta-service/meta_service_tablet_stats.h"

#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include "meta-store/codec.h"
#include "meta-store/keys.h"

namespace doris::cloud {

std::string size_value(int64_t tablet_stat_data_size) {
    std::string tablet_stat_data_size_value(sizeof(tablet_stat_data_size), '\0');
    memcpy(tablet_stat_data_size_value.data(), &tablet_stat_data_size,
           sizeof(tablet_stat_data_size));
    return tablet_stat_data_size_value;
}

TEST(MetaServiceTabletStatsTest, test_get_detached_tablet_stats) {
    std::vector<std::pair<std::string, std::string>> stats_kvs;
    StatsTabletKeyInfo tablet_key_info {"instance_0", 10000, 10001, 10002, 10003};

    // key->TabletStatsPB
    TabletStatsPB tablet_stat;
    std::string tablet_stat_value;
    auto tablet_stat_key = stats_tablet_key(tablet_key_info);
    tablet_stat.SerializeToString(&tablet_stat_value);
    stats_kvs.emplace_back(tablet_stat_key, tablet_stat_value);

    // key->data_size
    auto tablet_stat_data_size_key = stats_tablet_data_size_key(tablet_key_info);
    stats_kvs.emplace_back(tablet_stat_data_size_key, size_value(100));

    // key->num_rows
    auto tablet_stat_num_rows_key = stats_tablet_num_rows_key(tablet_key_info);
    stats_kvs.emplace_back(tablet_stat_num_rows_key, size_value(10));

    // key->num_rowsets
    auto tablet_stat_num_rowsets_key = stats_tablet_num_rowsets_key(tablet_key_info);
    stats_kvs.emplace_back(tablet_stat_num_rowsets_key, size_value(1));

    // key->num_segs
    auto tablet_stat_num_segs_key = stats_tablet_num_segs_key(tablet_key_info);
    stats_kvs.emplace_back(tablet_stat_num_segs_key, size_value(1));

    // key->index_size
    auto tablet_stat_index_size_key = stats_tablet_index_size_key(tablet_key_info);
    stats_kvs.emplace_back(tablet_stat_index_size_key, size_value(50));

    // key->segment_size
    auto tablet_stat_segment_size_key = stats_tablet_segment_size_key(tablet_key_info);
    stats_kvs.emplace_back(tablet_stat_segment_size_key, size_value(50));

    TabletStats res1;
    int ret = get_detached_tablet_stats(stats_kvs, res1);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(res1.data_size, 100);
    EXPECT_EQ(res1.num_rows, 10);
    EXPECT_EQ(res1.num_rowsets, 1);
    EXPECT_EQ(res1.num_segs, 1);
    EXPECT_EQ(res1.index_size, 50);
    EXPECT_EQ(res1.segment_size, 50);

    stats_kvs.resize(5);
    TabletStats res2;
    ret = get_detached_tablet_stats(stats_kvs, res2);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(res1.data_size, 100);
    EXPECT_EQ(res1.num_rows, 10);
    EXPECT_EQ(res1.num_rowsets, 1);
    EXPECT_EQ(res1.num_segs, 1);

    stats_kvs.resize(2);
    TabletStats res3;
    ret = get_detached_tablet_stats(stats_kvs, res3);
    EXPECT_EQ(ret, 0);
    EXPECT_EQ(res1.data_size, 100);

    stats_kvs.resize(1);
    TabletStats res4;
    ret = get_detached_tablet_stats(stats_kvs, res4);
    EXPECT_EQ(ret, 0);
}

} // namespace doris::cloud
