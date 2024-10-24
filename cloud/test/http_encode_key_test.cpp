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

#include <brpc/uri.h>
#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include "common/logging.h"
#include "cpp/sync_point.h"
#include "meta-service/keys.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/meta_service_http.h"

using namespace doris::cloud;

int main(int argc, char** argv) {
    if (!doris::cloud::init_glog("http_encode_key_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(HttpEncodeKeyTest, process_http_encode_key_test) {
    brpc::URI uri;
    HttpResponse http_res;

    // test unsupported key type
    uri.SetQuery("key_type", "foobarkey");
    http_res = process_http_encode_key(uri);
    EXPECT_EQ(http_res.status_code, 400);
    EXPECT_NE(http_res.body.find("key_type not supported"), std::string::npos);

    // test missing argument
    uri.SetQuery("key_type", "MetaRowsetKey");
    http_res = process_http_encode_key(uri);
    EXPECT_EQ(http_res.status_code, 400);
    EXPECT_NE(http_res.body.find("instance_id is not given or empty"), std::string::npos)
            << http_res.body;

    // clang-format off
    std::string unicode_res = R"(┌─────────────────────────────────────────────────────────────────────────────────────── 0. key space: 1
│ ┌───────────────────────────────────────────────────────────────────────────────────── 1. meta
│ │             ┌─────────────────────────────────────────────────────────────────────── 2. gavin-instance
│ │             │                                 ┌───────────────────────────────────── 3. rowset
│ │             │                                 │                 ┌─────────────────── 4. 10086
│ │             │                                 │                 │                 ┌─ 5. 10010
│ │             │                                 │                 │                 │ 
▼ ▼             ▼                                 ▼                 ▼                 ▼ 
01106d657461000110676176696e2d696e7374616e6365000110726f77736574000112000000000000276612000000000000271a
\x01\x10\x6d\x65\x74\x61\x00\x01\x10\x67\x61\x76\x69\x6e\x2d\x69\x6e\x73\x74\x61\x6e\x63\x65\x00\x01\x10\x72\x6f\x77\x73\x65\x74\x00\x01\x12\x00\x00\x00\x00\x00\x00\x27\x66\x12\x00\x00\x00\x00\x00\x00\x27\x1a
)";

    std::string nonunicode_res = R"(/--------------------------------------------------------------------------------------- 0. key space: 1
| /------------------------------------------------------------------------------------- 1. meta
| |             /----------------------------------------------------------------------- 2. gavin-instance
| |             |                                 /------------------------------------- 3. rowset
| |             |                                 |                 /------------------- 4. 10086
| |             |                                 |                 |                 /- 5. 10010
| |             |                                 |                 |                 | 
v v             v                                 v                 v                 v 
01106d657461000110676176696e2d696e7374616e6365000110726f77736574000112000000000000276612000000000000271a
\x01\x10\x6d\x65\x74\x61\x00\x01\x10\x67\x61\x76\x69\x6e\x2d\x69\x6e\x73\x74\x61\x6e\x63\x65\x00\x01\x10\x72\x6f\x77\x73\x65\x74\x00\x01\x12\x00\x00\x00\x00\x00\x00\x27\x66\x12\x00\x00\x00\x00\x00\x00\x27\x1a
)";
    // clang-format on

    // test normal path, with unicode
    uri.SetQuery("instance_id", "gavin-instance");
    uri.SetQuery("tablet_id", "10086");
    uri.SetQuery("version", "10010");
    http_res = process_http_encode_key(uri);
    EXPECT_EQ(http_res.status_code, 200);
    EXPECT_EQ(http_res.body, unicode_res);

    // test normal path, with non-unicode
    uri.SetQuery("unicode", "false");
    http_res = process_http_encode_key(uri);
    EXPECT_EQ(http_res.status_code, 200);
    EXPECT_EQ(http_res.body, nonunicode_res);

    // test empty body branch
    auto sp = doris::SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { doris::SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("process_http_encode_key::empty_body", [](auto&& args) {
        auto* body = doris::try_any_cast<std::string*>(args[0]);
        body->clear();
    });
    sp->enable_processing();

    http_res = process_http_encode_key(uri);
    EXPECT_EQ(http_res.status_code, 400);
    EXPECT_NE(http_res.body.find("failed to decode encoded key"), std::string::npos);
}

struct Input {
    std::string_view key_type;
    std::string_view param;
    std::vector<std::string> key;
    std::function<std::vector<std::string>()> gen_value;
    std::string_view value;
};

// clang-format off
static auto test_inputs = std::array {
    Input {
        "InstanceKey",
        "instance_id=gavin-instance",
        {"0110696e7374616e6365000110676176696e2d696e7374616e63650001"},
        []() -> std::vector<std::string> {
            InstanceInfoPB pb;
            pb.set_instance_id("gavin-instance");
            return {pb.SerializeAsString()};
        },
        R"({"instance_id":"gavin-instance"})",
    },
    Input {
        "TxnLabelKey",
        "instance_id=gavin-instance&db_id=10086&label=test-label",
        {"011074786e000110676176696e2d696e7374616e636500011074786e5f6c6162656c000112000000000000276610746573742d6c6162656c0001"},
        []() -> std::vector<std::string> {
            TxnLabelPB pb;
            pb.add_txn_ids(123456789);
            auto val = pb.SerializeAsString();
            MemTxnKv::gen_version_timestamp(123456790, 0, &val);
            return {val};
        },
        R"({"txn_ids":["123456789"]}
txn_id=126419752960)",
    },
    Input {
        "TxnInfoKey",
        "instance_id=gavin-instance&db_id=10086&txn_id=10010",
        {"011074786e000110676176696e2d696e7374616e636500011074786e5f696e666f000112000000000000276612000000000000271a"},
        []() -> std::vector<std::string> {
            TxnInfoPB pb;
            pb.set_db_id(10086);
            pb.set_txn_id(10010);
            return {pb.SerializeAsString()};
        },
        R"({"db_id":"10086","txn_id":"10010"})",
    },
    Input {
        "TxnIndexKey",
        "instance_id=gavin-instance&txn_id=10086",
        {"011074786e000110676176696e2d696e7374616e636500011074786e5f696e6465780001120000000000002766"},
        []() -> std::vector<std::string> {
            TxnIndexPB pb;
            pb.mutable_tablet_index()->set_db_id(10086);
            return {pb.SerializeAsString()};
        },
        R"({"tablet_index":{"db_id":"10086"}})",
    },
    Input {
        "TxnRunningKey",
        "instance_id=gavin-instance&db_id=10086&txn_id=10010",
        {"011074786e000110676176696e2d696e7374616e636500011074786e5f72756e6e696e67000112000000000000276612000000000000271a"},
        []() -> std::vector<std::string> {
            TxnRunningPB pb;
            pb.add_table_ids(10001);
            return {pb.SerializeAsString()};
        },
        R"({"table_ids":["10001"]})",
    },
    Input {
        "PartitionVersionKey",
        "instance_id=gavin-instance&db_id=10086&tbl_id=10010&partition_id=10000",
        {"011076657273696f6e000110676176696e2d696e7374616e6365000110706172746974696f6e000112000000000000276612000000000000271a120000000000002710"},
        []() -> std::vector<std::string> {
            VersionPB pb;
            pb.set_version(10);
            return {pb.SerializeAsString()};
        },
        R"({"version":"10"})",
    },
    Input {
        "TableVersionKey",
        "instance_id=gavin-instance&db_id=10086&tbl_id=10010",
        {"011076657273696f6e000110676176696e2d696e7374616e63650001107461626c65000112000000000000276612000000000000271a"},
        []() -> std::vector<std::string> {
            VersionPB pb;
            pb.set_version(10);
            return {pb.SerializeAsString()};
        },
        R"({"version":"10"})",
    },
    Input {
        "MetaRowsetKey",
        "instance_id=gavin-instance&tablet_id=10086&version=10010",
        {"01106d657461000110676176696e2d696e7374616e6365000110726f77736574000112000000000000276612000000000000271a"},
        []() -> std::vector<std::string> {
            doris::RowsetMetaCloudPB pb;
            pb.set_rowset_id(0);
            pb.set_rowset_id_v2("rowset_id_1");
            pb.set_tablet_id(10086);
            pb.set_start_version(10010);
            pb.set_end_version(10010);
            return {pb.SerializeAsString()};
        },
        R"({"rowset_id":"0","tablet_id":"10086","start_version":"10010","end_version":"10010","rowset_id_v2":"rowset_id_1"})",
    },
    Input {
        "MetaRowsetTmpKey",
        "instance_id=gavin-instance&txn_id=10086&tablet_id=10010",
        {"01106d657461000110676176696e2d696e7374616e6365000110726f777365745f746d70000112000000000000276612000000000000271a"},
        []() -> std::vector<std::string> {
            doris::RowsetMetaCloudPB pb;
            pb.set_rowset_id(0);
            pb.set_rowset_id_v2("rowset_id_1");
            pb.set_tablet_id(10010);
            pb.set_txn_id(10086);
            pb.set_start_version(2);
            pb.set_end_version(2);
            return {pb.SerializeAsString()};
        },
        R"({"rowset_id":"0","tablet_id":"10010","txn_id":"10086","start_version":"2","end_version":"2","rowset_id_v2":"rowset_id_1"})",
    },
    Input {
        "MetaTabletKey",
        "instance_id=gavin-instance&table_id=10086&index_id=100010&part_id=10000&tablet_id=1008601",
        {"01106d657461000110676176696e2d696e7374616e63650001107461626c657400011200000000000027661200000000000186aa1200000000000027101200000000000f63d9"},
        []() -> std::vector<std::string> {
            doris::TabletMetaCloudPB pb;
            pb.set_table_id(10086);
            pb.set_index_id(100010);
            pb.set_partition_id(10000);
            pb.set_tablet_id(1008601);
            return {pb.SerializeAsString()};
        },
        R"({"table_id":"10086","partition_id":"10000","tablet_id":"1008601","index_id":"100010"})",
    },
    Input {
        "MetaTabletIdxKey",
        "instance_id=gavin-instance&tablet_id=10086",
        {"01106d657461000110676176696e2d696e7374616e63650001107461626c65745f696e6465780001120000000000002766"},
        []() -> std::vector<std::string> {
            TabletIndexPB pb;
            pb.set_table_id(10006);
            pb.set_index_id(100010);
            pb.set_partition_id(10000);
            pb.set_tablet_id(10086);
            return {pb.SerializeAsString()};
        },
        R"({"table_id":"10006","index_id":"100010","partition_id":"10000","tablet_id":"10086"})",
    },
    Input {
        "RecycleIndexKey",
        "instance_id=gavin-instance&index_id=10086",
        {"011072656379636c65000110676176696e2d696e7374616e6365000110696e6465780001120000000000002766"},
        []() -> std::vector<std::string> {
            RecycleIndexPB pb;
            pb.set_creation_time(12345);
            pb.set_table_id(10000);
            return {pb.SerializeAsString()};
        },
        R"({"table_id":"10000","creation_time":"12345"})",
    },
    Input {
        "RecyclePartKey",
        "instance_id=gavin-instance&part_id=10086",
        {"011072656379636c65000110676176696e2d696e7374616e6365000110706172746974696f6e0001120000000000002766"},
        []() -> std::vector<std::string> {
            RecyclePartitionPB pb;
            pb.set_creation_time(12345);
            pb.set_table_id(10000);
            pb.add_index_id(10001);
            return {pb.SerializeAsString()};
        },
        R"({"table_id":"10000","index_id":["10001"],"creation_time":"12345"})",
    },
    Input {
        "RecycleRowsetKey",
        "instance_id=gavin-instance&tablet_id=10086&rowset_id=10010",
        {"011072656379636c65000110676176696e2d696e7374616e6365000110726f7773657400011200000000000027661031303031300001"},
        []() -> std::vector<std::string> {
            RecycleRowsetPB pb;
            pb.set_creation_time(12345);
            auto rs = pb.mutable_rowset_meta();
            rs->set_rowset_id(0);
            rs->set_rowset_id_v2("10010");
            rs->set_tablet_id(10086);
            return {pb.SerializeAsString()};
        },
        R"({"creation_time":"12345","rowset_meta":{"rowset_id":"0","tablet_id":"10086","rowset_id_v2":"10010"}})",
    },
    Input {
        "RecycleTxnKey",
        "instance_id=gavin-instance&db_id=10086&txn_id=10010",
        {"011072656379636c65000110676176696e2d696e7374616e636500011074786e000112000000000000276612000000000000271a"},
        []() -> std::vector<std::string> {
            RecycleTxnPB pb;
            pb.set_label("label_1");
            pb.set_creation_time(12345);
            return {pb.SerializeAsString()};
        },
        R"({"creation_time":"12345","label":"label_1"})",
    },
    Input { // aggregated_stats + full detached_stats, there are 5 KVs in total
        "StatsTabletKey",
        "instance_id=gavin-instance&table_id=10086&index_id=10010&part_id=10000&tablet_id=1008601",
        {
            "01107374617473000110676176696e2d696e7374616e63650001107461626c6574000112000000000000276612000000000000271a1200000000000027101200000000000f63d9",
            "01107374617473000110676176696e2d696e7374616e63650001107461626c6574000112000000000000276612000000000000271a1200000000000027101200000000000f63d910646174615f73697a650001",
            "01107374617473000110676176696e2d696e7374616e63650001107461626c6574000112000000000000276612000000000000271a1200000000000027101200000000000f63d9106e756d5f726f77730001",
            "01107374617473000110676176696e2d696e7374616e63650001107461626c6574000112000000000000276612000000000000271a1200000000000027101200000000000f63d9106e756d5f726f77736574730001",
            "01107374617473000110676176696e2d696e7374616e63650001107461626c6574000112000000000000276612000000000000271a1200000000000027101200000000000f63d9106e756d5f736567730001",
        },
        []() -> std::vector<std::string> {
            TabletStatsPB pb;
            auto idx = pb.mutable_idx();
            idx->set_table_id(10086);
            idx->set_index_id(100010);
            idx->set_partition_id(10000);
            idx->set_tablet_id(1008601);
            pb.set_data_size(1);
            pb.set_num_rows(10);
            pb.set_num_rowsets(11);
            pb.set_num_segments(12);
            return {pb.SerializeAsString(), {"\x01\x00\x00\x00\x00\x00\x00\x00",8}, {"\x02\x00\x00\x00\x00\x00\x00\x00",8}, {"\x03\x00\x00\x00\x00\x00\x00\x00",8}, {"\x04\x00\x00\x00\x00\x00\x00\x00",8}};
        },
        R"(aggregated_stats: {"idx":{"table_id":"10086","index_id":"100010","partition_id":"10000","tablet_id":"1008601"},"data_size":"1","num_rows":"10","num_rowsets":"11","num_segments":"12"}
detached_stats: {"data_size":"1","num_rows":"2","num_rowsets":"3","num_segments":"4"}
merged_stats: {"idx":{"table_id":"10086","index_id":"100010","partition_id":"10000","tablet_id":"1008601"},"data_size":"2","num_rows":"12","num_rowsets":"14","num_segments":"16"}
)",
    },
    Input { // aggregated_stats + half detached_stats (num_segs == 0, there is num_rowsets detached stats)
        "StatsTabletKey",
        "instance_id=gavin-instance&table_id=10086&index_id=10010&part_id=10000&tablet_id=1008602",
        {
            "01107374617473000110676176696e2d696e7374616e63650001107461626c6574000112000000000000276612000000000000271a1200000000000027101200000000000f63da",
            "01107374617473000110676176696e2d696e7374616e63650001107461626c6574000112000000000000276612000000000000271a1200000000000027101200000000000f63da106e756d5f726f77736574730001",
        },
        []() -> std::vector<std::string> {
            TabletStatsPB pb;
            auto idx = pb.mutable_idx();
            idx->set_table_id(10086);
            idx->set_index_id(100010);
            idx->set_partition_id(10000);
            idx->set_tablet_id(1008602);
            pb.set_data_size(1);
            pb.set_num_rows(10);
            pb.set_num_rowsets(11);
            pb.set_num_segments(12);
            return {pb.SerializeAsString(), {"\x03\x00\x00\x00\x00\x00\x00\x00",8}};
        },
        R"(aggregated_stats: {"idx":{"table_id":"10086","index_id":"100010","partition_id":"10000","tablet_id":"1008602"},"data_size":"1","num_rows":"10","num_rowsets":"11","num_segments":"12"}
detached_stats: {"data_size":"0","num_rows":"0","num_rowsets":"3","num_segments":"0"}
merged_stats: {"idx":{"table_id":"10086","index_id":"100010","partition_id":"10000","tablet_id":"1008602"},"data_size":"1","num_rows":"10","num_rowsets":"14","num_segments":"12"}
)",
    },
    Input { // aggregated_stats only, the legacy
        "StatsTabletKey",
        "instance_id=gavin-instance&table_id=10086&index_id=10010&part_id=10000&tablet_id=1008603",
        {
            "01107374617473000110676176696e2d696e7374616e63650001107461626c6574000112000000000000276612000000000000271a1200000000000027101200000000000f63db",
        },
        []() -> std::vector<std::string> {
            TabletStatsPB pb;
            auto idx = pb.mutable_idx();
            idx->set_table_id(10086);
            idx->set_index_id(100010);
            idx->set_partition_id(10000);
            idx->set_tablet_id(1008602);
            pb.set_data_size(1);
            pb.set_num_rows(10);
            pb.set_num_rowsets(11);
            pb.set_num_segments(12);
            return {pb.SerializeAsString()};
        },
        R"(aggregated_stats: {"idx":{"table_id":"10086","index_id":"100010","partition_id":"10000","tablet_id":"1008602"},"data_size":"1","num_rows":"10","num_rowsets":"11","num_segments":"12"}
detached_stats: {"data_size":"0","num_rows":"0","num_rowsets":"0","num_segments":"0"}
merged_stats: {"idx":{"table_id":"10086","index_id":"100010","partition_id":"10000","tablet_id":"1008602"},"data_size":"1","num_rows":"10","num_rowsets":"11","num_segments":"12"}
)",
    },
    Input {
        "JobTabletKey",
        "instance_id=gavin-instance&table_id=10086&index_id=10010&part_id=10000&tablet_id=1008601",
        {"01106a6f62000110676176696e2d696e7374616e63650001107461626c6574000112000000000000276612000000000000271a1200000000000027101200000000000f63d9"},
        []() -> std::vector<std::string> {
            TabletJobInfoPB pb;
            auto idx = pb.mutable_idx();
            idx->set_table_id(10086);
            idx->set_index_id(100010);
            idx->set_partition_id(10000);
            idx->set_tablet_id(1008601);
            auto c = pb.add_compaction();
            c->set_id("compaction_1");
            return {pb.SerializeAsString()};
        },
        R"({"idx":{"table_id":"10086","index_id":"100010","partition_id":"10000","tablet_id":"1008601"},"compaction":[{"id":"compaction_1"}]})",
    },
    Input {
        "CopyJobKey",
        "instance_id=gavin-instance&stage_id=10086&table_id=10010&copy_id=10000&group_id=1008601",
        {"0110636f7079000110676176696e2d696e7374616e63650001106a6f620001103130303836000112000000000000271a10313030303000011200000000000f63d9"},
        []() -> std::vector<std::string> {
            CopyJobPB pb;
            pb.set_stage_type(StagePB::EXTERNAL);
            pb.set_start_time_ms(12345);
            return {pb.SerializeAsString()};
        },
        R"({"stage_type":"EXTERNAL","start_time_ms":"12345"})",
    },
    Input {
        "CopyFileKey",
        "instance_id=gavin-instance&stage_id=10086&table_id=10010&obj_key=10000&obj_etag=1008601",
        {"0110636f7079000110676176696e2d696e7374616e63650001106c6f6164696e675f66696c650001103130303836000112000000000000271a103130303030000110313030383630310001"},
        []() -> std::vector<std::string> {
            CopyFilePB pb;
            pb.set_copy_id("copy_id_1");
            pb.set_group_id(10000);
            return {pb.SerializeAsString()};
        },
        R"({"copy_id":"copy_id_1","group_id":10000})",
    },
    Input {
        "RecycleStageKey",
        "instance_id=gavin-instance&stage_id=10086",
        {"011072656379636c65000110676176696e2d696e7374616e6365000110737461676500011031303038360001"},
        []() -> std::vector<std::string> {
            RecycleStagePB pb;
            pb.set_instance_id("gavin-instance");
            pb.set_reason("reason");
            return {pb.SerializeAsString()};
        },
        R"({"instance_id":"gavin-instance","reason":"reason"})",
    },
    Input {
        "JobRecycleKey",
        "instance_id=gavin-instance",
        {"01106a6f62000110676176696e2d696e7374616e6365000110636865636b0001"},
        []() -> std::vector<std::string> {
            JobRecyclePB pb;
            pb.set_instance_id("gavin-instance");
            pb.set_ip_port("host_1");
            return {pb.SerializeAsString()};
        },
        R"({"instance_id":"gavin-instance","ip_port":"host_1"})",
    },
    Input {
        "MetaSchemaKey",
        "instance_id=gavin-instance&index_id=10086&schema_version=10010",
        {"01106d657461000110676176696e2d696e7374616e6365000110736368656d61000112000000000000276612000000000000271a"},
        []() -> std::vector<std::string> {
            doris::TabletSchemaCloudPB pb;
            pb.set_schema_version(10010);
            auto col = pb.add_column();
            col->set_unique_id(6789);
            col->set_name("col_1");
            col->set_type("INT");
            return {pb.SerializeAsString()};
        },
        R"({"column":[{"unique_id":6789,"name":"col_1","type":"INT"}],"schema_version":10010})",
    },
    Input {
        "MetaDeleteBitmap",
        "instance_id=gavin-instance&tablet_id=10086&rowest_id=10010&version=10000&seg_id=1008601",
        {"01106d657461000110676176696e2d696e7374616e636500011064656c6574655f6269746d6170000112000000000000276610313030313000011200000000000027101200000000000f63d9"},
        []() -> std::vector<std::string> {
            return {"abcdefg"};
        },
        "61626364656667",
    },
    Input {
        "MetaDeleteBitmapUpdateLock",
        "instance_id=gavin-instance&table_id=10086&partition_id=10010",
        {"01106d657461000110676176696e2d696e7374616e636500011064656c6574655f6269746d61705f6c6f636b000112000000000000276612000000000000271a"},
        []() -> std::vector<std::string> {
            DeleteBitmapUpdateLockPB pb;
            pb.set_lock_id(12345);
            pb.add_initiators(114115);
            return {pb.SerializeAsString()};
        },
        R"({"lock_id":"12345","initiators":["114115"]})",
    },
    Input {
        "MetaPendingDeleteBitmap",
        "instance_id=gavin-instance&tablet_id=10086",
        {"01106d657461000110676176696e2d696e7374616e636500011064656c6574655f6269746d61705f70656e64696e670001120000000000002766"},
        []() -> std::vector<std::string> {
            PendingDeleteBitmapPB pb;
            pb.add_delete_bitmap_keys("key_1");
            return {pb.SerializeAsString()};
        },
        R"({"delete_bitmap_keys":["a2V5XzE="]})",
    },
    Input {
        "RLJobProgressKey",
        "instance_id=gavin-instance&db_id=10086&job_id=10010",
        {"01106a6f62000110676176696e2d696e7374616e6365000110726f7574696e655f6c6f61645f70726f6772657373000112000000000000276612000000000000271a"},
        []() -> std::vector<std::string> {
            RoutineLoadProgressPB pb;
            auto map = pb.mutable_partition_to_offset();
            map->insert({1000, 1234});
            return {pb.SerializeAsString()};
        },
        R"({"partition_to_offset":{"1000":"1234"}})",
    },
    Input {
        "MetaServiceRegistryKey",
        "",
        {"021073797374656d0001106d6574612d7365727669636500011072656769737472790001"},
        []() -> std::vector<std::string> {
            ServiceRegistryPB pb;
            auto i = pb.add_items();
            i->set_host("host_1");
            i->set_ctime_ms(123456);
            return {pb.SerializeAsString()};
        },
        R"({"items":[{"ctime_ms":"123456","host":"host_1"}]})",
    },
    Input {
        "MetaServiceArnInfoKey",
        "",
        {"021073797374656d0001106d6574612d7365727669636500011061726e5f696e666f0001"},
        []() -> std::vector<std::string> {
            RamUserPB pb;
            pb.set_user_id("user_1");
            pb.set_ak("ak");
            pb.set_sk("sk");
            return {pb.SerializeAsString()};
        },
        R"({"user_id":"user_1","ak":"ak","sk":"sk"})",
    },
    Input {
        "MetaServiceEncryptionKey",
        "",
        {"021073797374656d0001106d6574612d73657276696365000110656e6372797074696f6e5f6b65795f696e666f0001"},
        []() -> std::vector<std::string> {
            EncryptionKeyInfoPB pb;
            auto i = pb.add_items();
            i->set_key_id(23456);
            i->set_key("key_1");
            return {pb.SerializeAsString()};
        },
        R"({"items":[{"key_id":"23456","key":"key_1"}]})",
    },
};
// clang-format on

TEST(HttpEncodeKeyTest, process_http_encode_key_test_cover_all_template) {
    brpc::URI uri;
    (void)uri.get_query_map(); // initialize query map
    for (auto&& input : test_inputs) {
        std::stringstream url;
        url << "localhost:5000/MetaService/http?key_type=" << input.key_type;
        if (!input.param.empty()) {
            url << "&" << input.param;
        }
        // std::cout << url.str() << std::endl;
        EXPECT_EQ(uri.SetHttpURL(url.str()), 0); // clear and set query string
        auto http_res = process_http_encode_key(uri);
        EXPECT_EQ(http_res.status_code, 200);
        EXPECT_NE(http_res.body.find(input.key[0]), std::string::npos)
                << "real full text: " << http_res.body << "\nexpect contains: " << input.key[0];
    }
}

TEST(HttpGetValueTest, process_http_get_value_test_cover_all_template) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);
    // Generate kvs
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    for (auto&& input : test_inputs) {
        auto keys = input.key;
        auto vals = input.gen_value();
        EXPECT_EQ(keys.size(), vals.size()) << input.key_type;
        for (int i = 0; i < keys.size(); ++i) {
            txn->put(unhex(keys[i]), vals[i]);
        }
    }
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    brpc::URI uri;
    (void)uri.get_query_map(); // initialize query map

    auto gen_url = [](const Input& input, bool use_param) {
        std::stringstream url;
        url << "localhost:5000/MetaService/http?key_type=" << input.key_type;
        // Key mode
        if (!use_param) {
            url << "&key=" << input.key[0];
        } else if (!input.param.empty()) {
            url << "&" << input.param;
        }
        return url.str();
    };

    for (auto&& input : test_inputs) {
        auto url = gen_url(input, true);
        // std::cout << url.str() << std::endl;
        ASSERT_EQ(uri.SetHttpURL(url), 0); // clear and set query string
        auto http_res = process_http_get_value(txn_kv.get(), uri);
        EXPECT_EQ(http_res.status_code, 200);
        // std::cout << http_res.body << std::endl;
        EXPECT_EQ(http_res.body, input.value);
        // Key mode
        url = gen_url(input, false);
        // std::cout << url.str() << std::endl;
        ASSERT_EQ(uri.SetHttpURL(url), 0); // clear and set query string
        http_res = process_http_get_value(txn_kv.get(), uri);
        EXPECT_EQ(http_res.status_code, 200);
        // std::cout << http_res.body << std::endl;
        EXPECT_EQ(http_res.body, input.value);
    }
}
