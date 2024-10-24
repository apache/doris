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

#include "meta-service/keys.h"

#include <bthread/bthread.h>
#include <bthread/countdown_event.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <iostream>
#include <random>
#include <variant>
#include <vector>

#include "common/util.h"

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

static void remove_user_space_prefix(std::string_view* key_sv) {
    ASSERT_EQ(key_sv->front(), 0x01);
    key_sv->remove_prefix(1);
}

static void remove_system_space_prefix(std::string_view* key_sv) {
    ASSERT_EQ(key_sv->front(), 0x02);
    key_sv->remove_prefix(1);
}

// extern
namespace doris::cloud {
void encode_int64(int64_t val, std::string* b);
int decode_int64(std::string_view* in, int64_t* val);
void encode_bytes(std::string_view bytes, std::string* b);
int decode_bytes(std::string_view* in, std::string* out);
} // namespace doris::cloud

// clang-format off
// Possible key encoding schemas:
//
// 0x01 "instance" ${instance_id} -> InstanceInfoPB
// 
// 0x01 "txn" ${instance_id} "txn_label" ${db_id} ${label} -> TxnLabelPB ${version_timestamp}
// 0x01 "txn" ${instance_id} "txn_info" ${db_id} ${version_timestamp} -> TxnInfoPB
// 0x01 "txn" ${instance_id} "txn_index" ${version_timestamp} -> TxnIndexPB
// 0x01 "txn" ${instance_id} "txn_running" ${db_id} ${version_timestamp} -> TxnRunningPB // creaet at begin, delete at commit
//
// 0x01 "version" ${instance_id} "partition" ${db_id} ${tbl_id} ${partition_id} -> VersionPB
// 
// 0x01 "meta" ${instance_id} "rowset" ${tablet_id} ${version} ${rowset_id} -> RowsetMetaCloudPB
// 0x01 "meta" ${instance_id} "rowset_tmp" ${txn_id} ${rowset_id} -> RowsetMetaCloudPB
// 0x01 "meta" ${instance_id} "tablet" ${table_id} ${tablet_id} -> TabletMetaCloudPB
// 0x01 "meta" ${instance_id} "tablet_table" ${tablet_id} -> ${table_id}
// 0x01 "meta" ${instance_id} "tablet_tmp" ${table_id} ${tablet_id} -> TabletMetaCloudPB
// 
// 0x01 "trash" ${instacne_id} "table" -> TableTrashPB
// 
// 0x01 "node_status" ${instance_id} "compute" ${backend_id} -> ComputeNodeStatusPB
// clang-format on

TEST(KeysTest, InstanceKeyTest) {
    using namespace doris::cloud;

    // instance key
    // 0x01 "instance" ${instance_id}
    std::string instance_id = "instance_id_deadbeef";
    InstanceKeyInfo inst_key {instance_id};
    std::string encoded_instance_key;
    instance_key(inst_key, &encoded_instance_key);

    std::string dec_instance_id;

    std::string_view key_sv(encoded_instance_key);
    std::string dec_instance_prefix;
    remove_user_space_prefix(&key_sv);
    ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_prefix), 0);
    ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
    ASSERT_TRUE(key_sv.empty());

    EXPECT_EQ("instance", dec_instance_prefix);
    EXPECT_EQ(instance_id, dec_instance_id);
}

TEST(KeysTest, MetaKeysTest) {
    using namespace doris::cloud;
    std::string instance_id = "instance_id_deadbeef";

    // rowset meta key
    // 0x01 "meta" ${instance_id} "rowset" ${tablet_id} ${version}
    {
        int64_t tablet_id = 10086;
        int64_t version = 100;
        MetaRowsetKeyInfo rowset_key {instance_id, tablet_id, version};
        std::string encoded_rowset_key0;
        meta_rowset_key(rowset_key, &encoded_rowset_key0);
        std::cout << hex(encoded_rowset_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_tablet_id = 0;
        int64_t dec_version = 0;

        std::string_view key_sv(encoded_rowset_key0);
        std::string dec_meta_prefix;
        std::string dec_rowset_prefix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_meta_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_rowset_prefix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_tablet_id), 0) << hex(key_sv);
        ASSERT_EQ(decode_int64(&key_sv, &dec_version), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("meta", dec_meta_prefix);
        EXPECT_EQ("rowset", dec_rowset_prefix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(tablet_id, dec_tablet_id);
        EXPECT_EQ(version, dec_version);

        std::get<2>(rowset_key) = version + 1;
        std::string encoded_rowset_key1;
        meta_rowset_key(rowset_key, &encoded_rowset_key1);
        std::cout << hex(encoded_rowset_key1) << std::endl;

        ASSERT_GT(encoded_rowset_key1, encoded_rowset_key0);
    }

    // tmp rowset meta key
    // 0x01 "meta" ${instance_id} "rowset_tmp" ${tablet_id} ${version}
    {
        int64_t tablet_id = 10086;
        int64_t version = 100;
        MetaRowsetKeyInfo rowset_key {instance_id, tablet_id, version};
        std::string encoded_rowset_key0;
        meta_rowset_tmp_key(rowset_key, &encoded_rowset_key0);
        std::cout << hex(encoded_rowset_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_tablet_id = 0;
        int64_t dec_version = 0;

        std::string_view key_sv(encoded_rowset_key0);
        std::string dec_meta_prefix;
        std::string dec_rowset_prefix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_meta_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_rowset_prefix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_tablet_id), 0) << hex(key_sv);
        ASSERT_EQ(decode_int64(&key_sv, &dec_version), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("meta", dec_meta_prefix);
        EXPECT_EQ("rowset_tmp", dec_rowset_prefix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(tablet_id, dec_tablet_id);
        EXPECT_EQ(version, dec_version);

        std::get<2>(rowset_key) = version + 1;
        std::string encoded_rowset_key1;
        meta_rowset_tmp_key(rowset_key, &encoded_rowset_key1);
        std::cout << hex(encoded_rowset_key1) << std::endl;

        ASSERT_GT(encoded_rowset_key1, encoded_rowset_key0);
    }

    // tablet meta key
    // 0x01 "meta" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} -> TabletMetaCloudPB
    {
        int64_t table_id = 10010;
        int64_t index_id = 10011;
        int64_t partition_id = 10012;
        int64_t tablet_id = 10086;
        MetaTabletKeyInfo tablet_key {instance_id, table_id, index_id, partition_id, tablet_id};
        std::string encoded_rowset_key0;
        meta_tablet_key(tablet_key, &encoded_rowset_key0);
        std::cout << hex(encoded_rowset_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_table_id = 0;
        int64_t dec_tablet_id = 0;
        int64_t dec_index_id = 0;
        int64_t dec_partition_id = 0;

        std::string_view key_sv(encoded_rowset_key0);
        std::string dec_meta_prefix;
        std::string dec_tablet_prefix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_meta_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_tablet_prefix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_table_id), 0) << hex(key_sv);
        ASSERT_EQ(decode_int64(&key_sv, &dec_index_id), 0) << hex(key_sv);
        ASSERT_EQ(decode_int64(&key_sv, &dec_partition_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_tablet_id), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("meta", dec_meta_prefix);
        EXPECT_EQ("tablet", dec_tablet_prefix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(table_id, dec_table_id);
        EXPECT_EQ(index_id, dec_index_id);
        EXPECT_EQ(partition_id, dec_partition_id);
        EXPECT_EQ(tablet_id, dec_tablet_id);

        std::get<2>(tablet_key) = tablet_id + 1;
        std::string encoded_rowset_key1;
        meta_tablet_key(tablet_key, &encoded_rowset_key1);
        std::cout << hex(encoded_rowset_key1) << std::endl;

        ASSERT_GT(encoded_rowset_key1, encoded_rowset_key0);
    }

    // tablet index key
    // 0x01 "meta" ${instance_id} "tablet_index" ${tablet_id} -> ${table_id}
    {
        int64_t tablet_id = 10086;
        MetaTabletIdxKeyInfo tablet_tbl_key {instance_id, tablet_id};
        std::string encoded_rowset_key0;
        meta_tablet_idx_key(tablet_tbl_key, &encoded_rowset_key0);
        std::cout << hex(encoded_rowset_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_tablet_id = 0;

        std::string_view key_sv(encoded_rowset_key0);
        std::string dec_meta_prefix;
        std::string dec_tablet_prefix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_meta_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_tablet_prefix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_tablet_id), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("meta", dec_meta_prefix);
        EXPECT_EQ("tablet_index", dec_tablet_prefix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(tablet_id, dec_tablet_id);

        std::get<1>(tablet_tbl_key) = tablet_id + 1;
        std::string encoded_rowset_key1;
        meta_tablet_idx_key(tablet_tbl_key, &encoded_rowset_key1);
        std::cout << hex(encoded_rowset_key1) << std::endl;

        ASSERT_GT(encoded_rowset_key1, encoded_rowset_key0);
    }

    // tablet schema key
    // 0x01 "meta" ${instance_id} "schema" ${index_id} ${schema_version} -> TabletSchemaCloudPB
    {
        int64_t index_id = 10000;
        int32_t schema_version = 5;
        auto key = meta_schema_key({instance_id, index_id, schema_version});

        std::string dec_instance_id;
        int64_t dec_index_id = 0;
        int64_t dec_schema_version = 0;

        std::string_view key_sv(key);
        std::string dec_schema_prefix;
        std::string dec_schema_infix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_schema_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_schema_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_index_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_schema_version), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(index_id, dec_index_id);
        EXPECT_EQ(schema_version, dec_schema_version);
        EXPECT_EQ(dec_schema_prefix, "meta");
        EXPECT_EQ(dec_schema_infix, "schema");

        // TODO: the order of schema version?
    }
}

TEST(KeysTest, VersionKeyTest) {
    using namespace doris::cloud;
    std::string instance_id = "instance_id_deadbeef";

    // 0x01 "version" ${instance_id} "partition" ${db_id} ${tbl_id} ${partition_id} -> ${version}
    {
        int64_t db_id = 11111;
        int64_t table_id = 10086;
        int64_t partition_id = 9998;
        PartitionVersionKeyInfo v_key {instance_id, db_id, table_id, partition_id};
        std::string encoded_version_key0;
        partition_version_key(v_key, &encoded_version_key0);
        std::cout << "version key after encode: " << hex(encoded_version_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_db_id = 0;
        int64_t dec_table_id = 0;
        int64_t dec_partition_id = 0;

        std::string_view key_sv(encoded_version_key0);
        std::string dec_version_prefix;
        std::string dec_version_infix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_version_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_version_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_db_id), 0) << hex(key_sv);
        ASSERT_EQ(decode_int64(&key_sv, &dec_table_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_partition_id), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("version", dec_version_prefix);
        EXPECT_EQ("partition", dec_version_infix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(db_id, dec_db_id);
        EXPECT_EQ(table_id, dec_table_id);
        EXPECT_EQ(partition_id, dec_partition_id);

        std::get<3>(v_key) = partition_id + 1;
        std::string encoded_version_key1;
        partition_version_key(v_key, &encoded_version_key1);
        std::cout << "version key after encode: " << hex(encoded_version_key1) << std::endl;

        ASSERT_GT(encoded_version_key1, encoded_version_key0);
    }

    // 0x01 "version" ${instance_id} "table" ${db_id} ${tbl_id} -> ${version}
    {
        int64_t db_id = 11111;
        int64_t table_id = 10010;
        TableVersionKeyInfo v_key {instance_id, db_id, table_id};
        std::string encoded_version_key0;
        table_version_key(v_key, &encoded_version_key0);
        std::cout << "version key after encode: " << hex(encoded_version_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_db_id = 0;
        int64_t dec_table_id = 0;

        std::string_view key_sv(encoded_version_key0);
        std::string dec_version_prefix;
        std::string dec_version_infix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_version_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_version_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_db_id), 0) << hex(key_sv);
        ASSERT_EQ(decode_int64(&key_sv, &dec_table_id), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("version", dec_version_prefix);
        EXPECT_EQ("table", dec_version_infix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(db_id, dec_db_id);
        EXPECT_EQ(table_id, dec_table_id);

        std::get<2>(v_key) = table_id + 1;
        std::string encoded_version_key1;
        table_version_key(v_key, &encoded_version_key1);
        std::cout << "version key after encode: " << hex(encoded_version_key1) << std::endl;

        ASSERT_GT(encoded_version_key1, encoded_version_key0);
    }
}

TEST(KeysTest, TxnKeysTest) {
    using namespace doris::cloud;
    std::string instance_id = "instance_id_deadbeef";

    // 0x01 "txn" ${instance_id} "txn_label" ${db_id} ${label} -> set<${version_timestamp}>
    {
        int64_t db_id = 12345678;
        std::string label = "label1xxx";
        TxnLabelKeyInfo index_key {instance_id, db_id, label};
        std::string encoded_txn_index_key0;
        txn_label_key(index_key, &encoded_txn_index_key0);
        std::cout << hex(encoded_txn_index_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_db_id = 0;
        std::string dec_label;

        std::string_view key_sv(encoded_txn_index_key0);
        std::string dec_txn_prefix;
        std::string dec_txn_infix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_db_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_label), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("txn", dec_txn_prefix);
        EXPECT_EQ("txn_label", dec_txn_infix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(db_id, dec_db_id);
        EXPECT_EQ(label, dec_label);

        std::get<1>(index_key) = db_id + 1;
        std::string encoded_txn_index_key1;
        txn_label_key(index_key, &encoded_txn_index_key1);
        std::cout << hex(encoded_txn_index_key1) << std::endl;

        ASSERT_GT(encoded_txn_index_key1, encoded_txn_index_key0);
    }

    // 0x01 "txn" ${instance_id} "txn_info" ${db_id} ${version_timestamp} -> TxnInfoPB
    {
        int64_t db_id = 12345678;
        int64_t txn_id = 10086;
        TxnInfoKeyInfo info_key {instance_id, db_id, txn_id};
        std::string encoded_txn_info_key0;
        txn_info_key(info_key, &encoded_txn_info_key0);
        std::cout << hex(encoded_txn_info_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_db_id = 0;
        int64_t dec_txn_id = 0;

        std::string_view key_sv(encoded_txn_info_key0);
        std::string dec_txn_prefix;
        std::string dec_txn_infix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_db_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_txn_id), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("txn", dec_txn_prefix);
        EXPECT_EQ("txn_info", dec_txn_infix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(db_id, dec_db_id);
        EXPECT_EQ(txn_id, dec_txn_id);

        std::get<2>(info_key) = txn_id + 1;
        std::string encoded_txn_info_key1;
        txn_info_key(info_key, &encoded_txn_info_key1);
        std::cout << hex(encoded_txn_info_key1) << std::endl;

        ASSERT_GT(encoded_txn_info_key1, encoded_txn_info_key0);

        std::get<1>(info_key) = db_id + 1;
        std::string encoded_txn_info_key2;
        txn_info_key(info_key, &encoded_txn_info_key2);
        std::cout << hex(encoded_txn_info_key2) << std::endl;

        ASSERT_GT(encoded_txn_info_key2, encoded_txn_info_key0);
    }

    // 0x01 "txn" ${instance_id} "txn_index" ${version_timestamp} -> TxnIndexPB
    {
        int64_t txn_id = 12343212453;
        TxnIndexKeyInfo txn_index_key_ {instance_id, txn_id};
        std::string encoded_txn_index_key0;
        txn_index_key(txn_index_key_, &encoded_txn_index_key0);
        std::cout << hex(encoded_txn_index_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_txn_id = 0;

        std::string_view key_sv(encoded_txn_index_key0);
        std::string dec_txn_prefix;
        std::string dec_txn_infix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_txn_id), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("txn", dec_txn_prefix);
        EXPECT_EQ("txn_index", dec_txn_infix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(txn_id, dec_txn_id);

        std::get<1>(txn_index_key_) = txn_id + 1;
        std::string encoded_txn_index_key1;
        txn_index_key(txn_index_key_, &encoded_txn_index_key1);
        std::cout << hex(encoded_txn_index_key1) << std::endl;

        ASSERT_GT(encoded_txn_index_key1, encoded_txn_index_key0);
    }

    // 0x01 "txn" ${instance_id} "txn_running" ${db_id} ${version_timestamp} -> ${table_id_list}
    {
        int64_t db_id = 98712345;
        int64_t txn_id = 12343212453;
        TxnRunningKeyInfo running_key {instance_id, db_id, txn_id};
        std::string encoded_txn_running_key0;
        txn_running_key(running_key, &encoded_txn_running_key0);
        std::cout << hex(encoded_txn_running_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_db_id = 0;
        int64_t dec_txn_id = 0;

        std::string_view key_sv(encoded_txn_running_key0);
        std::string dec_txn_prefix;
        std::string dec_txn_infix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_db_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_txn_id), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("txn", dec_txn_prefix);
        EXPECT_EQ("txn_running", dec_txn_infix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(db_id, dec_db_id);
        EXPECT_EQ(txn_id, dec_txn_id);

        std::get<2>(running_key) = txn_id + 1;
        std::string encoded_txn_running_key1;
        txn_running_key(running_key, &encoded_txn_running_key1);
        std::cout << hex(encoded_txn_running_key1) << std::endl;

        ASSERT_GT(encoded_txn_running_key1, encoded_txn_running_key0);
    }
}

TEST(KeysTest, RecycleKeysTest) {
    using namespace doris::cloud;
    std::string instance_id = "instance_id_deadbeef";

    // 0x01 "recycle" ${instance_id} "index" ${index_id}                                         -> RecycleIndexPB
    {
        int64_t index_id = 1234545;
        RecycleIndexKeyInfo recycle_key {instance_id, index_id};
        std::string encoded_recycle_key0;
        recycle_index_key(recycle_key, &encoded_recycle_key0);
        std::cout << hex(encoded_recycle_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_index_id = 0;

        std::string_view key_sv(encoded_recycle_key0);
        std::string dec_recycle_prefix;
        std::string dec_recycle_infix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_recycle_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_recycle_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_index_id), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("recycle", dec_recycle_prefix);
        EXPECT_EQ("index", dec_recycle_infix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(index_id, dec_index_id);
    }

    // 0x01 "recycle" ${instance_id} "partition" ${partition_id}                                 -> RecyclePartitionPB
    {
        int64_t partition_id = 12345450;
        RecyclePartKeyInfo recycle_key {instance_id, partition_id};
        std::string encoded_recycle_key0;
        recycle_partition_key(recycle_key, &encoded_recycle_key0);
        std::cout << hex(encoded_recycle_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_partition_id = 0;

        std::string_view key_sv(encoded_recycle_key0);
        std::string dec_recycle_prefix;
        std::string dec_recycle_infix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_recycle_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_recycle_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_partition_id), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("recycle", dec_recycle_prefix);
        EXPECT_EQ("partition", dec_recycle_infix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(partition_id, dec_partition_id);
    }

    // 0x01 "recycle" ${instance_id} "rowset" ${tablet_id} ${rowset_id}                          -> RecycleRowsetPB
    {
        int64_t tablet_id = 100201;
        std::string rowset_id = "202201";
        RecycleRowsetKeyInfo recycle_key {instance_id, tablet_id, rowset_id};
        std::string encoded_recycle_key0;
        recycle_rowset_key(recycle_key, &encoded_recycle_key0);
        std::cout << hex(encoded_recycle_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_tablet_id = 0;
        std::string dec_rowset_id;

        std::string_view key_sv(encoded_recycle_key0);
        std::string dec_recycle_prefix;
        std::string dec_recycle_infix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_recycle_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_recycle_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_tablet_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_rowset_id), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("recycle", dec_recycle_prefix);
        EXPECT_EQ("rowset", dec_recycle_infix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(tablet_id, dec_tablet_id);
        EXPECT_EQ(rowset_id, dec_rowset_id);
    }

    // 0x01 "recycle" ${instance_id} "txn" ${db_id} ${txn_id}                                    -> RecycleTxnKeyInfo
    {
        int64_t db_id = 98712345;
        int64_t txn_id = 12343212453;
        RecycleTxnKeyInfo recycle_key {instance_id, db_id, txn_id};
        std::string encoded_recycle_txn_key0;
        recycle_txn_key(recycle_key, &encoded_recycle_txn_key0);
        std::cout << hex(encoded_recycle_txn_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_db_id = 0;
        int64_t dec_txn_id = 0;

        std::string_view key_sv(encoded_recycle_txn_key0);
        std::string dec_txn_prefix;
        std::string dec_txn_infix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_db_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_txn_id), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("recycle", dec_txn_prefix);
        EXPECT_EQ("txn", dec_txn_infix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(db_id, dec_db_id);
        EXPECT_EQ(txn_id, dec_txn_id);
    }

    // 0x01 "recycle" ${instance_id} "stage" ${stage_id}                                         -> RecycleStagePB
    {
        std::string stage_id = "100201";
        RecycleStageKeyInfo recycle_key {instance_id, stage_id};
        std::string encoded_recycle_key0;
        recycle_stage_key(recycle_key, &encoded_recycle_key0);
        std::cout << hex(encoded_recycle_key0) << std::endl;

        std::string dec_instance_id;
        std::string dec_stage_id;

        std::string_view key_sv(encoded_recycle_key0);
        std::string dec_recycle_prefix;
        std::string dec_recycle_infix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_recycle_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_recycle_infix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_stage_id), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("recycle", dec_recycle_prefix);
        EXPECT_EQ("stage", dec_recycle_infix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(stage_id, dec_stage_id);
    }
}

TEST(KeysTest, StatsKeysTest) {
    using namespace doris::cloud;
    std::string instance_id = "instance_id_deadbeef";

    int64_t table_id = 123;
    int64_t index_id = 345;
    int64_t partition_id = 1231231231;
    int64_t tablet_id = 543671234523;

    auto expect_stats_prefix = [&](std::string_view& key_sv) {
        std::string dec_instance_id;
        int64_t dec_table_id = 0;
        int64_t dec_index_id = 0;
        int64_t dec_partition_id = 0;
        int64_t dec_tablet_id = 0;

        std::string dec_stats_prefix;
        std::string dec_stats_infix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_stats_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_stats_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_table_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_index_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_partition_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_tablet_id), 0);

        EXPECT_EQ("stats", dec_stats_prefix);
        EXPECT_EQ("tablet", dec_stats_infix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(table_id, dec_table_id);
        EXPECT_EQ(index_id, dec_index_id);
        EXPECT_EQ(partition_id, dec_partition_id);
        EXPECT_EQ(tablet_id, dec_tablet_id);
    };

    // 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} -> TabletStatsPB
    {
        StatsTabletKeyInfo stats_key {instance_id, table_id, index_id, partition_id, tablet_id};
        std::string encoded_stats_key0;
        stats_tablet_key(stats_key, &encoded_stats_key0);
        std::cout << hex(encoded_stats_key0) << std::endl;

        std::string_view key_sv(encoded_stats_key0);
        expect_stats_prefix(key_sv);
        ASSERT_TRUE(key_sv.empty());
    }

    // 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} "data_size"   -> int64
    {
        StatsTabletKeyInfo stats_key {instance_id, table_id, index_id, partition_id, tablet_id};
        std::string encoded_stats_key0;
        stats_tablet_data_size_key(stats_key, &encoded_stats_key0);
        std::cout << hex(encoded_stats_key0) << std::endl;

        std::string dec_stats_suffix;

        std::string_view key_sv(encoded_stats_key0);
        expect_stats_prefix(key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_stats_suffix), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("data_size", dec_stats_suffix);
    }

    // 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} "num_rows"    -> int64
    {
        StatsTabletKeyInfo stats_key {instance_id, table_id, index_id, partition_id, tablet_id};
        std::string encoded_stats_key0;
        stats_tablet_num_rows_key(stats_key, &encoded_stats_key0);
        std::cout << hex(encoded_stats_key0) << std::endl;

        std::string dec_stats_suffix;

        std::string_view key_sv(encoded_stats_key0);
        expect_stats_prefix(key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_stats_suffix), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("num_rows", dec_stats_suffix);
    }

    // 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} "num_rowsets" -> int64
    {
        StatsTabletKeyInfo stats_key {instance_id, table_id, index_id, partition_id, tablet_id};
        std::string encoded_stats_key0;
        stats_tablet_num_rowsets_key(stats_key, &encoded_stats_key0);
        std::cout << hex(encoded_stats_key0) << std::endl;

        std::string dec_stats_suffix;

        std::string_view key_sv(encoded_stats_key0);
        expect_stats_prefix(key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_stats_suffix), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("num_rowsets", dec_stats_suffix);
    }

    // 0x01 "stats" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} "num_segs"    -> int64
    {
        StatsTabletKeyInfo stats_key {instance_id, table_id, index_id, partition_id, tablet_id};
        std::string encoded_stats_key0;
        stats_tablet_num_segs_key(stats_key, &encoded_stats_key0);
        std::cout << hex(encoded_stats_key0) << std::endl;

        std::string dec_stats_suffix;

        std::string_view key_sv(encoded_stats_key0);
        expect_stats_prefix(key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_stats_suffix), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("num_segs", dec_stats_suffix);
    }
}

TEST(KeysTest, JobKeysTest) {
    using namespace doris::cloud;
    std::string instance_id = "instance_id_deadbeef";

    // 0x01 "job" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id}   -> TabletJobInfoPB
    {
        int64_t table_id = 123;
        int64_t index_id = 345;
        int64_t partition_id = 1231231231;
        int64_t tablet_id = 543671234523;
        JobTabletKeyInfo job_key {instance_id, table_id, index_id, partition_id, tablet_id};
        std::string encoded_job_key0;
        job_tablet_key(job_key, &encoded_job_key0);
        std::cout << hex(encoded_job_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_table_id = 0;
        int64_t dec_index_id = 0;
        int64_t dec_partition_id = 0;
        int64_t dec_tablet_id = 0;

        std::string_view key_sv(encoded_job_key0);
        std::string dec_job_prefix;
        std::string dec_job_infix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_job_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_job_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_table_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_index_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_partition_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_tablet_id), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("job", dec_job_prefix);
        EXPECT_EQ("tablet", dec_job_infix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(table_id, dec_table_id);
        EXPECT_EQ(index_id, dec_index_id);
        EXPECT_EQ(partition_id, dec_partition_id);
        EXPECT_EQ(tablet_id, dec_tablet_id);
    }

    // 0x01 "job" ${instance_id} "recycle"                                                       -> JobRecyclePB
    {
        JobRecycleKeyInfo job_key {instance_id};
        std::string encoded_job_key0;
        job_recycle_key(job_key, &encoded_job_key0);
        std::cout << hex(encoded_job_key0) << std::endl;

        std::string dec_instance_id;

        std::string_view key_sv(encoded_job_key0);
        std::string dec_job_prefix;
        std::string dec_job_suffix;

        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_job_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_job_suffix), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("job", dec_job_prefix);
        EXPECT_EQ("recycle", dec_job_suffix);
        EXPECT_EQ(instance_id, dec_instance_id);
    }

    // 0x01 "job" ${instance_id} "check"                                                       -> JobRecyclePB
    {
        JobRecycleKeyInfo job_key {instance_id};
        std::string encoded_job_key0;
        job_check_key(job_key, &encoded_job_key0);
        std::cout << hex(encoded_job_key0) << std::endl;

        std::string dec_instance_id;

        std::string_view key_sv(encoded_job_key0);
        std::string dec_job_prefix;
        std::string dec_job_suffix;

        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_job_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_job_suffix), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("job", dec_job_prefix);
        EXPECT_EQ("check", dec_job_suffix);
        EXPECT_EQ(instance_id, dec_instance_id);
    }
}

TEST(KeysTest, SystemKeysTest) {
    using namespace doris::cloud;
    std::string instance_id = "instance_id_deadbeef";

    // 0x02 "system" "meta-service" "registry"                                                   -> MetaServiceRegistryPB
    // 0x02 "system" "meta-service" "arn_info"                                                   -> RamUserPB
    // 0x02 "system" "meta-service" "encryption_key_info"                                        -> EncryptionKeyInfoPB
    std::vector<std::string> suffixes {"registry", "arn_info", "encryption_key_info"};
    std::vector<std::function<std::string()>> fns {
            system_meta_service_registry_key,
            system_meta_service_arn_info_key,
            system_meta_service_encryption_key_info_key,
    };
    size_t num = suffixes.size();
    for (size_t i = 0; i < num; ++i) {
        std::string key = fns[i]();
        std::cout << hex(key) << std::endl;

        std::string_view key_sv(key);
        std::string prefix, infix, suffix;
        remove_system_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &infix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &suffix), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("system", prefix);
        EXPECT_EQ("meta-service", infix);
        EXPECT_EQ(suffixes[i], suffix) << i;
    }
}

TEST(KeysTest, CopyKeysTest) {
    using namespace doris::cloud;
    std::string instance_id = "instance_id_deadbeef";

    // 0x01 "copy" ${instance_id} "job" ${stage_id} ${table_id} ${copy_id} ${group_id}           -> CopyJobPB
    {
        int64_t table_id = 3745823784;
        int64_t group_id = 1238547388;
        std::string stage_id = "9482049283";
        std::string copy_id = "1284758385";

        CopyJobKeyInfo copy_key {instance_id, stage_id, table_id, copy_id, group_id};
        std::string encoded_copy_job_key0;
        copy_job_key(copy_key, &encoded_copy_job_key0);
        std::cout << hex(encoded_copy_job_key0) << std::endl;

        std::string dec_instance_id;
        std::string dec_stage_id;
        std::string dec_copy_id;
        int64_t dec_table_id = 0;
        int64_t dec_group_id = 0;

        std::string_view key_sv(encoded_copy_job_key0);
        std::string dec_copy_job_prefix, dec_copy_job_infix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_copy_job_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_copy_job_infix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_stage_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_table_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_copy_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_group_id), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("copy", dec_copy_job_prefix);
        EXPECT_EQ("job", dec_copy_job_infix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(stage_id, dec_stage_id);
        EXPECT_EQ(table_id, dec_table_id);
        EXPECT_EQ(copy_id, dec_copy_id);
        EXPECT_EQ(group_id, dec_group_id);
    }

    // 0x01 "copy" ${instance_id} "loading_files" ${stage_id} ${table_id} ${obj_name} ${etag}    -> CopyFilePB
    {
        std::string stage_id = "9482049283";
        int64_t table_id = 3745823784;
        std::string obj_name = "test-obj-name";
        std::string etag = "test-etag";

        CopyFileKeyInfo copy_key {instance_id, stage_id, table_id, obj_name, etag};
        std::string encoded_copy_job_key0;
        copy_file_key(copy_key, &encoded_copy_job_key0);
        std::cout << hex(encoded_copy_job_key0) << std::endl;

        std::string dec_instance_id;
        std::string dec_stage_id;
        int64_t dec_table_id = 0;
        std::string dec_obj_name;
        std::string dec_etag;

        std::string_view key_sv(encoded_copy_job_key0);
        std::string dec_copy_prefix, dec_copy_infix;
        remove_user_space_prefix(&key_sv);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_copy_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_copy_infix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_stage_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_table_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_obj_name), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_etag), 0);
        ASSERT_TRUE(key_sv.empty());

        EXPECT_EQ("copy", dec_copy_prefix);
        EXPECT_EQ("loading_file", dec_copy_infix);
        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(stage_id, dec_stage_id);
        EXPECT_EQ(table_id, dec_table_id);
        EXPECT_EQ(obj_name, dec_obj_name);
        EXPECT_EQ(etag, dec_etag);
    }
}

TEST(KeysTest, DecodeKeysTest) {
    using namespace doris::cloud;
    // clang-format off
    std::string key = "011074786e000110696e7374616e63655f69645f646561646265656600011074786e5f696e646578000112000000000000271310696e736572745f336664356164313264303035346139622d386337373664333231386336616462370001";
    // clang-format on
    auto pretty_key = prettify_key(key);
    ASSERT_TRUE(!pretty_key.empty()) << key;
    std::cout << "\n" << pretty_key << std::endl;

    pretty_key = prettify_key(key, true);
    ASSERT_TRUE(!pretty_key.empty()) << key;
    std::cout << "\n" << pretty_key << std::endl;
}

TEST(KeysTest, MetaSchemaPBDictionaryTest) {
    using namespace doris::cloud;
    std::string instance_id = "instance_id_meta_dict";
    int64_t index_id = 123456;

    // 0:instance_id  1:index_id
    MetaSchemaPBDictionaryInfo dict_key {instance_id, index_id};
    std::string encoded_dict_key;
    meta_schema_pb_dictionary_key(dict_key, &encoded_dict_key);
    std::cout << hex(encoded_dict_key) << std::endl;

    std::string decoded_instance_id;
    std::string decoded_prefix;
    std::string decoded_meta_prefix;
    int64_t decoded_index_id;
    std::string_view key_sv(encoded_dict_key);
    remove_user_space_prefix(&key_sv);
    ASSERT_EQ(decode_bytes(&key_sv, &decoded_prefix), 0);
    ASSERT_EQ(decode_bytes(&key_sv, &decoded_instance_id), 0);
    ASSERT_EQ(decode_bytes(&key_sv, &decoded_meta_prefix), 0);
    ASSERT_EQ(decode_int64(&key_sv, &decoded_index_id), 0);
    ASSERT_TRUE(key_sv.empty());

    EXPECT_EQ("meta", decoded_prefix);
    EXPECT_EQ("tablet_schema_pb_dict", decoded_meta_prefix);
    EXPECT_EQ(instance_id, decoded_instance_id);
    EXPECT_EQ(index_id, decoded_index_id);
}
