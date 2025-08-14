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

#include "meta-store/meta_reader.h"

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-death-test.h>
#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "meta-service/doris_txn.h"
#include "meta-store/codec.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"

using namespace doris::cloud;

int main(int argc, char** argv) {
    config::log_dir = "./log/";
    if (!doris::cloud::init_glog("meta_reader_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

// Convert a string to a hex-escaped string.
// A non-displayed character is represented as \xHH where HH is the hexadecimal value of the character.
// A displayed character is represented as itself.
static std::string escape_hex(std::string_view data) {
    std::string result;
    for (char c : data) {
        if (isprint(c)) {
            result += c;
        } else {
            result += fmt::format("\\x{:02x}", static_cast<unsigned char>(c));
        }
    }
    return result;
}

static std::string dump_range(TxnKv* txn_kv, std::string_view begin = "",
                              std::string_view end = "\xFF") {
    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != TxnErrorCode::TXN_OK) {
        return "Failed to create dump range transaction";
    }
    FullRangeGetOptions opts;
    opts.txn = txn.get();
    auto iter = txn_kv->full_range_get(std::string(begin), std::string(end), std::move(opts));
    std::string buffer;
    for (auto&& kv = iter->next(); kv.has_value(); kv = iter->next()) {
        buffer +=
                fmt::format("Key: {}, Value: {}\n", escape_hex(kv->first), escape_hex(kv->second));
    }
    EXPECT_TRUE(iter->is_valid()); // The iterator should still be valid after the next call.
    return buffer;
}

TEST(MetaReaderTest, GetTableVersion) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string instance_id = "test_instance";
    int64_t table_id = 1001;
    {
        // NOT FOUND
        MetaReader meta_reader(instance_id, txn_kv.get());
        Versionstamp table_version;
        TxnErrorCode err = meta_reader.get_table_version(table_id, &table_version);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    {
        // Put a table version
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string table_version_key = versioned::table_version_key({instance_id, table_id});
        versioned_put(txn.get(), table_version_key, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    Versionstamp version1;
    {
        MetaReader meta_reader(instance_id, txn_kv.get());
        TxnErrorCode err = meta_reader.get_table_version(table_id, &version1);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    }

    {
        // Put a table version
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string table_version_key = versioned::table_version_key({instance_id, table_id});
        versioned_put(txn.get(), table_version_key, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    Versionstamp version2;
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader meta_reader(instance_id, txn_kv.get());
        TxnErrorCode err = meta_reader.get_table_version(txn.get(), table_id, &version2);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    }

    ASSERT_LT(version1, version2);
}

TEST(MetaReaderTest, BatchGetTableVersion) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string instance_id = "test_instance";
    std::vector<int64_t> table_ids = {1001, 1002, 1003, 1004};

    {
        // Test empty input
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::vector<int64_t> empty_ids;
        std::unordered_map<int64_t, Versionstamp> table_versions;
        TxnErrorCode err = meta_reader.get_table_versions(empty_ids, &table_versions);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_TRUE(table_versions.empty());
    }

    {
        // Test all keys not found
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::unordered_map<int64_t, Versionstamp> table_versions;
        TxnErrorCode err = meta_reader.get_table_versions(table_ids, &table_versions);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_TRUE(table_versions.empty());
    }

    {
        // Put some table versions (skip table_ids[1] to test partial results)
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        for (size_t i = 0; i < table_ids.size(); ++i) {
            if (i == 1) continue; // Skip table_ids[1]
            std::string table_version_key =
                    versioned::table_version_key({instance_id, table_ids[i]});
            versioned_put(txn.get(), table_version_key, "");
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test partial results
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::unordered_map<int64_t, Versionstamp> table_versions;
        TxnErrorCode err = meta_reader.get_table_versions(table_ids, &table_versions);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(table_versions.size(), 3); // All except table_ids[1]

        for (size_t i = 0; i < table_ids.size(); ++i) {
            if (i == 1) {
                ASSERT_EQ(table_versions.find(table_ids[i]), table_versions.end());
            } else {
                ASSERT_NE(table_versions.find(table_ids[i]), table_versions.end());
            }
        }
    }

    {
        // Put the missing table version
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string table_version_key = versioned::table_version_key({instance_id, table_ids[1]});
        versioned_put(txn.get(), table_version_key, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test all keys found
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::unordered_map<int64_t, Versionstamp> table_versions;
        TxnErrorCode err = meta_reader.get_table_versions(txn.get(), table_ids, &table_versions);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(table_versions.size(), table_ids.size());

        for (int64_t table_id : table_ids) {
            ASSERT_NE(table_versions.find(table_id), table_versions.end());
        }
    }
}

TEST(MetaReaderTest, GetPartitionVersion) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string instance_id = "test_instance";
    int64_t partition_id = 2001;
    {
        // NOT FOUND
        MetaReader meta_reader(instance_id, txn_kv.get());
        VersionPB version_pb;
        Versionstamp partition_version;
        TxnErrorCode err =
                meta_reader.get_partition_version(partition_id, &version_pb, &partition_version);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    {
        // Put a partition version
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_version_key =
                versioned::partition_version_key({instance_id, partition_id});
        VersionPB version_pb;
        version_pb.set_version(100);
        versioned_put(txn.get(), partition_version_key, version_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    VersionPB version_pb1;
    Versionstamp partition_version1;
    {
        MetaReader meta_reader(instance_id, txn_kv.get());
        TxnErrorCode err =
                meta_reader.get_partition_version(partition_id, &version_pb1, &partition_version1);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(version_pb1.version(), 100);
    }

    {
        // Put another partition version
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_version_key =
                versioned::partition_version_key({instance_id, partition_id});
        VersionPB version_pb;
        version_pb.set_version(200);
        versioned_put(txn.get(), partition_version_key, version_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    VersionPB version_pb2;
    Versionstamp partition_version2;
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader meta_reader(instance_id, txn_kv.get());
        TxnErrorCode err = meta_reader.get_partition_version(txn.get(), partition_id, &version_pb2,
                                                             &partition_version2);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(version_pb2.version(), 200);
    }

    ASSERT_LT(partition_version1, partition_version2);
}

TEST(MetaReaderTest, BatchGetPartitionVersion) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string instance_id = "test_instance";
    std::vector<int64_t> partition_ids = {2001, 2002, 2003, 2004};

    {
        // Test empty input
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::vector<int64_t> empty_ids;
        std::unordered_map<int64_t, VersionPB> versions;
        std::unordered_map<int64_t, Versionstamp> versionstamps;
        TxnErrorCode err = meta_reader.get_partition_versions(empty_ids, &versions, &versionstamps);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_TRUE(versions.empty());
        ASSERT_TRUE(versionstamps.empty());
    }

    {
        // Put some partition versions (skip partition_ids[1] to test partial results)
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        for (size_t i = 0; i < partition_ids.size(); ++i) {
            if (i == 1) continue; // Skip partition_ids[1]
            std::string partition_version_key =
                    versioned::partition_version_key({instance_id, partition_ids[i]});
            VersionPB version_pb;
            version_pb.set_version(100 + i * 10); // Different versions: 100, 120, 130
            versioned_put(txn.get(), partition_version_key, version_pb.SerializeAsString());
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test partial results
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::unordered_map<int64_t, VersionPB> versions;
        std::unordered_map<int64_t, Versionstamp> versionstamps;
        TxnErrorCode err =
                meta_reader.get_partition_versions(partition_ids, &versions, &versionstamps);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(versions.size(), 3); // All except partition_ids[1]
        ASSERT_EQ(versionstamps.size(), 3);

        for (size_t i = 0; i < partition_ids.size(); ++i) {
            if (i == 1) {
                ASSERT_EQ(versions.find(partition_ids[i]), versions.end());
                ASSERT_EQ(versionstamps.find(partition_ids[i]), versionstamps.end());
            } else {
                ASSERT_NE(versions.find(partition_ids[i]), versions.end());
                ASSERT_NE(versionstamps.find(partition_ids[i]), versionstamps.end());
                ASSERT_EQ(versions[partition_ids[i]].version(), 100 + i * 10);
            }
        }
    }

    {
        // Put the missing partition version
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_version_key =
                versioned::partition_version_key({instance_id, partition_ids[1]});
        VersionPB version_pb;
        version_pb.set_version(110);
        versioned_put(txn.get(), partition_version_key, version_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test all keys found
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::unordered_map<int64_t, VersionPB> versions;
        std::unordered_map<int64_t, Versionstamp> versionstamps;
        TxnErrorCode err = meta_reader.get_partition_versions(txn.get(), partition_ids, &versions,
                                                              &versionstamps);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(versions.size(), partition_ids.size());
        ASSERT_EQ(versionstamps.size(), partition_ids.size());

        for (size_t i = 0; i < partition_ids.size(); ++i) {
            int64_t partition_id = partition_ids[i];
            ASSERT_NE(versions.find(partition_id), versions.end());
            ASSERT_NE(versionstamps.find(partition_id), versionstamps.end());
            int32_t expected_version = (i == 1) ? 110 : 100 + i * 10;
            ASSERT_EQ(versions[partition_id].version(), expected_version);
        }
    }

    {
        // Test only versionstamps (versions = nullptr)
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::unordered_map<int64_t, Versionstamp> versionstamps;
        TxnErrorCode err =
                meta_reader.get_partition_versions(partition_ids, nullptr, &versionstamps);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(versionstamps.size(), partition_ids.size());
    }

    {
        // Test only versions (versionstamps = nullptr)
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::unordered_map<int64_t, VersionPB> versions;
        TxnErrorCode err = meta_reader.get_partition_versions(partition_ids, &versions, nullptr);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(versions.size(), partition_ids.size());
    }
}

TEST(MetaReaderTest, GetPartitionVersionsWithPendingTxn) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string instance_id = "test_instance";
    std::vector<int64_t> partition_ids = {2101, 2102, 2103, 2104};

    {
        // Test empty input
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::vector<int64_t> empty_ids;
        std::unordered_map<int64_t, int64_t> versions;
        int64_t last_pending_txn_id = -1;
        TxnErrorCode err = meta_reader.get_partition_versions(txn.get(), empty_ids, &versions,
                                                              &last_pending_txn_id);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_TRUE(versions.empty());
        ASSERT_EQ(last_pending_txn_id, -1);
    }

    {
        // Put some partition versions (skip partition_ids[1] to test partial results)
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        for (size_t i = 0; i < partition_ids.size(); ++i) {
            if (i == 1) continue; // Skip partition_ids[1]
            std::string partition_version_key =
                    versioned::partition_version_key({instance_id, partition_ids[i]});
            VersionPB version_pb;
            version_pb.set_version(100 + i * 10); // Different versions: 100, 120, 130
            // Add pending transaction for partition_ids[2]
            if (i == 2) {
                version_pb.add_pending_txn_ids(3001);
                version_pb.add_pending_txn_ids(3002);
            }
            versioned_put(txn.get(), partition_version_key, version_pb.SerializeAsString());
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test partial results - partition not found should be set to 1
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::unordered_map<int64_t, int64_t> versions;
        int64_t last_pending_txn_id = -1;
        TxnErrorCode err = meta_reader.get_partition_versions(txn.get(), partition_ids, &versions,
                                                              &last_pending_txn_id);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(versions.size(), 4); // All partition_ids, missing one should be set to 1

        for (size_t i = 0; i < partition_ids.size(); ++i) {
            int64_t partition_id = partition_ids[i];
            ASSERT_NE(versions.find(partition_id), versions.end());
            if (i == 1) {
                // Missing partition should be set to 1
                ASSERT_EQ(versions[partition_id], 1);
            } else {
                ASSERT_EQ(versions[partition_id], 100 + i * 10);
            }
        }
        // Should return first pending transaction ID from partition_ids[2]
        ASSERT_EQ(last_pending_txn_id, 3001);
    }

    {
        // Put the missing partition version without pending transaction
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_version_key =
                versioned::partition_version_key({instance_id, partition_ids[1]});
        VersionPB version_pb;
        version_pb.set_version(110);
        versioned_put(txn.get(), partition_version_key, version_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test all keys found with no pending transactions
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::unordered_map<int64_t, int64_t> versions;
        int64_t last_pending_txn_id = -1;
        TxnErrorCode err = meta_reader.get_partition_versions(txn.get(), partition_ids, &versions,
                                                              &last_pending_txn_id);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(versions.size(), partition_ids.size());

        for (size_t i = 0; i < partition_ids.size(); ++i) {
            int64_t partition_id = partition_ids[i];
            ASSERT_NE(versions.find(partition_id), versions.end());
            int32_t expected_version = (i == 1) ? 110 : 100 + i * 10;
            ASSERT_EQ(versions[partition_id], expected_version);
        }
        // Should still return first pending transaction ID from partition_ids[2]
        ASSERT_EQ(last_pending_txn_id, 3001);
    }

    {
        // Remove pending transactions from partition_ids[2]
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_version_key =
                versioned::partition_version_key({instance_id, partition_ids[2]});
        VersionPB version_pb;
        version_pb.set_version(120);
        // No pending transactions this time
        versioned_put(txn.get(), partition_version_key, version_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test with no pending transactions
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::unordered_map<int64_t, int64_t> versions;
        int64_t last_pending_txn_id = -1;
        TxnErrorCode err = meta_reader.get_partition_versions(txn.get(), partition_ids, &versions,
                                                              &last_pending_txn_id);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(versions.size(), partition_ids.size());

        for (size_t i = 0; i < partition_ids.size(); ++i) {
            int64_t partition_id = partition_ids[i];
            ASSERT_NE(versions.find(partition_id), versions.end());
            int32_t expected_version = (i == 1) ? 110 : (i == 2) ? 120 : 100 + i * 10;
            ASSERT_EQ(versions[partition_id], expected_version);
        }
        // No pending transactions, so last_pending_txn_id should remain -1
        ASSERT_EQ(last_pending_txn_id, -1);
    }

    {
        // Test using the convenience method (without snapshot parameter)
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::unordered_map<int64_t, int64_t> versions;
        int64_t last_pending_txn_id = -1;
        TxnErrorCode err = meta_reader.get_partition_versions(txn.get(), partition_ids, &versions,
                                                              &last_pending_txn_id);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(versions.size(), partition_ids.size());
        // Verify same results as the explicit snapshot=false test
        for (size_t i = 0; i < partition_ids.size(); ++i) {
            int64_t partition_id = partition_ids[i];
            ASSERT_NE(versions.find(partition_id), versions.end());
            int32_t expected_version = (i == 1) ? 110 : (i == 2) ? 120 : 100 + i * 10;
            ASSERT_EQ(versions[partition_id], expected_version);
        }
        ASSERT_EQ(last_pending_txn_id, -1);
    }
}

TEST(MetaReaderTest, GetTabletLoadStats) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string instance_id = "test_instance";
    int64_t tablet_id = 3001;
    {
        // NOT FOUND
        MetaReader meta_reader(instance_id, txn_kv.get());
        TabletStatsPB tablet_stats;
        Versionstamp tablet_stats_version;
        TxnErrorCode err =
                meta_reader.get_tablet_load_stats(tablet_id, &tablet_stats, &tablet_stats_version);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    {
        // Put a tablet load stats
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_load_stats_key =
                versioned::tablet_load_stats_key({instance_id, tablet_id});
        TabletStatsPB tablet_stats;
        tablet_stats.set_num_rows(1000);
        tablet_stats.set_data_size(500000);
        versioned_put(txn.get(), tablet_load_stats_key, tablet_stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    TabletStatsPB tablet_stats1;
    Versionstamp tablet_stats_version1;
    {
        MetaReader meta_reader(instance_id, txn_kv.get());
        TxnErrorCode err = meta_reader.get_tablet_load_stats(tablet_id, &tablet_stats1,
                                                             &tablet_stats_version1);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats1.num_rows(), 1000);
        ASSERT_EQ(tablet_stats1.data_size(), 500000);
    }

    {
        // Put another tablet load stats
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_load_stats_key =
                versioned::tablet_load_stats_key({instance_id, tablet_id});
        TabletStatsPB tablet_stats;
        tablet_stats.set_num_rows(2000);
        tablet_stats.set_data_size(1000000);
        versioned_put(txn.get(), tablet_load_stats_key, tablet_stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    TabletStatsPB tablet_stats2;
    Versionstamp tablet_stats_version2;
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader meta_reader(instance_id, txn_kv.get());
        TxnErrorCode err = meta_reader.get_tablet_load_stats(txn.get(), tablet_id, &tablet_stats2,
                                                             &tablet_stats_version2);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats2.num_rows(), 2000);
        ASSERT_EQ(tablet_stats2.data_size(), 1000000);
    }

    ASSERT_LT(tablet_stats_version1, tablet_stats_version2);
}

TEST(MetaReaderTest, GetTabletCompactStats) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string instance_id = "test_instance";
    int64_t tablet_id = 3001;
    {
        // NOT FOUND
        MetaReader meta_reader(instance_id, txn_kv.get());
        TabletStatsPB tablet_stats;
        Versionstamp tablet_stats_version;
        TxnErrorCode err = meta_reader.get_tablet_compact_stats(tablet_id, &tablet_stats,
                                                                &tablet_stats_version);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    {
        // Put a tablet compact stats
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_compact_stats_key =
                versioned::tablet_compact_stats_key({instance_id, tablet_id});
        TabletStatsPB tablet_stats;
        tablet_stats.set_num_rows(500);
        tablet_stats.set_data_size(250000);
        versioned_put(txn.get(), tablet_compact_stats_key, tablet_stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    TabletStatsPB tablet_stats1;
    Versionstamp tablet_stats_version1;
    {
        MetaReader meta_reader(instance_id, txn_kv.get());
        TxnErrorCode err = meta_reader.get_tablet_compact_stats(tablet_id, &tablet_stats1,
                                                                &tablet_stats_version1);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats1.num_rows(), 500);
        ASSERT_EQ(tablet_stats1.data_size(), 250000);
    }

    {
        // Put another tablet compact stats
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_compact_stats_key =
                versioned::tablet_compact_stats_key({instance_id, tablet_id});
        TabletStatsPB tablet_stats;
        tablet_stats.set_num_rows(1000);
        tablet_stats.set_data_size(500000);
        versioned_put(txn.get(), tablet_compact_stats_key, tablet_stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    TabletStatsPB tablet_stats2;
    Versionstamp tablet_stats_version2;
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader meta_reader(instance_id, txn_kv.get());
        TxnErrorCode err = meta_reader.get_tablet_compact_stats(
                txn.get(), tablet_id, &tablet_stats2, &tablet_stats_version2);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats2.num_rows(), 1000);
        ASSERT_EQ(tablet_stats2.data_size(), 500000);
    }
    ASSERT_LT(tablet_stats_version1, tablet_stats_version2);
}

TEST(MetaReaderTest, GetTabletMergedStats) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string instance_id = "test_instance";
    int64_t tablet_id = 3001;

    {
        // Test when both load and compact stats are not found
        MetaReader meta_reader(instance_id, txn_kv.get());
        TabletStatsPB tablet_stats;
        Versionstamp tablet_stats_version;
        TxnErrorCode err = meta_reader.get_tablet_merged_stats(tablet_id, &tablet_stats,
                                                               &tablet_stats_version);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    {
        // Put tablet load stats
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_load_stats_key =
                versioned::tablet_load_stats_key({instance_id, tablet_id});
        TabletStatsPB load_stats;
        load_stats.set_num_rows(1000);
        load_stats.set_num_rowsets(10);
        load_stats.set_num_segments(20);
        load_stats.set_data_size(500000);
        load_stats.set_index_size(50000);
        load_stats.set_segment_size(600000);
        versioned_put(txn.get(), tablet_load_stats_key, load_stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test when only load stats exist (compact stats not found)
        MetaReader meta_reader(instance_id, txn_kv.get());
        TabletStatsPB tablet_stats;
        Versionstamp tablet_stats_version;
        TxnErrorCode err = meta_reader.get_tablet_merged_stats(tablet_id, &tablet_stats,
                                                               &tablet_stats_version);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    Versionstamp load_version, compact_version;
    {
        // Put tablet compact stats
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_compact_stats_key =
                versioned::tablet_compact_stats_key({instance_id, tablet_id});
        TabletStatsPB compact_stats;
        compact_stats.set_base_compaction_cnt(5);
        compact_stats.set_cumulative_compaction_cnt(10);
        compact_stats.set_cumulative_point(100);
        compact_stats.set_last_base_compaction_time_ms(1234567890);
        compact_stats.set_last_cumu_compaction_time_ms(2345678901);
        compact_stats.set_full_compaction_cnt(2);
        compact_stats.set_last_full_compaction_time_ms(3456789012);
        compact_stats.set_num_rows(500);
        compact_stats.set_num_rowsets(5);
        compact_stats.set_num_segments(15);
        compact_stats.set_data_size(250000);
        compact_stats.set_index_size(25000);
        compact_stats.set_segment_size(300000);
        versioned_put(txn.get(), tablet_compact_stats_key, compact_stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    TabletStatsPB merged_stats;
    Versionstamp merged_version;
    {
        // Test merged stats
        MetaReader meta_reader(instance_id, txn_kv.get());
        TxnErrorCode err =
                meta_reader.get_tablet_merged_stats(tablet_id, &merged_stats, &merged_version);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        // Check compaction-related fields (copied from compact stats)
        EXPECT_EQ(merged_stats.base_compaction_cnt(), 5);
        EXPECT_EQ(merged_stats.cumulative_compaction_cnt(), 10);
        EXPECT_EQ(merged_stats.cumulative_point(), 100);
        EXPECT_EQ(merged_stats.last_base_compaction_time_ms(), 1234567890);
        EXPECT_EQ(merged_stats.last_cumu_compaction_time_ms(), 2345678901);
        EXPECT_EQ(merged_stats.full_compaction_cnt(), 2);
        EXPECT_EQ(merged_stats.last_full_compaction_time_ms(), 3456789012);

        // Check data-related fields (sum of load stats and compact stats)
        EXPECT_EQ(merged_stats.num_rows(), 1500);       // 1000 + 500
        EXPECT_EQ(merged_stats.num_rowsets(), 15);      // 10 + 5
        EXPECT_EQ(merged_stats.num_segments(), 35);     // 20 + 15
        EXPECT_EQ(merged_stats.data_size(), 750000);    // 500000 + 250000
        EXPECT_EQ(merged_stats.index_size(), 75000);    // 50000 + 25000
        EXPECT_EQ(merged_stats.segment_size(), 900000); // 600000 + 300000
    }

    {
        // Get individual versions for comparison
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader meta_reader(instance_id, txn_kv.get());

        TabletStatsPB load_stats, compact_stats;
        TxnErrorCode err1 =
                meta_reader.get_tablet_load_stats(txn.get(), tablet_id, &load_stats, &load_version);
        TxnErrorCode err2 = meta_reader.get_tablet_compact_stats(txn.get(), tablet_id,
                                                                 &compact_stats, &compact_version);
        ASSERT_EQ(err1, TxnErrorCode::TXN_OK);
        ASSERT_EQ(err2, TxnErrorCode::TXN_OK);

        // Merged version should be the max of load and compact versions
        if (load_version < compact_version) {
            ASSERT_EQ(merged_version, compact_version);
        } else {
            ASSERT_EQ(merged_version, load_version);
        }
    }

    {
        // Update load stats and test version update
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_load_stats_key =
                versioned::tablet_load_stats_key({instance_id, tablet_id});
        TabletStatsPB updated_load_stats;
        updated_load_stats.set_num_rows(2000);
        updated_load_stats.set_num_rowsets(20);
        updated_load_stats.set_num_segments(40);
        updated_load_stats.set_data_size(1000000);
        updated_load_stats.set_index_size(100000);
        updated_load_stats.set_segment_size(1200000);
        versioned_put(txn.get(), tablet_load_stats_key, updated_load_stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    TabletStatsPB updated_merged_stats;
    Versionstamp updated_merged_version;
    {
        // Test updated merged stats
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader meta_reader(instance_id, txn_kv.get());
        TxnErrorCode err = meta_reader.get_tablet_merged_stats(
                txn.get(), tablet_id, &updated_merged_stats, &updated_merged_version);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        // Check updated data-related fields (sum of updated load stats and compact stats)
        EXPECT_EQ(updated_merged_stats.num_rows(), 2500);        // 2000 + 500
        EXPECT_EQ(updated_merged_stats.num_rowsets(), 25);       // 20 + 5
        EXPECT_EQ(updated_merged_stats.num_segments(), 55);      // 40 + 15
        EXPECT_EQ(updated_merged_stats.data_size(), 1250000);    // 1000000 + 250000
        EXPECT_EQ(updated_merged_stats.index_size(), 125000);    // 100000 + 25000
        EXPECT_EQ(updated_merged_stats.segment_size(), 1500000); // 1200000 + 300000

        // Compaction fields should remain the same
        EXPECT_EQ(updated_merged_stats.base_compaction_cnt(), 5);
        EXPECT_EQ(updated_merged_stats.cumulative_compaction_cnt(), 10);
    }

    // Merged version should be updated
    ASSERT_LT(merged_version, updated_merged_version);
}

TEST(MetaReaderTest, GetTabletIndex) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string instance_id = "test_instance";
    int64_t tablet_id = 5001;
    {
        // NOT FOUND
        MetaReader meta_reader(instance_id, txn_kv.get());
        TabletIndexPB tablet_index;
        TxnErrorCode err = meta_reader.get_tablet_index(tablet_id, &tablet_index);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    {
        // Put a tablet index
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_index_key = versioned::tablet_index_key({instance_id, tablet_id});
        TabletIndexPB tablet_index;
        tablet_index.set_db_id(1001);
        tablet_index.set_table_id(2001);
        tablet_index.set_index_id(3001);
        tablet_index.set_partition_id(4001);
        tablet_index.set_tablet_id(tablet_id);
        txn->put(tablet_index_key, tablet_index.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    TabletIndexPB tablet_index1;
    {
        MetaReader meta_reader(instance_id, txn_kv.get());
        TxnErrorCode err = meta_reader.get_tablet_index(tablet_id, &tablet_index1);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_index1.db_id(), 1001);
        ASSERT_EQ(tablet_index1.table_id(), 2001);
        ASSERT_EQ(tablet_index1.index_id(), 3001);
        ASSERT_EQ(tablet_index1.partition_id(), 4001);
        ASSERT_EQ(tablet_index1.tablet_id(), tablet_id);
    }

    {
        // Put another tablet index (update)
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_index_key = versioned::tablet_index_key({instance_id, tablet_id});
        TabletIndexPB tablet_index;
        tablet_index.set_db_id(1002);
        tablet_index.set_table_id(2002);
        tablet_index.set_index_id(3002);
        tablet_index.set_partition_id(4002);
        tablet_index.set_tablet_id(tablet_id);
        txn->put(tablet_index_key, tablet_index.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    TabletIndexPB tablet_index2;
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader meta_reader(instance_id, txn_kv.get());
        TxnErrorCode err = meta_reader.get_tablet_index(txn.get(), tablet_id, &tablet_index2);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_index2.db_id(), 1002);
        ASSERT_EQ(tablet_index2.table_id(), 2002);
        ASSERT_EQ(tablet_index2.index_id(), 3002);
        ASSERT_EQ(tablet_index2.partition_id(), 4002);
        ASSERT_EQ(tablet_index2.tablet_id(), tablet_id);
    }
}

TEST(MetaReaderTest, BatchGetTabletIndexes) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string instance_id = "test_instance";
    std::vector<int64_t> tablet_ids = {5001, 5002, 5003, 5004};

    {
        // Test empty input
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::vector<int64_t> empty_ids;
        std::unordered_map<int64_t, TabletIndexPB> tablet_indexes;
        TxnErrorCode err = meta_reader.get_tablet_indexes(empty_ids, &tablet_indexes);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_TRUE(tablet_indexes.empty());
    }

    {
        // Put some tablet indexes (skip tablet_ids[1] to test partial results)
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        for (size_t i = 0; i < tablet_ids.size(); ++i) {
            if (i == 1) continue; // Skip tablet_ids[1]
            std::string tablet_index_key =
                    versioned::tablet_index_key({instance_id, tablet_ids[i]});
            TabletIndexPB tablet_index;
            tablet_index.set_db_id(1000 + i);
            tablet_index.set_table_id(2000 + i);
            tablet_index.set_index_id(3000 + i);
            tablet_index.set_partition_id(4000 + i);
            tablet_index.set_tablet_id(tablet_ids[i]);
            txn->put(tablet_index_key, tablet_index.SerializeAsString());
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test partial results
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::unordered_map<int64_t, TabletIndexPB> tablet_indexes;
        TxnErrorCode err = meta_reader.get_tablet_indexes(tablet_ids, &tablet_indexes);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_indexes.size(), 3); // All except tablet_ids[1]

        for (size_t i = 0; i < tablet_ids.size(); ++i) {
            if (i == 1) {
                ASSERT_EQ(tablet_indexes.find(tablet_ids[i]), tablet_indexes.end());
            } else {
                ASSERT_NE(tablet_indexes.find(tablet_ids[i]), tablet_indexes.end());
                ASSERT_EQ(tablet_indexes[tablet_ids[i]].db_id(), 1000 + i);
                ASSERT_EQ(tablet_indexes[tablet_ids[i]].table_id(), 2000 + i);
                ASSERT_EQ(tablet_indexes[tablet_ids[i]].index_id(), 3000 + i);
                ASSERT_EQ(tablet_indexes[tablet_ids[i]].partition_id(), 4000 + i);
                ASSERT_EQ(tablet_indexes[tablet_ids[i]].tablet_id(), tablet_ids[i]);
            }
        }
    }

    {
        // Put the missing tablet index
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_index_key = versioned::tablet_index_key({instance_id, tablet_ids[1]});
        TabletIndexPB tablet_index;
        tablet_index.set_db_id(1001);
        tablet_index.set_table_id(2001);
        tablet_index.set_index_id(3001);
        tablet_index.set_partition_id(4001);
        tablet_index.set_tablet_id(tablet_ids[1]);
        txn->put(tablet_index_key, tablet_index.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test all keys found
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::unordered_map<int64_t, TabletIndexPB> tablet_indexes;
        TxnErrorCode err = meta_reader.get_tablet_indexes(txn.get(), tablet_ids, &tablet_indexes);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_indexes.size(), tablet_ids.size());

        for (size_t i = 0; i < tablet_ids.size(); ++i) {
            int64_t tablet_id = tablet_ids[i];
            ASSERT_NE(tablet_indexes.find(tablet_id), tablet_indexes.end());
            int64_t expected_db_id = (i == 1) ? 1001 : 1000 + i;
            int64_t expected_table_id = (i == 1) ? 2001 : 2000 + i;
            int64_t expected_index_id = (i == 1) ? 3001 : 3000 + i;
            int64_t expected_partition_id = (i == 1) ? 4001 : 4000 + i;
            ASSERT_EQ(tablet_indexes[tablet_id].db_id(), expected_db_id);
            ASSERT_EQ(tablet_indexes[tablet_id].table_id(), expected_table_id);
            ASSERT_EQ(tablet_indexes[tablet_id].index_id(), expected_index_id);
            ASSERT_EQ(tablet_indexes[tablet_id].partition_id(), expected_partition_id);
            ASSERT_EQ(tablet_indexes[tablet_id].tablet_id(), tablet_id);
        }
    }
}

TEST(MetaReaderTest, GetRowsetMetas) {
    using doris::RowsetMetaCloudPB;

    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string instance_id = "test_instance";
    int64_t tablet_id = 4001;
    int64_t start_version = 1;
    int64_t end_version = 10;

    {
        // Test empty result when no rowsets exist
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::vector<RowsetMetaCloudPB> rowset_metas;
        TxnErrorCode err =
                meta_reader.get_rowset_metas(tablet_id, start_version, end_version, &rowset_metas);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_TRUE(rowset_metas.empty());
    }

    // Create some load rowsets (import scenario)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Create load rowsets with versions 2, 3, 4, 5
        for (int64_t version = 2; version <= 5; ++version) {
            std::string load_key =
                    versioned::meta_rowset_load_key({instance_id, tablet_id, version});
            RowsetMetaCloudPB rowset_meta;
            rowset_meta.set_rowset_id(0);
            rowset_meta.set_rowset_id_v2(fmt::format("load_rowset_{}", version));
            rowset_meta.set_start_version(version);
            rowset_meta.set_end_version(version);
            rowset_meta.set_num_rows(100 * version);
            rowset_meta.set_tablet_id(tablet_id);
            ASSERT_TRUE(versioned::document_put(txn.get(), load_key, std::move(rowset_meta)));
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test getting load rowsets
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::vector<RowsetMetaCloudPB> rowset_metas;
        TxnErrorCode err = meta_reader.get_rowset_metas(tablet_id, 2, 5, &rowset_metas);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK) << dump_range(txn_kv.get());
        ASSERT_EQ(rowset_metas.size(), 4) << [&] {
            std::ostringstream oss;
            for (const auto& meta : rowset_metas) {
                oss << meta.rowset_id_v2() << "[" << meta.start_version() << ", "
                    << meta.end_version() << "]\n";
            }
            return oss.str();
        }() << dump_range(txn_kv.get());

        // Verify rowsets are sorted by end_version
        for (size_t i = 0; i < rowset_metas.size(); ++i) {
            ASSERT_EQ(rowset_metas[i].end_version(), 2 + i);
            ASSERT_EQ(rowset_metas[i].start_version(), 2 + i);
            ASSERT_EQ(rowset_metas[i].num_rows(), 100 * (2 + i));
        }
    }

    // Create compact rowset that covers versions 3-4
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::string compact_key = versioned::meta_rowset_compact_key({instance_id, tablet_id, 4});
        RowsetMetaCloudPB compact_rowset;
        compact_rowset.set_rowset_id(0);
        compact_rowset.set_rowset_id_v2("compact_rowset_3_4");
        compact_rowset.set_start_version(3);
        compact_rowset.set_end_version(4);
        compact_rowset.set_num_rows(700); // 300 + 400 = 700
        compact_rowset.set_tablet_id(tablet_id);
        ASSERT_TRUE(versioned::document_put(txn.get(), compact_key, std::move(compact_rowset)));

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test getting rowsets after compaction - compact should override load rowsets
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::vector<RowsetMetaCloudPB> rowset_metas;
        TxnErrorCode err = meta_reader.get_rowset_metas(tablet_id, 2, 5, &rowset_metas);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_metas.size(), 3); // version 2, compact(3-4), version 5

        // Check first rowset (version 2)
        ASSERT_EQ(rowset_metas[0].end_version(), 2);
        ASSERT_EQ(rowset_metas[0].start_version(), 2);
        ASSERT_EQ(rowset_metas[0].rowset_id_v2(), "load_rowset_2");

        // Check compact rowset (versions 3-4)
        ASSERT_EQ(rowset_metas[1].end_version(), 4);
        ASSERT_EQ(rowset_metas[1].start_version(), 3);
        ASSERT_EQ(rowset_metas[1].rowset_id_v2(), "compact_rowset_3_4");
        ASSERT_EQ(rowset_metas[1].num_rows(), 700);

        // Check last rowset (version 5)
        ASSERT_EQ(rowset_metas[2].end_version(), 5);
        ASSERT_EQ(rowset_metas[2].start_version(), 5);
        ASSERT_EQ(rowset_metas[2].rowset_id_v2(), "load_rowset_5");
    }

    // Test range query that only includes part of the rowsets
    {
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::vector<RowsetMetaCloudPB> rowset_metas;
        TxnErrorCode err = meta_reader.get_rowset_metas(tablet_id, 3, 4, &rowset_metas);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_metas.size(), 1); // Only the compact rowset
        ASSERT_EQ(rowset_metas[0].rowset_id_v2(), "compact_rowset_3_4");
    }

    // Test with snapshot version functionality
    Versionstamp snapshot_version;
    {
        // Get current snapshot version
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::vector<RowsetMetaCloudPB> rowset_metas;
        TxnErrorCode err = meta_reader.get_rowset_metas(txn.get(), tablet_id, 2, 5, &rowset_metas);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        // Get version from the transaction's read version
        int64_t version = 0;
        ASSERT_EQ(txn->get_read_version(&version), TxnErrorCode::TXN_OK);
        snapshot_version = Versionstamp(version, 1);
        LOG(INFO) << "Snapshot version: " << snapshot_version.version();
        LOG(INFO) << "Snapshot version: " << snapshot_version.to_string();
    }

    // Add another compact rowset that covers version 5
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::string compact_key = versioned::meta_rowset_compact_key({instance_id, tablet_id, 5});
        RowsetMetaCloudPB compact_rowset;
        compact_rowset.set_rowset_id(0);
        compact_rowset.set_rowset_id_v2("compact_rowset_5");
        compact_rowset.set_start_version(5);
        compact_rowset.set_end_version(5);
        compact_rowset.set_num_rows(500);
        compact_rowset.set_tablet_id(tablet_id);
        ASSERT_TRUE(versioned::document_put(txn.get(), compact_key, std::move(compact_rowset)));

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test reading with snapshot version - should get old data
        LOG(INFO) << "Reading with snapshot version: " << snapshot_version.version();
        MetaReader meta_reader(instance_id, txn_kv.get(), snapshot_version);
        std::vector<RowsetMetaCloudPB> rowset_metas;
        TxnErrorCode err = meta_reader.get_rowset_metas(tablet_id, 2, 5, &rowset_metas);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_metas.size(), 3) << [&]() {
            std::ostringstream oss;
            for (const auto& meta : rowset_metas) {
                oss << meta.rowset_id_v2() << "[" << meta.start_version() << ", "
                    << meta.end_version() << "]\n";
            }
            return oss.str();
        }() << dump_range(txn_kv.get()); // Should still see load_rowset_5, not compact_rowset_5
        ASSERT_EQ(rowset_metas[2].rowset_id_v2(), "load_rowset_5") << dump_range(txn_kv.get());
    }

    {
        // Test reading without snapshot - should get new data
        LOG(INFO) << "Reading without snapshot version";
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::vector<RowsetMetaCloudPB> rowset_metas;
        TxnErrorCode err = meta_reader.get_rowset_metas(tablet_id, 2, 5, &rowset_metas);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_metas.size(), 3); // version 2, compact(3-4), compact(5)
        ASSERT_EQ(rowset_metas[2].rowset_id_v2(), "compact_rowset_5");
    }

    // Add a compact rowset cover [3-5]
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::string compact_key = versioned::meta_rowset_compact_key({instance_id, tablet_id, 5});
        RowsetMetaCloudPB compact_rowset;
        compact_rowset.set_rowset_id(0);
        compact_rowset.set_rowset_id_v2("compact_rowset_3_5");
        compact_rowset.set_start_version(3);
        compact_rowset.set_end_version(5);
        compact_rowset.set_num_rows(1200); // 300 + 400 + 500
        compact_rowset.set_tablet_id(tablet_id);
        ASSERT_TRUE(versioned::document_put(txn.get(), compact_key, std::move(compact_rowset)));

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test getting rowsets after new compaction
        MetaReader meta_reader(instance_id, txn_kv.get());
        std::vector<RowsetMetaCloudPB> rowset_metas;
        TxnErrorCode err = meta_reader.get_rowset_metas(tablet_id, 2, 5, &rowset_metas);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_metas.size(), 2); // version 2, compact(3-5)

        // Check first rowset (version 2)
        ASSERT_EQ(rowset_metas[0].end_version(), 2);
        ASSERT_EQ(rowset_metas[0].start_version(), 2);
        ASSERT_EQ(rowset_metas[0].rowset_id_v2(), "load_rowset_2");

        // Check new compact rowset (versions 3-5)
        ASSERT_EQ(rowset_metas[1].end_version(), 5);
        ASSERT_EQ(rowset_metas[1].start_version(), 3);
        ASSERT_EQ(rowset_metas[1].rowset_id_v2(), "compact_rowset_3_5");
    }

    {
        // Test getting rowset with old snapshot version
        LOG(INFO) << "Reading with old snapshot version: " << snapshot_version.version();
        MetaReader meta_reader(instance_id, txn_kv.get(), snapshot_version);
        std::vector<RowsetMetaCloudPB> rowset_metas;
        TxnErrorCode err = meta_reader.get_rowset_metas(tablet_id, 2, 5, &rowset_metas);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_metas.size(), 3) << [&]() {
            std::ostringstream oss;
            for (const auto& meta : rowset_metas) {
                oss << meta.rowset_id_v2() << "[" << meta.start_version() << ", "
                    << meta.end_version() << "]\n";
            }
            return oss.str();
        }() << dump_range(txn_kv.get());
        ASSERT_EQ(rowset_metas[2].rowset_id_v2(), "load_rowset_5") << dump_range(txn_kv.get());
        // Should still see load_rowset_5, not compact_rowset_3_5
        ASSERT_EQ(rowset_metas[2].start_version(), 5);
        ASSERT_EQ(rowset_metas[2].end_version(), 5);
        ASSERT_EQ(rowset_metas[2].num_rows(), 500) << dump_range(txn_kv.get());
    }
}

TEST(MetaReaderTest, GetPartitionPendingTxnId) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string instance_id = "test_instance";
    int64_t partition_id = 3001;

    {
        // Test with non-existent partition
        MetaReader meta_reader(instance_id, txn_kv.get());
        int64_t first_txn_id;
        TxnErrorCode err = meta_reader.get_partition_pending_txn_id(partition_id, &first_txn_id);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        EXPECT_EQ(first_txn_id, -1) << "Expected -1 for non-existent partition";
    }

    {
        // Put a partition version without pending transactions
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_version_key =
                versioned::partition_version_key({instance_id, partition_id});
        VersionPB version_pb;
        version_pb.set_version(100);
        versioned_put(txn.get(), partition_version_key, version_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test with no pending transactions - should return -1
        MetaReader meta_reader(instance_id, txn_kv.get());
        int64_t first_txn_id;
        TxnErrorCode err = meta_reader.get_partition_pending_txn_id(partition_id, &first_txn_id);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(first_txn_id, -1);
    }

    {
        // Put a partition version with pending transactions
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_version_key =
                versioned::partition_version_key({instance_id, partition_id});
        VersionPB version_pb;
        version_pb.set_version(200);
        version_pb.add_pending_txn_ids(1001);
        version_pb.add_pending_txn_ids(1002);
        version_pb.add_pending_txn_ids(1003);
        versioned_put(txn.get(), partition_version_key, version_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test with pending transactions - should return first txn_id
        MetaReader meta_reader(instance_id, txn_kv.get());
        int64_t first_txn_id;
        TxnErrorCode err = meta_reader.get_partition_pending_txn_id(partition_id, &first_txn_id);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(first_txn_id, 1001);
    }

    {
        // Test with transaction parameter
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        MetaReader meta_reader(instance_id, txn_kv.get());
        int64_t first_txn_id;
        TxnErrorCode err =
                meta_reader.get_partition_pending_txn_id(txn.get(), partition_id, &first_txn_id);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(first_txn_id, 1001);
    }

    {
        // Put a partition version with single pending transaction
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_version_key =
                versioned::partition_version_key({instance_id, partition_id});
        VersionPB version_pb;
        version_pb.set_version(300);
        version_pb.add_pending_txn_ids(2001);
        versioned_put(txn.get(), partition_version_key, version_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test with single pending transaction
        MetaReader meta_reader(instance_id, txn_kv.get());
        int64_t first_txn_id;
        TxnErrorCode err = meta_reader.get_partition_pending_txn_id(partition_id, &first_txn_id);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(first_txn_id, 2001);
    }

    {
        // Test with snapshot functionality
        // First get the current snapshot version
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        VersionPB current_version;
        Versionstamp current_versionstamp;
        MetaReader current_reader(instance_id, txn_kv.get());
        TxnErrorCode err = current_reader.get_partition_version(
                txn.get(), partition_id, &current_version, &current_versionstamp);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        // Create a MetaReader with snapshot
        Versionstamp snapshot_version(current_version.version() + 1, 0);
        MetaReader snapshot_reader(instance_id, txn_kv.get(), snapshot_version, true);
        int64_t first_txn_id;
        err = snapshot_reader.get_partition_pending_txn_id(partition_id, &first_txn_id);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(first_txn_id, 2001);
    }
}

TEST(MetaReaderTest, GetIndexIndex) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string instance_id = "test_instance";
    int64_t index_id = 3001;

    {
        // NOT FOUND
        MetaReader meta_reader(instance_id, txn_kv.get());
        IndexIndexPB index;
        TxnErrorCode err = meta_reader.get_index_index(index_id, &index);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    {
        // Put an index index
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string index_index_key = versioned::index_index_key({instance_id, index_id});
        IndexIndexPB index_index;
        index_index.set_db_id(1001);
        index_index.set_table_id(2001);
        txn->put(index_index_key, index_index.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        MetaReader meta_reader(instance_id, txn_kv.get());
        IndexIndexPB index;
        TxnErrorCode err = meta_reader.get_index_index(index_id, &index);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(index.db_id(), 1001);
        ASSERT_EQ(index.table_id(), 2001);
    }
}

TEST(MetaReaderTest, GetPartitionIndex) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string instance_id = "test_instance";
    int64_t partition_id = 4001;

    {
        // NOT FOUND
        MetaReader meta_reader(instance_id, txn_kv.get());
        PartitionIndexPB partition_index;
        TxnErrorCode err = meta_reader.get_partition_index(partition_id, &partition_index);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    {
        // Put a partition index
        PartitionIndexPB partition_index_pb;
        partition_index_pb.set_db_id(100);
        partition_index_pb.set_table_id(200);

        std::string partition_index_value;
        ASSERT_TRUE(partition_index_pb.SerializeToString(&partition_index_value));

        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_index_key =
                versioned::partition_index_key({instance_id, partition_id});
        txn->put(partition_index_key, partition_index_value);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        MetaReader meta_reader(instance_id, txn_kv.get());
        PartitionIndexPB partition_index;
        TxnErrorCode err = meta_reader.get_partition_index(partition_id, &partition_index);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        ASSERT_EQ(partition_index.db_id(), 100);
        ASSERT_EQ(partition_index.table_id(), 200);
    }
}

TEST(MetaReaderTest, GetLoadRowsetMeta) {
    using doris::RowsetMetaCloudPB;

    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    std::string instance_id = "test_instance";
    int64_t tablet_id = 5001;
    int64_t version = 10;

    {
        // Test key not found when no rowset exists
        MetaReader meta_reader(instance_id, txn_kv.get());
        RowsetMetaCloudPB rowset_meta;
        TxnErrorCode err = meta_reader.get_load_rowset_meta(tablet_id, version, &rowset_meta);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    // Create a load rowset
    RowsetMetaCloudPB expected_rowset_meta;
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::string load_key = versioned::meta_rowset_load_key({instance_id, tablet_id, version});
        expected_rowset_meta.set_rowset_id(0);
        expected_rowset_meta.set_rowset_id_v2(fmt::format("test_load_rowset_{}", version));
        expected_rowset_meta.set_start_version(version);
        expected_rowset_meta.set_end_version(version);
        expected_rowset_meta.set_num_rows(1000);
        expected_rowset_meta.set_tablet_id(tablet_id);

        ASSERT_TRUE(versioned::document_put(txn.get(), load_key, std::move(expected_rowset_meta)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test successful get with created transaction
        MetaReader meta_reader(instance_id, txn_kv.get());
        RowsetMetaCloudPB rowset_meta;
        TxnErrorCode err = meta_reader.get_load_rowset_meta(tablet_id, version, &rowset_meta);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        // Verify fields match
        ASSERT_EQ(rowset_meta.rowset_id_v2(), expected_rowset_meta.rowset_id_v2());
        ASSERT_EQ(rowset_meta.start_version(), expected_rowset_meta.start_version());
        ASSERT_EQ(rowset_meta.end_version(), expected_rowset_meta.end_version());
        ASSERT_EQ(rowset_meta.num_rows(), expected_rowset_meta.num_rows());
        ASSERT_EQ(rowset_meta.tablet_id(), expected_rowset_meta.tablet_id());
    }

    {
        // Test successful get with provided transaction
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        MetaReader meta_reader(instance_id, txn_kv.get());
        RowsetMetaCloudPB rowset_meta;
        TxnErrorCode err =
                meta_reader.get_load_rowset_meta(txn.get(), tablet_id, version, &rowset_meta);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        // Verify key fields match
        ASSERT_EQ(rowset_meta.rowset_id_v2(), expected_rowset_meta.rowset_id_v2());
        ASSERT_EQ(rowset_meta.start_version(), expected_rowset_meta.start_version());
        ASSERT_EQ(rowset_meta.end_version(), expected_rowset_meta.end_version());
        ASSERT_EQ(rowset_meta.tablet_id(), expected_rowset_meta.tablet_id());
    }

    // Test with snapshot version functionality
    Versionstamp snapshot_version;
    {
        // Get current snapshot version
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        int64_t version_value = 0;
        ASSERT_EQ(txn->get_read_version(&version_value), TxnErrorCode::TXN_OK);
        snapshot_version = Versionstamp(version_value, 1);
    }

    // Update the rowset
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        std::string load_key = versioned::meta_rowset_load_key({instance_id, tablet_id, version});
        RowsetMetaCloudPB updated_rowset_meta = expected_rowset_meta;
        updated_rowset_meta.set_num_rows(2000); // Update row count

        ASSERT_TRUE(versioned::document_put(txn.get(), load_key, std::move(updated_rowset_meta)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    {
        // Test reading with snapshot version - should get old data
        MetaReader meta_reader(instance_id, txn_kv.get(), snapshot_version);
        RowsetMetaCloudPB rowset_meta;
        TxnErrorCode err = meta_reader.get_load_rowset_meta(tablet_id, version, &rowset_meta);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        // Should get original values
        ASSERT_EQ(rowset_meta.num_rows(), 1000);
    }

    {
        // Test reading without snapshot - should get new data
        MetaReader meta_reader(instance_id, txn_kv.get());
        RowsetMetaCloudPB rowset_meta;
        TxnErrorCode err = meta_reader.get_load_rowset_meta(tablet_id, version, &rowset_meta);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        // Should get updated values
        ASSERT_EQ(rowset_meta.num_rows(), 2000);
    }

    {
        // Test with snapshot flag
        MetaReader meta_reader(instance_id, txn_kv.get(), true);
        RowsetMetaCloudPB rowset_meta;
        TxnErrorCode err = meta_reader.get_load_rowset_meta(tablet_id, version, &rowset_meta);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        // Should get current values since snapshot flag is set but no snapshot version
        ASSERT_EQ(rowset_meta.num_rows(), 2000);
    }

    {
        // Test getting non-existent version
        MetaReader meta_reader(instance_id, txn_kv.get());
        RowsetMetaCloudPB rowset_meta;
        TxnErrorCode err = meta_reader.get_load_rowset_meta(tablet_id, version + 1, &rowset_meta);
        ASSERT_EQ(err, TxnErrorCode::TXN_KEY_NOT_FOUND);
    }
}
