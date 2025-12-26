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

#include "meta-store/clone_chain_reader.h"

#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-death-test.h>
#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "meta-store/blob_message.h"
#include "meta-store/document_message.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "resource-manager/resource_manager.h"

using namespace doris::cloud;

int main(int argc, char** argv) {
    config::log_dir = "./log/";
    if (!doris::cloud::init_glog("clone_chain_reader_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

// Convert a string to a hex-escaped string.
// A non-displayed character is represented as \xHH where HH is the hexadecimal value of the character.
// A displayed character is represented as itself.
std::string escape_hex(std::string_view data) {
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

std::string dump_range(TxnKv* txn_kv, std::string_view begin = "", std::string_view end = "\xFF") {
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

class MockResourceManager : public ResourceManager {
public:
    using ResourceManager::ResourceManager;
    ~MockResourceManager() override = default;

    void add_instance_source_snapshot_info(const std::string& instance_id,
                                           const std::string& source_instance_id,
                                           const Versionstamp& source_snapshot_version) {
        instance_source_snapshot_info_[instance_id] =
                std::make_pair(source_instance_id, source_snapshot_version);
    }

    bool get_source_snapshot_info(const std::string& instance_id, std::string* source_instance_id,
                                  Versionstamp* source_snapshot_version) override {
        auto it = instance_source_snapshot_info_.find(instance_id);
        if (it == instance_source_snapshot_info_.end()) {
            return false;
        }
        *source_instance_id = it->second.first;
        *source_snapshot_version = it->second.second;
        return true;
    }

    std::unordered_map<std::string, std::pair<std::string, Versionstamp>>
            instance_source_snapshot_info_;
};

class CloneChainReaderTest : public ::testing::Test {
public:
    void SetUp() override {
        txn_kv_ = std::make_shared<MemTxnKv>();
        auto resource_mgr = std::make_unique<MockResourceManager>(txn_kv_);

        // A (1000) -> B (2000) -> C (3000)
        snapshot_versions_ = {1000, 2000, 3000};
        instance_ids_ = {"A", "B", "C"};

        // A has no source
        for (size_t i = 1; i < instance_ids_.size(); ++i) {
            std::string instance_id = instance_ids_[i];
            resource_mgr->add_instance_source_snapshot_info(instance_id, instance_ids_[i - 1],
                                                            snapshot_versions_[i - 1]);
        }

        resource_mgr_ = std::move(resource_mgr);
    }

    void TearDown() override {}

protected:
    std::shared_ptr<TxnKv> txn_kv_;
    std::unique_ptr<ResourceManager> resource_mgr_;
    std::vector<int64_t> snapshot_versions_;
    std::vector<std::string> instance_ids_;
};

TEST_F(CloneChainReaderTest, GetTableVersion) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    // Insert table version in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string table_version_key = versioned::table_version_key({instance_ids_[0], 1});
        std::string key = encode_versioned_key(table_version_key, Versionstamp(100, 1));
        txn->put(key, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Get table version from instance C
    {
        Versionstamp table_version;
        ASSERT_EQ(reader.get_table_version(1, &table_version), TxnErrorCode::TXN_OK);
        ASSERT_EQ(table_version, Versionstamp(100, 1));
    }

    // Get non-existing table version
    {
        Versionstamp table_version;
        ASSERT_EQ(reader.get_table_version(2, &table_version), TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    // Insert a large version in instance B
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string table_version_key = versioned::table_version_key({instance_ids_[1], 1});
        std::string key = encode_versioned_key(
                table_version_key, Versionstamp(snapshot_versions_[1], 1)); // a large version
        txn->put(key, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Get table version from instance C
    {
        Versionstamp table_version;
        ASSERT_EQ(reader.get_table_version(1, &table_version), TxnErrorCode::TXN_OK);
        ASSERT_EQ(table_version, Versionstamp(100, 1))
                << "expect: 100, 1, but got: " << table_version.version() << ", "
                << table_version.order();
    }
}

TEST_F(CloneChainReaderTest, GetPartitionVersion) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    // Case 1: No data exists anywhere - should return NOT_FOUND
    {
        VersionPB version_pb;
        Versionstamp partition_version;
        ASSERT_EQ(reader.get_partition_version(1001, &version_pb, &partition_version),
                  TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    // Case 2: Insert partition version in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_version_key =
                versioned::partition_version_key({instance_ids_[0], 1001});
        VersionPB version_pb;
        version_pb.set_version(100);
        std::string key = encode_versioned_key(partition_version_key, Versionstamp(100, 1));
        txn->put(key, version_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find data from A through clone chain
    {
        VersionPB version_pb;
        Versionstamp partition_version;
        ASSERT_EQ(reader.get_partition_version(1001, &version_pb, &partition_version),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(version_pb.version(), 100);
        ASSERT_EQ(partition_version, Versionstamp(100, 1));
    }

    // Case 3: Insert data with version > snapshot_version in instance B (should be ignored)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_version_key =
                versioned::partition_version_key({instance_ids_[1], 1001});
        VersionPB version_pb;
        version_pb.set_version(200);
        // Use a version larger than snapshot_versions_[1] (2000)
        std::string key = encode_versioned_key(partition_version_key, Versionstamp(2500, 1));
        txn->put(key, version_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 3: Should still return data from A, not the large version from B
    {
        VersionPB version_pb;
        Versionstamp partition_version;
        ASSERT_EQ(reader.get_partition_version(1001, &version_pb, &partition_version),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(version_pb.version(), 100) << "Should not read data with version > snapshot";
        ASSERT_EQ(partition_version, Versionstamp(100, 1));
    }

    // Case 4: Insert valid data in instance B (within snapshot version)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_version_key =
                versioned::partition_version_key({instance_ids_[1], 1001});
        VersionPB version_pb;
        version_pb.set_version(150);
        std::string key = encode_versioned_key(partition_version_key, Versionstamp(1500, 1));
        txn->put(key, version_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 4: Should return the first encountered data (from B)
    {
        VersionPB version_pb;
        Versionstamp partition_version;
        ASSERT_EQ(reader.get_partition_version(1001, &version_pb, &partition_version),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(version_pb.version(), 150)
                << "Should return first valid data encountered in chain";
        ASSERT_EQ(partition_version, Versionstamp(1500, 1));
    }
}

TEST_F(CloneChainReaderTest, GetTabletLoadStats) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    // Case 1: No data exists anywhere - should return NOT_FOUND
    {
        TabletStatsPB tablet_stats;
        Versionstamp versionstamp;
        ASSERT_EQ(reader.get_tablet_load_stats(2001, &tablet_stats, &versionstamp),
                  TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    // Case 2: Insert tablet load stats in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_load_stats_key =
                versioned::tablet_load_stats_key({instance_ids_[0], 2001});
        TabletStatsPB tablet_stats;
        tablet_stats.set_data_size(1000);
        tablet_stats.set_num_rows(100);
        std::string key = encode_versioned_key(tablet_load_stats_key, Versionstamp(100, 1));
        txn->put(key, tablet_stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find data from A through clone chain
    {
        TabletStatsPB tablet_stats;
        Versionstamp versionstamp;
        ASSERT_EQ(reader.get_tablet_load_stats(2001, &tablet_stats, &versionstamp),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats.data_size(), 1000);
        ASSERT_EQ(tablet_stats.num_rows(), 100);
        ASSERT_EQ(versionstamp, Versionstamp(100, 1));
    }

    // Case 3: Insert data with version > snapshot_version in instance B (should be ignored)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_load_stats_key =
                versioned::tablet_load_stats_key({instance_ids_[1], 2001});
        TabletStatsPB tablet_stats;
        tablet_stats.set_data_size(2000);
        tablet_stats.set_num_rows(200);
        // Use a version larger than snapshot_versions_[1] (2000)
        std::string key = encode_versioned_key(tablet_load_stats_key, Versionstamp(2500, 1));
        txn->put(key, tablet_stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 3: Should still return data from A, not the large version from B
    {
        TabletStatsPB tablet_stats;
        Versionstamp versionstamp;
        ASSERT_EQ(reader.get_tablet_load_stats(2001, &tablet_stats, &versionstamp),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats.data_size(), 1000) << "Should not read data with version > snapshot";
        ASSERT_EQ(tablet_stats.num_rows(), 100);
        ASSERT_EQ(versionstamp, Versionstamp(100, 1));
    }

    // Case 4: Insert valid data in instance B (within snapshot version)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_load_stats_key =
                versioned::tablet_load_stats_key({instance_ids_[1], 2001});
        TabletStatsPB tablet_stats;
        tablet_stats.set_data_size(1500);
        tablet_stats.set_num_rows(150);
        std::string key = encode_versioned_key(tablet_load_stats_key, Versionstamp(1500, 1));
        txn->put(key, tablet_stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 4: Should return the first encountered data (from B)
    {
        TabletStatsPB tablet_stats;
        Versionstamp versionstamp;
        ASSERT_EQ(reader.get_tablet_load_stats(2001, &tablet_stats, &versionstamp),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats.data_size(), 1500)
                << "Should return first valid data encountered in chain";
        ASSERT_EQ(tablet_stats.num_rows(), 150);
        ASSERT_EQ(versionstamp, Versionstamp(1500, 1));
    }
}

TEST_F(CloneChainReaderTest, BatchGetTabletLoadStats) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    std::vector<int64_t> tablet_ids = {2001, 2002, 2003, 2004};

    // Case 1: Test empty input
    {
        std::vector<int64_t> empty_ids;
        std::unordered_map<int64_t, TabletStatsPB> tablet_stats;
        std::unordered_map<int64_t, Versionstamp> versionstamps;
        ASSERT_EQ(reader.get_tablet_load_stats(empty_ids, &tablet_stats, &versionstamps),
                  TxnErrorCode::TXN_OK);
        ASSERT_TRUE(tablet_stats.empty());
        ASSERT_TRUE(versionstamps.empty());
    }

    // Case 2: Test all keys not found
    {
        std::unordered_map<int64_t, TabletStatsPB> tablet_stats;
        std::unordered_map<int64_t, Versionstamp> versionstamps;
        ASSERT_EQ(reader.get_tablet_load_stats(tablet_ids, &tablet_stats, &versionstamps),
                  TxnErrorCode::TXN_OK);
        ASSERT_TRUE(tablet_stats.empty());
        ASSERT_TRUE(versionstamps.empty());
    }

    // Case 3: Insert some tablet load stats in instance A (skip tablet_ids[1])
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);

        for (size_t i = 0; i < tablet_ids.size(); ++i) {
            if (i == 1) continue; // Skip tablet_ids[1]
            std::string tablet_load_stats_key =
                    versioned::tablet_load_stats_key({instance_ids_[0], tablet_ids[i]});
            TabletStatsPB tablet_stats;
            tablet_stats.set_data_size(1000 * (i + 1));
            tablet_stats.set_num_rows(100 * (i + 1));
            std::string key = encode_versioned_key(tablet_load_stats_key, Versionstamp(100 + i, 1));
            txn->put(key, tablet_stats.SerializeAsString());
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 3: Read from instance C should find partial data from A through clone chain
    {
        std::unordered_map<int64_t, TabletStatsPB> tablet_stats;
        std::unordered_map<int64_t, Versionstamp> versionstamps;
        ASSERT_EQ(reader.get_tablet_load_stats(tablet_ids, &tablet_stats, &versionstamps),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats.size(), 3); // All except tablet_ids[1]
        ASSERT_EQ(versionstamps.size(), 3);

        // Check min_read_version
        Versionstamp min_expected = Versionstamp::max();
        for (const auto& [tablet_id, versionstamp] : versionstamps) {
            min_expected = std::min(min_expected, versionstamp);
        }
        ASSERT_EQ(reader.min_read_versionstamp(), min_expected);

        for (size_t i = 0; i < tablet_ids.size(); ++i) {
            if (i == 1) {
                ASSERT_EQ(tablet_stats.count(tablet_ids[i]), 0);
                ASSERT_EQ(versionstamps.count(tablet_ids[i]), 0);
            } else {
                ASSERT_EQ(tablet_stats.count(tablet_ids[i]), 1);
                ASSERT_EQ(versionstamps.count(tablet_ids[i]), 1);
                ASSERT_EQ(tablet_stats.at(tablet_ids[i]).data_size(), 1000 * (i + 1));
                ASSERT_EQ(tablet_stats.at(tablet_ids[i]).num_rows(), 100 * (i + 1));
                ASSERT_EQ(versionstamps.at(tablet_ids[i]), Versionstamp(100 + i, 1));
            }
        }
    }

    // Case 4: Insert data with version > snapshot_version in instance B (should be ignored)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);

        for (int64_t tablet_id : tablet_ids) {
            std::string tablet_load_stats_key =
                    versioned::tablet_load_stats_key({instance_ids_[1], tablet_id});
            TabletStatsPB tablet_stats;
            tablet_stats.set_data_size(9999);
            tablet_stats.set_num_rows(999);
            // Use a version larger than snapshot_versions_[1] (2000)
            std::string key = encode_versioned_key(tablet_load_stats_key, Versionstamp(2500, 1));
            txn->put(key, tablet_stats.SerializeAsString());
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 4: Should still return data from A, ignoring the large version from B
    {
        std::unordered_map<int64_t, TabletStatsPB> tablet_stats;
        std::unordered_map<int64_t, Versionstamp> versionstamps;
        ASSERT_EQ(reader.get_tablet_load_stats(tablet_ids, &tablet_stats, &versionstamps),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats.size(), 3); // Still missing tablet_ids[1]

        for (size_t i = 0; i < tablet_ids.size(); ++i) {
            if (i == 1) continue;
            ASSERT_EQ(tablet_stats.at(tablet_ids[i]).data_size(), 1000 * (i + 1))
                    << "Should not read data with version > snapshot";
            ASSERT_EQ(tablet_stats.at(tablet_ids[i]).num_rows(), 100 * (i + 1));
        }
    }

    // Case 5: Insert valid data in instance B (within snapshot version), including the missing one
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);

        for (size_t i = 0; i < tablet_ids.size(); ++i) {
            std::string tablet_load_stats_key =
                    versioned::tablet_load_stats_key({instance_ids_[1], tablet_ids[i]});
            TabletStatsPB tablet_stats;
            tablet_stats.set_data_size(1500 * (i + 1));
            tablet_stats.set_num_rows(150 * (i + 1));
            std::string key =
                    encode_versioned_key(tablet_load_stats_key, Versionstamp(1500 + i, 1));
            txn->put(key, tablet_stats.SerializeAsString());
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 5: Should return the first encountered data (from B for all tablets)
    {
        std::unordered_map<int64_t, TabletStatsPB> tablet_stats;
        std::unordered_map<int64_t, Versionstamp> versionstamps;
        ASSERT_EQ(reader.get_tablet_load_stats(tablet_ids, &tablet_stats, &versionstamps),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats.size(), tablet_ids.size());
        ASSERT_EQ(versionstamps.size(), tablet_ids.size());

        for (size_t i = 0; i < tablet_ids.size(); ++i) {
            ASSERT_EQ(tablet_stats.count(tablet_ids[i]), 1);
            ASSERT_EQ(tablet_stats.at(tablet_ids[i]).data_size(), 1500 * (i + 1))
                    << "Should return first valid data encountered in chain";
            ASSERT_EQ(tablet_stats.at(tablet_ids[i]).num_rows(), 150 * (i + 1));
            ASSERT_EQ(versionstamps.at(tablet_ids[i]), Versionstamp(1500 + i, 1));
        }
    }
}

TEST_F(CloneChainReaderTest, GetTabletCompactStats) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    // Case 1: No data exists anywhere - should return NOT_FOUND
    {
        TabletStatsPB tablet_stats;
        Versionstamp versionstamp;
        ASSERT_EQ(reader.get_tablet_compact_stats(3001, &tablet_stats, &versionstamp),
                  TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    // Case 2: Insert tablet compact stats in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_compact_stats_key =
                versioned::tablet_compact_stats_key({instance_ids_[0], 3001});
        TabletStatsPB tablet_stats;
        tablet_stats.set_data_size(500);
        tablet_stats.set_num_rows(50);
        std::string key = encode_versioned_key(tablet_compact_stats_key, Versionstamp(100, 1));
        txn->put(key, tablet_stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find data from A through clone chain
    {
        TabletStatsPB tablet_stats;
        Versionstamp versionstamp;
        ASSERT_EQ(reader.get_tablet_compact_stats(3001, &tablet_stats, &versionstamp),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats.data_size(), 500);
        ASSERT_EQ(tablet_stats.num_rows(), 50);
        ASSERT_EQ(versionstamp, Versionstamp(100, 1));
    }

    // Case 3: Insert data with version > snapshot_version in instance B (should be ignored)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_compact_stats_key =
                versioned::tablet_compact_stats_key({instance_ids_[1], 3001});
        TabletStatsPB tablet_stats;
        tablet_stats.set_data_size(1000);
        tablet_stats.set_num_rows(100);
        // Use a version larger than snapshot_versions_[1] (2000)
        std::string key = encode_versioned_key(tablet_compact_stats_key, Versionstamp(2500, 1));
        txn->put(key, tablet_stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 3: Should still return data from A, not the large version from B
    {
        TabletStatsPB tablet_stats;
        Versionstamp versionstamp;
        ASSERT_EQ(reader.get_tablet_compact_stats(3001, &tablet_stats, &versionstamp),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats.data_size(), 500) << "Should not read data with version > snapshot";
        ASSERT_EQ(tablet_stats.num_rows(), 50);
        ASSERT_EQ(versionstamp, Versionstamp(100, 1));
    }

    // Case 4: Insert valid data in instance B (within snapshot version)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_compact_stats_key =
                versioned::tablet_compact_stats_key({instance_ids_[1], 3001});
        TabletStatsPB tablet_stats;
        tablet_stats.set_data_size(750);
        tablet_stats.set_num_rows(75);
        std::string key = encode_versioned_key(tablet_compact_stats_key, Versionstamp(1500, 1));
        txn->put(key, tablet_stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 4: Should return the first encountered data (from B)
    {
        TabletStatsPB tablet_stats;
        Versionstamp versionstamp;
        ASSERT_EQ(reader.get_tablet_compact_stats(3001, &tablet_stats, &versionstamp),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats.data_size(), 750)
                << "Should return first valid data encountered in chain";
        ASSERT_EQ(tablet_stats.num_rows(), 75);
        ASSERT_EQ(versionstamp, Versionstamp(1500, 1));
    }
}

TEST_F(CloneChainReaderTest, GetTabletIndex) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    // Case 1: No data exists anywhere - should return NOT_FOUND
    {
        TabletIndexPB tablet_index;
        ASSERT_EQ(reader.get_tablet_index(4001, &tablet_index), TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    // Case 2: Insert tablet index in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_index_key = versioned::tablet_index_key({instance_ids_[0], 4001});
        TabletIndexPB tablet_index;
        tablet_index.set_table_id(1001);
        tablet_index.set_index_id(2001);
        tablet_index.set_partition_id(3001);
        txn->put(tablet_index_key, tablet_index.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find data from A through clone chain
    {
        TabletIndexPB tablet_index;
        ASSERT_EQ(reader.get_tablet_index(4001, &tablet_index), TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_index.table_id(), 1001);
        ASSERT_EQ(tablet_index.index_id(), 2001);
        ASSERT_EQ(tablet_index.partition_id(), 3001);
    }
}

TEST_F(CloneChainReaderTest, GetPartitionIndex) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    // Case 1: No data exists anywhere - should return NOT_FOUND
    {
        PartitionIndexPB partition_index;
        ASSERT_EQ(reader.get_partition_index(5001, &partition_index),
                  TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    // Case 2: Insert partition index in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_index_key = versioned::partition_index_key({instance_ids_[0], 5001});
        PartitionIndexPB partition_index;
        partition_index.set_db_id(1001);
        partition_index.set_table_id(2001);
        txn->put(partition_index_key, partition_index.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find data from A through clone chain
    {
        PartitionIndexPB partition_index;
        ASSERT_EQ(reader.get_partition_index(5001, &partition_index), TxnErrorCode::TXN_OK);
        ASSERT_EQ(partition_index.db_id(), 1001);
        ASSERT_EQ(partition_index.table_id(), 2001);
    }
}

TEST_F(CloneChainReaderTest, GetIndexIndex) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    // Case 1: No data exists anywhere - should return NOT_FOUND
    {
        IndexIndexPB index_index;
        ASSERT_EQ(reader.get_index_index(6001, &index_index), TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    // Case 2: Insert index index in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string index_index_key = versioned::index_index_key({instance_ids_[0], 6001});
        IndexIndexPB index_index;
        index_index.set_db_id(1001);
        index_index.set_table_id(2001);
        txn->put(index_index_key, index_index.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find data from A through clone chain
    {
        IndexIndexPB index_index;
        ASSERT_EQ(reader.get_index_index(6001, &index_index), TxnErrorCode::TXN_OK);
        ASSERT_EQ(index_index.db_id(), 1001);
        ASSERT_EQ(index_index.table_id(), 2001);
    }
}

TEST_F(CloneChainReaderTest, GetTableVersions) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    // Case 1: No data exists anywhere - should return empty map
    {
        std::vector<int64_t> table_ids = {1, 2, 3};
        std::unordered_map<int64_t, Versionstamp> table_versions;
        ASSERT_EQ(reader.get_table_versions(table_ids, &table_versions), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(table_versions.empty());
    }

    // Case 2: Insert table versions in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Insert table 1 and 3, skip table 2
        std::string table_version_key1 = versioned::table_version_key({instance_ids_[0], 1});
        std::string key1 = encode_versioned_key(table_version_key1, Versionstamp(100, 1));
        txn->put(key1, "");

        std::string table_version_key3 = versioned::table_version_key({instance_ids_[0], 3});
        std::string key3 = encode_versioned_key(table_version_key3, Versionstamp(300, 1));
        txn->put(key3, "");

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find partial data from A
    {
        std::vector<int64_t> table_ids = {1, 2, 3};
        std::unordered_map<int64_t, Versionstamp> table_versions;
        ASSERT_EQ(reader.get_table_versions(table_ids, &table_versions), TxnErrorCode::TXN_OK);
        ASSERT_EQ(table_versions.size(), 2);
        ASSERT_EQ(table_versions[1], Versionstamp(100, 1));
        ASSERT_EQ(table_versions[3], Versionstamp(300, 1));
        ASSERT_EQ(table_versions.count(2), 0);
    }

    // Case 3: Insert data with version > snapshot_version in instance B (should be ignored)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string table_version_key1 = versioned::table_version_key({instance_ids_[1], 1});
        std::string key1 = encode_versioned_key(table_version_key1, Versionstamp(2500, 1));
        txn->put(key1, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 3: Should still return data from A, ignoring large version from B
    {
        std::vector<int64_t> table_ids = {1, 2, 3};
        std::unordered_map<int64_t, Versionstamp> table_versions;
        ASSERT_EQ(reader.get_table_versions(table_ids, &table_versions), TxnErrorCode::TXN_OK);
        ASSERT_EQ(table_versions.size(), 2);
        ASSERT_EQ(table_versions[1], Versionstamp(100, 1))
                << "Should not read data with version > snapshot";
        ASSERT_EQ(table_versions[3], Versionstamp(300, 1));
    }

    // Case 4: Insert valid data in instance B (within snapshot version)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string table_version_key1 = versioned::table_version_key({instance_ids_[1], 1});
        std::string key1 = encode_versioned_key(table_version_key1, Versionstamp(150, 1));
        txn->put(key1, "");

        // Add missing table 2 in instance B
        std::string table_version_key2 = versioned::table_version_key({instance_ids_[1], 2});
        std::string key2 = encode_versioned_key(table_version_key2, Versionstamp(200, 1));
        txn->put(key2, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 4: Should return first encountered data (from B for tables 1&2, from A for table 3)
    {
        std::vector<int64_t> table_ids = {1, 2, 3};
        std::unordered_map<int64_t, Versionstamp> table_versions;
        ASSERT_EQ(reader.get_table_versions(table_ids, &table_versions), TxnErrorCode::TXN_OK);
        ASSERT_EQ(table_versions.size(), 3);
        ASSERT_EQ(table_versions[1], Versionstamp(150, 1))
                << "Should return first valid data encountered";
        ASSERT_EQ(table_versions[2], Versionstamp(200, 1))
                << "Should find missing data in clone chain";
        ASSERT_EQ(table_versions[3], Versionstamp(300, 1))
                << "Should return data from A when B doesn't have it";
    }
}

TEST_F(CloneChainReaderTest, IsIndexExists) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    // Case 1: No data exists anywhere - should return NOT_FOUND
    ASSERT_EQ(reader.is_index_exists(7001), TxnErrorCode::TXN_KEY_NOT_FOUND);

    // Case 2: Insert index in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string index_key = versioned::meta_index_key({instance_ids_[0], 7001});
        std::string key = encode_versioned_key(index_key, Versionstamp(100, 1));
        txn->put(key, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find index from A through clone chain
    ASSERT_EQ(reader.is_index_exists(7001), TxnErrorCode::TXN_OK);

    // Case 3: Insert data with version > snapshot_version in instance B (should be ignored)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string index_key = versioned::meta_index_key({instance_ids_[1], 7002});
        // Use a version larger than snapshot_versions_[1] (2000)
        std::string key = encode_versioned_key(index_key, Versionstamp(2500, 1));
        txn->put(key, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 3: Should not find index with large version from B
    ASSERT_EQ(reader.is_index_exists(7002), TxnErrorCode::TXN_KEY_NOT_FOUND)
            << "Should not read data with version > snapshot";

    // Case 4: Insert valid data in instance B (within snapshot version)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string index_key = versioned::meta_index_key({instance_ids_[1], 7003});
        std::string key = encode_versioned_key(index_key, Versionstamp(1500, 1));
        txn->put(key, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 4: Should find the first encountered data (from B)
    ASSERT_EQ(reader.is_index_exists(7003), TxnErrorCode::TXN_OK)
            << "Should return first valid data encountered in chain";
}

TEST_F(CloneChainReaderTest, IsPartitionExists) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    // Case 1: No data exists anywhere - should return NOT_FOUND
    ASSERT_EQ(reader.is_partition_exists(8001), TxnErrorCode::TXN_KEY_NOT_FOUND);

    // Case 2: Insert partition in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_key = versioned::meta_partition_key({instance_ids_[0], 8001});
        std::string key = encode_versioned_key(partition_key, Versionstamp(100, 1));
        txn->put(key, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find partition from A through clone chain
    ASSERT_EQ(reader.is_partition_exists(8001), TxnErrorCode::TXN_OK);

    // Case 3: Insert data with version > snapshot_version in instance B (should be ignored)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_key = versioned::meta_partition_key({instance_ids_[1], 8002});
        // Use a version larger than snapshot_versions_[1] (2000)
        std::string key = encode_versioned_key(partition_key, Versionstamp(2500, 1));
        txn->put(key, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 3: Should not find partition with large version from B
    ASSERT_EQ(reader.is_partition_exists(8002), TxnErrorCode::TXN_KEY_NOT_FOUND)
            << "Should not read data with version > snapshot";

    // Case 4: Insert valid data in instance B (within snapshot version)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_key = versioned::meta_partition_key({instance_ids_[1], 8003});
        std::string key = encode_versioned_key(partition_key, Versionstamp(1500, 1));
        txn->put(key, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 4: Should find the first encountered data (from B)
    ASSERT_EQ(reader.is_partition_exists(8003), TxnErrorCode::TXN_OK)
            << "Should return first valid data encountered in chain";
}

TEST_F(CloneChainReaderTest, HasNoIndexes) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    int64_t db_id = 1001;
    int64_t table_id = 2001;

    // Case 1: No index data exists anywhere - should return true (has no indexes)
    {
        bool no_indexes = false;
        ASSERT_EQ(reader.has_no_indexes(db_id, table_id, &no_indexes), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(no_indexes) << "Table should have no indexes when no index data exists";
    }

    // Case 2: Insert index data in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string index_inverted_key =
                versioned::index_inverted_key({instance_ids_[0], db_id, table_id, 3001});
        txn->put(index_inverted_key, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find index from A through clone chain
    {
        bool no_indexes = true;
        ASSERT_EQ(reader.has_no_indexes(db_id, table_id, &no_indexes), TxnErrorCode::TXN_OK);
        ASSERT_FALSE(no_indexes)
                << "Table should have indexes when index data exists in clone chain";
    }
}

TEST_F(CloneChainReaderTest, GetTabletMergedStats) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    // Case 1: No data exists anywhere - should return NOT_FOUND
    {
        TabletStatsPB tablet_stats;
        Versionstamp versionstamp;
        ASSERT_EQ(reader.get_tablet_merged_stats(9001, &tablet_stats, &versionstamp),
                  TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    // Case 2: Insert both load and compact stats in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        // Insert load stats
        std::string tablet_load_stats_key =
                versioned::tablet_load_stats_key({instance_ids_[0], 9001});
        TabletStatsPB load_stats;
        load_stats.set_data_size(1000);
        load_stats.set_num_rows(100);
        load_stats.set_num_rowsets(10);
        std::string load_key = encode_versioned_key(tablet_load_stats_key, Versionstamp(100, 1));
        txn->put(load_key, load_stats.SerializeAsString());

        // Insert compact stats
        std::string tablet_compact_stats_key =
                versioned::tablet_compact_stats_key({instance_ids_[0], 9001});
        TabletStatsPB compact_stats;
        compact_stats.set_data_size(500);
        compact_stats.set_num_rows(50);
        compact_stats.set_num_segments(5);
        std::string compact_key =
                encode_versioned_key(tablet_compact_stats_key, Versionstamp(100, 1));
        txn->put(compact_key, compact_stats.SerializeAsString());

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read merged stats should return combined stats
    {
        TabletStatsPB tablet_stats;
        Versionstamp versionstamp;
        ASSERT_EQ(reader.get_tablet_merged_stats(9001, &tablet_stats, &versionstamp),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats.data_size(), 1500); // 1000 + 500
        ASSERT_EQ(tablet_stats.num_rows(), 150);   // 100 + 50
        ASSERT_EQ(tablet_stats.num_rowsets(), 10); // load stats only
        ASSERT_EQ(tablet_stats.num_segments(), 5); // compact stats only
        ASSERT_EQ(versionstamp, Versionstamp(100, 1));
    }

    // Case 3: Insert both load and compact stats in instance B (with large version to be ignored)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Insert load stats with large version (should be ignored)
        std::string tablet_load_stats_key =
                versioned::tablet_load_stats_key({instance_ids_[1], 9001});
        TabletStatsPB load_stats;
        load_stats.set_data_size(2000);
        load_stats.set_num_rows(200);
        load_stats.set_num_rowsets(20);
        std::string load_key = encode_versioned_key(tablet_load_stats_key, Versionstamp(2500, 1));
        txn->put(load_key, load_stats.SerializeAsString());

        // Insert compact stats with large version (should be ignored)
        std::string tablet_compact_stats_key =
                versioned::tablet_compact_stats_key({instance_ids_[1], 9001});
        TabletStatsPB compact_stats;
        compact_stats.set_data_size(800);
        compact_stats.set_num_rows(80);
        compact_stats.set_num_segments(8);
        std::string compact_key =
                encode_versioned_key(tablet_compact_stats_key, Versionstamp(2500, 1));
        txn->put(compact_key, compact_stats.SerializeAsString());

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 3: Should still return data from A, ignoring large version from B
    {
        TabletStatsPB tablet_stats;
        Versionstamp versionstamp;
        ASSERT_EQ(reader.get_tablet_merged_stats(9001, &tablet_stats, &versionstamp),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats.data_size(), 1500)
                << "Should not read data with version > snapshot"; // 1000 + 500
        ASSERT_EQ(tablet_stats.num_rows(), 150);                   // 100 + 50
        ASSERT_EQ(versionstamp, Versionstamp(100, 1));
    }

    // Case 4: Insert both load and compact stats in instance B (within snapshot version)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Insert load stats with valid version
        std::string tablet_load_stats_key =
                versioned::tablet_load_stats_key({instance_ids_[1], 9001});
        TabletStatsPB load_stats;
        load_stats.set_data_size(1200);
        load_stats.set_num_rows(120);
        load_stats.set_num_rowsets(12);
        std::string load_key = encode_versioned_key(tablet_load_stats_key, Versionstamp(1500, 1));
        txn->put(load_key, load_stats.SerializeAsString());

        // Insert compact stats with valid version
        std::string tablet_compact_stats_key =
                versioned::tablet_compact_stats_key({instance_ids_[1], 9001});
        TabletStatsPB compact_stats;
        compact_stats.set_data_size(600);
        compact_stats.set_num_rows(60);
        compact_stats.set_num_segments(6);
        std::string compact_key =
                encode_versioned_key(tablet_compact_stats_key, Versionstamp(1500, 1));
        txn->put(compact_key, compact_stats.SerializeAsString());

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 4: Should return first valid merged stats (from B)
    {
        TabletStatsPB tablet_stats;
        Versionstamp versionstamp;
        ASSERT_EQ(reader.get_tablet_merged_stats(9001, &tablet_stats, &versionstamp),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats.data_size(), 1800)
                << "Should return first valid data encountered"; // 1200 + 600
        ASSERT_EQ(tablet_stats.num_rows(), 180);                 // 120 + 60
        ASSERT_EQ(tablet_stats.num_rowsets(), 12);               // load stats only
        ASSERT_EQ(tablet_stats.num_segments(), 6);               // compact stats only
        ASSERT_EQ(versionstamp, Versionstamp(1500, 1));
    }
}

TEST_F(CloneChainReaderTest, GetPartitionVersions) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    // Case 1: No data exists anywhere - should return empty maps
    {
        std::vector<int64_t> partition_ids = {10001, 10002, 10003};
        std::unordered_map<int64_t, VersionPB> versions;
        std::unordered_map<int64_t, Versionstamp> versionstamps;
        ASSERT_EQ(reader.get_partition_versions(partition_ids, &versions, &versionstamps),
                  TxnErrorCode::TXN_OK);
        ASSERT_TRUE(versions.empty());
        ASSERT_TRUE(versionstamps.empty());
    }

    // Case 2: Insert partition versions in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Insert partitions 10001 and 10003, skip 10002
        {
            std::string partition_version_key =
                    versioned::partition_version_key({instance_ids_[0], 10001});
            VersionPB version_pb;
            version_pb.set_version(100);
            std::string key = encode_versioned_key(partition_version_key, Versionstamp(100, 1));
            txn->put(key, version_pb.SerializeAsString());
        }
        {
            std::string partition_version_key =
                    versioned::partition_version_key({instance_ids_[0], 10003});
            VersionPB version_pb;
            version_pb.set_version(300);
            std::string key = encode_versioned_key(partition_version_key, Versionstamp(300, 1));
            txn->put(key, version_pb.SerializeAsString());
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find partial data from A
    {
        std::vector<int64_t> partition_ids = {10001, 10002, 10003};
        std::unordered_map<int64_t, VersionPB> versions;
        std::unordered_map<int64_t, Versionstamp> versionstamps;
        ASSERT_EQ(reader.get_partition_versions(partition_ids, &versions, &versionstamps),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(versions.size(), 2);
        ASSERT_EQ(versionstamps.size(), 2);
        ASSERT_EQ(versions[10001].version(), 100);
        ASSERT_EQ(versions[10003].version(), 300);
        ASSERT_EQ(versionstamps[10001], Versionstamp(100, 1));
        ASSERT_EQ(versionstamps[10003], Versionstamp(300, 1));
        ASSERT_EQ(versions.count(10002), 0);
        ASSERT_EQ(versionstamps.count(10002), 0);
    }

    // Case 3: Insert data with version > snapshot_version in instance B (should be ignored)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_version_key =
                versioned::partition_version_key({instance_ids_[1], 10001});
        VersionPB version_pb;
        version_pb.set_version(250);
        // Use a version larger than snapshot_versions_[1] (2000)
        std::string key = encode_versioned_key(partition_version_key, Versionstamp(2500, 1));
        txn->put(key, version_pb.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 3: Should still return data from A, ignoring large version from B
    {
        std::vector<int64_t> partition_ids = {10001, 10002, 10003};
        std::unordered_map<int64_t, VersionPB> versions;
        std::unordered_map<int64_t, Versionstamp> versionstamps;
        ASSERT_EQ(reader.get_partition_versions(partition_ids, &versions, &versionstamps),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(versions.size(), 2);
        ASSERT_EQ(versions[10001].version(), 100) << "Should not read data with version > snapshot";
        ASSERT_EQ(versions[10003].version(), 300);
    }

    // Case 4: Insert valid data in instance B (within snapshot version)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        {
            std::string partition_version_key =
                    versioned::partition_version_key({instance_ids_[1], 10001});
            VersionPB version_pb;
            version_pb.set_version(150);
            std::string key = encode_versioned_key(partition_version_key, Versionstamp(1500, 1));
            txn->put(key, version_pb.SerializeAsString());
        }
        // Add missing partition 10002 in instance B
        {
            std::string partition_version_key =
                    versioned::partition_version_key({instance_ids_[1], 10002});
            VersionPB version_pb;
            version_pb.set_version(200);
            std::string key = encode_versioned_key(partition_version_key, Versionstamp(1600, 1));
            txn->put(key, version_pb.SerializeAsString());
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 4: Should return first encountered data (from B for 10001&10002, from A for 10003)
    {
        std::vector<int64_t> partition_ids = {10001, 10002, 10003};
        std::unordered_map<int64_t, VersionPB> versions;
        std::unordered_map<int64_t, Versionstamp> versionstamps;
        ASSERT_EQ(reader.get_partition_versions(partition_ids, &versions, &versionstamps),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(versions.size(), 3);
        ASSERT_EQ(versions[10001].version(), 150) << "Should return first valid data encountered";
        ASSERT_EQ(versions[10002].version(), 200) << "Should find missing data in clone chain";
        ASSERT_EQ(versions[10003].version(), 300)
                << "Should return data from A when B doesn't have it";
        ASSERT_EQ(versionstamps[10001], Versionstamp(1500, 1));
        ASSERT_EQ(versionstamps[10002], Versionstamp(1600, 1));
        ASSERT_EQ(versionstamps[10003], Versionstamp(300, 1));
    }
}

TEST_F(CloneChainReaderTest, GetTabletIndexes) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    // Case 1: No data exists anywhere - should return empty map
    {
        std::vector<int64_t> tablet_ids = {11001, 11002, 11003};
        std::unordered_map<int64_t, TabletIndexPB> tablet_indexes;
        ASSERT_EQ(reader.get_tablet_indexes(tablet_ids, &tablet_indexes), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(tablet_indexes.empty());
    }

    // Case 2: Insert tablet indexes in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Insert tablets 11001 and 11003, skip 11002
        {
            std::string tablet_index_key = versioned::tablet_index_key({instance_ids_[0], 11001});
            TabletIndexPB tablet_index;
            tablet_index.set_table_id(1001);
            tablet_index.set_index_id(2001);
            tablet_index.set_partition_id(3001);
            txn->put(tablet_index_key, tablet_index.SerializeAsString());
        }
        {
            std::string tablet_index_key = versioned::tablet_index_key({instance_ids_[0], 11003});
            TabletIndexPB tablet_index;
            tablet_index.set_table_id(1003);
            tablet_index.set_index_id(2003);
            tablet_index.set_partition_id(3003);
            txn->put(tablet_index_key, tablet_index.SerializeAsString());
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find partial data from A
    {
        std::vector<int64_t> tablet_ids = {11001, 11002, 11003};
        std::unordered_map<int64_t, TabletIndexPB> tablet_indexes;
        ASSERT_EQ(reader.get_tablet_indexes(tablet_ids, &tablet_indexes), TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_indexes.size(), 2);
        ASSERT_EQ(tablet_indexes[11001].table_id(), 1001);
        ASSERT_EQ(tablet_indexes[11001].index_id(), 2001);
        ASSERT_EQ(tablet_indexes[11001].partition_id(), 3001);
        ASSERT_EQ(tablet_indexes[11003].table_id(), 1003);
        ASSERT_EQ(tablet_indexes[11003].index_id(), 2003);
        ASSERT_EQ(tablet_indexes[11003].partition_id(), 3003);
        ASSERT_EQ(tablet_indexes.count(11002), 0);
    }

    // Case 3: Insert valid data in instance B (within snapshot version)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        // Update tablet 11001 and add missing tablet 11002 in instance B
        {
            std::string tablet_index_key = versioned::tablet_index_key({instance_ids_[1], 11001});
            TabletIndexPB tablet_index;
            tablet_index.set_table_id(1001);
            tablet_index.set_index_id(2011);     // Different index_id
            tablet_index.set_partition_id(3011); // Different partition_id
            txn->put(tablet_index_key, tablet_index.SerializeAsString());
        }
        {
            std::string tablet_index_key = versioned::tablet_index_key({instance_ids_[1], 11002});
            TabletIndexPB tablet_index;
            tablet_index.set_table_id(1002);
            tablet_index.set_index_id(2002);
            tablet_index.set_partition_id(3002);
            txn->put(tablet_index_key, tablet_index.SerializeAsString());
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 3: Should return first encountered data (from B for 11001&11002, from A for 11003)
    {
        std::vector<int64_t> tablet_ids = {11001, 11002, 11003};
        std::unordered_map<int64_t, TabletIndexPB> tablet_indexes;
        ASSERT_EQ(reader.get_tablet_indexes(tablet_ids, &tablet_indexes), TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_indexes.size(), 3);
        ASSERT_EQ(tablet_indexes[11001].index_id(), 2011)
                << "Should return first valid data encountered";
        ASSERT_EQ(tablet_indexes[11001].partition_id(), 3011);
        ASSERT_EQ(tablet_indexes[11002].table_id(), 1002)
                << "Should find missing data in clone chain";
        ASSERT_EQ(tablet_indexes[11002].index_id(), 2002);
        ASSERT_EQ(tablet_indexes[11003].index_id(), 2003)
                << "Should return data from A when B doesn't have it";
        ASSERT_EQ(tablet_indexes[11003].partition_id(), 3003);
    }
}

TEST_F(CloneChainReaderTest, GetTabletCompactStatsBatch) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    // Case 1: No data exists anywhere - should return empty maps
    {
        std::vector<int64_t> tablet_ids = {12001, 12002, 12003};
        std::unordered_map<int64_t, TabletStatsPB> tablet_stats;
        std::unordered_map<int64_t, Versionstamp> versionstamps;
        ASSERT_EQ(reader.get_tablet_compact_stats(tablet_ids, &tablet_stats, &versionstamps),
                  TxnErrorCode::TXN_OK);
        ASSERT_TRUE(tablet_stats.empty());
        ASSERT_TRUE(versionstamps.empty());
    }

    // Case 2: Insert tablet compact stats in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Insert tablets 12001 and 12003, skip 12002
        {
            std::string tablet_compact_stats_key =
                    versioned::tablet_compact_stats_key({instance_ids_[0], 12001});
            TabletStatsPB tablet_stats;
            tablet_stats.set_data_size(1000);
            tablet_stats.set_num_rows(100);
            tablet_stats.set_num_segments(10);
            std::string key = encode_versioned_key(tablet_compact_stats_key, Versionstamp(100, 1));
            txn->put(key, tablet_stats.SerializeAsString());
        }
        {
            std::string tablet_compact_stats_key =
                    versioned::tablet_compact_stats_key({instance_ids_[0], 12003});
            TabletStatsPB tablet_stats;
            tablet_stats.set_data_size(3000);
            tablet_stats.set_num_rows(300);
            tablet_stats.set_num_segments(30);
            std::string key = encode_versioned_key(tablet_compact_stats_key, Versionstamp(300, 1));
            txn->put(key, tablet_stats.SerializeAsString());
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find partial data from A
    {
        std::vector<int64_t> tablet_ids = {12001, 12002, 12003};
        std::unordered_map<int64_t, TabletStatsPB> tablet_stats;
        std::unordered_map<int64_t, Versionstamp> versionstamps;
        ASSERT_EQ(reader.get_tablet_compact_stats(tablet_ids, &tablet_stats, &versionstamps),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats.size(), 2);
        ASSERT_EQ(versionstamps.size(), 2);
        ASSERT_EQ(tablet_stats[12001].data_size(), 1000);
        ASSERT_EQ(tablet_stats[12001].num_rows(), 100);
        ASSERT_EQ(tablet_stats[12001].num_segments(), 10);
        ASSERT_EQ(tablet_stats[12003].data_size(), 3000);
        ASSERT_EQ(tablet_stats[12003].num_rows(), 300);
        ASSERT_EQ(tablet_stats[12003].num_segments(), 30);
        ASSERT_EQ(versionstamps[12001], Versionstamp(100, 1));
        ASSERT_EQ(versionstamps[12003], Versionstamp(300, 1));
        ASSERT_EQ(tablet_stats.count(12002), 0);
        ASSERT_EQ(versionstamps.count(12002), 0);
    }

    // Case 3: Insert data with version > snapshot_version in instance B (should be ignored)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_compact_stats_key =
                versioned::tablet_compact_stats_key({instance_ids_[1], 12001});
        TabletStatsPB tablet_stats;
        tablet_stats.set_data_size(2000);
        tablet_stats.set_num_rows(200);
        tablet_stats.set_num_segments(20);
        // Use a version larger than snapshot_versions_[1] (2000)
        std::string key = encode_versioned_key(tablet_compact_stats_key, Versionstamp(2500, 1));
        txn->put(key, tablet_stats.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 3: Should still return data from A, ignoring large version from B
    {
        std::vector<int64_t> tablet_ids = {12001, 12002, 12003};
        std::unordered_map<int64_t, TabletStatsPB> tablet_stats;
        std::unordered_map<int64_t, Versionstamp> versionstamps;
        ASSERT_EQ(reader.get_tablet_compact_stats(tablet_ids, &tablet_stats, &versionstamps),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats.size(), 2);
        ASSERT_EQ(tablet_stats[12001].data_size(), 1000)
                << "Should not read data with version > snapshot";
        ASSERT_EQ(tablet_stats[12001].num_rows(), 100);
        ASSERT_EQ(tablet_stats[12003].data_size(), 3000);
    }

    // Case 4: Insert valid data in instance B (within snapshot version)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        {
            std::string tablet_compact_stats_key =
                    versioned::tablet_compact_stats_key({instance_ids_[1], 12001});
            TabletStatsPB tablet_stats;
            tablet_stats.set_data_size(1500);
            tablet_stats.set_num_rows(150);
            tablet_stats.set_num_segments(15);
            std::string key = encode_versioned_key(tablet_compact_stats_key, Versionstamp(1500, 1));
            txn->put(key, tablet_stats.SerializeAsString());
        }
        // Add missing tablet 12002 in instance B
        {
            std::string tablet_compact_stats_key =
                    versioned::tablet_compact_stats_key({instance_ids_[1], 12002});
            TabletStatsPB tablet_stats;
            tablet_stats.set_data_size(2000);
            tablet_stats.set_num_rows(200);
            tablet_stats.set_num_segments(20);
            std::string key = encode_versioned_key(tablet_compact_stats_key, Versionstamp(1600, 1));
            txn->put(key, tablet_stats.SerializeAsString());
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 4: Should return first encountered data (from B for 12001&12002, from A for 12003)
    {
        std::vector<int64_t> tablet_ids = {12001, 12002, 12003};
        std::unordered_map<int64_t, TabletStatsPB> tablet_stats;
        std::unordered_map<int64_t, Versionstamp> versionstamps;
        ASSERT_EQ(reader.get_tablet_compact_stats(tablet_ids, &tablet_stats, &versionstamps),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_stats.size(), 3);
        ASSERT_EQ(tablet_stats[12001].data_size(), 1500)
                << "Should return first valid data encountered";
        ASSERT_EQ(tablet_stats[12001].num_rows(), 150);
        ASSERT_EQ(tablet_stats[12002].data_size(), 2000)
                << "Should find missing data in clone chain";
        ASSERT_EQ(tablet_stats[12002].num_rows(), 200);
        ASSERT_EQ(tablet_stats[12003].data_size(), 3000)
                << "Should return data from A when B doesn't have it";
        ASSERT_EQ(tablet_stats[12003].num_rows(), 300);
        ASSERT_EQ(versionstamps[12001], Versionstamp(1500, 1));
        ASSERT_EQ(versionstamps[12002], Versionstamp(1600, 1));
        ASSERT_EQ(versionstamps[12003], Versionstamp(300, 1));
    }
}

TEST_F(CloneChainReaderTest, GetLoadRowsetMeta) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    int64_t tablet_id = 14001;
    int64_t version = 5;

    // Case 1: No data exists anywhere - should return NOT_FOUND
    {
        doris::RowsetMetaCloudPB rowset_meta;
        ASSERT_EQ(reader.get_load_rowset_meta(tablet_id, version, &rowset_meta),
                  TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    // Case 2: Insert load rowset meta in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string rowset_load_key =
                versioned::meta_rowset_load_key({instance_ids_[0], tablet_id, version});
        doris::RowsetMetaCloudPB rowset_meta;
        rowset_meta.set_rowset_id(0);
        rowset_meta.set_rowset_id_v2("load_rowset_5");
        rowset_meta.set_start_version(version);
        rowset_meta.set_end_version(version);
        rowset_meta.set_num_rows(500);
        rowset_meta.set_tablet_id(tablet_id);
        ASSERT_TRUE(versioned::document_put(txn.get(), rowset_load_key, std::move(rowset_meta)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find data from A through clone chain
    {
        doris::RowsetMetaCloudPB rowset_meta;
        ASSERT_EQ(reader.get_load_rowset_meta(tablet_id, version, &rowset_meta),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_meta.rowset_id_v2(), "load_rowset_5");
        ASSERT_EQ(rowset_meta.start_version(), version);
        ASSERT_EQ(rowset_meta.end_version(), version);
        ASSERT_EQ(rowset_meta.num_rows(), 500);
        ASSERT_EQ(rowset_meta.tablet_id(), tablet_id);
    }

    // Case 3: Insert data with version > snapshot_version in instance B (should be ignored)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string rowset_load_key =
                versioned::meta_rowset_load_key({instance_ids_[1], tablet_id, version});
        doris::RowsetMetaCloudPB rowset_meta;
        rowset_meta.set_rowset_id(0);
        rowset_meta.set_rowset_id_v2("load_rowset_5_large_version");
        rowset_meta.set_start_version(version);
        rowset_meta.set_end_version(version);
        rowset_meta.set_num_rows(600);
        rowset_meta.set_tablet_id(tablet_id);
        std::string key = encode_versioned_key(rowset_load_key, Versionstamp(2500, 1));
        txn->put(key, rowset_meta.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 3: Should still return data from A, ignoring large version from B
    {
        doris::RowsetMetaCloudPB rowset_meta;
        ASSERT_EQ(reader.get_load_rowset_meta(tablet_id, version, &rowset_meta),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_meta.rowset_id_v2(), "load_rowset_5")
                << "Should not read data with version > snapshot";
        ASSERT_EQ(rowset_meta.num_rows(), 500);
    }

    // Case 4: Insert valid data in instance B (within snapshot version)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string rowset_load_key =
                versioned::meta_rowset_load_key({instance_ids_[1], tablet_id, version});
        doris::RowsetMetaCloudPB rowset_meta;
        rowset_meta.set_rowset_id(0);
        rowset_meta.set_rowset_id_v2("load_rowset_5_updated");
        rowset_meta.set_start_version(version);
        rowset_meta.set_end_version(version);
        rowset_meta.set_num_rows(550);
        rowset_meta.set_tablet_id(tablet_id);
        ASSERT_TRUE(versioned::document_put(txn.get(), rowset_load_key, std::move(rowset_meta)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 4: Should return the first encountered data (from B)
    {
        doris::RowsetMetaCloudPB rowset_meta;
        ASSERT_EQ(reader.get_load_rowset_meta(tablet_id, version, &rowset_meta),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_meta.rowset_id_v2(), "load_rowset_5_updated")
                << "Should return first valid data encountered in chain";
        ASSERT_EQ(rowset_meta.num_rows(), 550);
    }
}

TEST_F(CloneChainReaderTest, GetCompactRowsetMeta) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    int64_t tablet_id = 14001;
    int64_t version = 5;

    // Case 1: No data exists anywhere - should return NOT_FOUND
    {
        doris::RowsetMetaCloudPB rowset_meta;
        ASSERT_EQ(reader.get_compact_rowset_meta(tablet_id, version, &rowset_meta),
                  TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    // Case 2: Insert load rowset meta in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string rowset_compact_key =
                versioned::meta_rowset_compact_key({instance_ids_[0], tablet_id, version});
        doris::RowsetMetaCloudPB rowset_meta;
        rowset_meta.set_rowset_id(0);
        rowset_meta.set_rowset_id_v2("compact_rowset_5");
        rowset_meta.set_start_version(version);
        rowset_meta.set_end_version(version);
        rowset_meta.set_num_rows(500);
        rowset_meta.set_tablet_id(tablet_id);
        ASSERT_TRUE(versioned::document_put(txn.get(), rowset_compact_key, std::move(rowset_meta)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find data from A through clone chain
    {
        doris::RowsetMetaCloudPB rowset_meta;
        ASSERT_EQ(reader.get_compact_rowset_meta(tablet_id, version, &rowset_meta),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_meta.rowset_id_v2(), "compact_rowset_5");
        ASSERT_EQ(rowset_meta.start_version(), version);
        ASSERT_EQ(rowset_meta.end_version(), version);
        ASSERT_EQ(rowset_meta.num_rows(), 500);
        ASSERT_EQ(rowset_meta.tablet_id(), tablet_id);
    }

    // Case 3: Insert data with version > snapshot_version in instance B (should be ignored)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string rowset_compact_key =
                versioned::meta_rowset_compact_key({instance_ids_[1], tablet_id, version});
        doris::RowsetMetaCloudPB rowset_meta;
        rowset_meta.set_rowset_id(0);
        rowset_meta.set_rowset_id_v2("compact_rowset_5_large_version");
        rowset_meta.set_start_version(version);
        rowset_meta.set_end_version(version);
        rowset_meta.set_num_rows(600);
        rowset_meta.set_tablet_id(tablet_id);
        std::string key = encode_versioned_key(rowset_compact_key, Versionstamp(2500, 1));
        txn->put(key, rowset_meta.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 3: Should still return data from A, ignoring large version from B
    {
        doris::RowsetMetaCloudPB rowset_meta;
        ASSERT_EQ(reader.get_compact_rowset_meta(tablet_id, version, &rowset_meta),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_meta.rowset_id_v2(), "compact_rowset_5")
                << "Should not read data with version > snapshot";
        ASSERT_EQ(rowset_meta.num_rows(), 500);
    }

    // Case 4: Insert valid data in instance B (within snapshot version)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string rowset_compact_key =
                versioned::meta_rowset_compact_key({instance_ids_[1], tablet_id, version});
        doris::RowsetMetaCloudPB rowset_meta;
        rowset_meta.set_rowset_id(0);
        rowset_meta.set_rowset_id_v2("compact_rowset_5_updated");
        rowset_meta.set_start_version(version);
        rowset_meta.set_end_version(version);
        rowset_meta.set_num_rows(550);
        rowset_meta.set_tablet_id(tablet_id);
        ASSERT_TRUE(versioned::document_put(txn.get(), rowset_compact_key, std::move(rowset_meta)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 4: Should return the first encountered data (from B)
    {
        doris::RowsetMetaCloudPB rowset_meta;
        ASSERT_EQ(reader.get_compact_rowset_meta(tablet_id, version, &rowset_meta),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_meta.rowset_id_v2(), "compact_rowset_5_updated")
                << "Should return first valid data encountered in chain";
        ASSERT_EQ(rowset_meta.num_rows(), 550);
    }
}

TEST_F(CloneChainReaderTest, GetRowsetMeta) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    int64_t tablet_id = 16001;
    int64_t version = 5;

    // Case 1: No data exists anywhere - should return NOT_FOUND
    {
        doris::RowsetMetaCloudPB rowset_meta;
        ASSERT_EQ(reader.get_rowset_meta(tablet_id, version, &rowset_meta),
                  TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    // Case 2: Insert only load rowset meta in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string rowset_load_key =
                versioned::meta_rowset_load_key({instance_ids_[0], tablet_id, version});
        doris::RowsetMetaCloudPB rowset_meta;
        rowset_meta.set_rowset_id(0);
        rowset_meta.set_rowset_id_v2("load_rowset_" + std::to_string(version));
        rowset_meta.set_start_version(version);
        rowset_meta.set_end_version(version);
        rowset_meta.set_num_rows(500);
        rowset_meta.set_tablet_id(tablet_id);
        // Use a specific versionstamp
        ASSERT_TRUE(versioned::document_put(txn.get(), rowset_load_key, Versionstamp(100, 1),
                                            std::move(rowset_meta)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Should return load rowset meta
    {
        doris::RowsetMetaCloudPB rowset_meta;
        ASSERT_EQ(reader.get_rowset_meta(tablet_id, version, &rowset_meta), TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_meta.rowset_id_v2(), "load_rowset_" + std::to_string(version));
        ASSERT_EQ(rowset_meta.num_rows(), 500);
    }

    // Case 3: Insert compact rowset meta with earlier versionstamp in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string rowset_compact_key =
                versioned::meta_rowset_compact_key({instance_ids_[0], tablet_id, version});
        doris::RowsetMetaCloudPB rowset_meta;
        rowset_meta.set_rowset_id(0);
        rowset_meta.set_rowset_id_v2("compact_rowset_" + std::to_string(version));
        rowset_meta.set_start_version(version);
        rowset_meta.set_end_version(version);
        rowset_meta.set_num_rows(600);
        rowset_meta.set_tablet_id(tablet_id);
        // Earlier versionstamp
        ASSERT_TRUE(versioned::document_put(txn.get(), rowset_compact_key, Versionstamp(50, 1),
                                            std::move(rowset_meta)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 3: Should return load rowset meta (higher versionstamp: 100 > 50)
    {
        doris::RowsetMetaCloudPB rowset_meta;
        ASSERT_EQ(reader.get_rowset_meta(tablet_id, version, &rowset_meta), TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_meta.rowset_id_v2(), "load_rowset_" + std::to_string(version))
                << "Should return rowset with higher versionstamp (100 > 50)";
        ASSERT_EQ(rowset_meta.num_rows(), 500);
    }

    // Case 4: Insert compact rowset meta with later versionstamp in instance B
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string rowset_compact_key =
                versioned::meta_rowset_compact_key({instance_ids_[1], tablet_id, version});
        doris::RowsetMetaCloudPB rowset_meta;
        rowset_meta.set_rowset_id(0);
        rowset_meta.set_rowset_id_v2("compact_rowset_updated_" + std::to_string(version));
        rowset_meta.set_start_version(version);
        rowset_meta.set_end_version(version);
        rowset_meta.set_num_rows(700);
        rowset_meta.set_tablet_id(tablet_id);
        ASSERT_TRUE(versioned::document_put(txn.get(), rowset_compact_key, Versionstamp(1500, 1),
                                            std::move(rowset_meta)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 4: Should return compact rowset meta from B (found first in clone chain)
    {
        doris::RowsetMetaCloudPB rowset_meta;
        ASSERT_EQ(reader.get_rowset_meta(tablet_id, version, &rowset_meta), TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_meta.rowset_id_v2(), "compact_rowset_updated_" + std::to_string(version))
                << "Should return first valid data encountered in chain";
        ASSERT_EQ(rowset_meta.num_rows(), 700);
    }

    // Case 5: Test with version beyond snapshot - should be ignored
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        int64_t another_version = 6;
        std::string rowset_load_key =
                versioned::meta_rowset_load_key({instance_ids_[1], tablet_id, another_version});
        doris::RowsetMetaCloudPB rowset_meta;
        rowset_meta.set_rowset_id(0);
        rowset_meta.set_rowset_id_v2("load_rowset_large_version_" +
                                     std::to_string(another_version));
        rowset_meta.set_start_version(another_version);
        rowset_meta.set_end_version(another_version);
        rowset_meta.set_num_rows(800);
        rowset_meta.set_tablet_id(tablet_id);
        // Use version larger than snapshot_versions_[1] (2000)
        ASSERT_TRUE(versioned::document_put(txn.get(), rowset_load_key, Versionstamp(2500, 1),
                                            std::move(rowset_meta)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 5: Should not find the large version rowset
    {
        doris::RowsetMetaCloudPB rowset_meta;
        ASSERT_EQ(reader.get_rowset_meta(tablet_id, 6, &rowset_meta),
                  TxnErrorCode::TXN_KEY_NOT_FOUND)
                << "Should not read data with version > snapshot";
    }

    // Case 6: Test only compact rowset meta exists
    {
        int64_t another_version = 7;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string rowset_compact_key =
                versioned::meta_rowset_compact_key({instance_ids_[0], tablet_id, another_version});
        doris::RowsetMetaCloudPB rowset_meta;
        rowset_meta.set_rowset_id(0);
        rowset_meta.set_rowset_id_v2("compact_only_" + std::to_string(another_version));
        rowset_meta.set_start_version(another_version);
        rowset_meta.set_end_version(another_version);
        rowset_meta.set_num_rows(900);
        rowset_meta.set_tablet_id(tablet_id);
        ASSERT_TRUE(versioned::document_put(txn.get(), rowset_compact_key, Versionstamp(100, 1),
                                            std::move(rowset_meta)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 6: Should return compact rowset meta when only compact exists
    {
        doris::RowsetMetaCloudPB rowset_meta;
        ASSERT_EQ(reader.get_rowset_meta(tablet_id, 7, &rowset_meta), TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_meta.rowset_id_v2(), "compact_only_7");
        ASSERT_EQ(rowset_meta.num_rows(), 900);
    }

    // Case 7: Test versionstamp comparison - compact rowset with higher versionstamp should win
    {
        int64_t another_version = 8;
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);

        // Insert load rowset with lower versionstamp
        std::string rowset_load_key =
                versioned::meta_rowset_load_key({instance_ids_[0], tablet_id, another_version});
        doris::RowsetMetaCloudPB load_rowset_meta;
        load_rowset_meta.set_rowset_id(0);
        load_rowset_meta.set_rowset_id_v2("load_rowset_" + std::to_string(another_version));
        load_rowset_meta.set_start_version(another_version);
        load_rowset_meta.set_end_version(another_version);
        load_rowset_meta.set_num_rows(1000);
        load_rowset_meta.set_tablet_id(tablet_id);
        ASSERT_TRUE(versioned::document_put(txn.get(), rowset_load_key, Versionstamp(100, 1),
                                            std::move(load_rowset_meta)));

        // Insert compact rowset with higher versionstamp
        std::string rowset_compact_key =
                versioned::meta_rowset_compact_key({instance_ids_[0], tablet_id, another_version});
        doris::RowsetMetaCloudPB compact_rowset_meta;
        compact_rowset_meta.set_rowset_id(0);
        compact_rowset_meta.set_rowset_id_v2("compact_rowset_" + std::to_string(another_version));
        compact_rowset_meta.set_start_version(another_version);
        compact_rowset_meta.set_end_version(another_version);
        compact_rowset_meta.set_num_rows(1100);
        compact_rowset_meta.set_tablet_id(tablet_id);
        ASSERT_TRUE(versioned::document_put(txn.get(), rowset_compact_key, Versionstamp(200, 1),
                                            std::move(compact_rowset_meta)));

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 7: Should return compact rowset meta (higher versionstamp)
    {
        doris::RowsetMetaCloudPB rowset_meta;
        ASSERT_EQ(reader.get_rowset_meta(tablet_id, 8, &rowset_meta), TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_meta.rowset_id_v2(), "compact_rowset_8")
                << "Should return rowset with higher versionstamp";
        ASSERT_EQ(rowset_meta.num_rows(), 1100);
    }
}

TEST_F(CloneChainReaderTest, GetRowsetMetas) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    int64_t tablet_id = 15001;
    int64_t start_version = 1;
    int64_t end_version = 5;

    // Case 1: No data exists anywhere - should return NOT_FOUND
    {
        std::vector<doris::RowsetMetaCloudPB> rowset_metas;
        ASSERT_EQ(reader.get_rowset_metas(tablet_id, start_version, end_version, &rowset_metas),
                  TxnErrorCode::TXN_OK);
        ASSERT_TRUE(rowset_metas.empty());
    }

    // Case 2: Insert rowset metas in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);

        for (int64_t version = start_version; version <= end_version; ++version) {
            std::string rowset_key =
                    versioned::meta_rowset_load_key({instance_ids_[0], tablet_id, version});
            doris::RowsetMetaCloudPB rowset_meta;
            rowset_meta.set_rowset_id(0);
            rowset_meta.set_rowset_id_v2("rowset_" + std::to_string(version));
            rowset_meta.set_start_version(version);
            rowset_meta.set_end_version(version);
            rowset_meta.set_num_rows(100 * version);
            rowset_meta.set_tablet_id(tablet_id);
            Versionstamp versionstamp(snapshot_versions_[0] - 1, 1);
            ASSERT_TRUE(versioned::document_put(txn.get(), rowset_key, versionstamp,
                                                std::move(rowset_meta)));
        }

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find data from A through clone chain
    {
        std::vector<doris::RowsetMetaCloudPB> rowset_metas;
        ASSERT_EQ(reader.get_rowset_metas(tablet_id, start_version, end_version, &rowset_metas),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_metas.size(), 5);
        for (size_t i = 0; i < rowset_metas.size(); ++i) {
            int64_t version = start_version + i;
            ASSERT_EQ(rowset_metas[i].rowset_id_v2(), "rowset_" + std::to_string(version));
            ASSERT_EQ(rowset_metas[i].start_version(), version);
            ASSERT_EQ(rowset_metas[i].end_version(), version);
            ASSERT_EQ(rowset_metas[i].num_rows(), 100 * version);
        }
    }

    // Case 3: Insert data with version > snapshot_version in instance B (should be ignored)
    {
        // [1, 1], [2, 5]
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);

        int64_t version = 1;
        std::string rowset_key =
                versioned::meta_rowset_load_key({instance_ids_[1], tablet_id, version});
        doris::RowsetMetaCloudPB rowset_meta;
        rowset_meta.set_rowset_id(0);
        rowset_meta.set_rowset_id_v2("large_rowset_" + std::to_string(version));
        rowset_meta.set_start_version(version);
        rowset_meta.set_end_version(version);
        rowset_meta.set_num_rows(200 * version);
        rowset_meta.set_tablet_id(tablet_id);
        Versionstamp versionstamp(snapshot_versions_[1], 1);
        ASSERT_TRUE(versioned::document_put(txn.get(), rowset_key, versionstamp,
                                            std::move(rowset_meta)));

        rowset_key = versioned::meta_rowset_compact_key({instance_ids_[1], tablet_id, end_version});
        rowset_meta.set_rowset_id(0);
        rowset_meta.set_rowset_id_v2("large_rowset_" + std::to_string(version));
        rowset_meta.set_start_version(version);
        rowset_meta.set_end_version(end_version);
        rowset_meta.set_num_rows(200 * version);
        rowset_meta.set_tablet_id(tablet_id);
        ASSERT_TRUE(versioned::document_put(txn.get(), rowset_key, versionstamp,
                                            std::move(rowset_meta)));

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 3: Should still return data from A, ignoring large version from B
    {
        std::vector<doris::RowsetMetaCloudPB> rowset_metas;
        ASSERT_EQ(reader.get_rowset_metas(tablet_id, start_version, end_version, &rowset_metas),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_metas.size(), 5);
        for (size_t i = 0; i < rowset_metas.size(); ++i) {
            int64_t version = start_version + i;
            ASSERT_EQ(rowset_metas[i].rowset_id_v2(), "rowset_" + std::to_string(version))
                    << "Should not read data with version > snapshot";
            ASSERT_EQ(rowset_metas[i].num_rows(), 100 * version);
        }
    }

    // Case 4: Insert valid data in instance B (within snapshot version)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);

        int64_t version = 1;
        std::string rowset_key =
                versioned::meta_rowset_load_key({instance_ids_[1], tablet_id, version});
        doris::RowsetMetaCloudPB rowset_meta;
        rowset_meta.set_rowset_id(0);
        rowset_meta.set_rowset_id_v2("large_rowset_" + std::to_string(version));
        rowset_meta.set_start_version(version);
        rowset_meta.set_end_version(version);
        rowset_meta.set_num_rows(200 * version);
        rowset_meta.set_tablet_id(tablet_id);
        Versionstamp versionstamp(snapshot_versions_[1] - 1, 1);
        ASSERT_TRUE(versioned::document_put(txn.get(), rowset_key, versionstamp,
                                            std::move(rowset_meta)));

        rowset_key = versioned::meta_rowset_compact_key({instance_ids_[1], tablet_id, end_version});
        rowset_meta.set_rowset_id(0);
        rowset_meta.set_rowset_id_v2("large_rowset_" + std::to_string(end_version));
        rowset_meta.set_start_version(version + 1);
        rowset_meta.set_end_version(end_version);
        rowset_meta.set_num_rows(200 * version);
        rowset_meta.set_tablet_id(tablet_id);
        ASSERT_TRUE(versioned::document_put(txn.get(), rowset_key, versionstamp,
                                            std::move(rowset_meta)));

        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 4: Should return the first encountered data (from B)
    {
        std::vector<doris::RowsetMetaCloudPB> rowset_metas;
        ASSERT_EQ(reader.get_rowset_metas(tablet_id, start_version, end_version, &rowset_metas),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(rowset_metas.size(), 2);
        ASSERT_EQ(rowset_metas[0].rowset_id_v2(), "large_rowset_1")
                << "Should return first valid data encountered in chain";
        ASSERT_EQ(rowset_metas[0].num_rows(), 200 * 1);
        ASSERT_EQ(rowset_metas[0].start_version(), 1);
        ASSERT_EQ(rowset_metas[0].end_version(), 1);
        ASSERT_EQ(rowset_metas[1].rowset_id_v2(), "large_rowset_5")
                << "Should return first valid data encountered in chain";
        ASSERT_EQ(rowset_metas[1].num_rows(), 200 * 1);
        ASSERT_EQ(rowset_metas[1].start_version(), 2);
        ASSERT_EQ(rowset_metas[1].end_version(), 5);
    }
}

TEST_F(CloneChainReaderTest, GetTabletMeta) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    int64_t tablet_id = 16001;

    // Case 1: No data exists anywhere - should return NOT_FOUND
    {
        doris::TabletMetaCloudPB tablet_meta;
        Versionstamp versionstamp;
        ASSERT_EQ(reader.get_tablet_meta(tablet_id, &tablet_meta, &versionstamp),
                  TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    // Case 2: Insert tablet meta in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_meta_key = versioned::meta_tablet_key({instance_ids_[0], tablet_id});
        doris::TabletMetaCloudPB tablet_meta;
        tablet_meta.set_tablet_id(tablet_id);
        tablet_meta.set_table_id(1001);
        tablet_meta.set_partition_id(2001);
        tablet_meta.set_index_id(3001);
        tablet_meta.set_schema_version(1);
        ASSERT_TRUE(versioned::document_put(txn.get(), tablet_meta_key, std::move(tablet_meta)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find data from A through clone chain
    {
        doris::TabletMetaCloudPB tablet_meta;
        Versionstamp versionstamp;
        ASSERT_EQ(reader.get_tablet_meta(tablet_id, &tablet_meta, &versionstamp),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_meta.tablet_id(), tablet_id);
        ASSERT_EQ(tablet_meta.table_id(), 1001);
        ASSERT_EQ(tablet_meta.partition_id(), 2001);
        ASSERT_EQ(tablet_meta.index_id(), 3001);
        ASSERT_EQ(tablet_meta.schema_version(), 1);
    }

    // Case 3: Insert data with version > snapshot_version in instance B (should be ignored)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_meta_key = versioned::meta_tablet_key({instance_ids_[1], tablet_id});
        doris::TabletMetaCloudPB tablet_meta;
        tablet_meta.set_tablet_id(tablet_id);
        tablet_meta.set_table_id(1002);
        tablet_meta.set_partition_id(2002);
        tablet_meta.set_index_id(3002);
        tablet_meta.set_schema_version(2);
        std::string key = encode_versioned_key(tablet_meta_key, Versionstamp(2500, 1));
        txn->put(key, tablet_meta.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 3: Should still return data from A, ignoring large version from B
    {
        doris::TabletMetaCloudPB tablet_meta;
        Versionstamp versionstamp;
        ASSERT_EQ(reader.get_tablet_meta(tablet_id, &tablet_meta, &versionstamp),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_meta.table_id(), 1001) << "Should not read data with version > snapshot";
        ASSERT_EQ(tablet_meta.partition_id(), 2001);
        ASSERT_EQ(tablet_meta.schema_version(), 1);
    }

    // Case 4: Insert valid data in instance B (within snapshot version)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_meta_key = versioned::meta_tablet_key({instance_ids_[1], tablet_id});
        doris::TabletMetaCloudPB tablet_meta;
        tablet_meta.set_tablet_id(tablet_id);
        tablet_meta.set_table_id(1003);
        tablet_meta.set_partition_id(2003);
        tablet_meta.set_index_id(3003);
        tablet_meta.set_schema_version(3);
        ASSERT_TRUE(versioned::document_put(txn.get(), tablet_meta_key, std::move(tablet_meta)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 4: Should return the first encountered data (from B)
    {
        doris::TabletMetaCloudPB tablet_meta;
        Versionstamp versionstamp;
        ASSERT_EQ(reader.get_tablet_meta(tablet_id, &tablet_meta, &versionstamp),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_meta.table_id(), 1003)
                << "Should return first valid data encountered in chain";
        ASSERT_EQ(tablet_meta.partition_id(), 2003);
        ASSERT_EQ(tablet_meta.schema_version(), 3);
    }
}

TEST_F(CloneChainReaderTest, GetTabletSchema) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    int64_t index_id = 17001;
    int64_t schema_version = 5;

    // Case 1: No data exists anywhere - should return NOT_FOUND
    {
        doris::TabletSchemaCloudPB tablet_schema;
        ASSERT_EQ(reader.get_tablet_schema(index_id, schema_version, &tablet_schema),
                  TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    // Case 2: Insert tablet schema in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_schema_key =
                versioned::meta_schema_key({instance_ids_[0], index_id, schema_version});
        doris::TabletSchemaCloudPB tablet_schema;
        tablet_schema.set_schema_version(schema_version);
        tablet_schema.set_keys_type(doris::DUP_KEYS);
        ASSERT_TRUE(document_put(txn.get(), tablet_schema_key, std::move(tablet_schema)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find data from A through clone chain
    {
        doris::TabletSchemaCloudPB tablet_schema;
        ASSERT_EQ(reader.get_tablet_schema(index_id, schema_version, &tablet_schema),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_schema.schema_version(), schema_version);
        ASSERT_EQ(tablet_schema.keys_type(), doris::DUP_KEYS);
    }

    // Case 4: Insert valid data in instance B
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string tablet_schema_key =
                versioned::meta_schema_key({instance_ids_[1], index_id, schema_version});
        doris::TabletSchemaCloudPB tablet_schema;
        tablet_schema.set_schema_version(schema_version);
        tablet_schema.set_keys_type(doris::AGG_KEYS);
        ASSERT_TRUE(document_put(txn.get(), tablet_schema_key, std::move(tablet_schema)));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 4: Should return the first encountered data (from B)
    {
        doris::TabletSchemaCloudPB tablet_schema;
        ASSERT_EQ(reader.get_tablet_schema(index_id, schema_version, &tablet_schema),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(tablet_schema.keys_type(), doris::AGG_KEYS)
                << "Should return first valid data encountered in chain";
    }
}

TEST_F(CloneChainReaderTest, GetPartitionPendingTxnId) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    int64_t partition_id = 18001;

    // Case 1: No data exists anywhere - should set first_txn_id to -1
    {
        int64_t first_txn_id;
        int64_t partition_version;
        ASSERT_EQ(reader.get_partition_pending_txn_id(partition_id, &first_txn_id,
                                                      &partition_version),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(first_txn_id, -1);
    }

    // Case 2: Insert partition pending txn data in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_version_key =
                versioned::partition_version_key({instance_ids_[0], partition_id});

        // Create PartitionPendingTxnInfo
        doris::cloud::VersionPB partition_version;
        partition_version.add_pending_txn_ids(1001);
        partition_version.set_version(100);

        versioned_put(txn.get(), partition_version_key, Versionstamp(100, 0),
                      partition_version.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find data from A through clone chain
    {
        int64_t first_txn_id;
        int64_t partition_version;
        ASSERT_EQ(reader.get_partition_pending_txn_id(partition_id, &first_txn_id,
                                                      &partition_version),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(first_txn_id, 1001);
        ASSERT_EQ(partition_version, 100);
    }

    // Case 3: Insert data with version > snapshot_version in instance B (should be ignored)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_pending_key =
                versioned::partition_version_key({instance_ids_[1], partition_id});

        VersionPB version;
        version.set_version(200);
        version.add_pending_txn_ids(2001);

        std::string key = encode_versioned_key(partition_pending_key, Versionstamp(2500, 1));
        versioned_put(txn.get(), key, Versionstamp(snapshot_versions_[1], 100),
                      version.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 3: Should still return data from A, ignoring large version from B
    {
        int64_t first_txn_id;
        int64_t partition_version;
        ASSERT_EQ(reader.get_partition_pending_txn_id(partition_id, &first_txn_id,
                                                      &partition_version),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(first_txn_id, 1001) << "Should not read data with version > snapshot";
        ASSERT_EQ(partition_version, 100);
    }

    // Case 4: Insert valid data in instance B (within snapshot version)
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string partition_version_key =
                versioned::partition_version_key({instance_ids_[1], partition_id});

        VersionPB version;
        version.set_version(150);
        version.add_pending_txn_ids(1500);

        versioned_put(txn.get(), partition_version_key, Versionstamp(snapshot_versions_[1] - 1, 0),
                      version.SerializeAsString());
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 4: Should return the first encountered data (from B)
    {
        int64_t first_txn_id;
        int64_t partition_version;
        ASSERT_EQ(reader.get_partition_pending_txn_id(partition_id, &first_txn_id,
                                                      &partition_version),
                  TxnErrorCode::TXN_OK);
        ASSERT_EQ(first_txn_id, 1500) << "Should return first valid data encountered in chain";
        ASSERT_EQ(partition_version, 150);
    }
}

TEST_F(CloneChainReaderTest, GetDeleteBitmapV2) {
    std::string instance_id = instance_ids_[2]; // C
    Versionstamp snapshot_version = snapshot_versions_[2];
    CloneChainReader reader(instance_id, snapshot_version, txn_kv_.get(), resource_mgr_.get());

    int64_t tablet_id = 19001;
    std::string rowset_id = "rowset_1";

    // Case 1: No data exists anywhere - should return NOT_FOUND
    {
        DeleteBitmapStoragePB delete_bitmap;
        ASSERT_EQ(reader.get_delete_bitmap_v2(tablet_id, rowset_id, &delete_bitmap),
                  TxnErrorCode::TXN_KEY_NOT_FOUND);
    }

    // Case 2: Insert delete bitmap in instance A
    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv_->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string delete_bitmap_key =
                versioned::meta_delete_bitmap_key({instance_ids_[0], tablet_id, rowset_id});
        DeleteBitmapStoragePB delete_bitmap;
        delete_bitmap.set_store_in_fdb(true);
        blob_put(txn.get(), delete_bitmap_key, delete_bitmap, 0);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    // Case 2: Read from instance C should find data from A through clone chain
    {
        DeleteBitmapStoragePB delete_bitmap;
        ASSERT_EQ(reader.get_delete_bitmap_v2(tablet_id, rowset_id, &delete_bitmap),
                  TxnErrorCode::TXN_OK)
                << dump_range(txn_kv_.get());
    }
}
