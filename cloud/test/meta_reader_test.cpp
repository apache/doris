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

#include <gtest/gtest-death-test.h>
#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "common/util.h"
#include "meta-service/doris_txn.h"
#include "meta-store/codec.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"

using namespace doris::cloud;

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
