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

#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>

#include "common/config.h"
#include "meta-service/meta_service.h"
#include "meta-store/keys.h"
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"
#include "meta-store/versioned_value.h"
#include "recycler/recycler.h"

using namespace doris::cloud;

extern std::string instance_id;

static void update_instance_info(TxnKv* txn_kv, const InstanceInfoPB& instance) {
    std::string key = instance_key({instance_id});
    std::string value = instance.SerializeAsString();
    ASSERT_FALSE(value.empty()) << "Failed to serialize instance info";
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    ASSERT_EQ(err, TxnErrorCode::TXN_OK) << "Failed to create transaction";
    txn->put(key, value);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK) << "Failed to commit transaction";
}

void get_instance(TxnKv* txn_kv, InstanceInfoPB& instance) {
    std::string key = instance_key({instance_id});
    std::string val;
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    ASSERT_EQ(err, TxnErrorCode::TXN_OK) << "Failed to create transaction";
    ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(instance.ParseFromString(val));
}

void update_snapshot(TxnKv* txn_kv, const Versionstamp& versionstamp, const SnapshotPB& snapshot) {
    std::string snapshot_full_key = versioned::snapshot_full_key({instance_id});
    std::string key = encode_versioned_key(snapshot_full_key, versionstamp);
    std::string value = snapshot.SerializeAsString();
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    ASSERT_EQ(err, TxnErrorCode::TXN_OK) << "Failed to create transaction";
    txn->put(key, value);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK) << "Failed to commit transaction";
}

void get_snapshot(TxnKv* txn_kv, const Versionstamp& versionstamp, SnapshotPB& snapshot) {
    std::string snapshot_full_key = versioned::snapshot_full_key({instance_id});
    std::string key = encode_versioned_key(snapshot_full_key, versionstamp);
    std::string val;
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    ASSERT_EQ(err, TxnErrorCode::TXN_OK) << "Failed to create transaction";
    ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
    ASSERT_TRUE(snapshot.ParseFromString(val));
}

TEST(SnapshotDataSizeCalculatorTest, SaveSnapshotDataSizeTest) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    instance.set_multi_version_status(MultiVersionStatus::MULTI_VERSION_ENABLED);
    auto* obj_info = instance.add_obj_info();
    obj_info->set_id("0");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_empty");
    update_instance_info(txn_kv.get(), instance);

    std::vector<std::pair<SnapshotPB, Versionstamp>> snapshots;

    auto create_snapshot = [&](Versionstamp versionstamp, int64_t image_size,
                               int logical_data_size) {
        SnapshotPB snapshot;
        snapshot.set_snapshot_meta_image_size(image_size);
        snapshot.set_snapshot_logical_data_size(logical_data_size);
        snapshots.emplace_back(std::make_pair(snapshot, versionstamp));
        update_snapshot(txn_kv.get(), versionstamp, snapshot);
    };

    Versionstamp vs1(1);
    create_snapshot(vs1, 100, 1000);
    Versionstamp vs2(2);
    create_snapshot(vs2, 200, 2000);
    Versionstamp vs3(3);
    create_snapshot(vs3, 300, 3000);
    Versionstamp vs4(4);
    create_snapshot(vs4, 400, 4000);
    Versionstamp vs5(5);
    create_snapshot(vs5, 500, 1000);
    Versionstamp vs6(6);
    create_snapshot(vs6, 600, 8000);
    Versionstamp vs7(7);
    create_snapshot(vs7, 700, 8000);
    Versionstamp vs8(8);
    create_snapshot(vs8, 800, 9000);

    SnapshotDataSizeCalculator calculator(instance_id, txn_kv);
    calculator.init(snapshots);
    std::map<Versionstamp, int64_t> retained_data_size;
    retained_data_size[vs1] = 500;
    retained_data_size[vs2] = 2000;
    retained_data_size[vs3] = 4000;
    retained_data_size[vs4] = 2000;
    retained_data_size[vs5] = 5000;
    retained_data_size[vs6] = 7000;
    retained_data_size[vs7] = 2000;
    retained_data_size[vs8] = 10000;
    calculator.instance_retained_data_size_ = 10;
    calculator.retained_data_size_ = std::move(retained_data_size);

    ASSERT_EQ(calculator.save_snapshot_data_size_with_retry(), 0);

    auto check_snapshot = [&](Versionstamp versionstamp, int64_t expected_retained_data_size,
                              int expected_billable_data_size) {
        SnapshotPB snapshot;
        get_snapshot(txn_kv.get(), versionstamp, snapshot);
        ASSERT_EQ(snapshot.snapshot_retained_data_size(), expected_retained_data_size);
        ASSERT_EQ(snapshot.snapshot_billable_data_size(), expected_billable_data_size);
    };

    check_snapshot(vs1, 500, 610);
    check_snapshot(vs2, 2000, 2200);
    check_snapshot(vs3, 4000, 3300);
    check_snapshot(vs4, 2000, 3400);
    check_snapshot(vs5, 5000, 1500);
    check_snapshot(vs6, 7000, 8600);
    check_snapshot(vs7, 2000, 2000 + 700 + 3000);
    check_snapshot(vs8, 10000, 9800);
    get_instance(txn_kv.get(), instance);
    ASSERT_EQ(instance.snapshot_retained_data_size(), 10);
    ASSERT_EQ(instance.snapshot_billable_data_size(), 1000);
}
