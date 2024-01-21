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

#include "meta-service/doris_txn.h"

#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <gtest/gtest.h>

#include "common/config.h"
#include "common/util.h"
#include "meta-service/meta_service.h"
#include "meta-service/txn_kv.h"

int main(int argc, char** argv) {
    doris::cloud::config::init(nullptr, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(TxnIdConvert, TxnIdTest) {
    using namespace doris::cloud;

    // Correctness test
    {
        // 00000182a5ed173f0012
        std::string ts("\x00\x00\x01\x82\xa5\xed\x17\x3f\x00\x12", 10);
        ASSERT_EQ(ts.size(), 10);
        int64_t txn_id0;
        int ret = get_txn_id_from_fdb_ts(ts, &txn_id0);
        ASSERT_EQ(ret, 0);
        std::string str((char*)&txn_id0, sizeof(txn_id0));
        std::cout << "fdb_ts0: " << hex(ts) << " "
                  << "txn_id0: " << txn_id0 << " hex: " << hex(str) << std::endl;

        // 00000182a5ed173f0013
        ts = std::string("\x00\x00\x01\x82\xa5\xed\x17\x3f\x00\x13", 10);
        ASSERT_EQ(ts.size(), 10);
        int64_t txn_id1;
        ret = get_txn_id_from_fdb_ts(ts, &txn_id1);
        ASSERT_EQ(ret, 0);
        ASSERT_GT(txn_id1, txn_id0);
        str = std::string((char*)&txn_id1, sizeof(txn_id1));
        std::cout << "fdb_ts1: " << hex(ts) << " "
                  << "txn_id1: " << txn_id1 << " hex: " << hex(str) << std::endl;

        // 00000182a5ed174f0013
        ts = std::string("\x00\x00\x01\x82\xa5\xed\x17\x4f\x00\x13", 10);
        ASSERT_EQ(ts.size(), 10);
        int64_t txn_id2;
        ret = get_txn_id_from_fdb_ts(ts, &txn_id2);
        ASSERT_EQ(ret, 0);
        ASSERT_GT(txn_id2, txn_id1);
        str = std::string((char*)&txn_id2, sizeof(txn_id2));
        std::cout << "fdb_ts2: " << hex(ts) << " "
                  << "txn_id2: " << txn_id2 << " hex: " << hex(str) << std::endl;
    }

    // Boundary test
    {
        //                 1024
        // 00000182a5ed174f0400
        std::string ts("\x00\x00\x01\x82\xa5\xed\x17\x4f\x04\x00", 10);
        ASSERT_EQ(ts.size(), 10);
        int64_t txn_id;
        int ret = get_txn_id_from_fdb_ts(ts, &txn_id);
        ASSERT_EQ(ret, 2); // Exceed max seq

        //                 1023
        // 00000182a5ed174f03ff
        ts = std::string("\x00\x00\x01\x82\xa5\xed\x17\x4f\x03\xff", 10);
        ret = get_txn_id_from_fdb_ts(ts, &txn_id);
        ASSERT_EQ(ret, 0);

        //                 0000
        // 00000182a5ed174f0000
        ts = std::string("\x00\x00\x01\x82\xa5\xed\x17\x4f\x03\x00", 10);
        ret = get_txn_id_from_fdb_ts(ts, &txn_id);
        ASSERT_EQ(ret, 0);

        // Insufficient length
        ts = std::string("\x00\x00\x01\x82\xa5\xed\x17\x4f\x03\x00", 9);
        ret = get_txn_id_from_fdb_ts(ts, &txn_id);
        ASSERT_EQ(ret, 1);
    }
}
