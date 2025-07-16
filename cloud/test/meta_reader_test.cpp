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
