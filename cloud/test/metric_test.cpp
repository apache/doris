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

#include "common/metric.h"

#include <gtest/gtest.h>

#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#include "common/bvars.h"
#include "common/config.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(MetricTest, FdbMetricExporterTest) {
    using namespace doris::cloud;

    // normal to collect fdb metirc
    {
        std::string fdb_metric_example = "./fdb_metric_example.json";
        std::ifstream inFile(fdb_metric_example);

        ASSERT_TRUE(inFile.is_open());
        std::string fileContent((std::istreambuf_iterator<char>(inFile)),
                                std::istreambuf_iterator<char>());

        std::shared_ptr<TxnKv> txn_kv = std::make_shared<MemTxnKv>();
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put("\xff\xff/status/json", fileContent);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        FdbMetricExporter fdb_metric_exporter(txn_kv);
        fdb_metric_exporter.sleep_interval_ms_ = 1;
        fdb_metric_exporter.start();
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        fdb_metric_exporter.stop();
        ASSERT_EQ(g_bvar_fdb_machines_count.get_value(), 3);
        ASSERT_EQ(g_bvar_fdb_client_count.get_value(), 8);
    }

    // empty fdb_status
    {
        g_bvar_fdb_machines_count.set_value(BVAR_FDB_INVALID_VALUE);
        g_bvar_fdb_client_count.set_value(BVAR_FDB_INVALID_VALUE);
        std::shared_ptr<TxnKv> txn_kv = std::make_shared<MemTxnKv>();
        {
            FdbMetricExporter fdb_metric_exporter(txn_kv);
            fdb_metric_exporter.sleep_interval_ms_ = 1;
            fdb_metric_exporter.start();
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        ASSERT_EQ(g_bvar_fdb_machines_count.get_value(), BVAR_FDB_INVALID_VALUE);
        ASSERT_EQ(g_bvar_fdb_client_count.get_value(), BVAR_FDB_INVALID_VALUE);
    }

    // The cluster field is missing
    {
        g_bvar_fdb_machines_count.set_value(BVAR_FDB_INVALID_VALUE);
        g_bvar_fdb_client_count.set_value(BVAR_FDB_INVALID_VALUE);

        std::string fdb_metric_example = "./fdb_metric_example.json";
        std::ifstream inFile(fdb_metric_example);

        ASSERT_TRUE(inFile.is_open());
        std::string fileContent((std::istreambuf_iterator<char>(inFile)),
                                std::istreambuf_iterator<char>());

        std::string word_to_replace = "cluster";
        std::string new_word = "xxxx";

        size_t start_pos = 0;
        while ((start_pos = fileContent.find(word_to_replace, start_pos)) != std::string::npos) {
            fileContent.replace(start_pos, word_to_replace.length(), new_word);
            start_pos += new_word.length();
        }
        std::shared_ptr<TxnKv> txn_kv = std::make_shared<MemTxnKv>();
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put("\xff\xff/status/json", fileContent);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        FdbMetricExporter fdb_metric_exporter(txn_kv);
        fdb_metric_exporter.sleep_interval_ms_ = 1;
        fdb_metric_exporter.start();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        fdb_metric_exporter.stop();
        ASSERT_EQ(g_bvar_fdb_machines_count.get_value(), BVAR_FDB_INVALID_VALUE);
        ASSERT_EQ(g_bvar_fdb_client_count.get_value(), BVAR_FDB_INVALID_VALUE);
    }

    // The client field is missing
    {
        g_bvar_fdb_machines_count.set_value(BVAR_FDB_INVALID_VALUE);
        g_bvar_fdb_client_count.set_value(BVAR_FDB_INVALID_VALUE);

        std::string fdb_metric_example = "./fdb_metric_example.json";
        std::ifstream inFile(fdb_metric_example);

        ASSERT_TRUE(inFile.is_open());
        std::string fileContent((std::istreambuf_iterator<char>(inFile)),
                                std::istreambuf_iterator<char>());

        std::string word_to_replace = "machines";
        std::string new_word = "xxxx";

        size_t start_pos = 0;
        while ((start_pos = fileContent.find(word_to_replace, start_pos)) != std::string::npos) {
            fileContent.replace(start_pos, word_to_replace.length(), new_word);
            start_pos += new_word.length();
        }
        std::shared_ptr<TxnKv> txn_kv = std::make_shared<MemTxnKv>();
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put("\xff\xff/status/json", fileContent);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        FdbMetricExporter fdb_metric_exporter(txn_kv);
        fdb_metric_exporter.sleep_interval_ms_ = 1;
        fdb_metric_exporter.start();
        std::this_thread::sleep_for(std::chrono::milliseconds(15));
        fdb_metric_exporter.stop();
        ASSERT_EQ(g_bvar_fdb_machines_count.get_value(), BVAR_FDB_INVALID_VALUE);
        ASSERT_EQ(g_bvar_fdb_client_count.get_value(), 8);
    }

    // stop without start
    {
        g_bvar_fdb_machines_count.set_value(BVAR_FDB_INVALID_VALUE);
        g_bvar_fdb_client_count.set_value(BVAR_FDB_INVALID_VALUE);

        std::string fdb_metric_example = "./fdb_metric_example.json";
        std::ifstream inFile(fdb_metric_example);

        ASSERT_TRUE(inFile.is_open());
        std::string fileContent((std::istreambuf_iterator<char>(inFile)),
                                std::istreambuf_iterator<char>());

        std::shared_ptr<TxnKv> txn_kv = std::make_shared<MemTxnKv>();
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put("\xff\xff/status/json", fileContent);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        FdbMetricExporter fdb_metric_exporter(txn_kv);
        fdb_metric_exporter.sleep_interval_ms_ = 1;
        std::this_thread::sleep_for(std::chrono::milliseconds(15));
        fdb_metric_exporter.stop();
        ASSERT_EQ(g_bvar_fdb_machines_count.get_value(), BVAR_FDB_INVALID_VALUE);
        ASSERT_EQ(g_bvar_fdb_client_count.get_value(), BVAR_FDB_INVALID_VALUE);
    }
}