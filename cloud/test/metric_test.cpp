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
#include "meta-store/mem_txn_kv.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"

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

    // process status
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
        fdb_metric_exporter.start();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        fdb_metric_exporter.stop();
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"09ca90b9f3f413e5816b2610ed8b465d", "cpu", "usage_cores"}),
                  0.0012292);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"09ca90b9f3f413e5816b2610ed8b465d", "disk", "busy"}),
                  0.0085999800000000001);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"09ca90b9f3f413e5816b2610ed8b465d", "disk", "free_bytes"}),
                  490412584960);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"09ca90b9f3f413e5816b2610ed8b465d", "disk", "reads_counter"}),
                  854857);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"09ca90b9f3f413e5816b2610ed8b465d", "disk", "reads_hz"}),
                  0);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"09ca90b9f3f413e5816b2610ed8b465d", "disk", "reads_sectors"}),
                  0);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"09ca90b9f3f413e5816b2610ed8b465d", "disk", "total_bytes"}),
                  527295578112);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"09ca90b9f3f413e5816b2610ed8b465d", "disk", "writes_counter"}),
                  73765457);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"09ca90b9f3f413e5816b2610ed8b465d", "disk", "writes_hz"}),
                  26.1999);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"09ca90b9f3f413e5816b2610ed8b465d", "disk", "writes_sectors"}),
                  1336);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"09ca90b9f3f413e5816b2610ed8b465d", "memory", "available_bytes"}),
                  3065090867);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"09ca90b9f3f413e5816b2610ed8b465d", "memory", "limit_bytes"}),
                  8589934592);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"09ca90b9f3f413e5816b2610ed8b465d", "memory", "rss_bytes"}),
                  46551040);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get({"09ca90b9f3f413e5816b2610ed8b465d", "memory",
                                                    "unused_allocated_memory"}),
                  655360);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"09ca90b9f3f413e5816b2610ed8b465d", "memory", "used_bytes"}),
                  122974208);

        // test second process
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"0a456165f04e1ec1a2ade0ce523d54a8", "cpu", "usage_cores"}),
                  0.0049765900000000004);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"0a456165f04e1ec1a2ade0ce523d54a8", "disk", "busy"}),
                  0.012200000000000001);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"0a456165f04e1ec1a2ade0ce523d54a8", "disk", "free_bytes"}),
                  489160159232);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"0a456165f04e1ec1a2ade0ce523d54a8", "disk", "reads_counter"}),
                  877107);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"0a456165f04e1ec1a2ade0ce523d54a8", "disk", "reads_hz"}),
                  0);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"0a456165f04e1ec1a2ade0ce523d54a8", "disk", "reads_sectors"}),
                  0);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"0a456165f04e1ec1a2ade0ce523d54a8", "disk", "total_bytes"}),
                  527295578112);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"0a456165f04e1ec1a2ade0ce523d54a8", "disk", "writes_counter"}),
                  79316112);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"0a456165f04e1ec1a2ade0ce523d54a8", "disk", "writes_hz"}),
                  30.9999);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"0a456165f04e1ec1a2ade0ce523d54a8", "disk", "writes_sectors"}),
                  744);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"0a456165f04e1ec1a2ade0ce523d54a8", "memory", "available_bytes"}),
                  3076787404);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"0a456165f04e1ec1a2ade0ce523d54a8", "memory", "limit_bytes"}),
                  8589934592);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"0a456165f04e1ec1a2ade0ce523d54a8", "memory", "rss_bytes"}),
                  72359936);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get({"0a456165f04e1ec1a2ade0ce523d54a8", "memory",
                                                    "unused_allocated_memory"}),
                  393216);
        ASSERT_EQ(g_bvar_fdb_cluster_processes.get(
                          {"0a456165f04e1ec1a2ade0ce523d54a8", "memory", "used_bytes"}),
                  157978624);
    }

    // workload status
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
        fdb_metric_exporter.start();
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        fdb_metric_exporter.stop();

        // Test transactions component
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"transactions", "started_hz"}), 12);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"transactions", "started_counter"}), 196874980);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"transactions", "committed_hz"}), 3.4);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"transactions", "committed_counter"}), 2083958);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"transactions", "conflicted_hz"}), 0);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"transactions", "conflicted_counter"}), 2970);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"transactions", "started_batch_priority_hz"}),
                  0);
        ASSERT_EQ(
                g_bvar_fdb_cluster_workload.get({"transactions", "started_batch_priority_counter"}),
                155);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"transactions", "started_default_priority_hz"}),
                  11);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get(
                          {"transactions", "started_default_priority_counter"}),
                  192248190);
        ASSERT_EQ(
                g_bvar_fdb_cluster_workload.get({"transactions", "started_immediate_priority_hz"}),
                0.999999);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get(
                          {"transactions", "started_immediate_priority_counter"}),
                  4626635);

        // Test operations component
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"operations", "reads_hz"}), 24.6);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"operations", "reads_counter"}), 324877376);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"operations", "writes_hz"}), 7.39999);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"operations", "writes_counter"}), 4336393);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"operations", "location_requests_hz"}), 0);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"operations", "location_requests_counter"}),
                  107543);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"operations", "read_requests_hz"}), 24.4);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"operations", "read_requests_counter"}),
                  324877376);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"operations", "memory_errors_hz"}), 0);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"operations", "memory_errors_counter"}), 0);

        // Test bytes component
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"bytes", "read_hz"}), 18468.2);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"bytes", "read_counter"}), 312243021624);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"bytes", "written_hz"}), 2042.2);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"bytes", "written_counter"}), 6678975461);

        // Test keys component
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"keys", "read_hz"}), 56.6);
        ASSERT_EQ(g_bvar_fdb_cluster_workload.get({"keys", "read_counter"}), 728858585);
    }
}