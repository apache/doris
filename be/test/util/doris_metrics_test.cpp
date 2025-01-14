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

#include "util/doris_metrics.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"

namespace doris {

class DorisMetricsTest : public testing::Test {
public:
    DorisMetricsTest() {}
    virtual ~DorisMetricsTest() {}
};

TEST_F(DorisMetricsTest, Normal) {
    auto server_entity = DorisMetrics::instance()->server_entity();
    // check metric
    {
        DorisMetrics::instance()->fragment_requests_total->set_value(0);
        DorisMetrics::instance()->fragment_requests_total->increment(12);
        auto metric = server_entity->get_metric("fragment_requests_total");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("12", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->fragment_request_duration_us->set_value(0);
        DorisMetrics::instance()->fragment_request_duration_us->increment(101);
        auto metric = server_entity->get_metric("fragment_request_duration_us");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("101", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->query_scan_bytes->increment(104);
        auto metric = server_entity->get_metric("query_scan_bytes");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("104", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->query_scan_rows->increment(105);
        auto metric = server_entity->get_metric("query_scan_rows");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("105", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->push_requests_success_total->increment(106);
        auto metric =
                server_entity->get_metric("push_requests_success_total", "push_requests_total");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("106", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->push_requests_fail_total->increment(107);
        auto metric = server_entity->get_metric("push_requests_fail_total", "push_requests_total");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("107", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->push_request_duration_us->increment(108);
        auto metric = server_entity->get_metric("push_request_duration_us");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("108", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->push_request_write_bytes->increment(109);
        auto metric = server_entity->get_metric("push_request_write_bytes");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("109", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->push_request_write_rows->increment(110);
        auto metric = server_entity->get_metric("push_request_write_rows");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("110", metric->to_string().c_str());
    }
    // engine request
    {
        DorisMetrics::instance()->create_tablet_requests_total->set_value(0);
        DorisMetrics::instance()->create_tablet_requests_total->increment(15);
        auto metric =
                server_entity->get_metric("create_tablet_requests_total", "engine_requests_total");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("15", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->drop_tablet_requests_total->set_value(0);
        DorisMetrics::instance()->drop_tablet_requests_total->increment(16);
        auto metric =
                server_entity->get_metric("drop_tablet_requests_total", "engine_requests_total");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("16", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->report_all_tablets_requests_skip->increment(1);
        auto metric = server_entity->get_metric("report_all_tablets_requests_skip",
                                                "engine_requests_total");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("1", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->schema_change_requests_total->increment(19);
        auto metric =
                server_entity->get_metric("schema_change_requests_total", "engine_requests_total");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("19", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->create_rollup_requests_total->increment(20);
        auto metric =
                server_entity->get_metric("create_rollup_requests_total", "engine_requests_total");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("20", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->storage_migrate_requests_total->set_value(0);
        DorisMetrics::instance()->storage_migrate_requests_total->increment(21);
        auto metric = server_entity->get_metric("storage_migrate_requests_total",
                                                "engine_requests_total");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("21", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->delete_requests_total->increment(22);
        auto metric = server_entity->get_metric("delete_requests_total", "engine_requests_total");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("22", metric->to_string().c_str());
    }
    //  compaction
    {
        DorisMetrics::instance()->base_compaction_deltas_total->increment(30);
        auto metric = server_entity->get_metric("base_compaction_deltas_total",
                                                "compaction_deltas_total");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("30", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->cumulative_compaction_deltas_total->increment(31);
        auto metric = server_entity->get_metric("cumulative_compaction_deltas_total",
                                                "compaction_deltas_total");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("31", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->base_compaction_bytes_total->increment(32);
        auto metric =
                server_entity->get_metric("base_compaction_bytes_total", "compaction_bytes_total");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("32", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->cumulative_compaction_bytes_total->increment(33);
        auto metric = server_entity->get_metric("cumulative_compaction_bytes_total",
                                                "compaction_bytes_total");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("33", metric->to_string().c_str());
    }
    // Gauge
    {
        DorisMetrics::instance()->memory_pool_bytes_total->increment(40);
        auto metric = server_entity->get_metric("memory_pool_bytes_total");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("40", metric->to_string().c_str());
    }
}

} // namespace doris
