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

#include "common/metrics/doris_metrics.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <algorithm>
#include <initializer_list>
#include <set>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "gtest/gtest_pred_impl.h"

namespace doris {

namespace {

std::string strip_metric_value(const std::string& line) {
    auto pos = line.rfind(' ');
    EXPECT_NE(std::string::npos, pos);
    return line.substr(0, pos);
}

std::vector<std::string> collect_metric_identities(const std::string& prometheus,
                                                   std::string_view prefix) {
    std::vector<std::string> identities;
    std::istringstream input(prometheus);
    std::string line;
    while (std::getline(input, line)) {
        if (line.rfind(prefix, 0) == 0) {
            identities.emplace_back(strip_metric_value(line));
        }
    }
    return identities;
}

size_t count_metric_identities(const std::vector<std::string>& identities,
                               std::initializer_list<std::string_view> expected_label_tokens) {
    return std::count_if(identities.begin(), identities.end(), [&](const std::string& identity) {
        return std::all_of(
                expected_label_tokens.begin(), expected_label_tokens.end(),
                [&](std::string_view token) { return identity.find(token) != std::string::npos; });
    });
}

void expect_unique_metric_identities(const std::vector<std::string>& identities,
                                     size_t min_expected_count) {
    EXPECT_GE(identities.size(), min_expected_count);
    std::set<std::string> unique_identities(identities.begin(), identities.end());
    EXPECT_EQ(unique_identities.size(), identities.size());
}

} // namespace

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
    {
        DorisMetrics::instance()->get_remote_tablet_slow_time_ms->increment(1000);
        auto* metric = server_entity->get_metric("get_remote_tablet_slow_time_ms");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("1000", metric->to_string().c_str());

        DorisMetrics::instance()->get_remote_tablet_slow_cnt->increment(10);
        metric = server_entity->get_metric("get_remote_tablet_slow_cnt");
        EXPECT_TRUE(metric != nullptr);
        EXPECT_STREQ("10", metric->to_string().c_str());
    }
}

TEST_F(DorisMetricsTest, PrometheusCompactionMetricsHaveUniqueSeries) {
    DorisMetrics::instance()->base_compaction_task_running_total->set_value(1);
    DorisMetrics::instance()->base_compaction_task_pending_total->set_value(2);
    DorisMetrics::instance()->cumulative_compaction_task_running_total->set_value(3);
    DorisMetrics::instance()->cumulative_compaction_task_pending_total->set_value(4);

    DorisMetrics::instance()->local_compaction_read_rows_total->set_value(5);
    DorisMetrics::instance()->local_compaction_write_rows_total->set_value(6);
    DorisMetrics::instance()->remote_compaction_read_rows_total->set_value(7);
    DorisMetrics::instance()->remote_compaction_write_rows_total->set_value(8);

    DorisMetrics::instance()->base_compaction_bytes_total->set_value(9);
    DorisMetrics::instance()->cumulative_compaction_bytes_total->set_value(10);
    DorisMetrics::instance()->full_compaction_bytes_total->set_value(11);
    DorisMetrics::instance()->local_compaction_read_bytes_total->set_value(12);
    DorisMetrics::instance()->local_compaction_write_bytes_total->set_value(13);
    DorisMetrics::instance()->remote_compaction_read_bytes_total->set_value(14);
    DorisMetrics::instance()->remote_compaction_write_bytes_total->set_value(15);

    const auto prometheus = DorisMetrics::instance()->metric_registry()->to_prometheus();

    const auto task_state_metrics =
            collect_metric_identities(prometheus, "doris_be_compaction_task_state_total");
    expect_unique_metric_identities(task_state_metrics, 4);
    EXPECT_EQ(1,
              count_metric_identities(task_state_metrics, {"type=\"base\"", "state=\"running\""}));
    EXPECT_EQ(1,
              count_metric_identities(task_state_metrics, {"type=\"base\"", "state=\"pending\""}));
    EXPECT_EQ(1, count_metric_identities(task_state_metrics,
                                         {"type=\"cumulative\"", "state=\"running\""}));
    EXPECT_EQ(1, count_metric_identities(task_state_metrics,
                                         {"type=\"cumulative\"", "state=\"pending\""}));

    const auto row_metrics =
            collect_metric_identities(prometheus, "doris_be_compaction_rows_total");
    expect_unique_metric_identities(row_metrics, 4);
    EXPECT_EQ(1, count_metric_identities(row_metrics, {"type=\"read\"", "location=\"local\""}));
    EXPECT_EQ(1, count_metric_identities(row_metrics, {"type=\"write\"", "location=\"local\""}));
    EXPECT_EQ(1, count_metric_identities(row_metrics, {"type=\"read\"", "location=\"remote\""}));
    EXPECT_EQ(1, count_metric_identities(row_metrics, {"type=\"write\"", "location=\"remote\""}));

    const auto byte_metrics =
            collect_metric_identities(prometheus, "doris_be_compaction_bytes_total");
    expect_unique_metric_identities(byte_metrics, 7);
    EXPECT_EQ(1, count_metric_identities(byte_metrics, {"type=\"base\""}));
    EXPECT_EQ(1, count_metric_identities(byte_metrics, {"type=\"cumulative\""}));
    EXPECT_EQ(1, count_metric_identities(byte_metrics, {"type=\"full\""}));
    EXPECT_EQ(1, count_metric_identities(byte_metrics, {"type=\"read\"", "location=\"local\""}));
    EXPECT_EQ(1, count_metric_identities(byte_metrics, {"type=\"write\"", "location=\"local\""}));
    EXPECT_EQ(1, count_metric_identities(byte_metrics, {"type=\"read\"", "location=\"remote\""}));
    EXPECT_EQ(1, count_metric_identities(byte_metrics, {"type=\"write\"", "location=\"remote\""}));
}

} // namespace doris
