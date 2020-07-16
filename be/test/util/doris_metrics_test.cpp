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

#include <gtest/gtest.h>

#include "common/config.h"
#include "util/logging.h"
#include "util/doris_metrics.h"

namespace doris {

class DorisMetricsTest : public testing::Test {
public:
    DorisMetricsTest() { }
    virtual ~DorisMetricsTest() {
    }
};

class TestMetricsVisitor : public MetricsVisitor {
public:
    virtual ~TestMetricsVisitor() { }
    void visit(const std::string& prefix, const std::string& name,
               MetricCollector* collector) {
        for (auto& it : collector->metrics()) {
            Metric* metric = it.second;
            auto& labels = it.first;
            switch (metric->type()) {
            case MetricType::COUNTER: {
                bool has_prev = false;
                if (!prefix.empty()) {
                    _ss << prefix;
                    has_prev = true;
                }
                if (!name.empty()) {
                    if (has_prev) {
                        _ss << "_";
                    }
                    _ss << name;
                }
                if (!labels.empty()) {
                    if (has_prev) {
                        _ss << "{";
                    }
                    _ss << labels.to_string();
                    if (has_prev) {
                        _ss << "}";
                    }
                }
                _ss << " " << metric->to_string() << std::endl;
                break;
            }
            default:
                break;
            }
        }
    }
    std::string to_string() {
        return _ss.str();
    }
private:
    std::stringstream _ss;
};

TEST_F(DorisMetricsTest, Normal) {
    TestMetricsVisitor visitor;
    auto metrics = DorisMetrics::instance()->metrics();
    metrics->collect(&visitor);
    LOG(INFO) << "\n" << visitor.to_string();
    // check metric
    {
        DorisMetrics::instance()->fragment_requests_total.increment(12);
        auto metric = metrics->get_metric("fragment_requests_total");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("12", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->fragment_request_duration_us.increment(101);
        auto metric = metrics->get_metric("fragment_request_duration_us");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("101", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->http_requests_total.increment(102);
        auto metric = metrics->get_metric("http_requests_total");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("102", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->http_request_send_bytes.increment(104);
        auto metric = metrics->get_metric("http_request_send_bytes");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("104", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->query_scan_bytes.increment(104);
        auto metric = metrics->get_metric("query_scan_bytes");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("104", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->query_scan_rows.increment(105);
        auto metric = metrics->get_metric("query_scan_rows");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("105", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->push_requests_success_total.increment(106);
        auto metric = metrics->get_metric("push_requests_total",
                                          MetricLabels().add("status", "SUCCESS"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("106", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->push_requests_fail_total.increment(107);
        auto metric = metrics->get_metric("push_requests_total",
                                          MetricLabels().add("status", "FAIL"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("107", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->push_request_duration_us.increment(108);
        auto metric = metrics->get_metric("push_request_duration_us");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("108", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->push_request_write_bytes.increment(109);
        auto metric = metrics->get_metric("push_request_write_bytes");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("109", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->push_request_write_rows.increment(110);
        auto metric = metrics->get_metric("push_request_write_rows");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("110", metric->to_string().c_str());
    }
    // engine request
    {
        DorisMetrics::instance()->create_tablet_requests_total.increment(15);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "create_tablet")
                                          .add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("15", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->drop_tablet_requests_total.increment(16);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "drop_tablet")
                                          .add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("16", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->report_all_tablets_requests_total.increment(17);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "report_all_tablets")
                                          .add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("17", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->report_tablet_requests_total.increment(18);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "report_tablet")
                                          .add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("18", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->schema_change_requests_total.increment(19);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "schema_change")
                                          .add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("19", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->create_rollup_requests_total.increment(20);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "create_rollup")
                                          .add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("20", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->storage_migrate_requests_total.increment(21);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "storage_migrate")
                                          .add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("21", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->delete_requests_total.increment(22);
        auto metric = metrics->get_metric("engine_requests_total",
                                          MetricLabels().add("type", "delete")
                                          .add("status", "total"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("22", metric->to_string().c_str());
    }
    //  comapction
    {
        DorisMetrics::instance()->base_compaction_deltas_total.increment(30);
        auto metric = metrics->get_metric("compaction_deltas_total",
                                          MetricLabels().add("type", "base"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("30", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->cumulative_compaction_deltas_total.increment(31);
        auto metric = metrics->get_metric("compaction_deltas_total",
                                          MetricLabels().add("type", "cumulative"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("31", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->base_compaction_bytes_total.increment(32);
        auto metric = metrics->get_metric("compaction_bytes_total",
                                          MetricLabels().add("type", "base"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("32", metric->to_string().c_str());
    }
    {
        DorisMetrics::instance()->cumulative_compaction_bytes_total.increment(33);
        auto metric = metrics->get_metric("compaction_bytes_total",
                                          MetricLabels().add("type", "cumulative"));
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("33", metric->to_string().c_str());
    }
    // Gauge
    {
        DorisMetrics::instance()->memory_pool_bytes_total.increment(40);
        auto metric = metrics->get_metric("memory_pool_bytes_total");
        ASSERT_TRUE(metric != nullptr);
        ASSERT_STREQ("40", metric->to_string().c_str());
    }
}

}

int main(int argc, char** argv) {
#if 0
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
#endif
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
