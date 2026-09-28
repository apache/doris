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

#include "service/http/action/metrics_action.h"

#include <gtest/gtest.h>

#include <string>

#include "common/config.h"
#include "common/metrics/metrics.h"
#include "service/http/ev_http_server.h"
#include "service/http/http_client.h"
#include "service/http/http_method.h"

namespace doris {

class MetricsActionTest : public testing::Test {
public:
    MetricsActionTest() {}
    virtual ~MetricsActionTest() {}
    void SetUp() override {
        _old_enable_all_http_auth = config::enable_all_http_auth;
        config::enable_all_http_auth = false;
    }
    void TearDown() override { config::enable_all_http_auth = _old_enable_all_http_auth; }

protected:
    // Serve the registry with MetricsAction and return the body a client gets.
    static std::string fetch_metrics(MetricRegistry* metric_registry) {
        MetricsAction action(metric_registry, nullptr, TPrivilegeHier::GLOBAL,
                             TPrivilegeType::NONE);
        EvHttpServer server(0);
        server.register_handler(GET, "/metrics", &action);
        server.start();

        HttpClient client;
        auto url = "http://127.0.0.1:" + std::to_string(server.get_real_port()) + "/metrics";
        EXPECT_TRUE(client.init(url).ok());
        client.set_method(GET);
        std::string body;
        auto st = client.execute(&body);
        EXPECT_TRUE(st.ok()) << st.to_string();
        return body;
    }

private:
    bool _old_enable_all_http_auth = false;
};

TEST_F(MetricsActionTest, prometheus_output) {
    MetricRegistry metric_registry("test");
    std::shared_ptr<MetricEntity> entity =
            metric_registry.register_entity("metrics_action_test.prometheus_output");

    IntGauge* cpu_idle = nullptr;
    DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(cpu_idle, MetricUnit::PERCENT);
    INT_GAUGE_METRIC_REGISTER(entity, cpu_idle);

    IntCounter* put_requests_total = nullptr;
    DEFINE_COUNTER_METRIC_PROTOTYPE_5ARG(put_requests_total, MetricUnit::NOUNIT, "", requests_total,
                                         Labels({{"type", "put"}, {"path", "/sports"}}));
    INT_COUNTER_METRIC_REGISTER(entity, put_requests_total);

    cpu_idle->set_value(50);
    put_requests_total->increment(2345);

    EXPECT_EQ(
            "# TYPE test_cpu_idle gauge\n"
            "test_cpu_idle 50\n"
            "# TYPE test_requests_total counter\n"
            "test_requests_total{path=\"/sports\",type=\"put\"} 2345\n",
            fetch_metrics(&metric_registry));
}

} // namespace doris
