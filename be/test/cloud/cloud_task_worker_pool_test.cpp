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

#include "agent/task_worker_pool.h"
#include "cloud/cloud_storage_engine.h"
#include "olap/options.h"
#include "runtime/cluster_info.h"

namespace doris {

TEST(CloudTaskWorkerPoolTest, ReportTabletCallbackWithDebugPoint) {
    bool original_enable_debug_points = config::enable_debug_points;
    config::enable_debug_points = true;

    ExecEnv::GetInstance()->set_storage_engine(
            std::make_unique<CloudStorageEngine>(EngineOptions {}));

    ClusterInfo cluster_info;
    cluster_info.master_fe_addr.__set_port(9030);

    Defer defer {[] { ExecEnv::GetInstance()->set_storage_engine(nullptr); }};

    {
        // debug point report_tablet_callback.skip is enabled
        DebugPoints::instance()->add("WorkPoolCloudReportTablet.report_tablet_callback.skip");
        EXPECT_TRUE(DebugPoints::instance()->is_enable(
                "WorkPoolCloudReportTablet.report_tablet_callback.skip"));
        report_tablet_callback(ExecEnv::GetInstance()->storage_engine().to_cloud(), &cluster_info);
        EXPECT_EQ(DorisMetrics::instance()->report_all_tablets_requests_skip->value(), 1);
        EXPECT_EQ(DorisMetrics::instance()->tablet_report_continuous_failure_duration_s->value(), 0);
        // debug point report_tablet_callback.skip is removed
        DebugPoints::instance()->remove("WorkPoolCloudReportTablet.report_tablet_callback.skip");
        EXPECT_FALSE(DebugPoints::instance()->is_enable(
                "WorkPoolCloudReportTablet.report_tablet_callback.skip"));
    }

    {
        // debug point report.fail is enabled
        DebugPoints::instance()->add("MasterServerClient::report.fail");
        EXPECT_TRUE(DebugPoints::instance()->is_enable("MasterServerClient::report.fail"));
        report_tablet_callback(ExecEnv::GetInstance()->storage_engine().to_cloud(), &cluster_info);
        EXPECT_GT(DorisMetrics::instance()->tablet_report_continuous_failure_duration_s->value(), 0);
        std::this_thread::sleep_for(std::chrono::seconds(1));
        report_tablet_callback(ExecEnv::GetInstance()->storage_engine().to_cloud(), &cluster_info);
        EXPECT_GT(DorisMetrics::instance()->tablet_report_continuous_failure_duration_s->value(), 0);
        // debug point report.fail is removed
        DebugPoints::instance()->remove("MasterServerClient::report.fail");
        EXPECT_FALSE(DebugPoints::instance()->is_enable("MasterServerClient::report.fail"));
    }

    {
        report_tablet_callback(ExecEnv::GetInstance()->storage_engine().to_cloud(), &cluster_info);
        EXPECT_EQ(DorisMetrics::instance()->tablet_report_continuous_failure_duration_s->value(), 0);
    }

    config::enable_debug_points = original_enable_debug_points;
}

} // namespace doris
