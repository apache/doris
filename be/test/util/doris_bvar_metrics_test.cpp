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

#include "util/doris_bvar_metrics.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include "gtest/gtest_pred_impl.h"

namespace doris {

class DorisBvarMetricsTest : public testing::Test {
public:
    DorisBvarMetricsTest() {}
    virtual ~DorisBvarMetricsTest() = default;
};

TEST_F(DorisBvarMetricsTest, Normal) {
    // check metric
    {
        DorisBvarMetrics::instance()->fragment_requests_total->reset();
        DorisBvarMetrics::instance()->fragment_requests_total->increment(12);
        EXPECT_STREQ(
                "12",
                DorisBvarMetrics::instance()->fragment_requests_total->value_string()().c_str());
    }
    {
        DorisBvarMetrics::instance()->fragment_request_duration_us->reset();
        DorisBvarMetrics::instance()->fragment_request_duration_us->increment(101);
        EXPECT_STREQ("101", DorisBvarMetrics::instance()
                                    ->fragment_request_duration_us->value_string()()
                                    .c_str());
    }
    {
        DorisBvarMetrics::instance()->query_scan_bytes->increment(104);
        EXPECT_STREQ("104",
                     DorisBvarMetrics::instance()->query_scan_bytes->value_string()().c_str());
    }
    {
        DorisBvarMetrics::instance()->query_scan_rows->increment(105);
        EXPECT_STREQ("105",
                     DorisBvarMetrics::instance()->query_scan_rows->value_string()().c_str());
    }
    {
        DorisBvarMetrics::instance()->push_requests_success_total->increment(106);
        EXPECT_STREQ("106", DorisBvarMetrics::instance()
                                    ->push_requests_success_total->value_string()()
                                    .c_str());
    }
    {
        DorisBvarMetrics::instance()->push_requests_fail_total->increment(107);
        EXPECT_STREQ(
                "107",
                DorisBvarMetrics::instance()->push_requests_fail_total->value_string()().c_str());
    }
    {
        DorisBvarMetrics::instance()->push_request_duration_us->increment(108);
        EXPECT_STREQ(
                "108",
                DorisBvarMetrics::instance()->push_request_duration_us->value_string()().c_str());
    }
    {
        DorisBvarMetrics::instance()->push_request_write_bytes->increment(109);
        EXPECT_STREQ(
                "109",
                DorisBvarMetrics::instance()->push_request_write_bytes->value_string()().c_str());
    }
    {
        DorisBvarMetrics::instance()->push_request_write_rows->increment(110);
        EXPECT_STREQ(
                "110",
                DorisBvarMetrics::instance()->push_request_write_rows->value_string()().c_str());
    }
    // engine request
    {
        DorisBvarMetrics::instance()->create_tablet_requests_total->reset();
        DorisBvarMetrics::instance()->create_tablet_requests_total->increment(15);
        EXPECT_STREQ("15", DorisBvarMetrics::instance()
                                   ->create_tablet_requests_total->value_string()()
                                   .c_str());
    }
    {
        DorisBvarMetrics::instance()->drop_tablet_requests_total->reset();
        DorisBvarMetrics::instance()->drop_tablet_requests_total->increment(16);
        EXPECT_STREQ(
                "16",
                DorisBvarMetrics::instance()->drop_tablet_requests_total->value_string()().c_str());
    }
    {
        DorisBvarMetrics::instance()->report_all_tablets_requests_skip->increment(1);
        EXPECT_STREQ("1", DorisBvarMetrics::instance()
                                  ->report_all_tablets_requests_skip->value_string()()
                                  .c_str());
    }
    {
        DorisBvarMetrics::instance()->schema_change_requests_total->increment(19);
        EXPECT_STREQ("19", DorisBvarMetrics::instance()
                                   ->schema_change_requests_total->value_string()()
                                   .c_str());
    }
    {
        DorisBvarMetrics::instance()->create_rollup_requests_total->increment(20);
        EXPECT_STREQ("20", DorisBvarMetrics::instance()
                                   ->create_rollup_requests_total->value_string()()
                                   .c_str());
    }
    {
        DorisBvarMetrics::instance()->storage_migrate_requests_total->reset();
        DorisBvarMetrics::instance()->storage_migrate_requests_total->increment(21);
        EXPECT_STREQ("21", DorisBvarMetrics::instance()
                                   ->storage_migrate_requests_total->value_string()()
                                   .c_str());
    }
    {
        DorisBvarMetrics::instance()->delete_requests_total->increment(22);
        EXPECT_STREQ("22",
                     DorisBvarMetrics::instance()->delete_requests_total->value_string()().c_str());
    }
    //  compaction
    {
        DorisBvarMetrics::instance()->base_compaction_deltas_total->increment(30);
        EXPECT_STREQ("30", DorisBvarMetrics::instance()
                                   ->base_compaction_deltas_total->value_string()()
                                   .c_str());
    }
    {
        DorisBvarMetrics::instance()->cumulative_compaction_deltas_total->increment(31);
        EXPECT_STREQ("31", DorisBvarMetrics::instance()
                                   ->cumulative_compaction_deltas_total->value_string()()
                                   .c_str());
    }
    {
        DorisBvarMetrics::instance()->base_compaction_bytes_total->increment(32);
        EXPECT_STREQ("32", DorisBvarMetrics::instance()
                                   ->base_compaction_bytes_total->value_string()()
                                   .c_str());
    }
    {
        DorisBvarMetrics::instance()->cumulative_compaction_bytes_total->increment(33);
        EXPECT_STREQ("33", DorisBvarMetrics::instance()
                                   ->cumulative_compaction_bytes_total->value_string()()
                                   .c_str());
    }
    // Gauge
    {
        DorisBvarMetrics::instance()->memory_pool_bytes_total->increment(40);
        EXPECT_STREQ(
                "40",
                DorisBvarMetrics::instance()->memory_pool_bytes_total->value_string()().c_str());
    }
}

} // namespace doris
