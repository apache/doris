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

#include "util/debug_util.h"

#include <butil/iobuf.h>
#include <butil/strings/string_piece.h>

// brpc's header uses the butil types above without including their definitions.
#include <brpc/builtin/prometheus_metrics_service.h>

#include <cstdint>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/version_internal.h"
#include "cpp/version_metrics.h"

namespace doris {

TEST(DebugUtilTest, BeVersionMetricIsExportedWithBuildLabels) {
    EXPECT_EQ(26000100ULL, encode_version_metric_value(26, 0, 1, 0));
    EXPECT_EQ(26000101ULL, encode_version_metric_value(26, 0, 1, 1));
    EXPECT_LT(encode_version_metric_value(4, 99, 0, 0), encode_version_metric_value(5, 0, 0, 0));
    EXPECT_FALSE(is_valid_version_metric_component(100));

    config::bvar_max_dump_multi_dimension_metric_num = "5000";
    init_be_version_metrics();

    butil::IOBuf output;
    ASSERT_EQ(0, brpc::DumpPrometheusMetricsToIOBuf(&output));
    const std::string body = output.to_string();

    const uint64_t value = encode_version_metric_value(
            version::doris_build_version_major(), version::doris_build_version_minor(),
            version::doris_build_version_patch(), version::doris_build_version_hotfix());

    std::stringstream sample;
    sample << "doris_be_version{version=\"" << version::doris_build_version() << "\",major=\""
           << version::doris_build_version_major() << "\",minor=\""
           << version::doris_build_version_minor() << "\",patch=\""
           << version::doris_build_version_patch() << "\",hotfix=\""
           << version::doris_build_version_hotfix() << "\",short_hash=\""
           << version::doris_build_short_hash() << "\"} " << value;

    EXPECT_NE(std::string::npos, body.find("# TYPE doris_be_version gauge")) << body;
    EXPECT_NE(std::string::npos, body.find(sample.str())) << body;
}

} // namespace doris
