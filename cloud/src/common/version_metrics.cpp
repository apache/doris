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

#include "common/version_metrics.h"

#include <bvar/bvar.h>
#include <bvar/multi_dimension.h>
#include <gen_cpp/cloud_version.h>
#include <gflags/gflags.h>

#include <cstdint>
#include <cstdlib>
#include <list>
#include <sstream>
#include <string>

#include "common/config.h"

namespace doris::cloud {
namespace {

uint64_t get_doris_cloud_version_metric_value() {
    std::stringstream ss;
    ss << DORIS_CLOUD_BUILD_VERSION_MAJOR << 0 << DORIS_CLOUD_BUILD_VERSION_MINOR << 0
       << DORIS_CLOUD_BUILD_VERSION_PATCH;
    if (DORIS_CLOUD_BUILD_VERSION_HOTFIX > 0) {
        ss << 0 << DORIS_CLOUD_BUILD_VERSION_HOTFIX;
    }
    return std::strtoul(ss.str().c_str(), nullptr, 10);
}

} // namespace

void init_doris_cloud_version_metrics() {
    // MultiDimension metrics are omitted from /brpc_metrics while this brpc flag is 0.
    CHECK(!google::SetCommandLineOption("bvar_max_dump_multi_dimension_metric_number",
                                        config::bvar_max_dump_multi_dimension_metric_num.c_str())
                   .empty());

    static const bool initialized = [] {
        // Keep the metric name role-neutral because one doris_cloud process can run meta-service,
        // recycler, or both. Runtime roles should be represented by scrape target labels.
        static bvar::MultiDimension<bvar::Status<uint64_t>> metrics(
                "doris_cloud_version",
                {"version", "major", "minor", "patch", "hotfix", "short_hash"});
        auto* metric = metrics.get_stats(std::list<std::string> {
                DORIS_CLOUD_BUILD_VERSION, std::to_string(DORIS_CLOUD_BUILD_VERSION_MAJOR),
                std::to_string(DORIS_CLOUD_BUILD_VERSION_MINOR),
                std::to_string(DORIS_CLOUD_BUILD_VERSION_PATCH),
                std::to_string(DORIS_CLOUD_BUILD_VERSION_HOTFIX), DORIS_CLOUD_BUILD_SHORT_HASH});
        CHECK(metric != nullptr);
        metric->set_value(get_doris_cloud_version_metric_value());
        return true;
    }();
    static_cast<void>(initialized);
}

} // namespace doris::cloud
