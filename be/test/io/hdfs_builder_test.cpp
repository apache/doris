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

#include "io/hdfs_builder.h"

#include <gtest/gtest.h>

#include <map>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "cpp/sync_point.h"

namespace doris {
namespace {

constexpr char AUTO_RENEWAL_KEY[] = "hadoop.kerberos.keytab.login.autorenewal.enabled";

struct RenewalConfigCase {
    const char* name;
    const char* authentication;
    std::optional<std::string> configured_renewal;
    std::optional<std::string> expected_renewal;
};

class HdfsBuilderRenewalTest : public testing::TestWithParam<RenewalConfigCase> {
protected:
    void SetUp() override {
        auto* sync_point = SyncPoint::get_instance();
        sync_point->set_call_back(
                "HDFSCommonBuilder::set_hdfs_conf_to_hdfs_builder",
                [this](auto&& args) {
                    _emitted_config.emplace_back(try_any_cast<std::string>(args[0]),
                                                 try_any_cast<std::string>(args[1]));
                },
                &_callback_guard);
        sync_point->enable_processing();
    }

    void TearDown() override { SyncPoint::get_instance()->disable_processing(); }

    std::vector<std::pair<std::string, std::string>> _emitted_config;
    SyncPoint::CallbackGuard _callback_guard;
};

TEST_P(HdfsBuilderRenewalTest, EmitsResolvedRenewalOnce) {
    const auto& test_case = GetParam();
    HDFSCommonBuilder builder;
    ASSERT_TRUE(builder.init_hdfs_builder().ok());
    builder.set_hdfs_conf(HADOOP_SECURITY_AUTHENTICATION, test_case.authentication);
    builder.set_hdfs_conf("dfs.nameservices", "test-cluster");
    if (test_case.configured_renewal) {
        builder.set_hdfs_conf(AUTO_RENEWAL_KEY, *test_case.configured_renewal);
    }

    // Exercise the real libhdfs configuration boundary without connecting to a JVM or KDC.
    // libhdfs replays duplicate keys in reverse order, so there must be only one resolved value.
    builder.set_hdfs_conf_to_hdfs_builder();
    std::map<std::string, std::string> emitted;
    for (const auto& [key, value] : _emitted_config) {
        ASSERT_TRUE(emitted.emplace(key, value).second) << "Duplicate libhdfs setting: " << key;
    }
    std::map<std::string, std::string> expected {
            {HADOOP_SECURITY_AUTHENTICATION, test_case.authentication},
            {"dfs.nameservices", "test-cluster"}};
    if (test_case.expected_renewal) {
        expected.emplace(AUTO_RENEWAL_KEY, *test_case.expected_renewal);
    }
    EXPECT_EQ(emitted, expected);
}

INSTANTIATE_TEST_SUITE_P(
        HdfsConfiguration, HdfsBuilderRenewalTest,
        testing::Values(RenewalConfigCase {"KerberosDefault", "kerberos", std::nullopt, "true"},
                        RenewalConfigCase {"KerberosIncomingFalse", "kerberos", "false", "true"},
                        RenewalConfigCase {"KerberosIncomingTrue", "kerberos", "true", "true"},
                        RenewalConfigCase {"SimpleDefault", "simple", std::nullopt, std::nullopt},
                        RenewalConfigCase {"SimpleIncomingFalse", "simple", "false", "false"}),
        [](const testing::TestParamInfo<RenewalConfigCase>& info) { return info.param.name; });

} // namespace
} // namespace doris
