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

#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "cpp/sync_point.h"
// The builder is local to this translation unit. Exercise its native option emission
// without the identity setters and connect call, which would start a JVM and require a KDC.
#include "recycler/hdfs_accessor.cpp"

namespace doris::cloud {
namespace {

constexpr char AUTO_RENEWAL_KEY[] = "hadoop.kerberos.keytab.login.autorenewal.enabled";
constexpr char TICKET_CACHE_KEY[] = "hadoop.security.kerberos.ticket.cache.path";

struct RenewalConfigCase {
    const char* name;
    bool kerberos_login;
    bool ticket_cache;
    std::vector<std::string> incoming_renewal;
    std::vector<std::string> expected_renewal;
};

class CloudHdfsBuilderRenewalTest : public testing::TestWithParam<RenewalConfigCase> {
protected:
    void SetUp() override {
        original_ticket_cache_ = config::kerberos_ccache_path;
        config::kerberos_ccache_path =
                GetParam().ticket_cache ? "/tmp/cloud-renewal-test.ccache" : "";
        auto* sync_point = SyncPoint::get_instance();
        sync_point->set_call_back(
                "HDFSBuilder::set_hdfs_conf",
                [this](auto&& args) {
                    emitted_config_.emplace_back(try_any_cast<const char*>(args[0]),
                                                 try_any_cast<const char*>(args[1]));
                },
                &callback_guard_);
        sync_point->enable_processing();
    }

    void TearDown() override {
        SyncPoint::get_instance()->disable_processing();
        config::kerberos_ccache_path = original_ticket_cache_;
    }

    std::string original_ticket_cache_;
    std::vector<std::pair<std::string, std::string>> emitted_config_;
    SyncPoint::CallbackGuard callback_guard_;
};

TEST_P(CloudHdfsBuilderRenewalTest, ResolvesNativeRenewalOptions) {
    const auto& test_case = GetParam();
    HdfsBuildConf conf;
    conf.set_fs_name("hdfs://test-cluster");
    if (test_case.kerberos_login) {
        conf.set_hdfs_kerberos_principal("recycler@EXAMPLE.INVALID");
        if (!test_case.ticket_cache) {
            conf.set_hdfs_kerberos_keytab("/tmp/recycler.keytab");
        }
    } else {
        conf.set_user("recycler");
    }
    auto* nameservice = conf.add_hdfs_confs();
    nameservice->set_key("dfs.nameservices");
    nameservice->set_value("test-cluster");
    for (const auto& value : test_case.incoming_renewal) {
        auto* setting = conf.add_hdfs_confs();
        setting->set_key(AUTO_RENEWAL_KEY);
        setting->set_value(value);
    }

    HDFSBuilder builder;
    ASSERT_EQ(builder.init_hdfs_builder(), 0);
    builder.set_hdfs_confs(conf, test_case.kerberos_login);
    if (test_case.kerberos_login) {
        ASSERT_EQ(builder.check_krb_params(conf.hdfs_kerberos_principal(),
                                           conf.hdfs_kerberos_keytab()),
                  0);
    }

    std::vector<std::string> renewal_values;
    std::vector<std::string> ticket_cache_values;
    std::vector<std::string> nameservices;
    for (const auto& [key, value] : emitted_config_) {
        if (key == AUTO_RENEWAL_KEY) {
            renewal_values.push_back(value);
        } else if (key == TICKET_CACHE_KEY) {
            ticket_cache_values.push_back(value);
        } else if (key == "dfs.nameservices") {
            nameservices.push_back(value);
        }
    }
    // One native true setting cannot be overridden by libhdfs's reverse replay.
    EXPECT_EQ(renewal_values, test_case.expected_renewal);
    EXPECT_EQ(nameservices, std::vector<std::string> {"test-cluster"});
    EXPECT_EQ(ticket_cache_values, test_case.ticket_cache
                                           ? std::vector<std::string> {config::kerberos_ccache_path}
                                           : std::vector<std::string> {});
}

INSTANTIATE_TEST_SUITE_P(
        CloudHdfsConfiguration, CloudHdfsBuilderRenewalTest,
        testing::Values(
                RenewalConfigCase {"KeytabDefault", true, false, {}, {"true"}},
                RenewalConfigCase {"KeytabFalse", true, false, {"false"}, {"true"}},
                RenewalConfigCase {"KeytabTrue", true, false, {"true"}, {"true"}},
                RenewalConfigCase {
                        "KeytabDuplicates", true, false, {"false", "true", "false"}, {"true"}},
                RenewalConfigCase {"SimpleDefault", false, false, {}, {}},
                RenewalConfigCase {"SimpleFalse", false, false, {"false"}, {"false"}},
                RenewalConfigCase {"TicketCacheDefault", true, true, {}, {}},
                RenewalConfigCase {"TicketCacheFalse", true, true, {"false"}, {"false"}}),
        [](const testing::TestParamInfo<RenewalConfigCase>& info) { return info.param.name; });

} // namespace
} // namespace doris::cloud
