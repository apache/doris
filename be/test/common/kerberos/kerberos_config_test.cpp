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

#include "common/kerberos/kerberos_config.h"

#include <gtest/gtest.h>

namespace doris::kerberos {

class KerberosConfigTest : public testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(KerberosConfigTest, DefaultValues) {
    KerberosConfig config;
    EXPECT_EQ(config.get_principal(), "");
    EXPECT_EQ(config.get_keytab_path(), "");
    EXPECT_EQ(config.get_krb5_conf_path(), "");
    EXPECT_EQ(config.get_refresh_interval_second(), 3600);
    EXPECT_EQ(config.get_min_time_before_refresh_second(), 600);
}

TEST_F(KerberosConfigTest, SetterAndGetter) {
    KerberosConfig config;

    // Test principal and keytab
    config.set_principal_and_keytab("test_principal", "/path/to/keytab");
    EXPECT_EQ(config.get_principal(), "test_principal");
    EXPECT_EQ(config.get_keytab_path(), "/path/to/keytab");

    // Test krb5 conf path
    config.set_krb5_conf_path("/etc/krb5.conf");
    EXPECT_EQ(config.get_krb5_conf_path(), "/etc/krb5.conf");

    // Test refresh interval
    config.set_refresh_interval(500);
    EXPECT_EQ(config.get_refresh_interval_second(), 500);

    // Test min time before refresh
    config.set_min_time_before_refresh(800);
    EXPECT_EQ(config.get_min_time_before_refresh_second(), 800);
}

} // namespace doris::kerberos
