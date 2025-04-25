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

#include <filesystem>
#include <string>

#include "common/kerberos/kerberos_config.h"
#include "common/kerberos/kerberos_ticket_cache.h"

namespace doris::kerberos {

class KerberosTicketCacheAuthTest : public testing::Test {
public:
    void SetUp() override {
        // Get Kerberos configuration from environment variables
        const char* krb5_conf = std::getenv("KRB5_CONFIG");
        const char* principal = std::getenv("KRB5_TEST_PRINCIPAL");
        const char* keytab = std::getenv("KRB5_TEST_KEYTAB");

        // Skip the test if any required environment variable is not set
        if (!krb5_conf || !principal || !keytab) {
            GTEST_SKIP() << "Skipping test because one or more required environment variables are "
                            "not set:\n"
                         << "  KRB5_CONFIG=" << (krb5_conf ? krb5_conf : "not set") << "\n"
                         << "  KRB5_TEST_PRINCIPAL=" << (principal ? principal : "not set") << "\n"
                         << "  KRB5_TEST_KEYTAB=" << (keytab ? keytab : "not set");
            return;
        }

        // Skip if the required files don't exist
        if (!std::filesystem::exists(krb5_conf) || !std::filesystem::exists(keytab)) {
            GTEST_SKIP() << "Skipping test because required files don't exist:\n"
                         << "  krb5.conf: "
                         << (std::filesystem::exists(krb5_conf) ? "exists" : "not found") << "\n"
                         << "  keytab: "
                         << (std::filesystem::exists(keytab) ? "exists" : "not found");
            return;
        }

        // Initialize test configuration
        _config.set_krb5_conf_path(krb5_conf);
        _config.set_principal_and_keytab(principal, keytab);
        _config.set_refresh_interval(300);        // 5 minutes
        _config.set_min_time_before_refresh(600); // 10 minutes

        // Create temporary directory for ticket cache
        _temp_dir = std::filesystem::temp_directory_path() / "doris_krb5_test";
        if (std::filesystem::exists(_temp_dir)) {
            std::filesystem::remove_all(_temp_dir);
        }
        std::filesystem::create_directories(_temp_dir);
    }

    void TearDown() override {
        if (std::filesystem::exists(_temp_dir)) {
            std::filesystem::remove_all(_temp_dir);
        }
    }

protected:
    KerberosConfig _config;
    std::filesystem::path _temp_dir;
};

// Test real Kerberos authentication
// How to run:
// KRB5_CONFIG=/to/krb5.conf \
// KRB5_TEST_PRINCIPAL=your_principal \
// KRB5_TEST_KEYTAB=/to/hdfs.keytab \
// sh run-be-ut.sh --run --filter=KerberosTicketCacheAuthTest.*
TEST_F(KerberosTicketCacheAuthTest, TestRealAuthentication) {
    // If SetUp() skipped the test, we should skip here too
    if (testing::Test::HasFatalFailure()) {
        return;
    }

    std::cout << "Starting real Kerberos authentication test with:" << std::endl;
    std::cout << "  krb5.conf: " << _config.get_krb5_conf_path() << std::endl;
    std::cout << "  principal: " << _config.get_principal() << std::endl;
    std::cout << "  keytab: " << _config.get_keytab_path() << std::endl;

    std::string cache_path;
    {
        // Create ticket cache instance
        KerberosTicketCache ticket_cache(_config, _temp_dir.string());

        // Initialize the ticket cache
        Status status = ticket_cache.initialize();
        ASSERT_TRUE(status.ok()) << "Failed to initialize ticket cache: " << status.to_string();

        // Perform Kerberos login
        status = ticket_cache.login();
        ASSERT_TRUE(status.ok()) << "Failed to perform Kerberos login: " << status.to_string();

        // Write ticket to cache file
        status = ticket_cache.write_ticket_cache();
        ASSERT_TRUE(status.ok()) << "Failed to write ticket cache: " << status.to_string();

        // Verify that the cache file exists
        ASSERT_TRUE(std::filesystem::exists(ticket_cache.get_ticket_cache_path()))
                << "Ticket cache file not found at: " << ticket_cache.get_ticket_cache_path();
        cache_path = ticket_cache.get_ticket_cache_path();

        // Try to login with the cache
        status = ticket_cache.login_with_cache();
        ASSERT_TRUE(status.ok()) << "Failed to login with cache: " << status.to_string();

        std::cout << "Successfully authenticated and created ticket cache at: "
                  << ticket_cache.get_ticket_cache_path() << std::endl;
    }

    // After ticket_cache delete, the cache file should be deleted too
    ASSERT_FALSE(std::filesystem::exists(cache_path)) << cache_path;
}

} // namespace doris::kerberos
