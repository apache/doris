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

#include "load/routine_load/kafka_properties.h"

#include <gen_cpp/BackendService_constants.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <librdkafka/rdkafkacpp.h>

#include <filesystem>
#include <memory>

#include "common/kerberos/kerberos_ticket_mgr.h"
#include "common/status.h"
#include "load/routine_load/data_consumer.h"

namespace doris {

TEST(KafkaPropertiesTest, RejectExecutableAndUnknownProperties) {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    for (const auto* key :
         {"sasl.kerberos.kinit.cmd", "sasl.kerberos.ticket.cache",
          "sasl.kerberos.min.time.before.relogin", "plugin.library.paths", "ssl.engine.location",
          "ssl.engine.id", "ssl.providers", "unknown.future.property", "sasl.jaas.config"}) {
        auto status = configure_kafka_security(*conf, {{key, "sensitive-value"}});
        EXPECT_FALSE(status.ok()) << key;
        EXPECT_NE(status.to_string().find("is not allowed in Routine Load"), std::string::npos);
        EXPECT_EQ(status.to_string().find("sensitive-value"), std::string::npos);
    }
}

TEST(KafkaPropertiesTest, RejectKerberosRefreshReenable) {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    for (const auto* value : {"1", "60000", "00", "FILE:123:checksum"}) {
        EXPECT_FALSE(
                configure_kafka_security(*conf, {{"sasl.kerberos.min.time.before.relogin", value}})
                        .ok());
    }
}

TEST(KafkaPropertiesTest, RequireManagedFiles) {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    for (const auto& key : g_BackendService_constants.KAFKA_FILE_PROPERTIES) {
        for (const auto* value : {"/etc/ssl/private/key.pem", "relative.pem", "", "FILE:"}) {
            EXPECT_FALSE(configure_kafka_security(*conf, {{key, value}}).ok()) << key;
        }
        EXPECT_TRUE(configure_kafka_security(*conf, {{key, "FILE:123:checksum"},
                                                     {"sasl.kerberos.principal", "user@REALM"},
                                                     {"security.protocol", "SASL_SSL"}})
                            .ok())
                << key;
    }
}

TEST(KafkaPropertiesTest, DisableDefaultKerberosCommand) {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    ASSERT_TRUE(configure_kafka_security(*conf, {}).ok());
    std::string value;
    ASSERT_EQ(RdKafka::Conf::CONF_OK, conf->get("sasl.kerberos.min.time.before.relogin", value));
    EXPECT_EQ("0", value);
    // Principal values can contain shell metacharacters. They remain data because
    // neither the initial kinit command nor its renewal timer can run.
    ASSERT_TRUE(configure_kafka_security(*conf, {{"sasl.kerberos.principal", "user;unused"},
                                                 {"security.protocol", "SASL_PLAINTEXT"},
                                                 {"sasl.mechanisms", "GSSAPI"}})
                        .ok());
    ASSERT_EQ(RdKafka::Conf::CONF_OK, conf->get("sasl.kerberos.min.time.before.relogin", value));
    EXPECT_EQ("0", value);
}

TEST(KafkaPropertiesTest, AcceptConsumerAndAwsProperties) {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    EXPECT_TRUE(configure_kafka_security(*conf, {{"group.id", "test-group"},
                                                 {"sasl.kerberos.min.time.before.relogin", "0"},
                                                 {"isolation.level", "read_committed"},
                                                 {"fetch.max.bytes", "1048576"},
                                                 {"security.protocol", "SASL_SSL"},
                                                 {"sasl.mechanism", "OAUTHBEARER"},
                                                 {"aws.region", "us-east-1"}})
                        .ok());
}

TEST(KafkaPropertiesTest, ConsumerRejectsBeforeResolvingFilesOrCreatingClient) {
    auto ctx = std::make_shared<StreamLoadContext>(nullptr);
    TKafkaLoadInfo info;
    info.__set_properties(
            {{"https.ca.location", "FILE:123:checksum"}, {"plugin.library.paths", "unused.so"}});
    ctx->kafka_info = std::make_unique<KafkaLoadInfo>(info);
    KafkaDataConsumer consumer(ctx);
    // No ExecEnv or Kafka broker: rejection must precede FILE resolution and
    // client creation even when an allowed property is present in the same map.
    auto status = consumer.init(ctx);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("plugin.library.paths"), std::string::npos);
}

TEST(KafkaPropertiesTest, KeytabRequiresExplicitIdentityAndGssapi) {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    std::map<std::string, std::string> properties = {{"sasl.kerberos.keytab", "FILE:123:checksum"}};
    EXPECT_FALSE(configure_kafka_security(*conf, properties).ok());
    properties["sasl.kerberos.principal"] = "user@REALM";
    EXPECT_FALSE(configure_kafka_security(*conf, properties).ok());
    properties["security.protocol"] = "SASL_SSL";
    EXPECT_TRUE(configure_kafka_security(*conf, properties).ok());
    for (const auto* mechanism : {"sasl.mechanism", "sasl.mechanisms"}) {
        properties[mechanism] = "PLAIN";
        EXPECT_FALSE(configure_kafka_security(*conf, properties).ok());
        properties[mechanism] = "GSSAPI";
        EXPECT_TRUE(configure_kafka_security(*conf, properties).ok());
    }
    properties["sasl.kerberos.principal"] = std::string("user\0suffix", 11);
    EXPECT_FALSE(configure_kafka_security(*conf, properties).ok());
}

namespace {
class KafkaMockTicketCache : public kerberos::KerberosTicketCache {
public:
    using KerberosTicketCache::KerberosTicketCache;
    MOCK_METHOD(Status, initialize, (), (override));
    MOCK_METHOD(Status, login, (), (override));
    MOCK_METHOD(Status, write_ticket_cache, (), (override));
    MOCK_METHOD(void, start_periodic_refresh, (), (override));
};

class KafkaMockTicketMgr : public kerberos::KerberosTicketMgr {
public:
    KafkaMockTicketMgr()
            : KerberosTicketMgr((std::filesystem::temp_directory_path() /
                                 ("kafka-kerberos-" + UniqueId::gen_uid().to_string()))
                                        .string()) {}
    bool fail_login = false;

protected:
    std::shared_ptr<kerberos::KerberosTicketCache> _make_new_ticket_cache(
            const kerberos::KerberosConfig& config) override {
        auto cache =
                std::make_shared<testing::StrictMock<KafkaMockTicketCache>>(config, _root_path);
        cache->set_ticket_cache_path(_root_path + "/" + config.get_hash_code());
        testing::InSequence sequence;
        EXPECT_CALL(*cache, initialize()).WillOnce(testing::Return(Status::OK()));
        if (fail_login) {
            EXPECT_CALL(*cache, login())
                    .WillOnce(testing::Return(Status::InternalError("KDC unavailable")));
        } else {
            EXPECT_CALL(*cache, login()).WillOnce(testing::Return(Status::OK()));
            EXPECT_CALL(*cache, write_ticket_cache()).WillOnce(testing::Return(Status::OK()));
            EXPECT_CALL(*cache, start_periodic_refresh()).Times(1);
        }
        return cache;
    }
};
} // namespace

TEST(KafkaPropertiesTest, ManagedLoginStartsRefreshInDefaultCacheMode) {
    KafkaMockTicketMgr manager;
    std::shared_ptr<kerberos::KerberosTicketCache> cache, shared;
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    ASSERT_TRUE(configure_kafka_security(*conf, {}).ok());
    ASSERT_TRUE(configure_kafka_kerberos(*conf, "alice@REALM", "/managed/1.keytab", manager, &cache)
                        .ok());
    ASSERT_TRUE(cache->get_config().use_default_cache());
    EXPECT_EQ(cache, manager.get_ticket_cache("alice@REALM", "/managed/1.keytab", true));
    EXPECT_EQ(nullptr, manager.get_ticket_cache("alice@REALM", "/managed/1.keytab"));
    std::string principal, interval;
    ASSERT_EQ(RdKafka::Conf::CONF_OK, conf->get("sasl.kerberos.principal", principal));
    EXPECT_EQ("alice@REALM", principal);
    ASSERT_EQ(RdKafka::Conf::CONF_OK, conf->get("sasl.kerberos.min.time.before.relogin", interval));
    EXPECT_EQ("0", interval);
    // Only upstream librdkafka properties are needed; the same identity reuses
    // its native refresh worker without creating a private Kafka cache binding.
    ASSERT_TRUE(
            configure_kafka_kerberos(*conf, "alice@REALM", "/managed/1.keytab", manager, &shared)
                    .ok());
    EXPECT_EQ(cache, shared);
}

TEST(KafkaPropertiesTest, FailedManagedLoginDoesNotPublishCache) {
    KafkaMockTicketMgr manager;
    manager.fail_login = true;
    std::shared_ptr<kerberos::KerberosTicketCache> cache;
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    auto status =
            configure_kafka_kerberos(*conf, "user@REALM", "/managed/1.keytab", manager, &cache);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("KDC unavailable"), std::string::npos);
    EXPECT_EQ(nullptr, cache);
    EXPECT_EQ(nullptr, manager.get_ticket_cache("user@REALM", "/managed/1.keytab", true));
}

} // namespace doris
