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
#include <gtest/gtest.h>
#include <librdkafka/rdkafkacpp.h>

#include <memory>

#include "common/status.h"
#include "load/routine_load/data_consumer.h"

namespace doris {

TEST(KafkaPropertiesTest, RejectExecutableAndUnknownProperties) {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    for (const auto* key :
         {"sasl.kerberos.kinit.cmd", "sasl.kerberos.keytab",
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

TEST(KafkaPropertiesTest, RequireManagedCertificateFiles) {
    std::unique_ptr<RdKafka::Conf> conf(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
    for (const auto& key : g_BackendService_constants.KAFKA_FILE_PROPERTIES) {
        for (const auto* value : {"/etc/ssl/private/key.pem", "relative.pem", "", "FILE:"}) {
            EXPECT_FALSE(configure_kafka_security(*conf, {{key, value}}).ok()) << key;
        }
        EXPECT_TRUE(configure_kafka_security(*conf, {{key, "FILE:123:checksum"}}).ok()) << key;
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

} // namespace doris
