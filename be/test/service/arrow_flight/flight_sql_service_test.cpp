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

#include <absl/strings/str_join.h>
#include <gtest/gtest.h>

// Service sources are excluded from unity batches in service/CMakeLists.txt.
#include "service/arrow_flight/flight_sql_service.cpp"

namespace doris::flight {
namespace {
// Keep this complete vector identical to FE FlightTicketCodecTest in
// fe/fe-core/src/test/java/org/apache/doris/arrowflight/FlightTicketCodecTest.java.
constexpr char kToken[] = "flight-test-token";
constexpr int32_t kClusterId = 42;
constexpr int64_t kNowMs = 1700000000000;
constexpr int64_t kLifetimeMs = 300000;
const std::string kUnsignedTicket = "0000000000000001-0000000000000002&127.0.0.1&8060&c2VsZWN0IDE=";
const std::string kSignedTicket =
        "0000000000000001-0000000000000002&127.0.0.1&8060&c2VsZWN0IDE="
        "&v1&42&1700000000000&1700000300000&"
        "4t0KqCaYZ4VyWvng0cNCURonABuOxYtJZ/HMWdpbTXc=";

Status verify(const std::string& ticket, const std::vector<std::string>& tokens = {kToken},
              int32_t cluster_id = kClusterId, int64_t now_ms = kNowMs) {
    std::vector<std::string> fields = absl::StrSplit(ticket, '&');
    return verify_flight_ticket(ticket, fields, tokens, cluster_id, now_ms);
}

TEST(FlightTicketTest, ProducesSameSignedStringAsFe) {
    const auto payload = kUnsignedTicket + "&v1&" + std::to_string(kClusterId) + "&" +
                         std::to_string(kNowMs) + "&" + std::to_string(kNowMs + kLifetimeMs);
    std::string key;
    ASSERT_TRUE(hmac_sha256(kToken, kKeyDomain, &key).ok());
    std::string message(kPayloadDomain, sizeof(kPayloadDomain));
    message.append(payload);
    std::string mac;
    ASSERT_TRUE(hmac_sha256(key, message, &mac).ok());
    std::string encoded;
    base64_encode(mac, &encoded);
    EXPECT_EQ(kSignedTicket, payload + "&" + encoded);
    EXPECT_TRUE(verify(kSignedTicket).ok());
}

TEST(FlightTicketTest, AcceptsFeVectorAndRetainedKeys) {
    EXPECT_TRUE(verify(kSignedTicket).ok());
    EXPECT_TRUE(
            verify(kSignedTicket, {"", "older-token", "flight-test-token", "newest-token"}).ok());
    EXPECT_FALSE(verify(kSignedTicket, {"newest-token"}).ok());
    EXPECT_FALSE(verify(kSignedTicket, {}).ok());
    EXPECT_FALSE(verify(kSignedTicket, {""}).ok());
}

TEST(FlightTicketTest, RejectsTamperingInEveryField) {
    const std::vector<std::string> replacements = {"0000000000000003-0000000000000002",
                                                   "127.0.0.2",
                                                   "8061",
                                                   "c2VsZWN0IDI=",
                                                   "v2",
                                                   "43",
                                                   "1700000000001",
                                                   "1700000300001",
                                                   "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="};
    for (size_t i = 0; i < replacements.size(); ++i) {
        SCOPED_TRACE(i);
        std::vector<std::string> fields = absl::StrSplit(kSignedTicket, '&');
        fields[i] = replacements[i];
        EXPECT_FALSE(verify(absl::StrJoin(fields, "&")).ok());
    }
}

TEST(FlightTicketTest, ChecksExpiryClockSkewAndCluster) {
    EXPECT_TRUE(verify(kSignedTicket, {"flight-test-token"}, 42, kNowMs - 30000).ok());
    EXPECT_TRUE(verify(kSignedTicket, {"flight-test-token"}, 42, kNowMs + 330000).ok());
    EXPECT_FALSE(verify(kSignedTicket, {"flight-test-token"}, 42, kNowMs - 30001).ok());
    EXPECT_FALSE(verify(kSignedTicket, {"flight-test-token"}, 42, kNowMs + 330001).ok());
    EXPECT_FALSE(verify(kSignedTicket, {"flight-test-token"}, 43).ok());
}

TEST(FlightTicketTest, RejectsUnsignedAndMalformedCredentials) {
    for (const auto& ticket : {std::string(), kUnsignedTicket, kSignedTicket + "&extra",
                               kSignedTicket.substr(0, kSignedTicket.rfind('&'))}) {
        EXPECT_FALSE(verify(ticket).ok());
    }
    for (const auto& value : {"", "1x", "-1", "9223372036854775808"}) {
        std::vector<std::string> fields = absl::StrSplit(kSignedTicket, '&');
        fields[6] = value;
        EXPECT_FALSE(verify(absl::StrJoin(fields, "&")).ok());
    }
    std::vector<std::string> fields = absl::StrSplit(kSignedTicket, '&');
    fields[8] = std::string(44, '!');
    EXPECT_FALSE(verify(absl::StrJoin(fields, "&")).ok());
}

class FlightTicketCompatibilityTest : public testing::Test {
protected:
    void SetUp() override {
        _previous_verify = config::enable_arrow_flight_ticket_signature_check;
        config::enable_arrow_flight_ticket_signature_check = true;
        _previous_cluster = ExecEnv::GetInstance()->cluster_info();
        ExecEnv::GetInstance()->set_cluster_info(&_cluster);
        _cluster.update_auth_tokens({"flight-test-token"}, 42);
    }
    void TearDown() override {
        ExecEnv::GetInstance()->set_cluster_info(_previous_cluster);
        config::enable_arrow_flight_ticket_signature_check = _previous_verify;
    }

    std::string sign_ticket(const std::string& ticket) {
        const auto now = UnixMillis();
        const auto payload =
                ticket + "&v1&42&" + std::to_string(now) + "&" + std::to_string(now + 300000);
        std::string key;
        EXPECT_TRUE(hmac_sha256("flight-test-token", kKeyDomain, &key).ok());
        std::string message(kPayloadDomain, sizeof(kPayloadDomain));
        message.append(payload);
        std::string mac;
        EXPECT_TRUE(hmac_sha256(key, message, &mac).ok());
        std::string encoded;
        base64_encode(mac, &encoded);
        return payload + "&" + encoded;
    }

    bool _previous_verify = false;
    ClusterInfo _cluster;
    ClusterInfo* _previous_cluster = nullptr;
};

TEST_F(FlightTicketCompatibilityTest, PreservesOriginalDecodingWhenVerificationDisabled) {
    config::enable_arrow_flight_ticket_signature_check = false;
    FlightSqlServer::Impl service;
    for (const auto& ticket : {kUnsignedTicket, sign_ticket(kUnsignedTicket)}) {
        auto result = service.decode_ticket(ticket);
        ASSERT_TRUE(result.ok()) << result.status();
        const auto& statement = *result;
        EXPECT_EQ(1, statement->query_id.hi);
        EXPECT_EQ(2, statement->query_id.lo);
        EXPECT_EQ("127.0.0.1", statement->result_addr.hostname);
        EXPECT_EQ(8060, statement->result_addr.port);
        EXPECT_EQ("select 1", statement->sql);
    }
    std::string sql(5000, 'a');
    std::string encoded;
    base64_encode(sql, &encoded);
    auto result = service.decode_ticket("1-2&::1&8060&" + encoded);
    ASSERT_TRUE(result.ok()) << result.status();
    EXPECT_EQ(sql, (*result)->sql);
}

TEST_F(FlightTicketCompatibilityTest, RejectsRemovedSignatureSuffix) {
    FlightSqlServer::Impl service;
    const auto signed_ticket = sign_ticket(kUnsignedTicket);
    ASSERT_TRUE(service.decode_ticket(signed_ticket).ok());
    // Removing all authentication fields must not turn an authenticated request into a legacy one.
    EXPECT_FALSE(service.decode_ticket(signed_ticket.substr(0, kUnsignedTicket.size())).ok());
    EXPECT_FALSE(service.decode_ticket(kUnsignedTicket).ok());
}

TEST_F(FlightTicketCompatibilityTest, RejectsInvalidSignedTickets) {
    FlightSqlServer::Impl service;
    const auto signed_ticket = sign_ticket(kUnsignedTicket);
    ASSERT_TRUE(service.decode_ticket(signed_ticket).ok());
    auto tampered = signed_ticket;
    tampered[0] = '1';
    EXPECT_FALSE(service.decode_ticket(tampered).ok());
    EXPECT_FALSE(service.decode_ticket(signed_ticket.substr(0, signed_ticket.rfind('&') + 1)).ok());
    EXPECT_FALSE(service.decode_ticket(signed_ticket + "&extra").ok());
    EXPECT_FALSE(service.decode_ticket(kSignedTicket).ok()); // Expired fixed vector.

    _cluster.update_auth_tokens({}, 42);
    EXPECT_FALSE(service.decode_ticket(signed_ticket).ok());
    EXPECT_FALSE(service.decode_ticket(kUnsignedTicket).ok());
}
} // namespace
} // namespace doris::flight
