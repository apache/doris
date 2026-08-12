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

#include "meta-service/meta_service_helper.h"

#include <google/protobuf/descriptor.pb.h>
#include <google/protobuf/dynamic_message.h>
#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <optional>
#include <set>
#include <string>
#include <string_view>

#include "common/config.h"
#include "meta-service/meta_service_rate_limit_helper.h"

namespace doris::cloud {
namespace internal {
int parse_cpuset_cpu_count(std::string_view cpuset_line);
std::optional<double> parse_cgroup_v2_cpu_limit(std::string_view cpu_max_line);
std::optional<double> parse_cgroup_v1_cpu_limit(int64_t quota_us, int64_t period_us);
int64_t calculate_usage_percent(int64_t usage_bytes, int64_t limit_bytes);
int64_t calculate_cpu_usage_percent(double delta_cpu_ns, double delta_wall_ns, double cpu_limit);
} // namespace internal

namespace {
// IMPORTANT: Never-Never-Never add new codes to this snapshot. New codes must be mapped to a
// LegacyFallbackCode and verified with expect_legacy_fallback_response_status().
const std::set<MetaServiceCode> identity_snapshot = {
        MetaServiceCode::OK,
        MetaServiceCode::INVALID_ARGUMENT,
        MetaServiceCode::KV_TXN_CREATE_ERR,
        MetaServiceCode::KV_TXN_GET_ERR,
        MetaServiceCode::KV_TXN_COMMIT_ERR,
        MetaServiceCode::KV_TXN_CONFLICT,
        MetaServiceCode::PROTOBUF_PARSE_ERR,
        MetaServiceCode::PROTOBUF_SERIALIZE_ERR,
        MetaServiceCode::KV_TXN_STORE_GET_RETRYABLE,
        MetaServiceCode::KV_TXN_STORE_COMMIT_RETRYABLE,
        MetaServiceCode::KV_TXN_STORE_CREATE_RETRYABLE,
        MetaServiceCode::KV_TXN_TOO_OLD,
        MetaServiceCode::KV_TXN_MAYBE_COMMITTED,
        MetaServiceCode::TXN_GEN_ID_ERR,
        MetaServiceCode::TXN_DUPLICATED_REQ,
        MetaServiceCode::TXN_LABEL_ALREADY_USED,
        MetaServiceCode::TXN_INVALID_STATUS,
        MetaServiceCode::TXN_LABEL_NOT_FOUND,
        MetaServiceCode::TXN_ID_NOT_FOUND,
        MetaServiceCode::TXN_ALREADY_ABORTED,
        MetaServiceCode::TXN_ALREADY_VISIBLE,
        MetaServiceCode::TXN_ALREADY_PRECOMMITED,
        MetaServiceCode::VERSION_NOT_FOUND,
        MetaServiceCode::TABLET_NOT_FOUND,
        MetaServiceCode::STALE_TABLET_CACHE,
        MetaServiceCode::STALE_PREPARE_ROWSET,
        MetaServiceCode::CLUSTER_NOT_FOUND,
        MetaServiceCode::ALREADY_EXISTED,
        MetaServiceCode::CLUSTER_ENDPOINT_MISSING,
        MetaServiceCode::STORAGE_VAULT_NOT_FOUND,
        MetaServiceCode::STAGE_NOT_FOUND,
        MetaServiceCode::STAGE_GET_ERR,
        MetaServiceCode::STATE_ALREADY_EXISTED_FOR_USER,
        MetaServiceCode::COPY_JOB_NOT_FOUND,
        MetaServiceCode::JOB_EXPIRED,
        MetaServiceCode::JOB_TABLET_BUSY,
        MetaServiceCode::JOB_ALREADY_SUCCESS,
        MetaServiceCode::ROUTINE_LOAD_DATA_INCONSISTENT,
        MetaServiceCode::ROUTINE_LOAD_PROGRESS_NOT_FOUND,
        MetaServiceCode::JOB_CHECK_ALTER_VERSION,
        MetaServiceCode::STREAMING_JOB_PROGRESS_NOT_FOUND,
        MetaServiceCode::MAX_QPS_LIMIT,
        MetaServiceCode::ERR_ENCRYPT,
        MetaServiceCode::ERR_DECPYPT,
        MetaServiceCode::LOCK_EXPIRED,
        MetaServiceCode::LOCK_CONFLICT,
        MetaServiceCode::ROWSETS_EXPIRED,
        MetaServiceCode::VERSION_NOT_MATCH,
        MetaServiceCode::UPDATE_OVERRIDE_EXISTING_KV,
        MetaServiceCode::ROWSET_META_NOT_FOUND,
        MetaServiceCode::KV_TXN_CONFLICT_RETRY_EXCEEDED_MAX_TIMES,
        MetaServiceCode::SCHEMA_DICT_NOT_FOUND,
        MetaServiceCode::UNDEFINED_ERR,
};

// IMPORTANT: Never-Never-Never modify or extend this enum. New error codes must be mapped to one of the
// existing legacy fallback codes below.
enum class LegacyFallbackCode : int32_t {
    UNDEFINED_ERR = static_cast<int32_t>(MetaServiceCode::UNDEFINED_ERR),
    KV_TXN_CONFLICT = static_cast<int32_t>(MetaServiceCode::KV_TXN_CONFLICT),
};

void verify_response_status_impl(std::set<MetaServiceCode>& covered_codes, MetaServiceCode code,
                                 int32_t expected_legacy_code) {
    EXPECT_TRUE(covered_codes.insert(code).second)
            << "Duplicate MetaServiceCode: " << MetaServiceCode_Name(code);

    MetaServiceResponseStatus status;
    set_response_code(&status, code, "");
    EXPECT_EQ(static_cast<int32_t>(status.code()), expected_legacy_code)
            << "MetaServiceCode: " << MetaServiceCode_Name(code);
    EXPECT_EQ(status.actual_code(), static_cast<int32_t>(code))
            << "MetaServiceCode: " << MetaServiceCode_Name(code);
}

void verify_response_status(std::set<MetaServiceCode>& covered_codes, MetaServiceCode code,
                            int32_t expected_legacy_code) {
    if (!identity_snapshot.contains(code)) {
        EXPECT_TRUE(false)
                << "MetaServiceCode " << MetaServiceCode_Name(code)
                << " is not in identity_snapshot. New error codes must be mapped to a "
                   "LegacyFallbackCode in resolve_response_code_and_msg() and verified with "
                   "expect_legacy_fallback_response_status().";
    }
    verify_response_status_impl(covered_codes, code, expected_legacy_code);
}

// New error codes may only be converted to a value allowed by LegacyFallbackCode.
// Resolve the conversion in resolve_response_code_and_msg();
// For example, MS_TOO_BUSY maps to KV_TXN_CONFLICT so that the BE can retry it.
void expect_legacy_fallback_response_status(std::set<MetaServiceCode>& covered_codes,
                                            MetaServiceCode code,
                                            LegacyFallbackCode expected_legacy_code) {
    verify_response_status_impl(covered_codes, code, static_cast<int32_t>(expected_legacy_code));
}

struct MsRateLimitInjectionConfigGuard {
    ~MsRateLimitInjectionConfigGuard() {
        config::enable_ms_rate_limit_injection = original_enable;
        config::ms_rate_limit_injection_probability = original_probability;
    }

    bool original_enable {config::enable_ms_rate_limit_injection};
    int32_t original_probability {config::ms_rate_limit_injection_probability};
};

google::protobuf::FileDescriptorProto legacy_status_file_descriptor() {
    // Frozen subset of the pre-actual_code schema used by released clients.
    google::protobuf::FileDescriptorProto file;
    file.set_name("legacy_meta_service_status.proto");
    file.set_package("doris.cloud.legacy");
    file.set_syntax("proto2");

    auto* code = file.add_enum_type();
    code->set_name("MetaServiceCode");
    auto* ok = code->add_value();
    ok->set_name("OK");
    ok->set_number(0);
    auto* conflict = code->add_value();
    conflict->set_name("KV_TXN_CONFLICT");
    conflict->set_number(1005);

    auto* status = file.add_message_type();
    status->set_name("MetaServiceResponseStatus");
    auto* code_field = status->add_field();
    code_field->set_name("code");
    code_field->set_number(1);
    code_field->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
    code_field->set_type(google::protobuf::FieldDescriptorProto::TYPE_ENUM);
    code_field->set_type_name(".doris.cloud.legacy.MetaServiceCode");
    auto* msg_field = status->add_field();
    msg_field->set_name("msg");
    msg_field->set_number(2);
    msg_field->set_label(google::protobuf::FieldDescriptorProto::LABEL_OPTIONAL);
    msg_field->set_type(google::protobuf::FieldDescriptorProto::TYPE_STRING);
    return file;
}
} // namespace

class MetaServiceWireCompatibilityTest : public testing::Test {
protected:
    void SetUp() override {
        const auto* file = legacy_pool_.BuildFile(legacy_status_file_descriptor());
        ASSERT_NE(file, nullptr);
        legacy_status_descriptor_ = file->FindMessageTypeByName("MetaServiceResponseStatus");
        ASSERT_NE(legacy_status_descriptor_, nullptr);
        legacy_code_field_ = legacy_status_descriptor_->FindFieldByName("code");
        ASSERT_NE(legacy_code_field_, nullptr);
        legacy_msg_field_ = legacy_status_descriptor_->FindFieldByName("msg");
        ASSERT_NE(legacy_msg_field_, nullptr);
        legacy_status_prototype_ = legacy_factory_.GetPrototype(legacy_status_descriptor_);
        ASSERT_NE(legacy_status_prototype_, nullptr);
    }

    std::unique_ptr<google::protobuf::Message> new_legacy_status() const {
        return std::unique_ptr<google::protobuf::Message>(legacy_status_prototype_->New());
    }

    google::protobuf::DescriptorPool legacy_pool_;
    google::protobuf::DynamicMessageFactory legacy_factory_ {&legacy_pool_};
    const google::protobuf::Descriptor* legacy_status_descriptor_ = nullptr;
    const google::protobuf::FieldDescriptor* legacy_code_field_ = nullptr;
    const google::protobuf::FieldDescriptor* legacy_msg_field_ = nullptr;
    const google::protobuf::Message* legacy_status_prototype_ = nullptr;
};

TEST(MetaServiceHelperTest, FdbClusterPressureNeedsLatencyAndNonWorkload) {
    MsStressMetrics metrics;
    metrics.fdb_commit_latency_ns = 51L * 1000 * 1000;
    metrics.fdb_performance_limited_by_name = -1;

    auto decision = update_ms_stress_detector_for_test(0, metrics, true);
    ASSERT_TRUE(decision.fdb_cluster_under_pressure);
    ASSERT_TRUE(decision.under_great_stress());
    std::cout << decision.debug_string() << std::endl;
    ASSERT_NE(decision.debug_string().find("fdb_cluster"), std::string::npos);

    metrics.fdb_performance_limited_by_name = 0;
    decision = update_ms_stress_detector_for_test(1000, metrics, true);
    ASSERT_FALSE(decision.fdb_cluster_under_pressure);
    ASSERT_FALSE(decision.under_great_stress());
}

TEST(MetaServiceHelperTest, FdbClientThreadPressureNeedsWindowAverageAndInstantValue) {
    MsStressMetrics metrics;
    for (int second = 0; second < 60; ++second) {
        metrics.fdb_client_thread_busyness_percent = 71;
        auto decision = update_ms_stress_detector_for_test(second * 1000, metrics, second == 0);
        ASSERT_FALSE(decision.fdb_client_thread_under_pressure);
    }

    metrics.fdb_client_thread_busyness_percent = 91;
    auto decision = update_ms_stress_detector_for_test(60 * 1000, metrics);
    ASSERT_TRUE(decision.fdb_client_thread_under_pressure);
    ASSERT_TRUE(decision.under_great_stress());
    std::cout << decision.debug_string() << std::endl;
    ASSERT_NE(decision.debug_string().find("fdb_client_thread"), std::string::npos);
}

TEST(MetaServiceHelperTest, MsResourcePressureNeedsCurrentAndWindowAverageHigh) {
    MsStressMetrics metrics;
    for (int second = 0; second < 59; ++second) {
        metrics.ms_cpu_usage_percent = 96;
        auto decision = update_ms_stress_detector_for_test(second * 1000, metrics, second == 0);
        ASSERT_FALSE(decision.ms_resource_under_pressure);
    }

    metrics.ms_cpu_usage_percent = 96;
    auto decision = update_ms_stress_detector_for_test(59 * 1000, metrics);
    ASSERT_TRUE(decision.ms_resource_under_pressure);
    ASSERT_TRUE(decision.under_great_stress());
    std::cout << decision.debug_string() << std::endl;
    ASSERT_NE(decision.debug_string().find("ms_resource"), std::string::npos);

    metrics.ms_cpu_usage_percent = 50;
    decision = update_ms_stress_detector_for_test(60 * 1000, metrics);
    ASSERT_FALSE(decision.ms_resource_under_pressure);
}

TEST(MetaServiceHelperTest, MsRateLimitInjectionRequiresSwitchAndProbabilityHit) {
    MsRateLimitInjectionConfigGuard guard;

    MsStressMetrics metrics;
    config::enable_ms_rate_limit_injection = false;
    config::ms_rate_limit_injection_probability = 100;
    auto decision = update_ms_stress_detector_for_test(0, metrics, true, 0);
    ASSERT_FALSE(decision.rate_limit_injected_for_test);
    ASSERT_FALSE(decision.under_great_stress());

    config::enable_ms_rate_limit_injection = true;
    config::ms_rate_limit_injection_probability = 30;
    decision = update_ms_stress_detector_for_test(1000, metrics, true, 30);
    ASSERT_FALSE(decision.rate_limit_injected_for_test);
    ASSERT_FALSE(decision.under_great_stress());

    decision = update_ms_stress_detector_for_test(2000, metrics, true, 29);
    ASSERT_TRUE(decision.rate_limit_injected_for_test);
    ASSERT_TRUE(decision.under_great_stress());
    ASSERT_NE(decision.debug_string().find("test_injection"), std::string::npos);
}

TEST(MetaServiceHelperTest, ParseCpusetCpuCount) {
    ASSERT_EQ(internal::parse_cpuset_cpu_count("0-3,5,7-8"), 7);
    ASSERT_EQ(internal::parse_cpuset_cpu_count("2"), 1);
    ASSERT_EQ(internal::parse_cpuset_cpu_count(""), -1);
    ASSERT_EQ(internal::parse_cpuset_cpu_count("3-1"), -1);
}

TEST(MetaServiceHelperTest, ParseCgroupCpuQuota) {
    auto v2_limit = internal::parse_cgroup_v2_cpu_limit("50000 100000");
    ASSERT_TRUE(v2_limit.has_value());
    ASSERT_DOUBLE_EQ(*v2_limit, 0.5);
    ASSERT_FALSE(internal::parse_cgroup_v2_cpu_limit("max 100000").has_value());

    auto v1_limit = internal::parse_cgroup_v1_cpu_limit(150000, 100000);
    ASSERT_TRUE(v1_limit.has_value());
    ASSERT_DOUBLE_EQ(*v1_limit, 1.5);
    ASSERT_FALSE(internal::parse_cgroup_v1_cpu_limit(-1, 100000).has_value());
}

TEST(MetaServiceHelperTest, UsagePercentCalculationUsesEffectiveLimit) {
    ASSERT_EQ(internal::calculate_usage_percent(512, 1024), 50);
    ASSERT_EQ(internal::calculate_usage_percent(-1, 1024), -1);
    ASSERT_EQ(internal::calculate_usage_percent(512, std::numeric_limits<int64_t>::max()), 0);

    ASSERT_EQ(internal::calculate_cpu_usage_percent(5e8, 1e9, 0.5), 100);
    ASSERT_EQ(internal::calculate_cpu_usage_percent(15e8, 1e9, 2.0), 75);
    ASSERT_EQ(internal::calculate_cpu_usage_percent(1, 0, 2.0), -1);
}

TEST_F(MetaServiceWireCompatibilityTest, LegacyClientReadsFallbackAndIgnoresActualCode) {
    MetaServiceResponseStatus current_status;
    set_response_code(&current_status, MetaServiceCode::MS_TOO_BUSY, "busy");

    std::string wire;
    ASSERT_TRUE(current_status.SerializeToString(&wire));
    auto legacy_status = new_legacy_status();
    ASSERT_TRUE(legacy_status->ParseFromString(wire));

    const auto* reflection = legacy_status->GetReflection();
    ASSERT_TRUE(reflection->HasField(*legacy_status, legacy_code_field_));
    EXPECT_EQ(reflection->GetEnumValue(*legacy_status, legacy_code_field_),
              MetaServiceCode::KV_TXN_CONFLICT);
    EXPECT_EQ(reflection->GetString(*legacy_status, legacy_msg_field_),
              "busy, [MS_TOO_BUSY will be converted to code=KV_TXN_CONFLICT for old version "
              "clients]");
    EXPECT_EQ(legacy_status_descriptor_->FindFieldByName("actual_code"), nullptr);

    const auto& unknown_fields = reflection->GetUnknownFields(*legacy_status);
    ASSERT_EQ(unknown_fields.field_count(), 1);
    EXPECT_EQ(unknown_fields.field(0).number(), 3);
    EXPECT_EQ(unknown_fields.field(0).type(), google::protobuf::UnknownField::TYPE_VARINT);
    EXPECT_EQ(unknown_fields.field(0).varint(), MetaServiceCode::MS_TOO_BUSY);

    ASSERT_TRUE(legacy_status->SerializeToString(&wire));
    MetaServiceResponseStatus round_trip_status;
    ASSERT_TRUE(round_trip_status.ParseFromString(wire));
    EXPECT_EQ(round_trip_status.code(), MetaServiceCode::KV_TXN_CONFLICT);
    ASSERT_TRUE(round_trip_status.has_actual_code());
    EXPECT_EQ(round_trip_status.actual_code(), MetaServiceCode::MS_TOO_BUSY);
}

TEST_F(MetaServiceWireCompatibilityTest, LegacyClientReadsUnknownEnumAsDefaultOk) {
    MetaServiceResponseStatus incompatible_status;
    incompatible_status.set_code(MetaServiceCode::MS_TOO_BUSY);

    std::string wire;
    ASSERT_TRUE(incompatible_status.SerializeToString(&wire));
    auto legacy_status = new_legacy_status();
    ASSERT_TRUE(legacy_status->ParseFromString(wire));

    const auto* reflection = legacy_status->GetReflection();
    EXPECT_FALSE(reflection->HasField(*legacy_status, legacy_code_field_));
    EXPECT_EQ(reflection->GetEnumValue(*legacy_status, legacy_code_field_), MetaServiceCode::OK);
    const auto& unknown_fields = reflection->GetUnknownFields(*legacy_status);
    ASSERT_EQ(unknown_fields.field_count(), 1);
    EXPECT_EQ(unknown_fields.field(0).number(), 1);
    EXPECT_EQ(unknown_fields.field(0).type(), google::protobuf::UnknownField::TYPE_VARINT);
    EXPECT_EQ(unknown_fields.field(0).varint(), MetaServiceCode::MS_TOO_BUSY);
}

TEST_F(MetaServiceWireCompatibilityTest, NewClientFallsBackForLegacyResponse) {
    auto legacy_status = new_legacy_status();
    const auto* reflection = legacy_status->GetReflection();
    const auto* conflict =
            legacy_code_field_->enum_type()->FindValueByNumber(MetaServiceCode::KV_TXN_CONFLICT);
    ASSERT_NE(conflict, nullptr);
    reflection->SetEnum(legacy_status.get(), legacy_code_field_, conflict);
    reflection->SetString(legacy_status.get(), legacy_msg_field_, "conflict");

    std::string wire;
    ASSERT_TRUE(legacy_status->SerializeToString(&wire));
    MetaServiceResponseStatus current_status;
    ASSERT_TRUE(current_status.ParseFromString(wire));
    EXPECT_EQ(current_status.code(), MetaServiceCode::KV_TXN_CONFLICT);
    EXPECT_FALSE(current_status.has_actual_code());
}

TEST(MetaServiceHelperTest, ResponseStatusUsesExactAndLegacyCodes) {
    MetaServiceResponseStatus status;

    set_response_code(&status, MetaServiceCode::MS_TOO_BUSY, "busy");
    EXPECT_EQ(status.code(), MetaServiceCode::KV_TXN_CONFLICT);
    EXPECT_EQ(status.actual_code(), MetaServiceCode::MS_TOO_BUSY);
    EXPECT_EQ(status.msg(),
              "busy, [MS_TOO_BUSY will be converted to code=KV_TXN_CONFLICT for old version "
              "clients]");

    set_response_code(&status, MetaServiceCode::KV_TXN_CONFLICT, "conflict");
    EXPECT_EQ(status.code(), MetaServiceCode::KV_TXN_CONFLICT);
    EXPECT_EQ(status.actual_code(), MetaServiceCode::KV_TXN_CONFLICT);
    EXPECT_EQ(status.msg(), "conflict");

    set_response_code(&status, MetaServiceCode::MS_TOO_BUSY, "");
    EXPECT_EQ(status.msg(),
              "[MS_TOO_BUSY will be converted to code=KV_TXN_CONFLICT for old version clients]");
}

TEST(MetaServiceHelperTest, ResponseStatusCoversEveryMetaServiceCode) {
    std::set<MetaServiceCode> covered_codes;
    for (auto code : identity_snapshot) {
        verify_response_status(covered_codes, code, static_cast<int32_t>(code));
    }

    expect_legacy_fallback_response_status(covered_codes, MetaServiceCode::MS_TOO_BUSY,
                                           LegacyFallbackCode::KV_TXN_CONFLICT);

    expect_legacy_fallback_response_status(covered_codes, MetaServiceCode::TXN_ALREADY_COMMITED,
                                           LegacyFallbackCode::UNDEFINED_ERR);
    EXPECT_EQ(covered_codes.size(),
              static_cast<size_t>(MetaServiceCode_descriptor()->value_count()))
            << "A new MetaServiceCode was added. Map it to a LegacyFallbackCode in "
               "resolve_response_code_and_msg() and verify it with "
               "expect_legacy_fallback_response_status().";
}

} // namespace doris::cloud
