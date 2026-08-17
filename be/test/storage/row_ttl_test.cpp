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

#include "storage/row_ttl.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "common/cast_set.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_timestamptz.h"
#include "core/value/timestamptz_value.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/descriptors.pb.h"
#include "storage/merger.h"
#include "storage/partial_update_info.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/tablet_info.h"
#include "storage/utils.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {
namespace {

Block make_ttl_block(const std::vector<int64_t>& expirations,
                     const std::vector<uint8_t>& null_map) {
    auto values = ColumnInt64::create();
    values->get_data().assign(expirations.begin(), expirations.end());
    auto nulls = ColumnUInt8::create();
    nulls->get_data().assign(null_map.begin(), null_map.end());
    auto ttl = ColumnNullable::create(std::move(values), std::move(nulls));
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    Block block;
    block.insert({std::move(ttl), type, TTL_COL});
    return block;
}

ColumnPB make_column_pb(int32_t uid, const std::string& name, const std::string& type, bool is_key,
                        bool nullable, const std::string& aggregation = "NONE", int32_t frac = -1,
                        const std::string& default_value = "") {
    ColumnPB column_pb;
    column_pb.set_unique_id(uid);
    column_pb.set_name(name);
    column_pb.set_type(type);
    column_pb.set_is_key(is_key);
    column_pb.set_is_nullable(nullable);
    column_pb.set_aggregation(aggregation);
    if (frac >= 0) {
        column_pb.set_frac(frac);
    }
    if (!default_value.empty()) {
        column_pb.set_default_value(default_value);
    }
    return column_pb;
}

TabletColumn make_tablet_column(const std::string& type, int32_t frac = -1, bool nullable = true,
                                const std::string& default_value = "") {
    TabletColumn column;
    column.init_from_pb(
            make_column_pb(2, "event_time", type, false, nullable, "NONE", frac, default_value));
    return column;
}

TabletSchema make_ttl_schema(KeysType keys_type) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(keys_type);
    *schema_pb.add_column() = make_column_pb(0, "k", "INT", true, false);
    *schema_pb.add_column() =
            make_column_pb(1, TTL_COL, "BIGINT", false, true,
                           keys_type == KeysType::DUP_KEYS ? "NONE" : "REPLACE", -1, "NULL");
    schema_pb.set_ttl_col_idx(1);

    TabletSchema schema;
    schema.init_from_pb(schema_pb);
    return schema;
}

TabletSchema make_non_ttl_schema(KeysType keys_type) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(keys_type);
    *schema_pb.add_column() = make_column_pb(0, "k", "INT", true, false);

    TabletSchema schema;
    schema.init_from_pb(schema_pb);
    return schema;
}

TabletSchema make_ttl_write_schema(const std::string& source_default = "") {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::UNIQUE_KEYS);
    *schema_pb.add_column() = make_column_pb(0, "k", "INT", true, false);
    *schema_pb.add_column() = make_column_pb(1, "event_time", "DATETIMEV2", false, true, "REPLACE",
                                             6, source_default);
    *schema_pb.add_column() =
            make_column_pb(2, TTL_COL, "DATETIMEV2", false, true, "REPLACE", 6, "NULL");
    schema_pb.set_ttl_col_idx(2);
    schema_pb.set_row_ttl_duration_us(7);
    schema_pb.set_row_ttl_time_zone_offset_seconds(8 * 60 * 60);

    TabletSchema schema;
    schema.init_from_pb(schema_pb);
    return schema;
}

TabletSchema make_ttl_rollup_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::UNIQUE_KEYS);
    *schema_pb.add_column() = make_column_pb(0, "k", "INT", true, false);
    *schema_pb.add_column() =
            make_column_pb(2, TTL_COL, "DATETIMEV2", false, true, "REPLACE", 6, "NULL");
    schema_pb.set_ttl_col_idx(1);
    schema_pb.set_row_ttl_duration_us(7);
    schema_pb.set_row_ttl_time_zone_offset_seconds(8 * 60 * 60);

    TabletSchema schema;
    schema.init_from_pb(schema_pb);
    return schema;
}

void expect_converted_time(const TabletColumn& source_column, const std::string& source_value,
                           int32_t time_zone_offset_seconds, int64_t expected) {
    auto expiration =
            convert_row_ttl_time_to_epoch_us(source_column, source_value, time_zone_offset_seconds);
    ASSERT_TRUE(expiration.has_value()) << expiration.error();
    ASSERT_TRUE(expiration->has_value());
    EXPECT_EQ(**expiration, expected);
}

DateV2Value<DateTimeV2ValueType> datetimev2_value(uint16_t year, uint8_t month, uint8_t day,
                                                  uint8_t hour, uint8_t minute, uint8_t second,
                                                  uint32_t microsecond) {
    DateV2Value<DateTimeV2ValueType> value;
    value.unchecked_set_time(year, month, day, hour, minute, second, microsecond);
    return value;
}

int64_t to_query_now_us(const ColumnDateTimeV2& values, size_t row, int64_t duration_us,
                        int32_t time_zone_offset_seconds) {
    int64_t query_now_us = 0;
    EXPECT_TRUE(calculate_row_ttl_expiration_us(
                        values, FieldType::OLAP_FIELD_TYPE_DATETIMEV2, row,
                        cctz::fixed_time_zone(cctz::seconds(time_zone_offset_seconds)), duration_us,
                        &query_now_us)
                        .ok());
    return query_now_us;
}

Status execute_row_ttl_is_visible(ColumnPtr ttl_column, DataTypePtr ttl_type,
                                  ColumnPtr duration_column, int64_t query_now_us,
                                  std::optional<int32_t> time_zone_offset_seconds,
                                  std::vector<uint8_t>* output) {
    DataTypePtr duration_type = std::make_shared<DataTypeInt64>();
    DataTypePtr return_type = std::make_shared<DataTypeUInt8>();
    ColumnsWithTypeAndName argument_template = {{nullptr, ttl_type, "ttl"},
                                                {nullptr, duration_type, "duration"}};
    DataTypes argument_types = {ttl_type, duration_type};
    if (time_zone_offset_seconds.has_value()) {
        DataTypePtr offset_type = std::make_shared<DataTypeInt32>();
        argument_template.emplace_back(nullptr, offset_type, "time_zone_offset_seconds");
        argument_types.emplace_back(std::move(offset_type));
    }
    SimpleFunctionFactory factory;
    register_function_row_ttl(factory);
    FunctionBasePtr function =
            factory.get_function("row_ttl_is_visible", argument_template, return_type, {},
                                 BeExecVersionManager::get_newest_version());
    if (!function) {
        return Status::InternalError("row_ttl_is_visible was not registered");
    }

    TQueryGlobals globals;
    globals.__set_timestamp_ms(query_now_us / 1'000'000 * 1'000);
    globals.__set_nano_seconds(query_now_us % 1'000'000 * 1'000);
    // The table offset argument, not the query/session time zone, determines TTL conversion.
    globals.__set_time_zone("-07:00");
    MockRuntimeState state(globals);
    std::unique_ptr<FunctionContext> context =
            FunctionContext::create_context(&state, return_type, argument_types);

    Block block;
    block.insert({std::move(ttl_column), ttl_type, "ttl"});
    block.insert({std::move(duration_column), duration_type, "duration"});
    ColumnNumbers arguments = {0, 1};
    if (time_zone_offset_seconds.has_value()) {
        auto offset = ColumnInt32::create();
        offset->insert_value(*time_zone_offset_seconds);
        block.insert({ColumnConst::create(std::move(offset), block.rows()), argument_types[2],
                      "time_zone_offset_seconds"});
        arguments.push_back(2);
    }
    const uint32_t result_position = cast_set<uint32_t>(block.columns());
    block.insert({return_type->create_column(), return_type, "result"});
    RETURN_IF_ERROR(
            function->execute(context.get(), block, arguments, result_position, block.rows()));

    const auto& result =
            assert_cast<const ColumnUInt8&>(*block.get_by_position(result_position).column);
    output->assign(result.get_data().begin(), result.get_data().end());
    return Status::OK();
}

} // namespace

TEST(RowTtlTest, ValidateFixedTimeZoneOffset) {
    EXPECT_TRUE(is_valid_row_ttl_time_zone_offset_seconds(-12 * 60 * 60));
    EXPECT_TRUE(is_valid_row_ttl_time_zone_offset_seconds(0));
    EXPECT_TRUE(is_valid_row_ttl_time_zone_offset_seconds(14 * 60 * 60));
    EXPECT_FALSE(is_valid_row_ttl_time_zone_offset_seconds(-12 * 60 * 60 - 60));
    EXPECT_FALSE(is_valid_row_ttl_time_zone_offset_seconds(14 * 60 * 60 + 60));
    EXPECT_FALSE(is_valid_row_ttl_time_zone_offset_seconds(1));
}

TEST(RowTtlTest, RestoreRequiresCompatiblePersistedPolicy) {
    TabletSchema source = make_ttl_write_schema();
    TabletSchema target = make_ttl_write_schema();
    EXPECT_TRUE(check_row_ttl_restore_schema_compatible(source, target).ok());

    // A historical rowset schema may predate a visible column inserted before the hidden TTL
    // column. The ordinal changes, but the hidden column unique id and policy remain stable.
    TabletSchema historical_rowset = make_ttl_rollup_schema();
    ASSERT_NE(historical_rowset.ttl_col_idx(), target.ttl_col_idx());
    EXPECT_EQ(historical_rowset.column(historical_rowset.ttl_col_idx()).unique_id(),
              target.column(target.ttl_col_idx()).unique_id());
    EXPECT_TRUE(check_row_ttl_restore_schema_compatible(historical_rowset, target).ok());

    TabletMetaPB restored_meta;
    target.to_schema_pb(restored_meta.mutable_schema());
    historical_rowset.to_schema_pb(restored_meta.add_rs_metas()->mutable_tablet_schema());
    EXPECT_TRUE(check_row_ttl_restore_tablet_meta_compatible(restored_meta, target).ok());

    TabletMetaPB incompatible_inc_meta = restored_meta;
    target.to_schema_pb(
            incompatible_inc_meta.add_inc_rs_metas()->mutable_tablet_schema());
    incompatible_inc_meta.mutable_inc_rs_metas(0)
            ->mutable_tablet_schema()
            ->set_row_ttl_duration_us(8);
    EXPECT_TRUE(check_row_ttl_restore_tablet_meta_compatible(incompatible_inc_meta, target)
                        .is<ErrorCode::INVALID_ARGUMENT>());

    TabletMetaPB incompatible_stale_meta = restored_meta;
    target.to_schema_pb(
            incompatible_stale_meta.add_stale_rs_metas()->mutable_tablet_schema());
    incompatible_stale_meta.mutable_stale_rs_metas(0)
            ->mutable_tablet_schema()
            ->set_row_ttl_duration_us(8);
    EXPECT_TRUE(check_row_ttl_restore_tablet_meta_compatible(incompatible_stale_meta, target)
                        .is<ErrorCode::INVALID_ARGUMENT>());

    TabletSchemaPB target_pb;
    target.to_schema_pb(&target_pb);
    target_pb.set_row_ttl_duration_us(8);
    TabletSchema different_duration;
    different_duration.init_from_pb(target_pb);
    EXPECT_TRUE(check_row_ttl_restore_schema_compatible(source, different_duration)
                        .is<ErrorCode::INVALID_ARGUMENT>());

    target.to_schema_pb(&target_pb);
    target_pb.clear_row_ttl_time_zone_offset_seconds();
    TabletSchema unknown_naive_time_zone;
    unknown_naive_time_zone.init_from_pb(target_pb);
    EXPECT_TRUE(check_row_ttl_restore_schema_compatible(source, unknown_naive_time_zone)
                        .is<ErrorCode::INVALID_ARGUMENT>());

    TabletSchema direct = make_ttl_schema(KeysType::UNIQUE_KEYS);
    EXPECT_TRUE(check_row_ttl_restore_schema_compatible(source, direct)
                        .is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(check_row_ttl_restore_schema_compatible(make_non_ttl_schema(KeysType::DUP_KEYS),
                                                        make_non_ttl_schema(KeysType::DUP_KEYS))
                        .ok());

    TabletSchemaPB timestamp_pb;
    timestamp_pb.set_keys_type(KeysType::DUP_KEYS);
    *timestamp_pb.add_column() = make_column_pb(1, TTL_COL, "TIMESTAMPTZ", false, true, "NONE", 6);
    timestamp_pb.set_ttl_col_idx(0);
    timestamp_pb.set_row_ttl_duration_us(7);
    TabletSchema legacy_timestamp;
    legacy_timestamp.init_from_pb(timestamp_pb);
    timestamp_pb.set_row_ttl_time_zone_offset_seconds(0);
    TabletSchema explicit_utc_timestamp;
    explicit_utc_timestamp.init_from_pb(timestamp_pb);
    EXPECT_TRUE(
            check_row_ttl_restore_schema_compatible(legacy_timestamp, explicit_utc_timestamp).ok());

    timestamp_pb.set_row_ttl_time_zone_offset_seconds(60);
    TabletSchema invalid_timestamp;
    invalid_timestamp.init_from_pb(timestamp_pb);
    EXPECT_TRUE(check_row_ttl_restore_schema_compatible(legacy_timestamp, invalid_timestamp)
                        .is<ErrorCode::INVALID_ARGUMENT>());
}

TEST(RowTtlTest, ConvertAllSupportedTemporalSources) {
    expect_converted_time(make_tablet_column("DATE"), "1970-01-02", 0, 86'400'000'000L);
    expect_converted_time(make_tablet_column("DATETIME"), "1970-01-01 00:00:01", 0, 1'000'000L);
    expect_converted_time(make_tablet_column("DATEV2"), "1970-01-02", 0, 86'400'000'000L);
    expect_converted_time(make_tablet_column("DATETIMEV2", 6), "1970-01-01 00:00:00.123456", 0,
                          123'456L);
    expect_converted_time(make_tablet_column("DATETIMEV2", 6), "1970-01-01 08:00:00.123456",
                          8 * 60 * 60, 123'456L);
    expect_converted_time(make_tablet_column("TIMESTAMPTZ", 6), "1970-01-01 00:00:00.123456+00:00",
                          0, 123'456L);

    auto immortal = convert_row_ttl_time_to_epoch_us(make_tablet_column("DATETIMEV2", 6), "NULL",
                                                     8 * 60 * 60);
    ASSERT_TRUE(immortal.has_value()) << immortal.error();
    EXPECT_FALSE(immortal->has_value());
}

TEST(RowTtlTest, RejectInvalidTemporalConversionInputs) {
    auto bad_offset = convert_row_ttl_time_to_epoch_us(make_tablet_column("DATETIMEV2", 6),
                                                       "1970-01-01 00:00:00", 1);
    EXPECT_FALSE(bad_offset.has_value());

    auto bad_source_type = convert_row_ttl_time_to_epoch_us(make_tablet_column("INT"), "1", 0);
    EXPECT_FALSE(bad_source_type.has_value());

    auto timestamp_non_utc = convert_row_ttl_time_to_epoch_us(make_tablet_column("TIMESTAMPTZ", 6),
                                                              "1970-01-01 00:00:00+00:00", 60);
    EXPECT_FALSE(timestamp_non_utc.has_value());

    auto values = ColumnDateTimeV2::create();
    values->insert_value(datetimev2_value(1970, 1, 2, 0, 0, 0, 0));
    int64_t expiration_us = 0;
    EXPECT_FALSE(calculate_row_ttl_expiration_us(
                         *values, FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 0, cctz::utc_time_zone(),
                         std::numeric_limits<int64_t>::max(), &expiration_us)
                         .ok());
}

TEST(RowTtlTest, PartialUpdateMetadataTracksSourceAndDefaults) {
    TabletSchema rollup_schema = make_ttl_rollup_schema();
    TabletColumn source = make_tablet_column("DATETIMEV2", 6, false, "1970-01-01 08:00:00.123456");

    PartialUpdateInfo rollup_info;
    ASSERT_TRUE(rollup_info
                        .init(1, 2, rollup_schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                              PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0, "+08:00", "",
                              -1, -1, source.unique_id(), &source)
                        .ok());
    EXPECT_EQ(rollup_info.row_ttl_source_cid(), -1);
    EXPECT_EQ(rollup_info.row_ttl_source_uid(), source.unique_id());
    ASSERT_EQ(rollup_info.missing_cids, std::vector<uint32_t>({1}));
    ASSERT_EQ(rollup_info.default_values.size(), 1);
    EXPECT_EQ(rollup_info.default_values[0], "1970-01-01 08:00:00.123456");

    TabletSchema source_schema = make_ttl_write_schema("1970-01-01 08:00:00.654321");
    PartialUpdateInfo source_info;
    ASSERT_TRUE(source_info
                        .init(1, 2, source_schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                              PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0, "+08:00", "",
                              -1, -1, source_schema.column(1).unique_id(), &source_schema.column(1))
                        .ok());
    EXPECT_EQ(source_info.row_ttl_source_cid(), 1);
    ASSERT_EQ(source_info.missing_cids, std::vector<uint32_t>({1, 2}));
    ASSERT_EQ(source_info.default_values.size(), 2);
    EXPECT_EQ(source_info.default_values[0], "1970-01-01 08:00:00.654321");
    EXPECT_EQ(source_info.default_values[1], "NULL");

    PartialUpdateInfoPB source_info_pb;
    source_info.to_pb(&source_info_pb);
    PartialUpdateInfo restored_source_info;
    restored_source_info.from_pb(&source_info_pb);
    EXPECT_EQ(restored_source_info.row_ttl_source_cid(), 1);
    EXPECT_EQ(restored_source_info.row_ttl_source_uid(), source_schema.column(1).unique_id());
    EXPECT_EQ(restored_source_info.missing_cids, source_info.missing_cids);
    EXPECT_EQ(restored_source_info.default_values, source_info.default_values);

    TabletColumn timestamp_source =
            make_tablet_column("TIMESTAMPTZ", 6, false, "CURRENT_TIMESTAMP(6)");
    PartialUpdateInfo timestamp_info;
    ASSERT_TRUE(timestamp_info
                        .init(1, 2, rollup_schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                              PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 123'456'000,
                              "+08:00", "", -1, -1, timestamp_source.unique_id(), &timestamp_source)
                        .ok());
    ASSERT_EQ(timestamp_info.default_values.size(), 1);
    EXPECT_EQ(timestamp_info.default_values[0], "1970-01-01 08:00:00.123456+08:00");

    TabletSchema direct_schema = make_ttl_schema(KeysType::UNIQUE_KEYS);
    PartialUpdateInfo direct_info;
    ASSERT_TRUE(direct_info
                        .init(1, 2, direct_schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                              PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0, "UTC", "")
                        .ok());
    EXPECT_EQ(direct_info.row_ttl_source_cid(), -1);
    EXPECT_EQ(direct_info.row_ttl_source_uid(), -1);
    ASSERT_EQ(direct_info.missing_cids, std::vector<uint32_t>({1}));
    ASSERT_EQ(direct_info.default_values.size(), 1);
    EXPECT_EQ(direct_info.default_values[0], "NULL");
}

TEST(RowTtlTest, FlexiblePartialUpdateRejectsSchemaWithoutSkipBitmap) {
    TabletSchema rollup_schema = make_ttl_rollup_schema();
    PartialUpdateInfo info;
    Status status = info.init(1, 2, rollup_schema, UniqueKeyUpdateModePB::UPDATE_FLEXIBLE_COLUMNS,
                              PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0, "UTC", "");

    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
    EXPECT_NE(status.to_string().find("requires a skip bitmap column"), std::string::npos);
}

TEST(RowTtlTest, VerticalSplitKeepsTtlInKeyGroup) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::UNIQUE_KEYS);
    schema_pb.set_num_short_key_columns(1);
    *schema_pb.add_column() = make_column_pb(0, "k", "INT", true, false);
    *schema_pb.add_column() = make_column_pb(1, "cluster_key", "INT", false, false, "REPLACE");
    *schema_pb.add_column() =
            make_column_pb(2, TTL_COL, "DATETIMEV2", false, true, "REPLACE", 6, "NULL");
    *schema_pb.add_column() = make_column_pb(3, DELETE_SIGN, "TINYINT", false, false, "REPLACE");
    *schema_pb.add_column() = make_column_pb(4, "v", "INT", false, false, "REPLACE");
    schema_pb.add_cluster_key_uids(1);
    schema_pb.set_ttl_col_idx(2);
    schema_pb.set_row_ttl_duration_us(7);

    TabletSchema schema;
    schema.init_from_pb(schema_pb);
    std::vector<std::vector<uint32_t>> column_groups;
    std::vector<uint32_t> key_group_cluster_key_idxes;
    Merger::vertical_split_columns(schema, &column_groups, &key_group_cluster_key_idxes, 2);

    ASSERT_EQ(column_groups.size(), 2);
    EXPECT_EQ(column_groups[0], std::vector<uint32_t>({0, 2, 3, 1}));
    EXPECT_EQ(column_groups[1], std::vector<uint32_t>({4}));
    EXPECT_EQ(key_group_cluster_key_idxes, std::vector<uint32_t>({3}));
}

TEST(RowTtlTest, OlapTableSchemaParamPreservesTtlSourceColumn) {
    ColumnPB source_pb = make_column_pb(2, "event_time", "TIMESTAMPTZ", false, true, "REPLACE", 6);
    POlapTableSchemaParam protobuf_schema;
    protobuf_schema.set_db_id(1);
    protobuf_schema.set_table_id(2);
    protobuf_schema.set_version(3);
    protobuf_schema.mutable_tuple_desc()->set_id(0);
    protobuf_schema.mutable_tuple_desc()->set_byte_size(0);
    protobuf_schema.mutable_tuple_desc()->set_num_null_bytes(0);
    protobuf_schema.set_row_ttl_source_column_unique_id(source_pb.unique_id());
    *protobuf_schema.mutable_row_ttl_source_column() = source_pb;

    OlapTableSchemaParam protobuf_param;
    ASSERT_TRUE(protobuf_param.init(protobuf_schema).ok());
    ASSERT_NE(protobuf_param.row_ttl_source_column(), nullptr);
    EXPECT_EQ(protobuf_param.row_ttl_source_column_uid(), source_pb.unique_id());
    EXPECT_EQ(protobuf_param.row_ttl_source_column()->name(), source_pb.name());
    EXPECT_EQ(protobuf_param.row_ttl_source_column()->type(),
              FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ);

    POlapTableSchemaParam protobuf_roundtrip;
    protobuf_param.to_protobuf(&protobuf_roundtrip);
    ASSERT_TRUE(protobuf_roundtrip.has_row_ttl_source_column());
    EXPECT_EQ(protobuf_roundtrip.row_ttl_source_column_unique_id(), source_pb.unique_id());
    EXPECT_EQ(protobuf_roundtrip.row_ttl_source_column().name(), source_pb.name());

    TColumnType source_type;
    source_type.__set_type(TPrimitiveType::TIMESTAMPTZ);
    source_type.__set_precision(6);
    source_type.__set_scale(6);
    TColumn thrift_source;
    thrift_source.__set_column_name("event_time");
    thrift_source.__set_column_type(source_type);
    thrift_source.__set_aggregation_type(TAggregationType::REPLACE);
    thrift_source.__set_is_key(false);
    thrift_source.__set_is_allow_null(true);
    thrift_source.__set_visible(true);
    thrift_source.__set_col_unique_id(2);

    TOlapTableSchemaParam thrift_schema;
    thrift_schema.db_id = 1;
    thrift_schema.table_id = 2;
    thrift_schema.version = 3;
    thrift_schema.tuple_desc.id = 0;
    thrift_schema.tuple_desc.byteSize = 0;
    thrift_schema.tuple_desc.numNullBytes = 0;
    thrift_schema.__set_row_ttl_source_column_unique_id(2);
    thrift_schema.__set_row_ttl_source_column(thrift_source);

    OlapTableSchemaParam thrift_param;
    ASSERT_TRUE(thrift_param.init(thrift_schema).ok());
    ASSERT_NE(thrift_param.row_ttl_source_column(), nullptr);
    EXPECT_EQ(thrift_param.row_ttl_source_column_uid(), 2);
    EXPECT_EQ(thrift_param.row_ttl_source_column()->name(), "event_time");
    EXPECT_EQ(thrift_param.row_ttl_source_column()->type(), FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ);

    POlapTableSchemaParam thrift_roundtrip;
    thrift_param.to_protobuf(&thrift_roundtrip);
    ASSERT_TRUE(thrift_roundtrip.has_row_ttl_source_column());
    EXPECT_EQ(thrift_roundtrip.row_ttl_source_column_unique_id(), 2);
    EXPECT_EQ(thrift_roundtrip.row_ttl_source_column().name(), "event_time");
    EXPECT_EQ(thrift_roundtrip.row_ttl_source_column().frac(), 6);
}

TEST(RowTtlTest, CopyPartialUpdateSourceSliceAndPreserveUnselectedRows) {
    TabletSchema schema = make_ttl_write_schema();
    auto keys = ColumnInt32::create();
    keys->get_data().assign({1, 2, 3});

    auto source_values = ColumnDateTimeV2::create();
    for (int microsecond : {1, 123'456, 9}) {
        source_values->insert_value(datetimev2_value(1970, 1, 1, 8, 0, 0, microsecond));
    }
    auto source_nulls = ColumnUInt8::create();
    source_nulls->get_data().assign({0, 0, 1});
    auto sources = ColumnNullable::create(std::move(source_values), std::move(source_nulls));

    auto ttl_values = ColumnDateTimeV2::create();
    for (int microsecond : {11, 22, 33}) {
        ttl_values->insert_value(datetimev2_value(1970, 1, 1, 0, 0, 0, microsecond));
    }
    const auto preserved_first_value = ttl_values->get_data()[0];
    auto ttl_nulls = ColumnUInt8::create();
    ttl_nulls->get_data().assign({0, 0, 0});
    auto ttl = ColumnNullable::create(std::move(ttl_values), std::move(ttl_nulls));

    Block block;
    block.insert({std::move(keys), std::make_shared<DataTypeInt32>(), "k"});
    block.insert({std::move(sources),
                  std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTimeV2>(6)),
                  "event_time"});
    block.insert({std::move(ttl),
                  std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTimeV2>(6)),
                  TTL_COL});

    ASSERT_TRUE(copy_row_ttl_source(&block, schema, 1, {true, true}, 1).ok());
    const auto& copied = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
    const auto& source = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
    EXPECT_EQ(assert_cast<const ColumnDateTimeV2&>(copied.get_nested_column()).get_data()[0],
              preserved_first_value);
    EXPECT_EQ(assert_cast<const ColumnDateTimeV2&>(copied.get_nested_column()).get_data()[1],
              assert_cast<const ColumnDateTimeV2&>(source.get_nested_column()).get_data()[1]);
    EXPECT_EQ(copied.get_null_map_data(), IColumn::Filter({0, 0, 1}));
}

TEST(RowTtlTest, BuildVisibilityFilterHandlesNoopDeleteSignAndDirectTtl) {
    TabletSchema schema = make_ttl_schema(KeysType::DUP_KEYS);
    Block block = make_ttl_block({0, 100, 101, 1}, {1, 0, 0, 0});

    RowVisibilityFilter keep_all;
    ASSERT_TRUE(build_row_visibility_filter(block, schema, false, false, 100, &keep_all).ok());
    EXPECT_EQ(keep_all.selection, IColumn::Filter({1, 1, 1, 1}));
    EXPECT_EQ(keep_all.rows_deleted, 0);

    RowVisibilityFilter ttl_filter;
    ASSERT_TRUE(build_row_visibility_filter(block, schema, false, true, 100, &ttl_filter).ok());
    EXPECT_EQ(ttl_filter.selection, IColumn::Filter({1, 0, 1, 0}));
    EXPECT_EQ(ttl_filter.rows_deleted, 2);

    ASSERT_TRUE(filter_block_by_row_visibility(&block, ttl_filter.selection).ok());
    EXPECT_EQ(block.rows(), 2);
    EXPECT_FALSE(filter_block_by_row_visibility(&block, {1}).ok());
}

TEST(RowTtlTest, TtlComposesWithDeleteSignWithoutDoubleCounting) {
    TabletSchema schema = make_ttl_schema(KeysType::UNIQUE_KEYS);
    Block block = make_ttl_block({0, 100, 101, 1}, {0, 0, 0, 1});
    auto delete_sign = ColumnInt8::create();
    delete_sign->get_data().assign({1, 0, 0, 1});
    block.insert({std::move(delete_sign), std::make_shared<DataTypeInt8>(), DELETE_SIGN});

    RowVisibilityFilter delete_only;
    ASSERT_TRUE(build_row_visibility_filter(block, schema, true, false, 100, &delete_only).ok());
    EXPECT_EQ(delete_only.selection, IColumn::Filter({0, 1, 1, 0}));
    EXPECT_EQ(delete_only.rows_deleted, 2);

    RowVisibilityFilter composed;
    ASSERT_TRUE(build_row_visibility_filter(block, schema, true, true, 100, &composed).ok());
    EXPECT_EQ(composed.selection, IColumn::Filter({0, 0, 1, 0}));
    EXPECT_EQ(composed.rows_deleted, 3);
}

TEST(RowTtlTest, TemporalFilterUsesFixedTimezoneDurationAndInclusiveBoundary) {
    constexpr int32_t time_zone_offset_seconds = 8 * 60 * 60;
    TabletSchema schema = make_ttl_write_schema();
    auto values = ColumnDateTimeV2::create();
    values->insert_value(datetimev2_value(2020, 1, 1, 0, 0, 0, 0));
    values->insert_value(datetimev2_value(2020, 1, 1, 0, 0, 0, 0));
    values->insert_value(datetimev2_value(2020, 1, 1, 0, 0, 0, 1));
    auto nulls = ColumnUInt8::create();
    nulls->get_data().assign({1, 0, 0});
    auto ttl = ColumnNullable::create(std::move(values), std::move(nulls));
    Block block;
    block.insert({std::move(ttl),
                  std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTimeV2>(6)),
                  TTL_COL});

    const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(0).column);
    int64_t boundary_us = 0;
    ASSERT_TRUE(calculate_row_ttl_expiration_us(
                        nullable.get_nested_column(), FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 1,
                        cctz::fixed_time_zone(cctz::seconds(time_zone_offset_seconds)), 7,
                        &boundary_us)
                        .ok());
    int64_t opposite_offset_boundary_us = 0;
    ASSERT_TRUE(calculate_row_ttl_expiration_us(
                        nullable.get_nested_column(), FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 1,
                        cctz::fixed_time_zone(cctz::seconds(-time_zone_offset_seconds)), 7,
                        &opposite_offset_boundary_us)
                        .ok());
    EXPECT_EQ(opposite_offset_boundary_us - boundary_us, 16 * 60 * 60 * 1'000'000L);
    RowVisibilityFilter filter;
    ASSERT_TRUE(build_row_visibility_filter(block, schema, false, true, boundary_us, &filter).ok());
    EXPECT_EQ(filter.selection, IColumn::Filter({1, 0, 1}));
    EXPECT_EQ(filter.rows_deleted, 1);

    TabletSchemaPB missing_duration_pb;
    schema.to_schema_pb(&missing_duration_pb);
    missing_duration_pb.clear_row_ttl_duration_us();
    TabletSchema missing_duration;
    missing_duration.init_from_pb(missing_duration_pb);
    RowVisibilityFilter missing_duration_filter;
    EXPECT_FALSE(build_row_visibility_filter(block, missing_duration, false, true, boundary_us,
                                             &missing_duration_filter)
                         .ok());

    TabletSchemaPB missing_offset_pb;
    schema.to_schema_pb(&missing_offset_pb);
    missing_offset_pb.clear_row_ttl_time_zone_offset_seconds();
    TabletSchema missing_offset;
    missing_offset.init_from_pb(missing_offset_pb);
    RowVisibilityFilter missing_offset_filter;
    EXPECT_FALSE(build_row_visibility_filter(block, missing_offset, false, true, boundary_us,
                                             &missing_offset_filter)
                         .ok());
}

TEST(RowTtlTest, RestoreLegacyTemporalTabletDurationAndWriteOnlyNewField) {
    TabletSchemaPB legacy_pb;
    legacy_pb.set_keys_type(KeysType::DUP_KEYS);
    *legacy_pb.add_column() = make_column_pb(1, TTL_COL, "DATETIMEV2", false, true, "NONE", 6);
    legacy_pb.set_legacy_row_ttl_duration_seconds(2);

    TabletSchema restored;
    restored.init_from_pb(legacy_pb);
    EXPECT_TRUE(restored.has_ttl_col());
    EXPECT_EQ(restored.ttl_col_idx(), 0);
    EXPECT_TRUE(row_ttl_uses_source_time(restored));
    EXPECT_EQ(restored.row_ttl_duration_us(), 2'000'000);
    EXPECT_FALSE(restored.has_row_ttl_time_zone_offset_seconds());

    TabletSchemaPB current_pb;
    restored.to_schema_pb(&current_pb);
    EXPECT_TRUE(current_pb.has_row_ttl_duration_us());
    EXPECT_EQ(current_pb.row_ttl_duration_us(), 2'000'000);
    EXPECT_FALSE(current_pb.has_legacy_row_ttl_duration_seconds());
    EXPECT_FALSE(current_pb.has_row_ttl_time_zone_offset_seconds());

    TabletSchemaPB non_ttl_pb;
    non_ttl_pb.set_keys_type(KeysType::DUP_KEYS);
    non_ttl_pb.set_legacy_row_ttl_duration_seconds(2);
    TabletSchema non_ttl;
    non_ttl.init_from_pb(non_ttl_pb);
    EXPECT_FALSE(non_ttl.has_ttl_col());
    EXPECT_EQ(non_ttl.row_ttl_duration_us(), -1);

    TabletSchemaPB direct_pb;
    make_ttl_schema(KeysType::DUP_KEYS).to_schema_pb(&direct_pb);
    direct_pb.clear_row_ttl_duration_us();
    direct_pb.set_legacy_row_ttl_duration_seconds(2);
    TabletSchema direct;
    direct.init_from_pb(direct_pb);
    EXPECT_TRUE(direct.has_ttl_col());
    EXPECT_FALSE(row_ttl_uses_source_time(direct));
    EXPECT_EQ(direct.row_ttl_duration_us(), -1);
}

TEST(RowTtlTest, TabletSchemaPreservesTimeZoneOffsetPresence) {
    TabletSchema schema = make_ttl_write_schema();
    ASSERT_TRUE(schema.has_row_ttl_time_zone_offset_seconds());
    EXPECT_EQ(schema.row_ttl_time_zone_offset_seconds(), 8 * 60 * 60);

    TabletSchemaPB serialized;
    schema.to_schema_pb(&serialized);
    ASSERT_TRUE(serialized.has_row_ttl_time_zone_offset_seconds());
    EXPECT_EQ(serialized.row_ttl_time_zone_offset_seconds(), 8 * 60 * 60);

    TabletSchema copied;
    copied.copy_from(schema);
    EXPECT_EQ(copied, schema);

    serialized.set_row_ttl_time_zone_offset_seconds(0);
    TabletSchema utc_schema;
    utc_schema.init_from_pb(serialized);
    ASSERT_TRUE(utc_schema.has_row_ttl_time_zone_offset_seconds());
    EXPECT_EQ(utc_schema.row_ttl_time_zone_offset_seconds(), 0);
    EXPECT_NE(schema, utc_schema);
}

TEST(RowTtlTest, FunctionRowTtlIsVisibleCoversDirectAndTemporalModes) {
    constexpr int64_t query_now_us = 100;
    auto values = ColumnInt64::create();
    values->get_data().assign({99, 100, 101, 0});
    auto nulls = ColumnUInt8::create();
    nulls->get_data().assign({0, 0, 0, 1});
    auto ttl = ColumnNullable::create(std::move(values), std::move(nulls));
    auto direct_duration = ColumnInt64::create();
    direct_duration->get_data().assign({-1, -1, -1, -1});
    std::vector<uint8_t> direct_result;
    ASSERT_TRUE(execute_row_ttl_is_visible(
                        std::move(ttl),
                        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
                        std::move(direct_duration), query_now_us, std::nullopt, &direct_result)
                        .ok());
    EXPECT_EQ(direct_result, std::vector<uint8_t>({0, 0, 1, 1}));

    auto temporal_values = ColumnDateTimeV2::create();
    temporal_values->insert_value(datetimev2_value(2020, 1, 1, 0, 0, 0, 0));
    temporal_values->insert_value(datetimev2_value(2020, 1, 1, 0, 0, 0, 1));
    temporal_values->insert_value(datetimev2_value(2020, 1, 1, 0, 0, 0, 0));
    auto temporal_nulls = ColumnUInt8::create();
    temporal_nulls->get_data().assign({0, 0, 1});
    constexpr int32_t time_zone_offset_seconds = 8 * 60 * 60;
    int64_t temporal_query_now_us =
            to_query_now_us(*temporal_values, 0, 7, time_zone_offset_seconds);
    auto temporal_ttl =
            ColumnNullable::create(std::move(temporal_values), std::move(temporal_nulls));
    auto temporal_duration_value = ColumnInt64::create();
    temporal_duration_value->insert_value(7);
    ColumnPtr temporal_duration = ColumnConst::create(std::move(temporal_duration_value), 3);
    std::vector<uint8_t> temporal_result;
    ASSERT_TRUE(execute_row_ttl_is_visible(
                        std::move(temporal_ttl),
                        std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTimeV2>(6)),
                        std::move(temporal_duration), temporal_query_now_us,
                        time_zone_offset_seconds, &temporal_result)
                        .ok());
    EXPECT_EQ(temporal_result, std::vector<uint8_t>({0, 1, 1}));
}

TEST(RowTtlTest, FunctionRowTtlIsVisibleKeepsConstNullableOwnerAlive) {
    constexpr size_t rows = 4;
    auto make_const_ttl = [rows](bool is_null) -> ColumnPtr {
        auto values = ColumnInt64::create();
        values->insert_value(101);
        auto nulls = ColumnUInt8::create();
        nulls->insert_value(is_null);
        return ColumnConst::create(ColumnNullable::create(std::move(values), std::move(nulls)),
                                   rows);
    };
    auto ttl_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());

    auto duration = ColumnInt64::create();
    duration->insert_many_vals(-1, rows);
    std::vector<uint8_t> non_null_result;
    ASSERT_TRUE(execute_row_ttl_is_visible(make_const_ttl(false), ttl_type, std::move(duration),
                                           100, std::nullopt, &non_null_result)
                        .ok());
    EXPECT_EQ(non_null_result, std::vector<uint8_t>(rows, 1));

    duration = ColumnInt64::create();
    duration->insert_many_vals(-1, rows);
    std::vector<uint8_t> null_result;
    ASSERT_TRUE(execute_row_ttl_is_visible(make_const_ttl(true), ttl_type, std::move(duration), 100,
                                           std::nullopt, &null_result)
                        .ok());
    EXPECT_EQ(null_result, std::vector<uint8_t>(rows, 1));
}

TEST(RowTtlTest, FunctionRowTtlIsVisibleRejectsTemporalWithoutOffset) {
    auto values = ColumnDateTimeV2::create();
    values->insert_value(datetimev2_value(2020, 1, 1, 0, 0, 0, 0));
    auto nulls = ColumnUInt8::create();
    nulls->insert_value(0);
    auto ttl = ColumnNullable::create(std::move(values), std::move(nulls));
    auto duration = ColumnInt64::create();
    duration->insert_value(7);
    std::vector<uint8_t> result;
    Status status = execute_row_ttl_is_visible(
            std::move(ttl),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTimeV2>(6)),
            std::move(duration), 100, std::nullopt, &result);
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
}

TEST(RowTtlTest, FunctionRowTtlIsVisibleRequiresUtcOffsetForTimestampTz) {
    auto make_ttl = []() -> ColumnPtr {
        auto values = ColumnTimeStampTz::create();
        TimestampTzValue value;
        value.unchecked_set_time(1970, 1, 1, 0, 0, 0, 0);
        values->insert_value(value);
        auto nulls = ColumnUInt8::create();
        nulls->insert_value(0);
        return ColumnNullable::create(std::move(values), std::move(nulls));
    };
    auto ttl_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeTimeStampTz>(6));

    auto duration = ColumnInt64::create();
    duration->insert_value(0);
    std::vector<uint8_t> result;
    ASSERT_TRUE(execute_row_ttl_is_visible(make_ttl(), ttl_type, std::move(duration), 0, 0, &result)
                        .ok());
    EXPECT_EQ(result, std::vector<uint8_t>({0}));

    duration = ColumnInt64::create();
    duration->insert_value(0);
    Status status =
            execute_row_ttl_is_visible(make_ttl(), ttl_type, std::move(duration), 0, 60, &result);
    EXPECT_TRUE(status.is<ErrorCode::INVALID_ARGUMENT>()) << status;
}

TEST(RowTtlTest, CompactionGcSafetyKeepsOnlySafeReaders) {
    TabletSchema non_ttl = make_non_ttl_schema(KeysType::DUP_KEYS);
    EXPECT_FALSE(should_gc_row_ttl(non_ttl, false, false, ReaderType::READER_FULL_COMPACTION,
                                   Version(0, 3)));

    TabletSchema agg = make_ttl_schema(KeysType::AGG_KEYS);
    EXPECT_FALSE(should_gc_row_ttl(agg, false, false, ReaderType::READER_FULL_COMPACTION,
                                   Version(0, 3)));

    TabletSchema dup = make_ttl_schema(KeysType::DUP_KEYS);
    EXPECT_TRUE(should_gc_row_ttl(dup, false, false, ReaderType::READER_CUMULATIVE_COMPACTION,
                                  Version(2, 3)));
    EXPECT_TRUE(should_gc_row_ttl(dup, false, false, ReaderType::READER_BASE_COMPACTION,
                                  Version(1, 3)));
    EXPECT_TRUE(should_gc_row_ttl(dup, false, false, ReaderType::READER_FULL_COMPACTION,
                                  Version(0, 3)));
    EXPECT_TRUE(should_gc_row_ttl(dup, false, false, ReaderType::READER_SEGMENT_COMPACTION,
                                  Version(-1, 0)));
    EXPECT_FALSE(should_gc_row_ttl(dup, false, true, ReaderType::READER_CUMULATIVE_COMPACTION,
                                   Version(0, 3)));
    EXPECT_FALSE(should_gc_row_ttl(dup, false, false, ReaderType::READER_COLD_DATA_COMPACTION,
                                   Version(0, 3)));
    EXPECT_FALSE(
            should_gc_row_ttl(dup, false, false, ReaderType::READER_CHECKSUM, Version(0, 3)));
    EXPECT_FALSE(
            should_gc_row_ttl(dup, false, false, ReaderType::READER_ALTER_TABLE, Version(0, 3)));

    TabletSchemaPB legacy_naive_pb;
    make_ttl_write_schema().to_schema_pb(&legacy_naive_pb);
    legacy_naive_pb.clear_row_ttl_time_zone_offset_seconds();
    TabletSchema legacy_naive;
    legacy_naive.init_from_pb(legacy_naive_pb);
    EXPECT_FALSE(should_gc_row_ttl(legacy_naive, true, false,
                                   ReaderType::READER_CUMULATIVE_COMPACTION, Version(2, 3)));

    TabletSchemaPB legacy_timestamp_pb;
    legacy_timestamp_pb.set_keys_type(KeysType::DUP_KEYS);
    *legacy_timestamp_pb.add_column() =
            make_column_pb(1, TTL_COL, "TIMESTAMPTZ", false, true, "NONE", 6);
    legacy_timestamp_pb.set_ttl_col_idx(0);
    legacy_timestamp_pb.set_row_ttl_duration_us(7);
    TabletSchema legacy_timestamp;
    legacy_timestamp.init_from_pb(legacy_timestamp_pb);
    EXPECT_TRUE(should_gc_row_ttl(legacy_timestamp, false, false,
                                  ReaderType::READER_CUMULATIVE_COMPACTION, Version(2, 3)));

    legacy_timestamp_pb.clear_row_ttl_duration_us();
    TabletSchema legacy_timestamp_without_duration;
    legacy_timestamp_without_duration.init_from_pb(legacy_timestamp_pb);
    EXPECT_FALSE(should_gc_row_ttl(legacy_timestamp_without_duration, false, false,
                                   ReaderType::READER_CUMULATIVE_COMPACTION, Version(2, 3)));

    TabletSchema unique = make_ttl_schema(KeysType::UNIQUE_KEYS);
    EXPECT_TRUE(should_gc_row_ttl(unique, true, false, ReaderType::READER_CUMULATIVE_COMPACTION,
                                  Version(2, 3)));
    EXPECT_TRUE(should_gc_row_ttl(unique, true, false, ReaderType::READER_BASE_COMPACTION,
                                  Version(1, 3)));
    EXPECT_TRUE(should_gc_row_ttl(unique, true, false, ReaderType::READER_FULL_COMPACTION,
                                  Version(0, 3)));
    EXPECT_TRUE(should_gc_row_ttl(unique, true, false, ReaderType::READER_SEGMENT_COMPACTION,
                                  Version(-1, 0)));

    EXPECT_FALSE(should_gc_row_ttl(unique, false, false, ReaderType::READER_CUMULATIVE_COMPACTION,
                                   Version(2, 3)));
    EXPECT_FALSE(should_gc_row_ttl(unique, false, false, ReaderType::READER_SEGMENT_COMPACTION,
                                   Version(-1, 0)));
    EXPECT_TRUE(should_gc_row_ttl(unique, false, false, ReaderType::READER_BASE_COMPACTION,
                                  Version(0, 3)));
    EXPECT_FALSE(should_gc_row_ttl(unique, false, false, ReaderType::READER_BASE_COMPACTION,
                                   Version(1, 3)));
    EXPECT_TRUE(should_gc_row_ttl(unique, false, false, ReaderType::READER_FULL_COMPACTION,
                                  Version(0, 3)));
}

} // namespace doris
