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

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_date_or_datetime_v2.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/value/vdatetime_value.h"
#include "exprs/function/simple_function_factory.h"
#include "exprs/function_context.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "runtime/be_exec_version_manager.h"
#include "storage/partial_update_info.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/timezone_utils.h"

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

ColumnPB make_column_pb(int32_t uid, const std::string& name, const std::string& type,
                        bool is_key, bool nullable, const std::string& aggregation = "NONE",
                        int32_t frac = -1, const std::string& default_value = "") {
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

TabletColumn make_tablet_column(const std::string& type, int32_t frac = -1,
                                bool nullable = true,
                                const std::string& default_value = "") {
    TabletColumn column;
    column.init_from_pb(make_column_pb(2, "event_time", type, false, nullable, "NONE", frac,
                                       default_value));
    return column;
}

TabletSchema make_ttl_schema(KeysType keys_type) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(keys_type);
    *schema_pb.add_column() = make_column_pb(0, "k", "INT", true, false);
    *schema_pb.add_column() = make_column_pb(1, TTL_COL, "BIGINT", false, true,
                                             keys_type == KeysType::DUP_KEYS ? "NONE"
                                                                             : "REPLACE",
                                             -1, "NULL");
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
    *schema_pb.add_column() = make_column_pb(1, "event_time", "DATETIMEV2", false, true,
                                             "REPLACE", 6, source_default);
    *schema_pb.add_column() = make_column_pb(2, TTL_COL, "DATETIMEV2", false, true, "REPLACE",
                                             6, "NULL");
    schema_pb.set_ttl_col_idx(2);
    schema_pb.set_row_ttl_duration_us(7);

    TabletSchema schema;
    schema.init_from_pb(schema_pb);
    return schema;
}

TabletSchema make_ttl_rollup_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::UNIQUE_KEYS);
    *schema_pb.add_column() = make_column_pb(0, "k", "INT", true, false);
    *schema_pb.add_column() = make_column_pb(2, TTL_COL, "DATETIMEV2", false, true, "REPLACE",
                                             6, "NULL");
    schema_pb.set_ttl_col_idx(1);
    schema_pb.set_row_ttl_duration_us(7);

    TabletSchema schema;
    schema.init_from_pb(schema_pb);
    return schema;
}

void expect_expiration(RowTtlOperation operation, int64_t input, int64_t now_us, int64_t expected) {
    auto result = calculate_row_ttl_expiration_us(operation, input, now_us);
    ASSERT_TRUE(result.has_value()) << result.error();
    ASSERT_TRUE(result->has_value());
    EXPECT_EQ(**result, expected);
}

void expect_converted_time(const TabletColumn& source_column, const std::string& source_value,
                           const std::string& timezone, int64_t expected) {
    auto expiration = convert_row_ttl_time_to_epoch_us(source_column, source_value, timezone);
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

int64_t to_query_now_us(const ColumnDateTimeV2& values, size_t row, int64_t duration_us) {
    int64_t query_now_us = 0;
    EXPECT_TRUE(calculate_row_ttl_expiration_us(values, FieldType::OLAP_FIELD_TYPE_DATETIMEV2,
                                                row, cctz::local_time_zone(), duration_us,
                                                &query_now_us)
                        .ok());
    return query_now_us;
}

std::vector<uint8_t> execute_row_ttl_is_visible(ColumnPtr ttl_column, DataTypePtr ttl_type,
                                                ColumnPtr duration_column,
                                                int64_t query_now_us) {
    DataTypePtr duration_type = std::make_shared<DataTypeInt64>();
    DataTypePtr return_type = std::make_shared<DataTypeUInt8>();
    ColumnsWithTypeAndName argument_template = {{nullptr, ttl_type, "ttl"},
                                                {nullptr, duration_type, "duration"}};
    SimpleFunctionFactory factory;
    register_function_row_ttl(factory);
    FunctionBasePtr function = factory.get_function("row_ttl_is_visible", argument_template,
                                                    return_type, {},
                                                    BeExecVersionManager::get_newest_version());
    EXPECT_TRUE(function);

    TQueryGlobals globals;
    globals.__set_timestamp_ms(query_now_us / 1'000'000 * 1'000);
    globals.__set_nano_seconds(query_now_us % 1'000'000 * 1'000);
    globals.__set_time_zone("UTC");
    MockRuntimeState state(globals);
    std::unique_ptr<FunctionContext> context = FunctionContext::create_context(
            &state, return_type, {ttl_type, duration_type});

    Block block;
    block.insert({std::move(ttl_column), ttl_type, "ttl"});
    block.insert({std::move(duration_column), duration_type, "duration"});
    block.insert({return_type->create_column(), return_type, "result"});
    Status status = function->execute(context.get(), block, {0, 1}, 2, block.rows());
    EXPECT_TRUE(status.ok()) << status.to_string();

    const auto& result = assert_cast<const ColumnUInt8&>(*block.get_by_position(2).column);
    return {result.get_data().begin(), result.get_data().end()};
}

} // namespace

TEST(RowTtlTest, CalculateSingleRowOperationsInMicroseconds) {
    constexpr int64_t now_us = 10'000'123;
    expect_expiration(RowTtlOperation::EXPIRE, 2, now_us, 12'000'123);
    expect_expiration(RowTtlOperation::PEXPIRE, 2, now_us, 10'002'123);
    expect_expiration(RowTtlOperation::EXPIREAT, 2, now_us, 2'000'000);
    expect_expiration(RowTtlOperation::PEXPIREAT, 2, now_us, 2'000);

    auto persist = calculate_row_ttl_expiration_us(RowTtlOperation::PERSIST, std::nullopt, now_us);
    ASSERT_TRUE(persist.has_value()) << persist.error();
    EXPECT_FALSE(persist->has_value());

    expect_expiration(RowTtlOperation::EXPIRE, 0, now_us, now_us);
    expect_expiration(RowTtlOperation::PEXPIRE, -1, now_us, now_us - 1'000);
}

TEST(RowTtlTest, RejectOverflowAndMissingOperands) {
    auto multiply_overflow = calculate_row_ttl_expiration_us(
            RowTtlOperation::EXPIREAT, std::numeric_limits<int64_t>::max(), 0);
    EXPECT_FALSE(multiply_overflow.has_value());

    auto add_overflow = calculate_row_ttl_expiration_us(RowTtlOperation::PEXPIRE, 1,
                                                        std::numeric_limits<int64_t>::max());
    EXPECT_FALSE(add_overflow.has_value());

    auto missing = calculate_row_ttl_expiration_us(RowTtlOperation::EXPIRE, std::nullopt, 0);
    EXPECT_FALSE(missing.has_value());
}

TEST(RowTtlTest, ConvertAllSupportedTemporalSources) {
    expect_converted_time(make_tablet_column("DATE"), "1970-01-02", "UTC", 86'400'000'000L);
    expect_converted_time(make_tablet_column("DATETIME"), "1970-01-01 00:00:01", "UTC",
                          1'000'000L);
    expect_converted_time(make_tablet_column("DATEV2"), "1970-01-02", "UTC",
                          86'400'000'000L);
    expect_converted_time(make_tablet_column("DATETIMEV2", 6), "1970-01-01 00:00:00.123456",
                          "UTC", 123'456L);
    expect_converted_time(make_tablet_column("TIMESTAMPTZ", 6),
                          "1970-01-01 00:00:00.123456+00:00", "Asia/Shanghai", 123'456L);

    auto immortal = convert_row_ttl_time_to_epoch_us(
            make_tablet_column("DATETIMEV2", 6), "NULL", "Asia/Shanghai");
    ASSERT_TRUE(immortal.has_value()) << immortal.error();
    EXPECT_FALSE(immortal->has_value());
}

TEST(RowTtlTest, RejectInvalidTemporalConversionInputs) {
    auto bad_timezone = convert_row_ttl_time_to_epoch_us(
            make_tablet_column("DATETIMEV2", 6), "1970-01-01 00:00:00", "invalid/timezone");
    EXPECT_FALSE(bad_timezone.has_value());

    auto bad_source_type = convert_row_ttl_time_to_epoch_us(make_tablet_column("INT"), "1", "UTC");
    EXPECT_FALSE(bad_source_type.has_value());

    auto values = ColumnDateTimeV2::create();
    values->insert_value(datetimev2_value(1970, 1, 2, 0, 0, 0, 0));
    int64_t expiration_us = 0;
    EXPECT_FALSE(calculate_row_ttl_expiration_us(*values, FieldType::OLAP_FIELD_TYPE_DATETIMEV2,
                                                 0, cctz::utc_time_zone(),
                                                 std::numeric_limits<int64_t>::max(),
                                                 &expiration_us)
                         .ok());
}

TEST(RowTtlTest, PartialUpdateMetadataTracksSourceAndDefaults) {
    TabletSchema rollup_schema = make_ttl_rollup_schema();
    TabletColumn source = make_tablet_column("DATETIMEV2", 6, false,
                                             "1970-01-01 08:00:00.123456");

    PartialUpdateInfo rollup_info;
    ASSERT_TRUE(rollup_info.init(1, 2, rollup_schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                                 PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0,
                                 "Asia/Shanghai", "", -1, -1, source.unique_id(), &source)
                        .ok());
    EXPECT_EQ(rollup_info.row_ttl_source_cid(), -1);
    EXPECT_EQ(rollup_info.row_ttl_source_uid(), source.unique_id());
    ASSERT_EQ(rollup_info.missing_cids, std::vector<uint32_t>({1}));
    ASSERT_EQ(rollup_info.default_values.size(), 1);
    EXPECT_EQ(rollup_info.default_values[0], "1970-01-01 08:00:00.123456");

    TabletSchema source_schema = make_ttl_write_schema("1970-01-01 08:00:00.654321");
    PartialUpdateInfo source_info;
    ASSERT_TRUE(source_info.init(1, 2, source_schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                                 PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0,
                                 "Asia/Shanghai", "", -1, -1, source_schema.column(1).unique_id(),
                                 &source_schema.column(1))
                        .ok());
    EXPECT_EQ(source_info.row_ttl_source_cid(), 1);
    ASSERT_EQ(source_info.missing_cids, std::vector<uint32_t>({1, 2}));
    ASSERT_EQ(source_info.default_values.size(), 2);
    EXPECT_EQ(source_info.default_values[0], "1970-01-01 08:00:00.654321");
    EXPECT_EQ(source_info.default_values[1], "NULL");

    TabletSchema direct_schema = make_ttl_schema(KeysType::UNIQUE_KEYS);
    PartialUpdateInfo direct_info;
    ASSERT_TRUE(direct_info.init(1, 2, direct_schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                                 PartialUpdateNewRowPolicyPB::APPEND, {"k"}, false, 0, 0, "UTC",
                                 "")
                        .ok());
    EXPECT_EQ(direct_info.row_ttl_source_cid(), -1);
    EXPECT_EQ(direct_info.row_ttl_source_uid(), -1);
    ASSERT_EQ(direct_info.missing_cids, std::vector<uint32_t>({1}));
    ASSERT_EQ(direct_info.default_values.size(), 1);
    EXPECT_EQ(direct_info.default_values[0], "NULL");
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

TEST(RowTtlTest, TemporalFilterUsesLocalTimezoneDurationAndInclusiveBoundary) {
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
    ASSERT_TRUE(calculate_row_ttl_expiration_us(nullable.get_nested_column(),
                                                FieldType::OLAP_FIELD_TYPE_DATETIMEV2, 1,
                                                cctz::local_time_zone(), 7, &boundary_us)
                        .ok());
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

    TabletSchemaPB current_pb;
    restored.to_schema_pb(&current_pb);
    EXPECT_TRUE(current_pb.has_row_ttl_duration_us());
    EXPECT_EQ(current_pb.row_ttl_duration_us(), 2'000'000);
    EXPECT_FALSE(current_pb.has_legacy_row_ttl_duration_seconds());

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

TEST(RowTtlTest, UpdateHooksSupportColumnAndPhysicalLocation) {
    Block block = make_ttl_block({1, 2}, {0, 0});
    MutableColumnPtr ttl = IColumn::mutate(block.get_by_position(0).column);
    ASSERT_TRUE(apply_row_ttl_update(ttl, 1, RowTtlOperation::PEXPIRE, 5, 10).ok());
    const auto& nullable = assert_cast<const ColumnNullable&>(*ttl);
    EXPECT_EQ(assert_cast<const ColumnInt64&>(nullable.get_nested_column()).get_data()[1], 5'010);
    EXPECT_EQ(nullable.get_null_map_data()[1], 0);

    ASSERT_TRUE(apply_row_ttl_update(ttl, 1, RowTtlOperation::PERSIST, std::nullopt, 10).ok());
    EXPECT_EQ(assert_cast<const ColumnNullable&>(*ttl).get_null_map_data()[1], 1);
    EXPECT_FALSE(apply_row_ttl_update(ttl, 3, RowTtlOperation::PEXPIRE, 5, 10).ok());

    auto not_nullable_values = ColumnInt64::create();
    not_nullable_values->get_data().assign({0});
    MutableColumnPtr not_nullable = std::move(not_nullable_values);
    EXPECT_FALSE(apply_row_ttl_update(not_nullable, 0, RowTtlOperation::PEXPIRE, 5, 10).ok());

    auto wrong_values = ColumnInt32::create();
    wrong_values->get_data().assign({0});
    auto wrong_nulls = ColumnUInt8::create();
    wrong_nulls->get_data().assign({0});
    MutableColumnPtr wrong_nested =
            ColumnNullable::create(std::move(wrong_values), std::move(wrong_nulls));
    EXPECT_FALSE(apply_row_ttl_update(wrong_nested, 0, RowTtlOperation::PEXPIRE, 5, 10).ok());

    RowLocation expected_location(3, 7);
    bool called = false;
    auto writer = [&](const RowLocation& location, std::optional<int64_t> expiration) {
        called = true;
        EXPECT_EQ(location, expected_location);
        EXPECT_EQ(expiration, 2'000'000);
        return Status::OK();
    };
    ASSERT_TRUE(
            apply_row_ttl_update(expected_location, RowTtlOperation::EXPIREAT, 2, 0, writer).ok());
    EXPECT_TRUE(called);
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
    auto direct_result = execute_row_ttl_is_visible(
            std::move(ttl), std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>()),
            std::move(direct_duration), query_now_us);
    EXPECT_EQ(direct_result, std::vector<uint8_t>({0, 0, 1, 1}));

    auto temporal_values = ColumnDateTimeV2::create();
    temporal_values->insert_value(datetimev2_value(2020, 1, 1, 0, 0, 0, 0));
    temporal_values->insert_value(datetimev2_value(2020, 1, 1, 0, 0, 0, 1));
    temporal_values->insert_value(datetimev2_value(2020, 1, 1, 0, 0, 0, 0));
    auto temporal_nulls = ColumnUInt8::create();
    temporal_nulls->get_data().assign({0, 0, 1});
    int64_t temporal_query_now_us = to_query_now_us(*temporal_values, 0, 7);
    auto temporal_ttl =
            ColumnNullable::create(std::move(temporal_values), std::move(temporal_nulls));
    auto temporal_duration_value = ColumnInt64::create();
    temporal_duration_value->insert_value(7);
    ColumnPtr temporal_duration = ColumnConst::create(std::move(temporal_duration_value), 3);
    auto temporal_result = execute_row_ttl_is_visible(
            std::move(temporal_ttl),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTimeV2>(6)),
            std::move(temporal_duration), temporal_query_now_us);
    EXPECT_EQ(temporal_result, std::vector<uint8_t>({0, 1, 1}));
}

TEST(RowTtlTest, CompactionGcSafetyKeepsOnlySafeReaders) {
    TabletSchema non_ttl = make_non_ttl_schema(KeysType::DUP_KEYS);
    EXPECT_FALSE(should_gc_row_ttl(non_ttl, false, ReaderType::READER_FULL_COMPACTION,
                                   Version(0, 3)));

    TabletSchema agg = make_ttl_schema(KeysType::AGG_KEYS);
    EXPECT_FALSE(should_gc_row_ttl(agg, false, ReaderType::READER_FULL_COMPACTION, Version(0, 3)));

    TabletSchema dup = make_ttl_schema(KeysType::DUP_KEYS);
    EXPECT_TRUE(
            should_gc_row_ttl(dup, false, ReaderType::READER_CUMULATIVE_COMPACTION, Version(2, 3)));
    EXPECT_TRUE(should_gc_row_ttl(dup, false, ReaderType::READER_BASE_COMPACTION, Version(1, 3)));
    EXPECT_TRUE(should_gc_row_ttl(dup, false, ReaderType::READER_FULL_COMPACTION, Version(0, 3)));
    EXPECT_TRUE(
            should_gc_row_ttl(dup, false, ReaderType::READER_SEGMENT_COMPACTION, Version(-1, 0)));
    EXPECT_FALSE(should_gc_row_ttl(dup, false, ReaderType::READER_BINLOG_COMPACTION,
                                   Version(0, 3)));
    EXPECT_FALSE(should_gc_row_ttl(dup, false, ReaderType::READER_COLD_DATA_COMPACTION,
                                   Version(0, 3)));
    EXPECT_FALSE(should_gc_row_ttl(dup, false, ReaderType::READER_ALTER_TABLE, Version(0, 3)));

    TabletSchema unique = make_ttl_schema(KeysType::UNIQUE_KEYS);
    EXPECT_TRUE(should_gc_row_ttl(unique, true, ReaderType::READER_CUMULATIVE_COMPACTION,
                                  Version(2, 3)));
    EXPECT_TRUE(
            should_gc_row_ttl(unique, true, ReaderType::READER_BASE_COMPACTION, Version(1, 3)));
    EXPECT_TRUE(
            should_gc_row_ttl(unique, true, ReaderType::READER_FULL_COMPACTION, Version(0, 3)));
    EXPECT_TRUE(should_gc_row_ttl(unique, true, ReaderType::READER_SEGMENT_COMPACTION,
                                  Version(-1, 0)));

    EXPECT_FALSE(should_gc_row_ttl(unique, false, ReaderType::READER_CUMULATIVE_COMPACTION,
                                   Version(2, 3)));
    EXPECT_FALSE(should_gc_row_ttl(unique, false, ReaderType::READER_SEGMENT_COMPACTION,
                                   Version(-1, 0)));
    EXPECT_TRUE(
            should_gc_row_ttl(unique, false, ReaderType::READER_BASE_COMPACTION, Version(0, 3)));
    EXPECT_FALSE(
            should_gc_row_ttl(unique, false, ReaderType::READER_BASE_COMPACTION, Version(1, 3)));
    EXPECT_TRUE(
            should_gc_row_ttl(unique, false, ReaderType::READER_FULL_COMPACTION, Version(0, 3)));
}

} // namespace doris
