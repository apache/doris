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

#include "storage/partial_update_info.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "storage/tablet/tablet_schema.h"

namespace doris {
namespace {

TabletColumn make_column(std::string name, FieldType type, bool is_key,
                         const std::string& default_value = {}) {
    TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, type, false);
    column.set_name(std::move(name));
    column.set_is_key(is_key);
    if (!default_value.empty()) {
        column.set_default_value(default_value);
    }
    return column;
}

TEST(PartialUpdateInfoTest, MaterializesTimestampNsCurrentTimestampDefaults) {
    TabletSchema schema;
    schema.append_column(make_column("id", FieldType::OLAP_FIELD_TYPE_INT, true));
    schema.append_column(make_column("ts0", FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS, false,
                                     "CURRENT_TIMESTAMP"));
    schema.append_column(make_column("ts6", FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS, false,
                                     "CURRENT_TIMESTAMP(6)"));
    schema.append_column(make_column("ts7", FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS, false,
                                     "CURRENT_TIMESTAMP(7)"));
    schema.append_column(make_column("ts8", FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS, false,
                                     "CURRENT_TIMESTAMP(8)"));
    auto schema_change_ts9 = make_column("ts9", FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS, false,
                                         "2000-01-01 00:00:00.000000000");
    schema_change_ts9.set_default_value_expr("CURRENT_TIMESTAMP(9)");
    schema.append_column(std::move(schema_change_ts9));

    ColumnPB schema_change_column_pb;
    schema.column(5).to_schema_pb(&schema_change_column_pb);
    TabletColumn schema_change_column(schema_change_column_pb);
    EXPECT_EQ(schema_change_column.default_value(), "2000-01-01 00:00:00.000000000");
    EXPECT_EQ(schema_change_column.default_value_expr(), "CURRENT_TIMESTAMP(9)");

    PartialUpdateInfo utc_info;
    ASSERT_TRUE(utc_info.init(1, 2, schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                              PartialUpdateNewRowPolicyPB::APPEND, {"id"}, false, 1704164645000,
                              123456789, "UTC", "")
                        .ok());
    EXPECT_EQ(utc_info.default_values,
              (std::vector<std::string> {
                      "2024-01-02 03:04:05.000000000", "2024-01-02 03:04:05.123456000",
                      "2024-01-02 03:04:05.123456700", "2024-01-02 03:04:05.123456780",
                      "2024-01-02 03:04:05.123456789"}));

    PartialUpdateInfo shanghai_info;
    ASSERT_TRUE(shanghai_info
                        .init(1, 3, schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                              PartialUpdateNewRowPolicyPB::APPEND, {"id"}, false, 1704164645000,
                              123456789, "+08:00", "")
                        .ok());
    EXPECT_EQ(shanghai_info.default_values.back(), "2024-01-02 11:04:05.123456789");
}

} // namespace
} // namespace doris
