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

#include "storage/tablet/tablet_schema.h"

namespace doris {

TEST(PartialUpdateInfoTest, CurrentTimestampUsesOriginalDefaultExpression) {
    TabletSchema tablet_schema;

    TabletColumn key_column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                            FieldType::OLAP_FIELD_TYPE_INT, false, 0, 4);
    key_column.set_name("id");
    key_column.set_is_key(true);
    key_column.set_index_length(4);
    tablet_schema.append_column(key_column);

    TabletColumn timestamp_column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                  FieldType::OLAP_FIELD_TYPE_DATETIMEV2, true, 1, 8);
    timestamp_column.set_name("created_at");
    timestamp_column.set_index_length(8);
    timestamp_column.set_default_value("2023-01-01 00:00:00.000000");
    timestamp_column.set_default_value_expr("CURRENT_TIMESTAMP(6)");
    tablet_schema.append_column(timestamp_column);

    PartialUpdateInfo partial_update_info;
    ASSERT_TRUE(partial_update_info
                        .init(1, 1, tablet_schema, UniqueKeyUpdateModePB::UPDATE_FIXED_COLUMNS,
                              PartialUpdateNewRowPolicyPB::APPEND, {"id"}, false, 1704067200000,
                              123456000, "UTC", "")
                        .ok());

    ASSERT_EQ(1, partial_update_info.default_values.size());
    EXPECT_EQ("2024-01-01 00:00:00.123456", partial_update_info.default_values[0]);
    EXPECT_EQ("2023-01-01 00:00:00.000000", tablet_schema.column(1).default_value());
}

} // namespace doris
