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

#include "exec/scan/predicate_lm_utils.h"

#include <gtest/gtest.h>

namespace doris {

static TabletSchemaSPtr build_tablet_schema_for_test() {
    auto schema = std::make_shared<TabletSchema>();

    {
        TabletColumn col(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                         FieldType::OLAP_FIELD_TYPE_INT);
        col.set_name("k");
        schema->append_column(col);
    }
    {
        TabletColumn col(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                         FieldType::OLAP_FIELD_TYPE_INT);
        col.set_name("a");
        schema->append_column(col);
    }
    {
        TabletColumn col(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                         FieldType::OLAP_FIELD_TYPE_INT);
        col.set_name("b");
        schema->append_column(col);
    }

    return schema;
}

TEST(PredicateLmUtilsTest, ParseEmptyString) {
    auto schema = build_tablet_schema_for_test();

    std::vector<ColumnId> cids;
    Status st = parse_predicate_lm_stage1_cols_to_column_ids("", schema, &cids);
    EXPECT_TRUE(st.ok()) << st.to_string();
    EXPECT_TRUE(cids.empty());
}

TEST(PredicateLmUtilsTest, ParseBasicColumns) {
    auto schema = build_tablet_schema_for_test();

    std::vector<ColumnId> cids;
    Status st = parse_predicate_lm_stage1_cols_to_column_ids("a,b", schema, &cids);
    EXPECT_TRUE(st.ok()) << st.to_string();

    // schema is [k,a,b] => a=1, b=2
    ASSERT_EQ(2u, cids.size());
    EXPECT_EQ(static_cast<ColumnId>(1), cids[0]);
    EXPECT_EQ(static_cast<ColumnId>(2), cids[1]);
}

TEST(PredicateLmUtilsTest, ParseTrimBackticksDedupAndCaseInsensitive) {
    auto schema = build_tablet_schema_for_test();

    std::vector<ColumnId> cids;
    Status st = parse_predicate_lm_stage1_cols_to_column_ids(" `A` , b , a ", schema, &cids);
    EXPECT_TRUE(st.ok()) << st.to_string();

    // Dedup + sorted
    ASSERT_EQ(2u, cids.size());
    EXPECT_EQ(static_cast<ColumnId>(1), cids[0]);
    EXPECT_EQ(static_cast<ColumnId>(2), cids[1]);
}

TEST(PredicateLmUtilsTest, ParseUnknownColumnsShouldFail) {
    auto schema = build_tablet_schema_for_test();

    std::vector<ColumnId> cids;
    Status st = parse_predicate_lm_stage1_cols_to_column_ids("a,not_exist", schema, &cids);
    EXPECT_FALSE(st.ok());
    EXPECT_EQ(ErrorCode::INVALID_ARGUMENT, st.code());
    EXPECT_TRUE(st.to_string().find("predicate_lm_stage1_cols") != std::string::npos)
            << st.to_string();
    EXPECT_TRUE(st.to_string().find("not_exist") != std::string::npos) << st.to_string();
}

} // namespace doris
