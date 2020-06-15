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

#include "olap/schema.h"
#include "olap/byte_buffer.h"
#include "olap/stream_name.h"
#include "olap/rowset/column_reader.h"
#include "olap/rowset/column_writer.h"
#include "olap/field.h"
#include "olap/olap_define.h"
#include "olap/olap_common.h"
#include "olap/row_cursor.h"
#include "olap/row_block.h"
#include "runtime/mem_pool.h"
#include "runtime/string_value.hpp"
#include "runtime/vectorized_row_batch.h"
#include "util/logging.h"

namespace doris {
class TestSchemaFieldInit : public testing::Test {
public:
    void AddTabletSchemaColumn(int uid,
                               const std::string& name,
                               const std::string& type,
                               const std::string& aggregation,
                               uint32_t length,
                               bool is_allow_null,
                               bool is_key,
                               TabletSchemaPB* tablet_schema_pb) {
        ColumnPB* column = tablet_schema_pb->add_column();
        column->set_unique_id(uid);
        column->set_name(name);
        column->set_type(type);
        column->set_is_key(is_key);
        column->set_is_nullable(is_allow_null);
        column->set_length(length);
        column->set_aggregation(aggregation);
    }

    void CreateTabletSchema(TabletSchema* tablet_schema, TabletSchemaPB* tablet_schema_pb) {
        tablet_schema->init_from_pb(*tablet_schema_pb);
    }

    void test_invalid_schema_create(TabletSchema* tablet_schema, TabletSchemaPB* tablet_schema_pb) {
        CreateTabletSchema(tablet_schema, tablet_schema_pb);
        Schema schema;
        ASSERT_EQ(OLAP_ERR_SCHEMA_SCHEMA_FIELD_INVALID, schema.init(*tablet_schema));
    }

    void test_normal_schema_create(TabletSchema* tablet_schema, TabletSchemaPB* tablet_schema_pb) {
        CreateTabletSchema(tablet_schema, tablet_schema_pb);
        Schema schema;
        ASSERT_EQ(OLAP_SUCCESS, schema.init(*tablet_schema));
    }

};

TEST_F(TestSchemaFieldInit, test_agg_schema) {
    TabletSchema agg_schema;
    TabletSchemaPB agg_schema_pb;
    AddTabletSchemaColumn(0, "olap_date", "INT", "", 4, false, true, &agg_schema_pb);
    // Min Aggregate Function
    AddTabletSchemaColumn(1, "olap_tinyint", "TINYINT", "MIN", 1, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(2, "olap_smallint", "SMALLINT", "MIN", 2, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(3, "olap_int", "INT", "MIN", 4, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(4, "olap_bigint", "BIGINT", "MIN", 8, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(5, "olap_largeint", "LARGEINT", "MIN", 16, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(6, "olap_float", "FLOAT", "MIN", 4, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(7, "olap_double", "DOUBLE", "MIN", 8, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(8, "olap_decimal", "DECIMAL", "MIN", 12, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(9, "olap_date", "DATE", "MIN", 3, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(10, "olap_datetime", "DATETIME", "MIN", 8, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(11, "olap_char", "CHAR", "MIN", 4, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(12, "olap_varchar", "VARCHAR", "MIN", 4, false, false, &agg_schema_pb);

    // Max Aggregate Function
    AddTabletSchemaColumn(13, "olap_tinyint2", "TINYINT", "MAX", 1, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(14, "olap_smallint2", "SMALLINT", "MAX", 2, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(15, "olap_int2", "INT", "MAX", 4, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(16, "olap_bigint2", "BIGINT", "MAX", 8, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(17, "olap_largeint2", "LARGEINT", "MAX", 16, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(18, "olap_float2", "FLOAT", "MAX", 4, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(19, "olap_double2", "DOUBLE", "MAX", 8, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(20, "olap_decimal2", "DECIMAL", "MAX", 12, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(21, "olap_date2", "DATE", "MAX", 3, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(22, "olap_datetime2", "DATETIME", "MAX", 8, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(23, "olap_char2", "CHAR", "MAX", 4, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(24, "olap_varchar2", "VARCHAR", "MAX", 4, false, false, &agg_schema_pb);

    //Sum Aggregate Function
    AddTabletSchemaColumn(25, "olap_tinyint3", "TINYINT", "SUM", 1, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(26, "olap_smallint3", "SMALLINT", "SUM", 2, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(27, "olap_int3", "INT", "SUM", 4, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(28, "olap_bigint3", "BIGINT", "SUM", 8, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(29, "olap_largeint3", "LARGEINT", "SUM", 16, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(30, "olap_float3", "FLOAT", "SUM", 4, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(31, "olap_double3", "DOUBLE", "SUM", 8, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(32, "olap_decimal3", "DECIMAL", "SUM", 12, false, false, &agg_schema_pb);

    // REPLACE_IF_NOT_NULL Aggregate Function
    AddTabletSchemaColumn(33, "olap_tinyint4", "TINYINT", "REPLACE_IF_NOT_NULL", 1, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(34, "olap_smallint4", "SMALLINT", "REPLACE_IF_NOT_NULL", 2, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(35, "olap_int4", "INT", "REPLACE_IF_NOT_NULL", 4, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(36, "olap_bigint4", "BIGINT", "REPLACE_IF_NOT_NULL", 8, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(37, "olap_largeint4", "LARGEINT", "REPLACE_IF_NOT_NULL", 16, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(38, "olap_float4", "FLOAT", "REPLACE_IF_NOT_NULL", 4, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(39, "olap_double4", "DOUBLE", "REPLACE_IF_NOT_NULL", 8, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(40, "olap_decimal4", "DECIMAL", "REPLACE_IF_NOT_NULL", 12, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(41, "olap_date4", "DATE", "REPLACE_IF_NOT_NULL", 3, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(42, "olap_datetime4", "DATETIME", "REPLACE_IF_NOT_NULL", 8, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(43, "olap_char4", "CHAR", "REPLACE_IF_NOT_NULL", 4, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(44, "olap_varchar4", "VARCHAR", "REPLACE_IF_NOT_NULL", 4, false, false, &agg_schema_pb);

    // Hyperloglog Aggregate Function
    AddTabletSchemaColumn(45, "olap_hll5", "HLL", "HLL_UNION", 4, false, false, &agg_schema_pb);

    // Bitmap Aggregate Function
    AddTabletSchemaColumn(46, "olap_bitmap6", "OBJECT", "BITMAP_UNION", 4, false, false, &agg_schema_pb);
    AddTabletSchemaColumn(47, "olap_varchar6", "VARCHAR", "BITMAP_UNION", 4, false, false, &agg_schema_pb);

    test_normal_schema_create(&agg_schema, &agg_schema_pb);
}

TEST_F(TestSchemaFieldInit, test_agg_invalid_schema) {
    TabletSchema agg_schema;
    TabletSchemaPB agg_schema_pb;
    // Min Aggregate Function
    AddTabletSchemaColumn(0, "olap_bool", "BOOLEAN", "MIN", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_hll", "HLL", "MIN", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_struct", "STRUCT", "MIN", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_list", "LIST", "MIN", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_map", "MAP", "MIN", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_object", "OBJECT", "MIN", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);

    // MAX Aggregate Function
    AddTabletSchemaColumn(0, "olap_bool1", "BOOLEAN", "MAX", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_hll1", "HLL", "MAX", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_struct1", "STRUCT", "MAX", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_list1", "LIST", "MAX", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_map1", "MAP", "MAX", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_object1", "OBJECT", "MAX", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);

    // SUM Aggregate Function
    AddTabletSchemaColumn(0, "olap_bool2", "BOOLEAN", "SUM", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_hll2", "HLL", "SUM", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_struct2", "STRUCT", "SUM", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_list2", "LIST", "SUM", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_map2", "MAP", "SUM", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_object2", "OBJECT", "SUM", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_date2", "DATE", "SUM", 3, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_datetime2", "DATETIME", "SUM", 8, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_char2", "CHAR", "SUM", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_varchar2", "VARCHAR", "SUM", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);

    // REPLACE Aggregate Function
    AddTabletSchemaColumn(0, "olap_bool1", "BOOLEAN", "REPLACE", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_hll1", "HLL", "REPLACE", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_struct1", "STRUCT", "REPLACE", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_list1", "LIST", "REPLACE", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_map1", "MAP", "REPLACE", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_object1", "OBJECT", "REPLACE", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);

    // REPLACE_IF_NOT_NULL Aggregate Function
    AddTabletSchemaColumn(0, "olap_bool1", "BOOLEAN", "REPLACE_IF_NOT_NULL", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_hll1", "HLL", "REPLACE_IF_NOT_NULL", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_struct1", "STRUCT", "REPLACE_IF_NOT_NULL", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_list1", "LIST", "REPLACE_IF_NOT_NULL", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_map1", "MAP", "REPLACE_IF_NOT_NULL", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_object1", "OBJECT", "REPLACE_IF_NOT_NULL", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);

    // Hyperloglog Aggregate Function
    AddTabletSchemaColumn(0, "olap_tinyint", "TINYINT", "HLL_UNION", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_smallint", "SMALLINT", "HLL_UNION", 2, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_int", "INT", "HLL_UNION", 4, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_bigint", "BIGINT", "HLL_UNION", 8, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_largeint", "LARGEINT", "HLL_UNION", 16, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_float", "FLOAT", "HLL_UNION", 4, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_double", "DOUBLE", "HLL_UNION", 8, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_decimal", "DECIMAL", "HLL_UNION", 12, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_date", "DATE", "HLL_UNION", 3, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_datetime", "DATETIME", "HLL_UNION", 8, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_char", "CHAR", "HLL_UNION", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_varchar", "VARCHAR", "HLL_UNION", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_bool", "BOOLEAN", "HLL_UNION", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_struct", "STRUCT", "HLL_UNION", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_list", "LIST", "HLL_UNION", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_map", "MAP", "HLL_UNION", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_object", "OBJECT", "HLL_UNION", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);

    // Bitmap Aggregate Function
    AddTabletSchemaColumn(0, "olap_tinyint", "TINYINT", "BITMAP_UNION", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_smallint", "SMALLINT", "BITMAP_UNION", 2, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_int", "INT", "BITMAP_UNION", 4, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_bigint", "BIGINT", "BITMAP_UNION", 8, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_largeint", "LARGEINT", "BITMAP_UNION", 16, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_float", "FLOAT", "BITMAP_UNION", 4, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_double", "DOUBLE", "BITMAP_UNION", 8, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_decimal", "DECIMAL", "BITMAP_UNION", 12, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_date", "DATE", "BITMAP_UNION", 3, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_datetime", "DATETIME", "BITMAP_UNION", 8, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_char", "CHAR", "BITMAP_UNION", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_bool", "BOOLEAN", "BITMAP_UNION", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_struct", "STRUCT", "BITMAP_UNION", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_list", "LIST", "BITMAP_UNION", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_map", "MAP", "BITMAP_UNION", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
    AddTabletSchemaColumn(0, "olap_hll", "HLL", "BITMAP_UNION", 1, false, false, &agg_schema_pb);
    test_invalid_schema_create(&agg_schema, &agg_schema_pb);
}
}

int main(int argc, char** argv) {
    std::string conf_file = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conf_file.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    int ret = doris::OLAP_SUCCESS;
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
    return ret;
}

