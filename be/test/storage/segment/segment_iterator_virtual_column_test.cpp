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

#include <memory>

#include "core/column/column_nothing.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "storage/olap_common.h"
#include "storage/schema.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_iterator.h"
#include "storage/tablet/tablet_schema.h"

namespace doris::segment_v2 {
namespace {

TabletSchemaSPtr make_tablet_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* column = schema_pb.add_column();
    column->set_unique_id(0);
    column->set_name("virtual_column");
    column->set_type("DOUBLE");
    column->set_is_key(true);
    column->set_is_nullable(true);
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);
    return tablet_schema;
}

} // namespace

TEST(SegmentIteratorVirtualColumnTest, MaterializationExpandsConstNullableResult) {
    auto tablet_schema = make_tablet_schema();
    auto segment = std::make_shared<Segment>(0, RowsetId(), tablet_schema, InvertedIndexFileInfo());
    auto read_schema = std::make_shared<::doris::ReadSchema>(tablet_schema->columns());
    SegmentIterator iterator(segment, read_schema);

    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>());
    auto expr = std::make_shared<VLiteral>(type, Field());
    iterator._virtual_column_exprs[0] = std::make_shared<VExprContext>(std::move(expr));
    iterator._selected_size = 4;

    Block block;
    block.insert({ColumnNothing::create(0), type, "virtual_column"});

    ASSERT_TRUE(iterator._materialization_of_virtual_column(&block).ok());
    const auto& result = block.get_by_position(0).column;
    EXPECT_FALSE(is_column_const(*result));
    const auto* nullable = check_and_get_column<ColumnNullable>(result.get());
    ASSERT_NE(nullable, nullptr);
    EXPECT_EQ(nullable->size(), 4);
    EXPECT_TRUE(nullable->only_null());
}

} // namespace doris::segment_v2
