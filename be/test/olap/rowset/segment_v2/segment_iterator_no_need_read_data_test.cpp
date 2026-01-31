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

#include "gtest/gtest.h"
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "olap/tablet_schema.h"

namespace doris::segment_v2 {

TEST(SegmentIteratorNoNeedReadDataTest, extracted_variant_count_on_index) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* root = schema_pb.add_column();
    root->set_unique_id(1);
    root->set_name("data");
    root->set_type("VARIANT");
    root->set_is_key(false);
    root->set_is_nullable(true);
    root->set_variant_max_subcolumns_count(3);
    root->set_variant_max_sparse_column_statistics_size(10000);
    root->set_variant_sparse_hash_shard_count(1);

    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);

    TabletColumn subcol =
            TabletColumn::create_materialized_variant_column("data", {"items", "content"}, 1, 3);
    tablet_schema->append_column(subcol, TabletSchema::ColumnType::VARIANT);

    const ColumnId subcol_cid = tablet_schema->field_index(*subcol.path_info_ptr());
    ASSERT_GE(subcol_cid, 0);

    auto read_schema = std::make_shared<Schema>(tablet_schema);
    SegmentIterator iter(nullptr, read_schema);
    iter._opts.tablet_schema = tablet_schema;
    iter._opts.push_down_agg_type_opt = TPushAggOp::COUNT_ON_INDEX;
    iter._need_read_data_indices[static_cast<uint32_t>(subcol_cid)] = false;
    iter._output_columns.emplace(1);

    EXPECT_FALSE(iter._need_read_data(subcol_cid));

    iter._opts.push_down_agg_type_opt = TPushAggOp::NONE;
    EXPECT_TRUE(iter._need_read_data(subcol_cid));
}

} // namespace doris::segment_v2
