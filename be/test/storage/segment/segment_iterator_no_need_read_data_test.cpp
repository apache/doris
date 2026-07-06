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
#include "storage/segment/segment_iterator.h"
#include "storage/tablet/tablet_schema.h"

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

    std::vector<ColumnId> read_column_ids(tablet_schema->num_columns());
    for (uint32_t cid = 0; cid < read_column_ids.size(); ++cid) {
        read_column_ids[cid] = cid;
    }
    auto read_schema = std::make_shared<Schema>(tablet_schema->columns(), read_column_ids);
    SegmentIterator iter(nullptr, read_schema);
    iter._opts.tablet_schema = tablet_schema;
    iter._opts.push_down_agg_type_opt = TPushAggOp::COUNT_ON_INDEX;
    iter._need_read_data_indices[static_cast<uint32_t>(subcol_cid)] = false;
    iter._output_columns.emplace(1);

    EXPECT_FALSE(iter._need_read_data(subcol_cid));

    iter._opts.push_down_agg_type_opt = TPushAggOp::NONE;
    EXPECT_TRUE(iter._need_read_data(subcol_cid));
}

TEST(SegmentIteratorNoNeedReadDataTest, zonemap_always_true_predicate_column) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* key = schema_pb.add_column();
    key->set_unique_id(1);
    key->set_name("k");
    key->set_type("INT");
    key->set_is_key(true);
    key->set_is_nullable(false);

    auto* pred_col = schema_pb.add_column();
    pred_col->set_unique_id(2);
    pred_col->set_name("event_time");
    pred_col->set_type("DATETIMEV2");
    pred_col->set_is_key(false);
    pred_col->set_is_nullable(false);

    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);

    std::vector<ColumnId> read_column_ids(tablet_schema->num_columns());
    for (uint32_t cid = 0; cid < read_column_ids.size(); ++cid) {
        read_column_ids[cid] = cid;
    }
    auto read_schema = std::make_shared<Schema>(tablet_schema->columns(), read_column_ids);
    SegmentIterator iter(nullptr, read_schema);
    iter._opts.tablet_schema = tablet_schema;
    iter._opts.zonemap_always_true_pred_cols.emplace(1);

    EXPECT_FALSE(iter._need_read_data(1));
    iter._opts.zonemap_always_true_pred_cols.emplace(0);
    EXPECT_TRUE(iter._need_read_data(0));

    iter._output_columns.emplace(2);
    EXPECT_TRUE(iter._need_read_data(1));

    iter._opts.push_down_agg_type_opt = TPushAggOp::COUNT_ON_INDEX;
    EXPECT_FALSE(iter._need_read_data(1));
}

} // namespace doris::segment_v2
