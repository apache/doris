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

#include "common/consts.h"
#include "core/assert_cast.h"
#include "core/column/column_vector.h"
#include "storage/segment/column_reader_cache.h"
#include "storage/segment/row_ranges.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_iterator.h"
#include "storage/segment/virtual_column_iterator.h"

namespace doris::segment_v2 {

class VariantProjectionInitTest : public testing::Test {
protected:
    void SetUp() override {
        _schema = std::make_shared<TabletSchema>();
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        _schema->init_from_pb(schema_pb);
        TabletColumn key(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                         FieldType::OLAP_FIELD_TYPE_INT, true, 0, sizeof(int32_t));
        key.set_name("k");
        key.set_is_key(true);
        _schema->append_column(key);
        TabletColumn value(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                           FieldType::OLAP_FIELD_TYPE_VARIANT, true, 1, 0);
        value.set_name("v");
        value.set_variant_max_subcolumns_count(2048);
        _schema->append_column(value);

        RowsetId rowset_id;
        rowset_id.init(1);
        _segment = std::shared_ptr<Segment>(new Segment(0, rowset_id, _schema));
        _segment->_num_rows = 8;
        // Model a segment predating both nullable columns. It needs no data file:
        // the actual default-value iterators supply NULL for surviving rows.
        ASSERT_TRUE(_segment->_create_column_meta_once_call
                            .call([&] { return _segment->_create_column_meta(SegmentFooterPB {}); })
                            .ok());
    }

    std::unique_ptr<SegmentIterator> make_iterator() {
        return std::make_unique<SegmentIterator>(
                _segment, std::make_shared<ReadSchema>(_schema->columns()), _opts);
    }

    TabletSchemaSPtr _schema;
    std::shared_ptr<Segment> _segment;
    OlapReaderStatistics _stats;
    StorageReadOptions _opts {_stats};
};

TEST_F(VariantProjectionInitTest, InitializeProjectionOnlyForSurvivingRows) {
    auto iterator = make_iterator();
    ASSERT_TRUE(iterator->init(_opts).ok());
    EXPECT_NE(iterator->_column_iterators[0], nullptr);
    EXPECT_EQ(iterator->_column_iterators[1], nullptr);
    auto block = iterator->schema().create_read_block();
    ASSERT_TRUE(iterator->_lazy_init(&block).ok());
    EXPECT_NE(iterator->_column_iterators[1], nullptr);
    EXPECT_EQ(iterator->_row_bitmap.cardinality(), 8);
    ASSERT_TRUE(iterator->next_batch(&block).ok());
    EXPECT_EQ(block.rows(), 8);
    EXPECT_TRUE(block.get_by_position(1).column->is_null_at(0));
}

TEST_F(VariantProjectionInitTest, DeletedRowsDoNotInitializeProjection) {
    auto deleted = std::make_shared<roaring::Roaring>();
    deleted->addRange(0, 8);
    _opts.delete_bitmap.emplace(0, deleted);
    auto iterator = make_iterator();
    ASSERT_TRUE(iterator->init(_opts).ok());
    auto block = iterator->schema().create_read_block();
    ASSERT_TRUE(iterator->_lazy_init(&block).ok());
    EXPECT_TRUE(iterator->_row_bitmap.isEmpty());
    EXPECT_EQ(iterator->_column_iterators[1], nullptr);
    EXPECT_TRUE(iterator->next_batch(&block).is<ErrorCode::END_OF_FILE>());
    EXPECT_EQ(block.rows(), 0);
}

TEST_F(VariantProjectionInitTest, RowRangesDoNotInitializeExcludedProjection) {
    _opts.row_ranges = RowRanges::create_single(10, 12);
    auto iterator = make_iterator();
    ASSERT_TRUE(iterator->init(_opts).ok());
    auto block = iterator->schema().create_read_block();
    ASSERT_TRUE(iterator->_lazy_init(&block).ok());
    EXPECT_TRUE(iterator->_row_bitmap.isEmpty());
    EXPECT_EQ(iterator->_column_iterators[1], nullptr);
    EXPECT_TRUE(iterator->next_batch(&block).is<ErrorCode::END_OF_FILE>());
    EXPECT_EQ(block.rows(), 0);
}

TEST_F(VariantProjectionInitTest, ConditionColumnIsInitializedBeforePruning) {
    _opts.col_id_to_predicates.emplace(1, AndBlockColumnPredicate::create_shared());
    auto iterator = make_iterator();
    ASSERT_TRUE(iterator->init(_opts).ok());
    EXPECT_NE(iterator->_column_iterators[1], nullptr);
}

TEST_F(VariantProjectionInitTest, TopNRetainsEagerInitialization) {
    _opts.topn_filter_source_node_ids.push_back(0);
    auto iterator = make_iterator();
    ASSERT_TRUE(iterator->init(_opts).ok());
    EXPECT_NE(iterator->_column_iterators[1], nullptr);
}

TEST_F(VariantProjectionInitTest, PreserveMaterializedVirtualIterator) {
    _schema->mutable_column(0).set_name(BeConsts::VIRTUAL_COLUMN_PREFIX + "score");
    auto iterator = make_iterator();
    ASSERT_TRUE(iterator->init(_opts).ok());
    auto* virtual_iterator =
            assert_cast<VirtualColumnIterator*>(iterator->_column_iterators[0].get());
    auto values = ColumnInt32::create();
    values->insert_value(7);
    virtual_iterator->prepare_materialization(
            std::move(values),
            std::make_shared<std::vector<uint64_t>>(std::initializer_list<uint64_t> {3}));
    auto materialized_values = virtual_iterator->get_materialized_column();

    // Model a virtual iterator populated by index evaluation before projections
    // are initialized. Replacing it would discard both values and row labels.
    auto block = iterator->schema().create_read_block();
    ASSERT_TRUE(iterator->_lazy_init(&block).ok());
    EXPECT_EQ(iterator->_column_iterators[0].get(), virtual_iterator);
    EXPECT_EQ(virtual_iterator->get_materialized_column().get(), materialized_values.get());
    EXPECT_EQ(virtual_iterator->get_materialized_column()->get_int(0), 7);
    EXPECT_EQ(virtual_iterator->get_row_id_to_idx().at(3), 0);
}

} // namespace doris::segment_v2
