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
#include <string>

#include "core/block/adaptive_block_size_predictor.h"
#include "core/block/block.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "storage/schema.h"
#include "storage/segment/mock/mock_segment.h"
#include "storage/segment/segment_iterator.h"

namespace doris::segment_v2 {

class SegmentIteratorBlockSizeTest : public testing::Test {
protected:
    static constexpr size_t kSegmentRows = 8192;
    static constexpr size_t kVariantBytesPerRow = 1024 * 1024;
    static constexpr size_t kBlockBytes = 8 * 1024 * 1024;
    static constexpr size_t kMaxRows = 8160;

    std::unique_ptr<SegmentIterator> make_iterator(const std::string& value_type) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        auto* key = schema_pb.add_column();
        key->set_unique_id(1);
        key->set_name("k");
        key->set_type("INT");
        key->set_is_key(true);
        key->set_is_nullable(false);
        auto* value = schema_pb.add_column();
        value->set_unique_id(2);
        value->set_name("v");
        value->set_type(value_type);
        value->set_is_nullable(true);

        auto tablet_schema = std::make_shared<TabletSchema>();
        tablet_schema->init_from_pb(schema_pb);
        auto segment = std::make_shared<MockSegment>(tablet_schema);
        EXPECT_CALL(*segment, num_rows()).WillRepeatedly(testing::Return(kSegmentRows));
        segment->set_column_raw_data_bytes(1, kSegmentRows * sizeof(int32_t));
        segment->set_column_raw_data_bytes(2, kSegmentRows * kVariantBytesPerRow);
        auto read_schema = std::make_shared<ReadSchema>(tablet_schema->columns());
        auto iterator = std::make_unique<SegmentIterator>(segment, read_schema);
        iterator->_opts.preferred_block_size_bytes = kBlockBytes;
        iterator->_opts.block_row_max = kMaxRows;
        return iterator;
    }
};

TEST_F(SegmentIteratorBlockSizeTest, WideVariantDoesNotShrinkFullyFilteredBatches) {
    auto iterator = make_iterator("VARIANT");
    auto predictor = iterator->_make_block_size_predictor();
    ASSERT_NE(predictor, nullptr);
    Block empty;
    for (int batch = 0; batch < 4; ++batch) {
        // A filtered batch does not materialize the projected VARIANT or supply history.
        EXPECT_EQ(predictor->predict_next_rows(), AdaptiveBlockSizePredictor::kDefaultProbeRows);
        predictor->update(empty);
    }
    EXPECT_FALSE(predictor->has_history_for_test());
    // Excluding VARIANT from the hint must not discard its diagnostic footer statistics.
    EXPECT_EQ(iterator->_segment->column_raw_data_bytes(2), kSegmentRows * kVariantBytesPerRow);
}

TEST_F(SegmentIteratorBlockSizeTest, ScalarFooterHintStillLimitsProbeRows) {
    auto iterator = make_iterator("STRING");
    auto predictor = iterator->_make_block_size_predictor();
    ASSERT_NE(predictor, nullptr);
    EXPECT_EQ(predictor->predict_next_rows(),
              kBlockBytes / (kVariantBytesPerRow + sizeof(int32_t)));
}

TEST_F(SegmentIteratorBlockSizeTest, MaterializedBlockStillUpdatesVariantPrediction) {
    auto iterator = make_iterator("VARIANT");
    auto predictor = iterator->_make_block_size_predictor();
    ASSERT_NE(predictor, nullptr);
    auto column = ColumnVector<TYPE_INT>::create();
    column->insert_value(1);
    Block block;
    block.insert({std::move(column), std::make_shared<DataTypeInt32>(), "k"});
    predictor->update(block);
    EXPECT_TRUE(predictor->has_history_for_test());
    EXPECT_EQ(predictor->predict_next_rows(), kMaxRows);
}

} // namespace doris::segment_v2
