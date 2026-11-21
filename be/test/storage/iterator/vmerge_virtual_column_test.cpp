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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "core/column/column_nothing.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "storage/iterator/block_reader.h"
#include "storage/iterator/vcollect_iterator.h"
#include "storage/iterator/vgeneric_iterators.h"
#include "storage/iterators.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
namespace {

// A segment returns already materialized columns, while the caller's first
// output block can still contain the virtual-slot placeholders from its tuple.
class MaterializedSegmentIterator final : public RowwiseIterator {
public:
    MaterializedSegmentIterator(ReadSchemaSPtr schema, Block data)
            : _schema(std::move(schema)), _data(std::move(data)) {}

    Status init(const StorageReadOptions&) override { return Status::OK(); }

    Status next_batch(Block* block) override {
        if (_offset == _data.rows()) {
            block->clear_column_data();
            return Status::EndOfFile("materialized segment exhausted");
        }
        const auto count = std::min(size_t {2}, _data.rows() - _offset);
        MutableColumns columns;
        for (const auto& column : _data.get_columns()) {
            columns.push_back(column->cut(_offset, count)->assert_mutable());
        }
        block->set_columns(std::move(columns));
        _offset += count;
        return Status::OK();
    }

    const ReadSchema& schema() const override { return *_schema; }

private:
    ReadSchemaSPtr _schema;
    Block _data;
    size_t _offset = 0;
};

// Keep the real segment merge and VCollectIterator on either side of the
// RowsetReader boundary, without involving a storage engine or tablet catalog.
class MergedRowsetReader final : public RowsetReader {
public:
    MergedRowsetReader(RowwiseIteratorUPtr iterator, StorageReadOptions opts)
            : _iterator(std::move(iterator)), _opts(std::move(opts)) {}

    Status init(RowsetReaderContext*, const RowSetSplits&) override {
        return _iterator->init(_opts);
    }
    Status next_batch(Block* block) override { return _iterator->next_batch(block); }
    Status next_batch(BlockView*) override { return Status::NotSupported("unused"); }
    Status next_batch(BlockWithSameBit*) override { return Status::NotSupported("unused"); }
    Status get_segment_iterators(RowsetReaderContext*, std::vector<RowwiseIteratorUPtr>*,
                                 bool) override {
        return Status::NotSupported("unused");
    }
    const ReadSchema& read_schema() const override { return _iterator->schema(); }
    void reset_read_options() override {}
    bool delete_flag() override { return false; }
    Version version() override { return {2, 2}; }
    RowsetSharedPtr rowset() override { return nullptr; }
    int64_t filtered_rows() override { return 0; }
    uint64_t merged_rows() override { return 0; }
    RowsetTypePB type() const override { return BETA_ROWSET; }
    int64_t newest_write_timestamp() override { return 0; }
    void update_profile(RuntimeProfile*) override {}
    RowsetReaderSharedPtr clone() override { return nullptr; }
    void set_topn_limit(size_t) override {}

private:
    RowwiseIteratorUPtr _iterator;
    StorageReadOptions _opts;
};

class VMergeVirtualColumnTest : public testing::TestWithParam<bool> {
protected:
    void SetUp() override {
        TabletSchemaPB pb;
        pb.set_keys_type(DUP_KEYS);
        pb.set_num_short_key_columns(1);

        auto* key = pb.add_column();
        key->set_unique_id(0);
        key->set_name("k");
        key->set_type("INT");
        key->set_length(4);
        key->set_index_length(4);
        key->set_is_key(true);
        key->set_is_nullable(false);

        auto* virtual_flag = pb.add_column();
        virtual_flag->set_unique_id(1);
        virtual_flag->set_name("virtual_1");
        virtual_flag->set_type("BOOLEAN");
        virtual_flag->set_length(1);
        virtual_flag->set_index_length(1);
        virtual_flag->set_is_key(false);
        virtual_flag->set_is_nullable(true);

        auto* virtual_value = pb.add_column();
        virtual_value->set_unique_id(2);
        virtual_value->set_name("virtual_2");
        virtual_value->set_type("INT");
        virtual_value->set_length(4);
        virtual_value->set_index_length(4);
        virtual_value->set_is_key(false);
        virtual_value->set_is_nullable(false);

        auto tablet_schema = std::make_shared<TabletSchema>();
        tablet_schema->init_from_pb(pb);
        _schema = std::make_shared<ReadSchema>(tablet_schema->columns());
    }

    Block data(std::vector<int32_t> keys, bool reverse) const {
        if (reverse) {
            std::ranges::reverse(keys);
        }
        auto block = _schema->create_read_block();
        {
            auto columns_guard = block.mutate_columns_scoped();
            auto& columns = columns_guard.mutable_columns();
            for (auto key : keys) {
                assert_cast<ColumnInt32&>(*columns[0]).insert_value(key);
                auto& nullable = assert_cast<ColumnNullable&>(*columns[1]);
                if (key == 3 || key == 4) {
                    nullable.insert_default();
                } else {
                    assert_cast<ColumnUInt8&>(nullable.get_nested_column()).insert_value(key % 2);
                    nullable.get_null_map_data().push_back(0);
                }
                assert_cast<ColumnInt32&>(*columns[2]).insert_value(key * 10);
            }
        }
        return block;
    }

    Block output() const {
        auto block = _schema->create_read_block();
        block.replace_by_position(1, ColumnNothing::create(0));
        block.replace_by_position(2, ColumnNothing::create(0));
        return block;
    }

    StorageReadOptions options() {
        StorageReadOptions opts(_stats);
        opts.block_row_max = 2;
        opts.read_orderby_key_columns = &_sort_columns;
        return opts;
    }

    RowwiseIteratorUPtr merge(bool reverse) {
        std::vector<RowwiseIteratorUPtr> inputs;
        inputs.push_back(
                std::make_unique<MaterializedSegmentIterator>(_schema, data({1, 3, 5}, reverse)));
        inputs.push_back(
                std::make_unique<MaterializedSegmentIterator>(_schema, data({2, 4, 6}, reverse)));
        return new_merge_iterator(std::move(inputs), -1, false, reverse, &_merged_rows, _schema);
    }

    void check_values(const Block& block) const {
        ASSERT_NE(check_and_get_column<ColumnNullable>(block.get_by_position(1).column.get()),
                  nullptr);
        ASSERT_NE(check_and_get_column<ColumnInt32>(block.get_by_position(2).column.get()),
                  nullptr);
        ASSERT_TRUE(block.check_type_and_column().ok());
        const auto& keys = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
        const auto& nullable = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        const auto& flags = assert_cast<const ColumnUInt8&>(nullable.get_nested_column());
        const auto& values = assert_cast<const ColumnInt32&>(*block.get_by_position(2).column);
        ASSERT_EQ(nullable.size(), block.rows());
        ASSERT_EQ(values.size(), block.rows());
        for (size_t i = 0; i < block.rows(); ++i) {
            const auto key = keys.get_element(i);
            EXPECT_EQ(nullable.is_null_at(i), key == 3 || key == 4);
            if (!nullable.is_null_at(i)) {
                EXPECT_EQ(flags.get_element(i), key % 2);
            }
            EXPECT_EQ(values.get_element(i), key * 10);
        }
    }

    ReadSchemaSPtr _schema;
    std::vector<uint32_t> _sort_columns {0};
    uint64_t _merged_rows = 0;
    OlapReaderStatistics _stats;
};

TEST_P(VMergeVirtualColumnTest, MaterializesDestinationAndPreservesValuesAcrossBatches) {
    const bool reverse = GetParam();
    auto iter = merge(reverse);
    ASSERT_TRUE(iter->init(options()).ok());
    auto block = output();
    std::vector<int32_t> actual;
    bool eof = false;
    for (int batch = 0; batch < 6 && !eof; ++batch) {
        block.clear_column_data();
        auto status = iter->next_batch(&block);
        ASSERT_TRUE(status.ok() || status.is<ErrorCode::END_OF_FILE>()) << status;
        if (block.rows() > 0) {
            ASSERT_NO_FATAL_FAILURE(check_values(block));
            const auto& keys = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
            for (size_t i = 0; i < block.rows(); ++i) {
                actual.push_back(keys.get_element(i));
            }
        }
        eof = status.is<ErrorCode::END_OF_FILE>();
    }
    EXPECT_TRUE(eof);
    auto expected = std::vector<int32_t> {1, 2, 3, 4, 5, 6};
    if (reverse) {
        std::ranges::reverse(expected);
    }
    EXPECT_EQ(actual, expected);
}

TEST_P(VMergeVirtualColumnTest, MaterializesDestinationForBlockWithSameBit) {
    const bool reverse = GetParam();
    auto iter = merge(reverse);
    ASSERT_TRUE(iter->init(options()).ok());
    auto block = output();
    std::vector<bool> same_bit;
    BlockWithSameBit output_with_same_bit {.block = &block, .same_bit = same_bit};
    auto status = iter->next_batch(&output_with_same_bit);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(block.rows(), 2);
    ASSERT_EQ(output_with_same_bit.same_bit.size(), block.rows());
    ASSERT_NO_FATAL_FAILURE(check_values(block));
    const auto& keys = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    EXPECT_EQ(keys.get_element(0), reverse ? 6 : 1);
    EXPECT_EQ(keys.get_element(1), reverse ? 5 : 2);
}

TEST_P(VMergeVirtualColumnTest, TopnConsumesMergedVirtualColumns) {
    const bool reverse = GetParam();
    BlockReader reader;
    reader._reader_context.read_orderby_key_columns = &_sort_columns;
    reader._reader_context.read_orderby_key_reverse = reverse;
    auto block = output();
    // The collector uses these expression types to initialize its candidate block;
    // the rowset read buffer still starts with ColumnNothing placeholders.
    for (auto ordinal : {1, 2}) {
        const auto& type = block.get_by_position(ordinal).type;
        auto expr = std::make_shared<VLiteral>(
                type, ordinal == 1 ? Field() : Field::create_field<TYPE_INT>(0));
        reader._reader_context.virtual_column_exprs[ordinal] =
                std::make_shared<VExprContext>(std::move(expr));
    }
    VCollectIterator collector;
    collector._reader = &reader;
    collector._topn_limit = 3;
    collector._is_reverse = reverse;
    auto rowset_reader = std::make_shared<MergedRowsetReader>(merge(reverse), options());
    ASSERT_TRUE(collector.add_child(RowSetSplits(rowset_reader)).ok());
    auto status = collector.next(&block);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(block.rows(), 3);
    ASSERT_NO_FATAL_FAILURE(check_values(block));
    const auto& keys = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(keys.get_element(i), reverse ? 6 - static_cast<int>(i) : 1 + static_cast<int>(i));
    }
}

INSTANTIATE_TEST_SUITE_P(SortOrder, VMergeVirtualColumnTest, testing::Bool());

} // namespace
} // namespace doris
