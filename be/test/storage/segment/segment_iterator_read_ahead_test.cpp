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
#include <utility>
#include <vector>

#include "common/cast_set.h"
#include "common/status.h"
#include "storage/olap_common.h"
#include "storage/segment/column_read_ahead.h"
#include "storage/segment/column_reader.h"
#include "storage/tablet/tablet_schema.h"

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#define protected public
#include "storage/segment/segment_iterator.h"
#undef private
#undef protected
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

namespace doris::segment_v2 {
namespace {

struct RecordedReadAheadCall {
    std::vector<rowid_t> rowids;
    ColumnReadAheadRole role {ColumnReadAheadRole::EAGER};
    ColumnIterator::ReadPhase phase {ColumnIterator::ReadPhase::NORMAL};
    bool reverse {false};
};

class RecordingColumnIterator final : public ColumnIterator {
public:
    Status seek_to_ordinal(ordinal_t) override { return Status::OK(); }
    ordinal_t get_current_ordinal() const override { return 0; }

    Status prepare_read_ahead(const ColumnReadAheadRequest& request,
                              std::vector<ColumnReadAheadPlan>* plans) override {
        request.sanity_check();
        DORIS_CHECK(plans != nullptr);
        RecordedReadAheadCall call {.rowids = {},
                                    .role = request.role,
                                    .phase = _read_phase,
                                    .reverse = request.reverse};
        call.rowids.assign(request.current_rowids,
                           request.current_rowids + request.current_rowid_count);
        calls.push_back(std::move(call));
        if (!_prepare_status.ok()) {
            return _prepare_status;
        }
        plans->push_back({});
        return Status::OK();
    }

    void set_prepare_status(Status status) { _prepare_status = std::move(status); }
    ReadPhase phase() const { return _read_phase; }

    std::vector<RecordedReadAheadCall> calls;

private:
    Status _prepare_status;
};

TabletSchemaSPtr make_tablet_schema(size_t column_count) {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    for (size_t index = 0; index < column_count; ++index) {
        auto* column = schema_pb.add_column();
        column->set_unique_id(cast_set<int32_t>(index));
        column->set_name("c" + std::to_string(index));
        column->set_type("INT");
        column->set_is_key(index == 0);
        column->set_is_nullable(false);
    }
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);
    return tablet_schema;
}

ColumnReadAheadContext make_context() {
    return {.eager_options = {.high_watermark_bytes = 8 * 1024 * 1024,
                              .low_watermark_bytes = 4 * 1024 * 1024},
            .lazy_options = {.high_watermark_bytes = 256 * 1024,
                             .low_watermark_bytes = 128 * 1024}};
}

class SegmentIteratorReadAheadTest : public testing::Test {
protected:
    void SetUp() override {
        _tablet_schema = make_tablet_schema(3);
        _read_schema = std::make_shared<ReadSchema>(_tablet_schema->columns());
    }

    std::unique_ptr<SegmentIterator> make_iterator() {
        auto iterator = std::make_unique<SegmentIterator>(nullptr, _read_schema);
        iterator->_opts.tablet_schema = _tablet_schema;
        iterator->_column_read_ahead_context =
                std::make_unique<ColumnReadAheadContext>(make_context());
        iterator->_row_bitmap.addRange(0, 100);
        iterator->_block_rowids = {10, 20, 30};
        return iterator;
    }

    RecordingColumnIterator* set_column(SegmentIterator* iterator, ColumnId ordinal) {
        auto column = std::make_unique<RecordingColumnIterator>();
        auto* result = column.get();
        iterator->_column_iterators[ordinal] = std::move(column);
        return result;
    }

    TabletSchemaSPtr _tablet_schema;
    ReadSchemaSPtr _read_schema;
};

TEST_F(SegmentIteratorReadAheadTest, SubmitsAllPlannableRolesBeforeDecoding) {
    auto iterator = make_iterator();
    auto* predicate = set_column(iterator.get(), 0);
    auto* split_common_expr = set_column(iterator.get(), 1);
    auto* output = set_column(iterator.get(), 2);
    iterator->_is_need_short_eval = true;
    iterator->_is_need_expr_eval = true;
    iterator->_predicate_ordinals = {0};
    iterator->_common_expr_ordinals = {1};
    iterator->_output_ordinals = {2};
    iterator->_lazy_pruned_ordinals = {1};
    iterator->_opts.read_orderby_key_reverse = true;

    const auto plans = iterator->_plan_batch_read_ahead(3);

    ASSERT_EQ(predicate->calls.size(), 1);
    EXPECT_EQ(predicate->calls[0].role, ColumnReadAheadRole::EAGER);
    EXPECT_EQ(predicate->calls[0].phase, ColumnIterator::ReadPhase::NORMAL);
    EXPECT_TRUE(predicate->calls[0].reverse);
    EXPECT_EQ(predicate->calls[0].rowids, (std::vector<rowid_t> {10, 20, 30}));

    ASSERT_EQ(split_common_expr->calls.size(), 2);
    EXPECT_EQ(split_common_expr->calls[0].role, ColumnReadAheadRole::LAZY);
    EXPECT_EQ(split_common_expr->calls[0].phase, ColumnIterator::ReadPhase::PREDICATE);
    EXPECT_EQ(split_common_expr->calls[1].role, ColumnReadAheadRole::LAZY);
    EXPECT_EQ(split_common_expr->calls[1].phase, ColumnIterator::ReadPhase::LAZY);
    EXPECT_EQ(split_common_expr->phase(), ColumnIterator::ReadPhase::NORMAL);

    ASSERT_EQ(output->calls.size(), 1);
    EXPECT_EQ(output->calls[0].role, ColumnReadAheadRole::LAZY);
    EXPECT_EQ(output->calls[0].phase, ColumnIterator::ReadPhase::NORMAL);
    EXPECT_EQ(plans.size(), 4);
}

TEST_F(SegmentIteratorReadAheadTest, UsesEagerWindowForFirstCommonExpressionStage) {
    auto iterator = make_iterator();
    auto* common_expr = set_column(iterator.get(), 0);
    auto* output = set_column(iterator.get(), 1);
    set_column(iterator.get(), 2);
    iterator->_is_need_expr_eval = true;
    iterator->_common_expr_ordinals = {0};
    iterator->_output_ordinals = {1};

    const auto plans = iterator->_plan_batch_read_ahead(3);

    ASSERT_EQ(common_expr->calls.size(), 1);
    ASSERT_EQ(output->calls.size(), 1);
    EXPECT_EQ(common_expr->calls[0].role, ColumnReadAheadRole::EAGER);
    EXPECT_EQ(output->calls[0].role, ColumnReadAheadRole::LAZY);
    EXPECT_EQ(plans.size(), 2);
}

TEST_F(SegmentIteratorReadAheadTest, UsesEagerWindowWhenOutputHasNoDependency) {
    auto iterator = make_iterator();
    auto* first = set_column(iterator.get(), 0);
    auto* second = set_column(iterator.get(), 1);
    set_column(iterator.get(), 2);
    iterator->_output_ordinals = {0, 1};

    const auto plans = iterator->_plan_batch_read_ahead(3);

    ASSERT_EQ(first->calls.size(), 1);
    ASSERT_EQ(second->calls.size(), 1);
    EXPECT_EQ(first->calls[0].role, ColumnReadAheadRole::EAGER);
    EXPECT_EQ(second->calls[0].role, ColumnReadAheadRole::EAGER);
    EXPECT_EQ(plans.size(), 2);
}

TEST_F(SegmentIteratorReadAheadTest, PreparationFailureFallsBackAndKeepsOtherColumns) {
    auto iterator = make_iterator();
    auto* failed = set_column(iterator.get(), 0);
    auto* healthy = set_column(iterator.get(), 1);
    set_column(iterator.get(), 2);
    failed->set_prepare_status(Status::IOError("injected planning failure"));
    iterator->_output_ordinals = {0, 1};

    const auto plans = iterator->_plan_batch_read_ahead(3);

    EXPECT_EQ(failed->calls.size(), 1);
    EXPECT_EQ(healthy->calls.size(), 1);
    EXPECT_EQ(failed->phase(), ColumnIterator::ReadPhase::NORMAL);
    EXPECT_EQ(plans.size(), 1);
}

} // namespace
} // namespace doris::segment_v2
