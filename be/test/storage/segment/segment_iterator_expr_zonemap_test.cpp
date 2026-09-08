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
#include <atomic>
#include <memory>
#include <roaring/roaring.hh>
#include <set>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "core/typeid_cast.h"
#include "exprs/late_runtime_filter.h"
#include "exprs/runtime_filter_expr.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vslot_ref.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/comparison_predicate.h"
#include "storage/row_cursor.h"
#include "storage/segment/condition_cache.h"
#include "storage/segment/row_ranges.h"
#include "storage/segment/segment.h"

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

#include "storage/segment/test_segment_writer.h"
#include "storage/tablet/tablet_schema_helper.h"
#include "storage/utils.h"
#include "testutil/index_storage_test_util.h"

namespace doris::segment_v2 {
namespace {

constexpr auto kTestDir = "./ut_dir/segment_iterator_expr_zonemap_test";
constexpr int kNumRows = 8192;
constexpr int kCommitTsoRows = 8;
const RowsetId kRowsetId {.version = 1};

Field int_field(int32_t value) {
    return Field::create_field<TYPE_INT>(value);
}

class IntMaxAtLeastExpr final : public VExpr {
public:
    IntMaxAtLeastExpr(int column_id, int32_t threshold)
            : IntMaxAtLeastExpr(column_id, threshold, column_id, std::to_string(column_id)) {}

    IntMaxAtLeastExpr(int column_id, int32_t threshold, int column_unique_id,
                      std::string column_name, bool supports_zonemap = true)
            : _column_id(column_id), _threshold(threshold), _supports_zonemap(supports_zonemap) {
        _data_type = std::make_shared<DataTypeUInt8>();
        set_children({std::make_shared<VSlotRef>(-1, column_id, column_unique_id,
                                                 std::make_shared<DataTypeInt32>(),
                                                 std::move(column_name))});
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block* block, const Selector*, size_t count,
                               ColumnPtr& result) const override {
        DORIS_CHECK(block != nullptr);
        const auto* input =
                assert_cast<const ColumnInt32*>(block->get_by_position(_column_id).column.get());
        auto filter = ColumnUInt8::create();
        auto& filter_data = filter->get_data();
        filter_data.resize(count);
        for (size_t i = 0; i < count; ++i) {
            filter_data[i] = input->get_data()[i] >= _threshold;
        }
        result = std::move(filter);
        return Status::OK();
    }

    bool can_evaluate_zonemap_filter() const override { return _supports_zonemap; }

    bool is_constant() const override { return false; }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }

    ZoneMapFilterResult evaluate_zonemap_filter(const ZoneMapEvalContext& ctx) const override {
        auto zone_map = ctx.zone_map(_column_id);
        if (zone_map == nullptr) {
            return unsupported_zonemap_filter(ctx);
        }
        if (!zone_map->has_not_null) {
            return ZoneMapFilterResult::kNoMatch;
        }
        return zone_map->max_value.get<TYPE_INT>() >= _threshold ? ZoneMapFilterResult::kMayMatch
                                                                 : ZoneMapFilterResult::kNoMatch;
    }

private:
    int _column_id;
    int32_t _threshold;
    bool _supports_zonemap;
    std::string _expr_name = "int_max_at_least_expr";
};

class BigIntEqualsExpr final : public VExpr {
public:
    BigIntEqualsExpr(int column_id, int64_t target, int column_unique_id, std::string column_name)
            : _column_id(column_id), _target(target) {
        _data_type = std::make_shared<DataTypeUInt8>();
        set_children({std::make_shared<VSlotRef>(-1, column_id, column_unique_id,
                                                 std::make_shared<DataTypeInt64>(),
                                                 std::move(column_name))});
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block* block, const Selector*, size_t count,
                               ColumnPtr& result) const override {
        DORIS_CHECK(block != nullptr);
        const auto* column = block->get_by_position(_column_id).column.get();
        const ColumnInt64* input = nullptr;
        const ColumnUInt8::Container* null_map = nullptr;
        if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
            input = assert_cast<const ColumnInt64*>(&nullable->get_nested_column());
            null_map = &nullable->get_null_map_data();
        } else {
            input = assert_cast<const ColumnInt64*>(column);
        }
        auto filter = ColumnUInt8::create();
        auto& filter_data = filter->get_data();
        filter_data.resize(count);
        for (size_t i = 0; i < count; ++i) {
            filter_data[i] =
                    (null_map == nullptr || !(*null_map)[i]) && input->get_data()[i] == _target;
        }
        result = std::move(filter);
        return Status::OK();
    }

    bool can_evaluate_zonemap_filter() const override { return true; }

    bool is_constant() const override { return false; }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }

    ZoneMapFilterResult evaluate_zonemap_filter(const ZoneMapEvalContext& ctx) const override {
        auto zone_map = ctx.zone_map(_column_id);
        if (zone_map == nullptr) {
            return unsupported_zonemap_filter(ctx);
        }
        if (!zone_map->has_not_null) {
            return ZoneMapFilterResult::kNoMatch;
        }
        return zone_map->min_value.get<TYPE_BIGINT>() <= _target &&
                               zone_map->max_value.get<TYPE_BIGINT>() >= _target
                       ? ZoneMapFilterResult::kMayMatch
                       : ZoneMapFilterResult::kNoMatch;
    }

private:
    int _column_id;
    int64_t _target;
    std::string _expr_name = "bigint_equals_expr";
};

VExprContextSPtr make_runtime_filter_context(VExprSPtr impl, int32_t filter_id) {
    TTypeDesc bool_type;
    TTypeNode bool_node;
    TScalarType bool_scalar_type;
    bool_scalar_type.__set_type(TPrimitiveType::BOOLEAN);
    bool_node.__set_type(TTypeNodeType::SCALAR);
    bool_node.__set_scalar_type(bool_scalar_type);
    bool_type.types.push_back(bool_node);

    TExprNode node;
    node.__set_type(bool_type);
    node.__set_node_type(TExprNodeType::IN_PRED);
    node.in_predicate.__set_is_not_in(false);
    node.__set_opcode(TExprOpcode::FILTER_IN);
    node.__set_is_nullable(false);
    return std::make_shared<VExprContext>(
            RuntimeFilterExpr::create_shared(node, std::move(impl), 0.0, false, filter_id));
}

TabletSchemaSPtr make_tablet_schema() {
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->append_column(*create_int_key(0, false));
    tablet_schema->append_column(*create_int_key(1, false));
    tablet_schema->set_storage_page_size(4096);
    return tablet_schema;
}

TabletSchemaSPtr make_version_tablet_schema() {
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->append_column(*create_int_key(0, false));
    TabletColumn version_column;
    version_column.set_unique_id(1);
    version_column.set_name(VERSION_COL);
    version_column.set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
    version_column.set_is_key(false);
    version_column.set_is_nullable(false);
    version_column.set_length(8);
    version_column.set_index_length(8);
    version_column.set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);
    tablet_schema->append_column(std::move(version_column));
    tablet_schema->set_storage_page_size(4096);
    return tablet_schema;
}

TabletSchemaSPtr make_commit_tso_tablet_schema() {
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->append_column(*create_int_key(0, false));
    tablet_schema->append_column(*create_commit_tso_column(1));
    tablet_schema->set_storage_page_size(4096);
    return tablet_schema;
}

TabletSchemaSPtr make_binlog_tso_tablet_schema() {
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->append_column(*create_int_key(0, false));
    TabletColumn tso_column;
    tso_column.set_unique_id(1);
    tso_column.set_name(BINLOG_TSO_COL);
    tso_column.set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
    tso_column.set_is_key(false);
    tso_column.set_is_nullable(true);
    tso_column.set_length(8);
    tso_column.set_index_length(8);
    tso_column.set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);
    tablet_schema->append_column(std::move(tso_column));
    tablet_schema->set_storage_page_size(4096);
    return tablet_schema;
}

std::shared_ptr<AndBlockColumnPredicate> make_commit_tso_gt_predicate(int32_t column_id,
                                                                      int64_t value) {
    auto predicates = AndBlockColumnPredicate::create_shared();
    std::shared_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<TYPE_BIGINT, PredicateType::GT>(
                    column_id, COMMIT_TSO_COL, Field::create_field<TYPE_BIGINT>(value)));
    predicates->add_column_predicate(SingleColumnBlockPredicate::create_unique(pred));
    return predicates;
}

std::shared_ptr<AndBlockColumnPredicate> make_version_eq_predicate(int32_t column_id,
                                                                   int64_t value) {
    auto predicates = AndBlockColumnPredicate::create_shared();
    std::shared_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<TYPE_BIGINT, PredicateType::EQ>(
                    column_id, VERSION_COL, Field::create_field<TYPE_BIGINT>(value)));
    predicates->add_column_predicate(SingleColumnBlockPredicate::create_unique(pred));
    return predicates;
}

// Read schema covers all tablet columns in order, so ordinal == tablet cid.
ReadSchemaSPtr make_read_schema(const TabletSchemaSPtr& tablet_schema) {
    return std::make_shared<ReadSchema>(tablet_schema->columns());
}

Block make_read_block() {
    Block block;
    block.insert({ColumnInt32::create(), std::make_shared<DataTypeInt32>(), "0"});
    block.insert({ColumnInt32::create(), std::make_shared<DataTypeInt32>(), "1"});
    return block;
}

Block make_bigint_read_block(const std::string& column_name) {
    Block block;
    block.insert({ColumnInt32::create(), std::make_shared<DataTypeInt32>(), "0"});
    block.insert(
            {ColumnInt64::create(), std::make_shared<DataTypeInt64>(), std::string(column_name)});
    return block;
}

std::shared_ptr<LateRuntimeFilterContainer> make_late_runtime_filter_container(int32_t filter_id) {
    return std::make_shared<LateRuntimeFilterContainer>(std::vector<int32_t> {filter_id});
}

void publish_late_runtime_filter(const std::shared_ptr<LateRuntimeFilterContainer>& container,
                                 const VExprContextSPtrs& expr_contexts) {
    DORIS_CHECK(container != nullptr);
    DORIS_CHECK_EQ(container->filters.size(), 1);
    DORIS_CHECK(!expr_contexts.empty());
    auto expr_group = std::make_shared<LateRuntimeFilterExprGroup>();
    expr_group->insert(expr_group->end(), expr_contexts.begin(), expr_contexts.end());
    container->filters[0].expr = std::move(expr_group);
    container->filters[0].valid.store(true, std::memory_order_release);
    container->arrived_cnt.fetch_add(1, std::memory_order_release);
}

void publish_late_runtime_filter(const std::shared_ptr<LateRuntimeFilterContainer>& container,
                                 const VExprContextSPtr& expr_context) {
    publish_late_runtime_filter(container, VExprContextSPtrs {expr_context});
}

class ScopedConditionCacheForTest {
public:
    ScopedConditionCacheForTest()
            : _previous(ExecEnv::GetInstance()->get_condition_cache()),
              _cache(ConditionCache::create_global_cache(1024 * 1024, 4)) {
        ExecEnv::GetInstance()->_condition_cache = _cache.get();
    }

    ~ScopedConditionCacheForTest() { ExecEnv::GetInstance()->_condition_cache = _previous; }

    ConditionCache* get() const { return _cache.get(); }

private:
    ConditionCache* _previous;
    std::unique_ptr<ConditionCache> _cache;
};

} // namespace

class SegmentIteratorExprZonemapTest : public testing::Test {
protected:
    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        _tablet_schema = make_tablet_schema();
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    void build_segment(std::shared_ptr<Segment>* segment) {
        const auto path = std::string(kTestDir) + "/expr_zonemap_segment.dat";
        auto fs = io::global_local_filesystem();
        io::FileWriterPtr file_writer;
        auto st = fs->create_file(path, &file_writer);
        ASSERT_TRUE(st.ok()) << st;

        SegmentWriterOptions opts;
        opts.num_rows_per_block = 1024;
        TestSegmentWriter writer(file_writer.get(), 0, _tablet_schema, nullptr, nullptr, opts,
                                 nullptr);
        st = writer.init();
        ASSERT_TRUE(st.ok()) << st;

        RowCursor row;
        std::vector<Field> fields(_tablet_schema->num_columns(), Field(PrimitiveType::TYPE_NULL));
        st = row.init_scan_key(_tablet_schema, std::move(fields));
        ASSERT_TRUE(st.ok()) << st;
        for (int rid = 0; rid < kNumRows; ++rid) {
            row.mutable_field(0) = int_field(rid);
            row.mutable_field(1) = int_field(rid < kNumRows / 2 ? 0 : 1000);
            st = writer.append_row(row);
            ASSERT_TRUE(st.ok()) << st;
        }

        uint64_t file_size = 0;
        uint64_t index_size = 0;
        st = writer.finalize(&file_size, &index_size);
        ASSERT_TRUE(st.ok()) << st;
        st = file_writer->close();
        ASSERT_TRUE(st.ok()) << st;

        st = Segment::open(fs, path, 100, 0, kRowsetId, _tablet_schema, io::FileReaderOptions {},
                           segment);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(kNumRows, (*segment)->num_rows());
    }

    void build_commit_tso_segment(std::shared_ptr<Segment>* segment) {
        ASSERT_NO_FATAL_FAILURE(
                build_hidden_bigint_segment("commit_tso_segment.dat", kCommitTsoRows, 4,
                                            Field::create_field<TYPE_BIGINT>(0), segment));
    }

    void build_version_segment(std::shared_ptr<Segment>* segment) {
        ASSERT_NO_FATAL_FAILURE(build_hidden_bigint_segment("version_segment.dat", kNumRows, 1024,
                                                            Field::create_field<TYPE_BIGINT>(0),
                                                            segment));
    }

    void build_binlog_tso_segment(std::shared_ptr<Segment>* segment) {
        ASSERT_NO_FATAL_FAILURE(build_hidden_bigint_segment("binlog_tso_segment.dat", kNumRows,
                                                            1024, Field(PrimitiveType::TYPE_NULL),
                                                            segment));
    }

    void build_hidden_bigint_segment(const std::string& file_name, int num_rows,
                                     uint32_t num_rows_per_block, const Field& hidden_value,
                                     std::shared_ptr<Segment>* segment) {
        const auto path = std::string(kTestDir) + "/" + file_name;
        auto fs = io::global_local_filesystem();
        io::FileWriterPtr file_writer;
        auto st = fs->create_file(path, &file_writer);
        ASSERT_TRUE(st.ok()) << st;

        SegmentWriterOptions opts;
        opts.num_rows_per_block = num_rows_per_block;
        TestSegmentWriter writer(file_writer.get(), 0, _tablet_schema, nullptr, nullptr, opts,
                                 nullptr);
        st = writer.init();
        ASSERT_TRUE(st.ok()) << st;

        RowCursor row;
        std::vector<Field> fields(_tablet_schema->num_columns(), Field(PrimitiveType::TYPE_NULL));
        st = row.init_scan_key(_tablet_schema, std::move(fields));
        ASSERT_TRUE(st.ok()) << st;
        for (int rid = 0; rid < num_rows; ++rid) {
            row.mutable_field(0) = int_field(rid);
            row.mutable_field(1) = hidden_value;
            st = writer.append_row(row);
            ASSERT_TRUE(st.ok()) << st;
        }

        uint64_t file_size = 0;
        uint64_t index_size = 0;
        st = writer.finalize(&file_size, &index_size);
        ASSERT_TRUE(st.ok()) << st;
        st = file_writer->close();
        ASSERT_TRUE(st.ok()) << st;

        st = Segment::open(fs, path, 100, 0, kRowsetId, _tablet_schema, io::FileReaderOptions {},
                           segment);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(num_rows, (*segment)->num_rows());
    }

    void prepare_expr_context(const VExprContextSPtr& expr_ctx) {
        RowDescriptor row_desc;
        auto st = expr_ctx->prepare(&_runtime_state, row_desc);
        ASSERT_TRUE(st.ok()) << st;
        st = expr_ctx->open(&_runtime_state);
        ASSERT_TRUE(st.ok()) << st;
    }

    TabletSchemaSPtr _tablet_schema;
    OlapReaderStatistics _stats;
    RuntimeState _runtime_state;
};

TEST_F(SegmentIteratorExprZonemapTest, NewIteratorPrunesWholeSegmentByExprZonemap) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    auto expr_ctx = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 2000));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_ctx));
    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.common_expr_ctxs_push_down = {expr_ctx};

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    EXPECT_TRUE(iter->empty());
    EXPECT_EQ(1, _stats.total_segment_number);
    EXPECT_EQ(1, _stats.filtered_segment_number);
    EXPECT_EQ(1, _stats.expr_zonemap_filtered_segments);
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_row_filtered);
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_zonemap_filtered);
}

TEST_F(SegmentIteratorExprZonemapTest, NewIteratorKeepsSegmentWhenExprZonemapMayMatch) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    auto expr_ctx = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 500));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_ctx));
    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.common_expr_ctxs_push_down = {expr_ctx};

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    EXPECT_FALSE(iter->empty());
    EXPECT_EQ(1, _stats.total_segment_number);
    EXPECT_EQ(0, _stats.filtered_segment_number);
    EXPECT_EQ(0, _stats.expr_zonemap_filtered_segments);
}

TEST_F(SegmentIteratorExprZonemapTest, LateRuntimeFilterPrunesWholeSegmentBeforeIteratorInit) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    auto expr_context =
            std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 2000));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_context));
    auto container = make_late_runtime_filter_container(1);
    publish_late_runtime_filter(container, expr_context);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.late_runtime_filter_container = container;

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    EXPECT_TRUE(iter->empty());
    EXPECT_EQ(1, _stats.filtered_segment_number);
    EXPECT_EQ(1, _stats.expr_zonemap_filtered_segments);
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_row_filtered);
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_zonemap_filtered);
}

TEST_F(SegmentIteratorExprZonemapTest, LateRuntimeFilterPrunesSegmentAfterCommonExprMayMatch) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    auto common_expr = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 500));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(common_expr));
    auto late_expr = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 2000));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(late_expr));
    auto container = make_late_runtime_filter_container(11);
    publish_late_runtime_filter(container, late_expr);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.common_expr_ctxs_push_down = {common_expr};
    read_options.late_runtime_filter_container = container;

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    EXPECT_TRUE(iter->empty());
    EXPECT_EQ(1, _stats.filtered_segment_number);
    EXPECT_EQ(1, _stats.expr_zonemap_filtered_segments);
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_row_filtered);
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_zonemap_filtered);
}

TEST_F(SegmentIteratorExprZonemapTest, CommonExprPrunesSegmentWhenLateFilterAlsoRejects) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    auto common_expr = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 2000));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(common_expr));
    auto late_expr = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 2000));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(late_expr));
    auto container = make_late_runtime_filter_container(12);
    publish_late_runtime_filter(container, late_expr);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.common_expr_ctxs_push_down = {common_expr};
    read_options.late_runtime_filter_container = container;

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    EXPECT_TRUE(iter->empty());
    EXPECT_EQ(1, _stats.filtered_segment_number);
    EXPECT_EQ(1, _stats.expr_zonemap_filtered_segments);
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_row_filtered);
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_zonemap_filtered);
}

TEST_F(SegmentIteratorExprZonemapTest, ValidLateRuntimeFilterPrunesPagesBeforeFirstBatch) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    auto expr_context = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 500));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_context));
    auto container = make_late_runtime_filter_container(2);
    publish_late_runtime_filter(container, expr_context);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.block_row_max = 1024;
    read_options.late_runtime_filter_container = container;

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    st = iter->init(read_options);
    ASSERT_TRUE(st.ok()) << st;
    auto block = make_read_block();
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_GT(block.rows(), 0);
    const auto* key_column = assert_cast<const ColumnInt32*>(block.get_by_position(0).column.get());
    EXPECT_GE(key_column->get_data().front(), kNumRows / 2);
    EXPECT_GT(_stats.expr_zonemap_filtered_pages, 0);
    EXPECT_EQ(1, _stats.late_runtime_filters_installed);
    EXPECT_EQ(0, _stats.late_runtime_filters_installed_after_lazy_init);
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_row_filtered);
    EXPECT_EQ(kNumRows / 2, _stats.rows_late_runtime_filter_zonemap_filtered);
}

TEST_F(SegmentIteratorExprZonemapTest, InitialLateRuntimeFilterPagePruningIgnoresUnassignedRows) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    auto expr_context = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 500));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_context));
    auto container = make_late_runtime_filter_container(13);
    publish_late_runtime_filter(container, expr_context);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.block_row_max = 1024;
    read_options.late_runtime_filter_container = container;
    read_options.row_ranges = RowRanges::create_single(kNumRows / 2, kNumRows);

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    st = iter->init(read_options);
    ASSERT_TRUE(st.ok()) << st;

    auto block = make_read_block();
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(1024, block.rows());
    const auto* key_column = assert_cast<const ColumnInt32*>(block.get_by_position(0).column.get());
    EXPECT_EQ(kNumRows / 2, key_column->get_data().front());
    EXPECT_GT(_stats.expr_zonemap_filtered_pages, 0);
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_row_filtered);
    // The pruned pages hold only rows this iterator would never return, so they are
    // not reported as late RF pruning benefit.
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_zonemap_filtered);
}

TEST_F(SegmentIteratorExprZonemapTest, InitialLateRuntimeFilterPagePruningIgnoresDeletedRows) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    auto expr_context = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 500));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_context));
    auto container = make_late_runtime_filter_container(14);
    publish_late_runtime_filter(container, expr_context);

    auto delete_bitmap = std::make_shared<roaring::Roaring>();
    delete_bitmap->addRange(0, kNumRows / 2);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.block_row_max = 1024;
    read_options.late_runtime_filter_container = container;
    read_options.delete_bitmap.emplace(segment->id(), std::move(delete_bitmap));

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    st = iter->init(read_options);
    ASSERT_TRUE(st.ok()) << st;

    auto block = make_read_block();
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(1024, block.rows());
    const auto* key_column = assert_cast<const ColumnInt32*>(block.get_by_position(0).column.get());
    EXPECT_EQ(kNumRows / 2, key_column->get_data().front());
    EXPECT_GT(_stats.expr_zonemap_filtered_pages, 0);
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_row_filtered);
    // The pruned pages hold only rows this iterator would never return, so they are
    // not reported as late RF pruning benefit.
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_zonemap_filtered);
}

TEST_F(SegmentIteratorExprZonemapTest, PublishedLateRuntimeFilterPrunesOnlyRemainingPages) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);
    auto container = make_late_runtime_filter_container(3);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.block_row_max = 1024;
    read_options.late_runtime_filter_container = container;

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    st = iter->init(read_options);
    ASSERT_TRUE(st.ok()) << st;

    auto block = make_read_block();
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(1024, block.rows());
    const auto* first_batch_keys =
            assert_cast<const ColumnInt32*>(block.get_by_position(0).column.get());
    EXPECT_EQ(0, first_batch_keys->get_data().front());
    EXPECT_EQ(1023, first_batch_keys->get_data().back());

    auto expr_context = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 500));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_context));
    publish_late_runtime_filter(container, expr_context);

    block.clear_column_data();
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_GT(block.rows(), 0);
    const auto* second_batch_keys =
            assert_cast<const ColumnInt32*>(block.get_by_position(0).column.get());
    EXPECT_GE(second_batch_keys->get_data().front(), kNumRows / 2);
    EXPECT_GT(_stats.expr_zonemap_filtered_pages, 0);
    EXPECT_EQ(1, _stats.late_runtime_filters_installed);
    EXPECT_EQ(1, _stats.late_runtime_filters_installed_after_lazy_init);
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_row_filtered);
    EXPECT_EQ(kNumRows / 2 - 1024, _stats.rows_late_runtime_filter_zonemap_filtered);

    block.clear_column_data();
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(1, _stats.late_runtime_filters_installed);
    EXPECT_EQ(1, _stats.late_runtime_filters_installed_after_lazy_init);
    EXPECT_EQ(kNumRows / 2 - 1024, _stats.rows_late_runtime_filter_zonemap_filtered);
}

TEST_F(SegmentIteratorExprZonemapTest, ReadyAtOpenVersionPredicateUsesReadTimeVersion) {
    constexpr int64_t kVersion = 7;
    _tablet_schema = make_version_tablet_schema();
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_version_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    // A runtime filter that is ready when the scan opens is normalized into a column predicate.
    // The physical zonemap of __DORIS_VERSION_COL__ is [0,0]; the predicate must be evaluated
    // against the read-time version at both the segment and the page level.
    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(kVersion, kVersion);
    read_options.block_row_max = 1024;
    read_options.col_id_to_predicates.emplace(1, make_version_eq_predicate(1, kVersion));

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    ASSERT_FALSE(iter->empty());
    EXPECT_EQ(0, _stats.filtered_segment_number);
    st = iter->init(read_options);
    ASSERT_TRUE(st.ok()) << st;

    size_t total_rows = 0;
    auto block = make_bigint_read_block(VERSION_COL);
    while (true) {
        st = iter->next_batch(&block);
        if (st.is<ErrorCode::END_OF_FILE>()) {
            break;
        }
        ASSERT_TRUE(st.ok()) << st;
        total_rows += block.rows();
        const auto* version_column =
                assert_cast<const ColumnInt64*>(block.get_by_position(1).column.get());
        EXPECT_TRUE(std::ranges::all_of(version_column->get_data(),
                                        [](int64_t value) { return value == kVersion; }));
        block.clear_column_data();
    }
    EXPECT_EQ(kNumRows, total_rows);
    EXPECT_EQ(0, _stats.rows_stats_filtered);
}

TEST_F(SegmentIteratorExprZonemapTest, ReadyAtOpenVersionPredicatePrunesMismatchedVersion) {
    constexpr int64_t kVersion = 7;
    _tablet_schema = make_version_tablet_schema();
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_version_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(kVersion, kVersion);
    read_options.block_row_max = 1024;
    read_options.col_id_to_predicates.emplace(1, make_version_eq_predicate(1, kVersion + 1));

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    EXPECT_TRUE(iter->empty());
    EXPECT_EQ(1, _stats.filtered_segment_number);
    EXPECT_EQ(kNumRows, _stats.rows_stats_filtered);
}

TEST_F(SegmentIteratorExprZonemapTest, LateRuntimeFilterUsesReadTimeVersionBeforeLazyInit) {
    constexpr int64_t kVersion = 7;
    _tablet_schema = make_version_tablet_schema();
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_version_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    auto expr_context = std::make_shared<VExprContext>(
            std::make_shared<BigIntEqualsExpr>(1, kVersion, 1, VERSION_COL));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_context));
    auto container = make_late_runtime_filter_container(8);
    publish_late_runtime_filter(container, expr_context);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(kVersion, kVersion);
    read_options.block_row_max = 1024;
    read_options.late_runtime_filter_container = container;

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    ASSERT_FALSE(iter->empty());
    st = iter->init(read_options);
    ASSERT_TRUE(st.ok()) << st;

    auto block = make_bigint_read_block(VERSION_COL);
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(1024, block.rows());
    const auto* version_column =
            assert_cast<const ColumnInt64*>(block.get_by_position(1).column.get());
    EXPECT_TRUE(std::ranges::all_of(version_column->get_data(),
                                    [](int64_t value) { return value == kVersion; }));
    EXPECT_EQ(0, _stats.expr_zonemap_filtered_segments);
    EXPECT_EQ(0, _stats.expr_zonemap_filtered_pages);
    EXPECT_EQ(1, _stats.late_runtime_filters_installed);
}

TEST_F(SegmentIteratorExprZonemapTest, LateRuntimeFilterUsesReadTimeVersionAfterLazyInit) {
    constexpr int64_t kVersion = 7;
    _tablet_schema = make_version_tablet_schema();
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_version_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);
    auto container = make_late_runtime_filter_container(9);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(kVersion, kVersion);
    read_options.block_row_max = 1024;
    read_options.late_runtime_filter_container = container;

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    st = iter->init(read_options);
    ASSERT_TRUE(st.ok()) << st;

    auto block = make_bigint_read_block(VERSION_COL);
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(1024, block.rows());
    auto* segment_iter = dynamic_cast<SegmentIterator*>(iter.get());
    ASSERT_NE(nullptr, segment_iter);
    EXPECT_TRUE(segment_iter->_common_expr_ctxs_push_down.empty());
    EXPECT_EQ(0, segment_iter->_late_runtime_filter_common_expr_start);
    const auto* first_version_column =
            assert_cast<const ColumnInt64*>(block.get_by_position(1).column.get());
    EXPECT_TRUE(std::ranges::all_of(first_version_column->get_data(),
                                    [](int64_t value) { return value == kVersion; }));

    auto expr_context = std::make_shared<VExprContext>(
            std::make_shared<BigIntEqualsExpr>(1, kVersion, 1, VERSION_COL));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_context));
    publish_late_runtime_filter(container, expr_context);

    block.clear_column_data();
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(1024, block.rows());
    const auto* second_version_column =
            assert_cast<const ColumnInt64*>(block.get_by_position(1).column.get());
    EXPECT_TRUE(std::ranges::all_of(second_version_column->get_data(),
                                    [](int64_t value) { return value == kVersion; }));
    EXPECT_EQ(0, _stats.expr_zonemap_filtered_pages);
    EXPECT_EQ(1, _stats.late_runtime_filters_installed);
    EXPECT_EQ(1, _stats.late_runtime_filters_installed_after_lazy_init);
    EXPECT_TRUE(segment_iter->_late_runtime_filter_ctxs.empty());
}

TEST_F(SegmentIteratorExprZonemapTest, NoopLateRuntimeFilterPageZonemapKeepsRangeIterator) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);
    // The range iterator only tracks the remaining row range when late runtime filters may
    // arrive, so attach a container even though this test publishes the filter directly.
    auto container = make_late_runtime_filter_container(5);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.block_row_max = 1024;
    read_options.late_runtime_filter_container = container;

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    st = iter->init(read_options);
    ASSERT_TRUE(st.ok()) << st;

    auto block = make_read_block();
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(1024, block.rows());

    auto* segment_iter = dynamic_cast<SegmentIterator*>(iter.get());
    ASSERT_NE(nullptr, segment_iter);
    auto* range_iter_before = segment_iter->_range_iter.get();
    const auto bitmap_cardinality_before = segment_iter->_row_bitmap.cardinality();
    const auto rows_stats_filtered_before = _stats.rows_stats_filtered;
    const auto late_zonemap_filtered_before = _stats.rows_late_runtime_filter_zonemap_filtered;
    const auto filtered_pages_before = _stats.expr_zonemap_filtered_pages;

    auto expr_context = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 0));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_context));
    st = segment_iter->_apply_new_late_runtime_filter_page_zonemap({expr_context});
    ASSERT_TRUE(st.ok()) << st;

    EXPECT_EQ(range_iter_before, segment_iter->_range_iter.get());
    EXPECT_EQ(bitmap_cardinality_before, segment_iter->_row_bitmap.cardinality());
    EXPECT_EQ(rows_stats_filtered_before, _stats.rows_stats_filtered);
    EXPECT_EQ(late_zonemap_filtered_before, _stats.rows_late_runtime_filter_zonemap_filtered);
    EXPECT_EQ(filtered_pages_before, _stats.expr_zonemap_filtered_pages);
}

TEST_F(SegmentIteratorExprZonemapTest, SharedContainerInstallsNonZonemapFilterPerIterator) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);
    auto container = make_late_runtime_filter_container(4);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.block_row_max = 1024;
    read_options.late_runtime_filter_container = container;

    std::unique_ptr<RowwiseIterator> first_iter;
    auto st = segment->new_iterator(read_schema, read_options, &first_iter);
    ASSERT_TRUE(st.ok()) << st;
    st = first_iter->init(read_options);
    ASSERT_TRUE(st.ok()) << st;

    std::unique_ptr<RowwiseIterator> second_iter;
    st = segment->new_iterator(read_schema, read_options, &second_iter);
    ASSERT_TRUE(st.ok()) << st;
    st = second_iter->init(read_options);
    ASSERT_TRUE(st.ok()) << st;

    auto first_expr_context = std::make_shared<VExprContext>(
            std::make_shared<IntMaxAtLeastExpr>(0, 400, 0, "0", false));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(first_expr_context));
    auto second_expr_context = std::make_shared<VExprContext>(
            std::make_shared<IntMaxAtLeastExpr>(0, 500, 0, "0", false));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(second_expr_context));
    publish_late_runtime_filter(container, {first_expr_context, second_expr_context});

    auto first_block = make_read_block();
    st = first_iter->next_batch(&first_block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(524, first_block.rows());
    const auto* first_keys =
            assert_cast<const ColumnInt32*>(first_block.get_by_position(0).column.get());
    EXPECT_EQ(500, first_keys->get_data().front());
    EXPECT_EQ(1023, first_keys->get_data().back());

    auto second_block = make_read_block();
    st = second_iter->next_batch(&second_block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(524, second_block.rows());
    const auto* second_keys =
            assert_cast<const ColumnInt32*>(second_block.get_by_position(0).column.get());
    EXPECT_EQ(500, second_keys->get_data().front());
    EXPECT_EQ(1023, second_keys->get_data().back());

    EXPECT_EQ(2, _stats.late_runtime_filters_installed);
    EXPECT_EQ(0, _stats.late_runtime_filters_installed_after_lazy_init);
    EXPECT_EQ(1000, _stats.rows_late_runtime_filter_row_filtered);
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_zonemap_filtered);
    EXPECT_EQ(0, _stats.expr_zonemap_filtered_pages);
    EXPECT_EQ(2048, _stats.expr_cond_input_rows);

    auto* first_segment_iter = dynamic_cast<SegmentIterator*>(first_iter.get());
    auto* second_segment_iter = dynamic_cast<SegmentIterator*>(second_iter.get());
    ASSERT_NE(nullptr, first_segment_iter);
    ASSERT_NE(nullptr, second_segment_iter);
    EXPECT_EQ((std::vector<uint8_t> {1}), first_segment_iter->_processed_late_runtime_filters);
    EXPECT_EQ((std::vector<uint8_t> {1}), second_segment_iter->_processed_late_runtime_filters);
    ASSERT_EQ(2, first_segment_iter->_common_expr_ctxs_push_down.size());
    ASSERT_EQ(2, second_segment_iter->_common_expr_ctxs_push_down.size());
    EXPECT_EQ(0, first_segment_iter->_late_runtime_filter_common_expr_start);
    EXPECT_EQ(0, second_segment_iter->_late_runtime_filter_common_expr_start);
    EXPECT_TRUE(first_segment_iter->_late_runtime_filter_ctxs.empty());
    EXPECT_TRUE(second_segment_iter->_late_runtime_filter_ctxs.empty());
    EXPECT_NE(first_segment_iter->_common_expr_ctxs_push_down[0].get(),
              second_segment_iter->_common_expr_ctxs_push_down[0].get());

    first_block.clear_column_data();
    st = first_iter->next_batch(&first_block);
    ASSERT_TRUE(st.ok()) << st;
    second_block.clear_column_data();
    st = second_iter->next_batch(&second_block);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(2, _stats.late_runtime_filters_installed);
}

TEST_F(SegmentIteratorExprZonemapTest, ReadyBeforeLazyInitLateFilterSkipsNonPredicateColumnRead) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);
    auto container = make_late_runtime_filter_container(16);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.block_row_max = 1024;
    read_options.late_runtime_filter_container = container;

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    st = iter->init(read_options);
    ASSERT_TRUE(st.ok()) << st;
    auto* segment_iter = dynamic_cast<SegmentIterator*>(iter.get());
    ASSERT_NE(nullptr, segment_iter);
    ASSERT_FALSE(segment_iter->_lazy_inited);

    auto expr_context = make_runtime_filter_context(
            std::make_shared<IntMaxAtLeastExpr>(0, kNumRows, 0, "0", false), 16);
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_context));
    publish_late_runtime_filter(container, expr_context);

    index_storage_test::ScopedDebugPoint debug_point("segment_iterator._read_columns_by_index",
                                                     {{"column_name", "1"}});
    auto block = make_read_block();
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(0, block.rows());
    EXPECT_GT(debug_point.execute_num(), 0);

    ASSERT_EQ(1, segment_iter->_common_expr_ctxs_push_down.size());
    EXPECT_EQ(0, segment_iter->_late_runtime_filter_common_expr_start);
    EXPECT_TRUE(segment_iter->_late_runtime_filter_ctxs.empty());
    EXPECT_EQ(1, _stats.late_runtime_filters_installed);
    EXPECT_EQ(0, _stats.late_runtime_filters_installed_after_lazy_init);
    EXPECT_EQ(1024, _stats.rows_late_runtime_filter_row_filtered);
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_zonemap_filtered);
    EXPECT_EQ(1024, _stats.expr_cond_input_rows);
    EXPECT_EQ(1024, _stats.rows_expr_cond_filtered);
    EXPECT_EQ(1024, _stats.raw_rows_read);
}

TEST_F(SegmentIteratorExprZonemapTest, ReadyBeforeLazyInitLateFilterUsesCommonExpr) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);
    auto container = make_late_runtime_filter_container(17);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.block_row_max = 1024;
    read_options.late_runtime_filter_container = container;

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    st = iter->init(read_options);
    ASSERT_TRUE(st.ok()) << st;

    auto expr_context = make_runtime_filter_context(
            std::make_shared<IntMaxAtLeastExpr>(0, 500, 0, "0", false), 17);
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_context));
    publish_late_runtime_filter(container, expr_context);

    auto block = make_read_block();
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(524, block.rows());
    const auto* keys = assert_cast<const ColumnInt32*>(block.get_by_position(0).column.get());
    EXPECT_EQ(500, keys->get_data().front());
    EXPECT_EQ(1023, keys->get_data().back());

    auto* segment_iter = dynamic_cast<SegmentIterator*>(iter.get());
    ASSERT_NE(nullptr, segment_iter);
    ASSERT_EQ(1, segment_iter->_common_expr_ctxs_push_down.size());
    EXPECT_EQ(0, segment_iter->_late_runtime_filter_common_expr_start);
    EXPECT_TRUE(segment_iter->_late_runtime_filter_ctxs.empty());

    EXPECT_EQ(1, _stats.late_runtime_filters_installed);
    EXPECT_EQ(0, _stats.late_runtime_filters_installed_after_lazy_init);
    EXPECT_EQ(500, _stats.rows_late_runtime_filter_row_filtered);
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_zonemap_filtered);
    EXPECT_EQ(1024, _stats.expr_cond_input_rows);
    EXPECT_EQ(500, _stats.rows_expr_cond_filtered);
}

TEST_F(SegmentIteratorExprZonemapTest,
       ReadyBeforeLazyInitLateFilterUsesCommonExprSuffixStatistics) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);
    auto container = make_late_runtime_filter_container(18);

    auto common_expr = std::make_shared<VExprContext>(
            std::make_shared<IntMaxAtLeastExpr>(0, 256, 0, "0", false));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(common_expr));

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.block_row_max = 1024;
    read_options.common_expr_ctxs_push_down = {common_expr};
    read_options.late_runtime_filter_container = container;

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    st = iter->init(read_options);
    ASSERT_TRUE(st.ok()) << st;

    auto late_expr = make_runtime_filter_context(
            std::make_shared<IntMaxAtLeastExpr>(0, 500, 0, "0", false), 18);
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(late_expr));
    publish_late_runtime_filter(container, late_expr);

    auto block = make_read_block();
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(524, block.rows());
    const auto* keys = assert_cast<const ColumnInt32*>(block.get_by_position(0).column.get());
    EXPECT_EQ(500, keys->get_data().front());
    EXPECT_EQ(1023, keys->get_data().back());

    auto* segment_iter = dynamic_cast<SegmentIterator*>(iter.get());
    ASSERT_NE(nullptr, segment_iter);
    ASSERT_EQ(2, segment_iter->_common_expr_ctxs_push_down.size());
    EXPECT_EQ(1, segment_iter->_late_runtime_filter_common_expr_start);
    EXPECT_TRUE(segment_iter->_late_runtime_filter_ctxs.empty());

    EXPECT_EQ(1, _stats.late_runtime_filters_installed);
    EXPECT_EQ(0, _stats.late_runtime_filters_installed_after_lazy_init);
    EXPECT_EQ(244, _stats.rows_late_runtime_filter_row_filtered);
    EXPECT_EQ(500, _stats.rows_expr_cond_filtered);
    EXPECT_EQ(1024, _stats.expr_cond_input_rows);
}

TEST_F(SegmentIteratorExprZonemapTest, ConditionCacheHitAndLateRuntimeFilterAreIntersected) {
    ScopedConditionCacheForTest scoped_cache;
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    constexpr uint64_t digest = 101;
    auto cached_result = std::make_shared<std::vector<bool>>(
            kNumRows / SegmentIterator::CONDITION_CACHE_OFFSET + 1, true);
    (*cached_result)[0] = false;
    scoped_cache.get()->insert(ConditionCache::CacheKey(kRowsetId, segment->id(), digest),
                               cached_result);

    auto static_expr = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(0, 0));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(static_expr));
    auto container = make_late_runtime_filter_container(6);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.block_row_max = 1024;
    read_options.rowset_id = kRowsetId;
    read_options.condition_cache_digest = digest;
    read_options.common_expr_ctxs_push_down = {static_expr};
    read_options.late_runtime_filter_container = container;

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    st = iter->init(read_options);
    ASSERT_TRUE(st.ok()) << st;

    auto late_expr = std::make_shared<VExprContext>(
            std::make_shared<IntMaxAtLeastExpr>(0, 3000, 0, "0", false));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(late_expr));
    publish_late_runtime_filter(container, late_expr);

    auto block = make_read_block();
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(72, block.rows());
    const auto* keys = assert_cast<const ColumnInt32*>(block.get_by_position(0).column.get());
    EXPECT_EQ(3000, keys->get_data().front());
    EXPECT_EQ(3071, keys->get_data().back());
    EXPECT_EQ(SegmentIterator::CONDITION_CACHE_OFFSET, _stats.condition_cache_filtered_rows);
    EXPECT_EQ(1, _stats.late_runtime_filters_installed);
    EXPECT_EQ(952, _stats.rows_late_runtime_filter_row_filtered);
    EXPECT_EQ(0, _stats.rows_late_runtime_filter_zonemap_filtered);
}

TEST_F(SegmentIteratorExprZonemapTest, ArrivedLateRuntimeFilterPreventsConditionCacheInsert) {
    ScopedConditionCacheForTest scoped_cache;
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    constexpr uint64_t digest = 202;
    auto static_expr = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(0, 0));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(static_expr));
    auto container = make_late_runtime_filter_container(7);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.block_row_max = 1024;
    read_options.rowset_id = kRowsetId;
    read_options.condition_cache_digest = digest;
    read_options.common_expr_ctxs_push_down = {static_expr};
    read_options.late_runtime_filter_container = container;

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    st = iter->init(read_options);
    ASSERT_TRUE(st.ok()) << st;

    auto block = make_read_block();
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    auto* segment_iter = dynamic_cast<SegmentIterator*>(iter.get());
    ASSERT_NE(nullptr, segment_iter);
    EXPECT_EQ(digest, segment_iter->_opts.condition_cache_digest);
    EXPECT_NE(nullptr, segment_iter->_condition_cache);
    EXPECT_FALSE(segment_iter->_find_condition_cache);

    auto late_expr = std::make_shared<VExprContext>(
            std::make_shared<IntMaxAtLeastExpr>(0, 500, 0, "0", false));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(late_expr));
    publish_late_runtime_filter(container, late_expr);

    block.clear_column_data();
    while (true) {
        st = iter->next_batch(&block);
        if (st.is<ErrorCode::END_OF_FILE>()) {
            break;
        }
        ASSERT_TRUE(st.ok()) << st;
        block.clear_column_data();
    }

    ConditionCacheHandle handle;
    EXPECT_FALSE(scoped_cache.get()->lookup(
            ConditionCache::CacheKey(kRowsetId, segment->id(), digest), &handle));
}

TEST_F(SegmentIteratorExprZonemapTest, ForwardRemainingBitmapIntersectionDoesNotRevisitRows) {
    auto read_schema = make_read_schema(_tablet_schema);
    SegmentIterator iter(nullptr, read_schema);
    iter._opts.stats = &_stats;
    // The range iterator only tracks the remaining row range when late runtime filters may
    // arrive; the bare iterator skips _init_impl(), so attach the container directly.
    iter._late_runtime_filter_container = make_late_runtime_filter_container(19);
    iter._row_bitmap.addRange(0, 10);
    iter._block_rowids.resize(10);
    iter._rebuild_range_iterator();

    uint16_t rows_read = 0;
    auto st = iter._read_columns_by_index({}, 3, rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(3, rows_read);
    EXPECT_EQ((std::vector<rowid_t> {0, 1, 2}),
              (std::vector<rowid_t>(iter._block_rowids.begin(),
                                    iter._block_rowids.begin() + rows_read)));

    EXPECT_EQ(3, iter._intersect_remaining_row_bitmap(RowRanges::create_single(6, 10)));
    st = iter._read_columns_by_index({}, 10, rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(4, rows_read);
    EXPECT_EQ((std::vector<rowid_t> {6, 7, 8, 9}),
              (std::vector<rowid_t>(iter._block_rowids.begin(),
                                    iter._block_rowids.begin() + rows_read)));
}

TEST_F(SegmentIteratorExprZonemapTest, ReverseRemainingBitmapIntersectionDoesNotRevisitRows) {
    auto read_schema = make_read_schema(_tablet_schema);
    SegmentIterator iter(nullptr, read_schema);
    iter._opts.stats = &_stats;
    // The range iterator only tracks the remaining row range when late runtime filters may
    // arrive; the bare iterator skips _init_impl(), so attach the container directly.
    iter._late_runtime_filter_container = make_late_runtime_filter_container(20);
    iter._opts.read_orderby_key_reverse = true;
    iter._row_bitmap.addRange(0, 10);
    iter._block_rowids.resize(10);
    iter._rebuild_range_iterator();

    uint16_t rows_read = 0;
    auto st = iter._read_columns_by_index({}, 3, rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(3, rows_read);
    EXPECT_EQ((std::vector<rowid_t> {7, 8, 9}),
              (std::vector<rowid_t>(iter._block_rowids.begin(),
                                    iter._block_rowids.begin() + rows_read)));

    EXPECT_EQ(3, iter._intersect_remaining_row_bitmap(RowRanges::create_single(0, 4)));
    st = iter._read_columns_by_index({}, 10, rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(4, rows_read);
    EXPECT_EQ((std::vector<rowid_t> {0, 1, 2, 3}),
              (std::vector<rowid_t>(iter._block_rowids.begin(),
                                    iter._block_rowids.begin() + rows_read)));
}

TEST_F(SegmentIteratorExprZonemapTest,
       SparseForwardRemainingBitmapIntersectionClipsWindowAndSkipsNoop) {
    auto read_schema = make_read_schema(_tablet_schema);
    SegmentIterator iter(nullptr, read_schema);
    iter._opts.stats = &_stats;
    // The range iterator only tracks the remaining row range when late runtime filters may
    // arrive; the bare iterator skips _init_impl(), so attach the container directly.
    iter._late_runtime_filter_container = make_late_runtime_filter_container(21);
    const std::vector<rowid_t> rowids {0, 2, 4, 6, 8, 10, 12};
    iter._row_bitmap.addMany(rowids.size(), rowids.data());
    iter._block_rowids.resize(rowids.size());
    iter._rebuild_range_iterator();

    uint16_t rows_read = 0;
    auto st = iter._read_columns_by_index({}, 2, rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(2, rows_read);
    EXPECT_EQ((std::vector<rowid_t> {0, 2}),
              (std::vector<rowid_t>(iter._block_rowids.begin(),
                                    iter._block_rowids.begin() + rows_read)));

    auto* range_iter_before = iter._range_iter.get();
    EXPECT_EQ(0, iter._intersect_remaining_row_bitmap(RowRanges::create_single(3, 13)));
    EXPECT_EQ(range_iter_before, iter._range_iter.get());
    EXPECT_EQ(rowids.size(), iter._row_bitmap.cardinality());

    RowRanges retained_ranges;
    retained_ranges.add(RowRange(0, 1));
    retained_ranges.add(RowRange(5, 11));
    retained_ranges.add(RowRange(20, 30));
    EXPECT_EQ(2, iter._intersect_remaining_row_bitmap(retained_ranges));
    EXPECT_EQ(3, iter._row_bitmap.cardinality());

    st = iter._read_columns_by_index({}, 10, rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(3, rows_read);
    EXPECT_EQ((std::vector<rowid_t> {6, 8, 10}),
              (std::vector<rowid_t>(iter._block_rowids.begin(),
                                    iter._block_rowids.begin() + rows_read)));
    st = iter._read_columns_by_index({}, 10, rows_read);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(0, rows_read);
}

TEST_F(SegmentIteratorExprZonemapTest,
       SparseReverseRemainingBitmapIntersectionClipsWindowAndSkipsNoop) {
    auto read_schema = make_read_schema(_tablet_schema);
    SegmentIterator iter(nullptr, read_schema);
    iter._opts.stats = &_stats;
    // The range iterator only tracks the remaining row range when late runtime filters may
    // arrive; the bare iterator skips _init_impl(), so attach the container directly.
    iter._late_runtime_filter_container = make_late_runtime_filter_container(22);
    iter._opts.read_orderby_key_reverse = true;
    const std::vector<rowid_t> rowids {0, 2, 4, 6, 8, 10, 12};
    iter._row_bitmap.addMany(rowids.size(), rowids.data());
    iter._block_rowids.resize(rowids.size());
    iter._rebuild_range_iterator();

    uint16_t rows_read = 0;
    auto st = iter._read_columns_by_index({}, 2, rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(2, rows_read);
    EXPECT_EQ((std::vector<rowid_t> {10, 12}),
              (std::vector<rowid_t>(iter._block_rowids.begin(),
                                    iter._block_rowids.begin() + rows_read)));

    auto* range_iter_before = iter._range_iter.get();
    EXPECT_EQ(0, iter._intersect_remaining_row_bitmap(RowRanges::create_single(0, 10)));
    EXPECT_EQ(range_iter_before, iter._range_iter.get());
    EXPECT_EQ(rowids.size(), iter._row_bitmap.cardinality());

    RowRanges retained_ranges;
    retained_ranges.add(RowRange(1, 8));
    retained_ranges.add(RowRange(11, 20));
    EXPECT_EQ(2, iter._intersect_remaining_row_bitmap(retained_ranges));
    EXPECT_EQ(3, iter._row_bitmap.cardinality());

    st = iter._read_columns_by_index({}, 10, rows_read);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(3, rows_read);
    EXPECT_EQ((std::vector<rowid_t> {2, 4, 6}),
              (std::vector<rowid_t>(iter._block_rowids.begin(),
                                    iter._block_rowids.begin() + rows_read)));
    st = iter._read_columns_by_index({}, 10, rows_read);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(0, rows_read);
}

TEST_F(SegmentIteratorExprZonemapTest,
       PageIntersectionCursorPreservesHalfOpenBoundariesAndExhaustion) {
    RowRanges row_ranges;
    row_ranges.add(RowRange(2, 4));
    row_ranges.add(RowRange(8, 10));
    row_ranges.add(RowRange(14, 18));

    size_t row_range_index = 0;
    const auto expect_page = [&](const RowRange& page_range, bool expected, size_t expected_index) {
        EXPECT_EQ(expected, SegmentIterator::_page_intersects_row_ranges(page_range, row_ranges,
                                                                         row_range_index));
        EXPECT_EQ(expected_index, row_range_index);
    };
    expect_page(RowRange(0, 2), false, 0);
    expect_page(RowRange(2, 4), true, 0);
    expect_page(RowRange(4, 8), false, 1);
    expect_page(RowRange(8, 10), true, 1);
    expect_page(RowRange(10, 14), false, 2);
    expect_page(RowRange(14, 18), true, 2);
    expect_page(RowRange(18, 21), false, 3);
    expect_page(RowRange(21, 22), false, 3);

    RowRanges empty_ranges;
    size_t empty_range_index = 0;
    EXPECT_FALSE(SegmentIterator::_page_intersects_row_ranges(RowRange(0, 1), empty_ranges,
                                                              empty_range_index));
    EXPECT_EQ(0, empty_range_index);
}

TEST_F(SegmentIteratorExprZonemapTest, ExprZonemapClipsMinRowidAndResetsCursorPerSlot) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);
    SegmentIterator iter(segment, read_schema);
    iter._file_reader = segment->_file_reader;
    iter._opts.stats = &_stats;
    iter._opts.tablet_schema = _tablet_schema;

    VExprContextSPtrs conjuncts {
            std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(0, 0)),
            std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 0)),
    };
    constexpr rowid_t kMinRowid = kNumRows / 2 + 1;
    RowRanges row_ranges;
    row_ranges.add(RowRange(0, 1));
    row_ranges.add(RowRange(kMinRowid - 1, kMinRowid + 1));

    auto st = iter._apply_expr_zonemap_to_row_ranges(conjuncts, kMinRowid, &row_ranges);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(1, row_ranges.range_size());
    EXPECT_EQ(kMinRowid, row_ranges.get_range_from(0));
    EXPECT_EQ(kMinRowid + 1, row_ranges.get_range_to(0));
    EXPECT_EQ(1, row_ranges.count());
    EXPECT_EQ(0, _stats.expr_zonemap_filtered_pages);
}

TEST_F(SegmentIteratorExprZonemapTest, ApplyExprZonemapPrunesPageRowRanges) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);
    SegmentIterator iter(segment, read_schema);
    iter._file_reader = segment->_file_reader;
    iter._opts.stats = &_stats;
    iter._opts.tablet_schema = _tablet_schema;

    auto expr_ctx = std::make_shared<VExprContext>(std::make_shared<IntMaxAtLeastExpr>(1, 500));
    VExprContextSPtrs conjuncts {expr_ctx};
    auto row_ranges = RowRanges::create_single(kNumRows);

    auto st = iter._apply_expr_zonemap_to_row_ranges(conjuncts, 0, &row_ranges);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_GT(_stats.expr_zonemap_filtered_pages, 0);
    EXPECT_GT(row_ranges.from(), 0);
    EXPECT_LT(row_ranges.count(), kNumRows);
    EXPECT_EQ(kNumRows, row_ranges.to());
}

TEST_F(SegmentIteratorExprZonemapTest, NewColumnIteratorReadsCommitTsoFromReadOptions) {
    constexpr int64_t kCommitTso = 466872251335573505L;
    _tablet_schema = make_commit_tso_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_commit_tso_segment(&segment));

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(7, 7);
    read_options.commit_tso = TsoRange(kCommitTso, kCommitTso);
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;

    ColumnIteratorUPtr iter;
    auto st = segment->new_column_iterator(_tablet_schema->column(1), &iter, &read_options);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);

    auto file_reader = segment->file_reader();
    ColumnIteratorOptions iter_opts;
    iter_opts.stats = &_stats;
    iter_opts.file_reader = file_reader.get();
    iter_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
    st = iter->init(iter_opts);
    ASSERT_TRUE(st.ok()) << st;

    MutableColumnPtr dst = ColumnVector<TYPE_BIGINT>::create();
    size_t n = kCommitTsoRows;
    bool has_null = true;
    st = iter->next_batch(&n, dst, &has_null);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_FALSE(has_null);
    ASSERT_EQ(kCommitTsoRows, dst->size());
    auto* col = assert_cast<ColumnInt64*>(dst.get());
    for (size_t i = 0; i < dst->size(); ++i) {
        EXPECT_EQ(kCommitTso, col->get_element(i));
    }
}

TEST_F(SegmentIteratorExprZonemapTest, LateRuntimeFilterUsesReadTimeCommitTso) {
    constexpr int64_t kCommitTso = 466872251335573505L;
    _tablet_schema = make_commit_tso_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_commit_tso_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    auto expr_context = std::make_shared<VExprContext>(
            std::make_shared<BigIntEqualsExpr>(1, kCommitTso, 1, COMMIT_TSO_COL));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_context));
    auto container = make_late_runtime_filter_container(10);
    publish_late_runtime_filter(container, expr_context);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(7, 7);
    read_options.commit_tso = TsoRange(kCommitTso, kCommitTso);
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_options.block_row_max = 1024;
    read_options.late_runtime_filter_container = container;

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    ASSERT_FALSE(iter->empty());
    st = iter->init(read_options);
    ASSERT_TRUE(st.ok()) << st;

    auto block = make_bigint_read_block(COMMIT_TSO_COL);
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(kCommitTsoRows, block.rows());
    const auto* commit_tso_column =
            assert_cast<const ColumnInt64*>(block.get_by_position(1).column.get());
    EXPECT_TRUE(std::ranges::all_of(commit_tso_column->get_data(),
                                    [](int64_t value) { return value == kCommitTso; }));
    EXPECT_EQ(0, _stats.expr_zonemap_filtered_segments);
    EXPECT_EQ(0, _stats.expr_zonemap_filtered_pages);
}

TEST_F(SegmentIteratorExprZonemapTest, NewIteratorPrunesCommitTsoByReadOptionValue) {
    constexpr int64_t kCommitTso = 466872251335573505L;
    _tablet_schema = make_commit_tso_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_commit_tso_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(7, 7);
    read_options.commit_tso = TsoRange(kCommitTso, kCommitTso);
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_options.col_id_to_predicates.emplace(1, make_commit_tso_gt_predicate(1, kCommitTso));

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    EXPECT_TRUE(iter->empty());
    EXPECT_EQ(1, _stats.total_segment_number);
    EXPECT_EQ(1, _stats.filtered_segment_number);
}

TEST_F(SegmentIteratorExprZonemapTest, LateRuntimeFilterUsesReadTimeBinlogTsoAfterLazyInit) {
    constexpr int64_t kCommitTso = 466872251335573505L;
    _tablet_schema = make_binlog_tso_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_binlog_tso_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);
    ASSERT_EQ(1, read_schema->tso_ordinal());

    std::shared_ptr<ColumnReader> physical_reader;
    auto st = segment->get_column_reader(_tablet_schema->column(1), &physical_reader, &_stats);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, physical_reader);
    ASSERT_TRUE(physical_reader->has_zone_map());
    ZoneMap physical_zone_map;
    st = physical_reader->get_segment_zone_map(&physical_zone_map);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_TRUE(physical_zone_map.has_null);
    EXPECT_FALSE(physical_zone_map.has_not_null);

    auto container = make_late_runtime_filter_container(15);
    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(7, 7);
    read_options.commit_tso = TsoRange(kCommitTso, kCommitTso);
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_options.read_row_binlog = true;
    read_options.block_row_max = 1024;
    read_options.late_runtime_filter_container = container;

    std::unique_ptr<RowwiseIterator> iter;
    st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    ASSERT_FALSE(iter->empty());
    st = iter->init(read_options);
    ASSERT_TRUE(st.ok()) << st;

    const auto make_block = [&]() {
        Block block;
        block.insert({ColumnInt32::create(), std::make_shared<DataTypeInt32>(), "0"});
        const auto tso_type = read_schema->data_type(1);
        block.insert({tso_type->create_column(), tso_type, BINLOG_TSO_COL});
        return block;
    };
    auto block = make_block();
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(1024, block.rows());

    auto expr_context = std::make_shared<VExprContext>(
            std::make_shared<BigIntEqualsExpr>(1, kCommitTso, 1, BINLOG_TSO_COL));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_context));
    publish_late_runtime_filter(container, expr_context);

    block.clear_column_data();
    st = iter->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(1024, block.rows());
    const auto* tso_column =
            assert_cast<const ColumnNullable*>(block.get_by_position(1).column.get());
    const auto* tso_values = assert_cast<const ColumnInt64*>(&tso_column->get_nested_column());
    EXPECT_TRUE(std::ranges::all_of(tso_column->get_null_map_data(),
                                    [](uint8_t is_null) { return is_null == 0; }));
    EXPECT_TRUE(std::ranges::all_of(tso_values->get_data(),
                                    [](int64_t value) { return value == kCommitTso; }));
    EXPECT_EQ(0, _stats.expr_zonemap_filtered_pages);
    EXPECT_EQ(1, _stats.late_runtime_filters_installed_after_lazy_init);
}

} // namespace doris::segment_v2
