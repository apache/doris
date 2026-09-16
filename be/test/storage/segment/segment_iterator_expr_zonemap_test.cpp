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
#include <set>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "exprs/vcompound_pred.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_iterator.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/comparison_predicate.h"
#include "storage/row_cursor.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/row_ranges.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_iterator.h"
#include "storage/segment/test_segment_writer.h"
#include "storage/tablet/tablet_schema_helper.h"
#include "storage/utils.h"

namespace doris::segment_v2 {
namespace {

constexpr auto kTestDir = "./ut_dir/segment_iterator_expr_zonemap_test";
constexpr int kNumRows = 8192;
constexpr int kCommitTsoRows = 8;
const RowsetId kRowsetId {.version = 1};

Field int_field(int32_t value) {
    return Field::create_field<TYPE_INT>(value);
}

template <PrimitiveType Type>
class NumericMaxAtLeastExpr final : public VExpr {
public:
    NumericMaxAtLeastExpr(int column_id, int64_t threshold)
            : _column_id(column_id), _threshold(threshold) {
        _data_type = std::make_shared<DataTypeUInt8>();
    }

    const std::string& expr_name() const override { return _expr_name; }

    Status execute_column_impl(VExprContext*, const Block*, const Selector*, size_t,
                               ColumnPtr&) const override {
        return Status::InternalError("IntMaxAtLeastExpr is only used by zonemap tests");
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
        return zone_map->max_value.get<Type>() >= _threshold ? ZoneMapFilterResult::kMayMatch
                                                             : ZoneMapFilterResult::kNoMatch;
    }

private:
    int _column_id;
    int64_t _threshold;
    std::string _expr_name = "int_max_at_least_expr";
};

using IntMaxAtLeastExpr = NumericMaxAtLeastExpr<TYPE_INT>;
using BigIntMaxAtLeastExpr = NumericMaxAtLeastExpr<TYPE_BIGINT>;

TabletSchemaSPtr make_tablet_schema() {
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->append_column(*create_int_key(0, false));
    tablet_schema->append_column(*create_int_key(1, false));
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

std::shared_ptr<AndBlockColumnPredicate> make_commit_tso_gt_predicate(int32_t column_id,
                                                                      int64_t value) {
    auto predicates = AndBlockColumnPredicate::create_shared();
    std::shared_ptr<ColumnPredicate> pred(
            new ComparisonPredicateBase<TYPE_BIGINT, PredicateType::GT>(
                    column_id, COMMIT_TSO_COL, Field::create_field<TYPE_BIGINT>(value)));
    predicates->add_column_predicate(SingleColumnBlockPredicate::create_unique(pred));
    return predicates;
}

// Read schema covers all tablet columns in order, so ordinal == tablet cid.
ReadSchemaSPtr make_read_schema(const TabletSchemaSPtr& tablet_schema) {
    return std::make_shared<ReadSchema>(tablet_schema->columns());
}

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

        VerticalSegmentWriterOptions opts;
        opts.num_rows_per_block = 1024;
        TestVerticalSegmentWriter writer(file_writer.get(), 0, _tablet_schema, nullptr, nullptr,
                                         opts, nullptr);
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
        st = writer.finalize_columns(&index_size);
        ASSERT_TRUE(st.ok()) << st;
        st = writer.finalize_footer(&file_size);
        ASSERT_TRUE(st.ok()) << st;
        st = file_writer->close();
        ASSERT_TRUE(st.ok()) << st;

        st = Segment::open(fs, path, 100, 0, kRowsetId, _tablet_schema, io::FileReaderOptions {},
                           segment);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(kNumRows, (*segment)->num_rows());
    }

    void build_commit_tso_segment(std::shared_ptr<Segment>* segment,
                                  const std::vector<int64_t>& disk_tsos = {}) {
        const auto path = std::string(kTestDir) + "/commit_tso_segment.dat";
        auto fs = io::global_local_filesystem();
        io::FileWriterPtr file_writer;
        auto st = fs->create_file(path, &file_writer);
        ASSERT_TRUE(st.ok()) << st;

        VerticalSegmentWriterOptions opts;
        opts.num_rows_per_block = 4;
        TestVerticalSegmentWriter writer(file_writer.get(), 0, _tablet_schema, nullptr, nullptr,
                                         opts, nullptr);
        st = writer.init();
        ASSERT_TRUE(st.ok()) << st;

        RowCursor row;
        std::vector<Field> fields(_tablet_schema->num_columns(), Field(PrimitiveType::TYPE_NULL));
        st = row.init_scan_key(_tablet_schema, std::move(fields));
        ASSERT_TRUE(st.ok()) << st;
        for (int rid = 0; rid < kCommitTsoRows; ++rid) {
            row.mutable_field(0) = int_field(rid);
            row.mutable_field(1) =
                    Field::create_field<TYPE_BIGINT>(disk_tsos.empty() ? 0 : disk_tsos.at(rid));
            st = writer.append_row(row);
            ASSERT_TRUE(st.ok()) << st;
        }

        uint64_t file_size = 0;
        uint64_t index_size = 0;
        st = writer.finalize_columns(&index_size);
        ASSERT_TRUE(st.ok()) << st;
        st = writer.finalize_footer(&file_size);
        ASSERT_TRUE(st.ok()) << st;
        st = file_writer->close();
        ASSERT_TRUE(st.ok()) << st;

        st = Segment::open(fs, path, 100, 0, kRowsetId, _tablet_schema, io::FileReaderOptions {},
                           segment);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(kCommitTsoRows, (*segment)->num_rows());
    }

    StorageReadOptions commit_tso_read_options(int64_t tso = 42) {
        StorageReadOptions opts;
        opts.stats = &_stats;
        opts.runtime_state = &_runtime_state;
        opts.tablet_schema = _tablet_schema;
        opts.rowset_id = kRowsetId;
        opts.version = Version(7, 7);
        opts.commit_tso = TsoRange(tso, tso);
        return opts;
    }

    void expect_tso_rows(const std::shared_ptr<Segment>& segment, const TabletColumn& column,
                         const StorageReadOptions& opts, const std::vector<int64_t>& expected) {
        ColumnIteratorUPtr iter;
        ASSERT_TRUE(segment->new_column_iterator(column, &iter, &opts).ok());
        ColumnIteratorOptions iter_opts;
        iter_opts.stats = &_stats;
        iter_opts.file_reader = segment->file_reader().get();
        ASSERT_TRUE(iter->init(iter_opts).ok());
        ASSERT_TRUE(iter->seek_to_ordinal(0).ok());
        MutableColumnPtr dst = ColumnInt64::create();
        size_t count = expected.size();
        bool has_null = false;
        ASSERT_TRUE(iter->next_batch(&count, dst, &has_null).ok());
        ASSERT_EQ(expected.size(), count);
        ASSERT_EQ(expected.size(), dst->size());
        EXPECT_FALSE(has_null);
        for (size_t i = 0; i < count; ++i) {
            EXPECT_EQ(expected[i], assert_cast<ColumnInt64&>(*dst).get_element(i));
        }
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

// Internal unpublished reads and logical reads share a Segment, but must never change each
// other's values through cache state. Exercise both orders using a real physical placeholder.
TEST_F(SegmentIteratorExprZonemapTest, CommitTsoReadsAreIndependentOfPhysicalCacheOrder) {
    _tablet_schema = make_commit_tso_tablet_schema();
    for (bool physical_first : {false, true}) {
        SCOPED_TRACE(physical_first);
        std::shared_ptr<Segment> segment;
        ASSERT_NO_FATAL_FAILURE(build_commit_tso_segment(&segment));
        auto unpublished = commit_tso_read_options(-1);
        auto published = commit_tso_read_options();
        // The column identity comes from the column descriptor, not an optional schema pointer.
        published.tablet_schema = nullptr;
        const auto& column = _tablet_schema->column(1);
        if (physical_first) {
            ASSERT_NO_FATAL_FAILURE(expect_tso_rows(segment, column, unpublished,
                                                    std::vector<int64_t>(kCommitTsoRows, 0)));
        }
        ASSERT_NO_FATAL_FAILURE(expect_tso_rows(segment, column, published,
                                                std::vector<int64_t>(kCommitTsoRows, 42)));
        ASSERT_NO_FATAL_FAILURE(expect_tso_rows(segment, column, unpublished,
                                                std::vector<int64_t>(kCommitTsoRows, 0)));
        ASSERT_NO_FATAL_FAILURE(expect_tso_rows(segment, column, published,
                                                std::vector<int64_t>(kCommitTsoRows, 42)));
        std::shared_ptr<ColumnReader> physical;
        ASSERT_TRUE(segment->get_physical_column_reader(column, &physical, &_stats).ok());
        ZoneMap zone_map;
        ASSERT_TRUE(physical->get_segment_zone_map(&zone_map).ok());
        EXPECT_EQ(0, zone_map.min_value.get<TYPE_BIGINT>());
        EXPECT_EQ(0, zone_map.max_value.get<TYPE_BIGINT>());
    }
}

TEST_F(SegmentIteratorExprZonemapTest, MultiVersionCommitTsoReadsMaterializedValues) {
    _tablet_schema = make_commit_tso_tablet_schema();
    std::vector<int64_t> disk_tsos {42, 42, 42, 42, 84, 84, 84, 84};
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_commit_tso_segment(&segment, disk_tsos));
    auto opts = commit_tso_read_options();
    opts.version = Version(7, 8);
    opts.commit_tso = TsoRange(42, 84);
    ASSERT_NO_FATAL_FAILURE(expect_tso_rows(segment, _tablet_schema->column(1), opts, disk_tsos));
    std::shared_ptr<ColumnReader> reader;
    ASSERT_TRUE(segment->get_column_reader(_tablet_schema->column(1), &reader, opts).ok());
    ZoneMap zone_map;
    ASSERT_TRUE(reader->get_segment_zone_map(&zone_map).ok());
    EXPECT_EQ(42, zone_map.min_value.get<TYPE_BIGINT>());
    EXPECT_EQ(84, zone_map.max_value.get<TYPE_BIGINT>());
}

TEST_F(SegmentIteratorExprZonemapTest, CommitTsoDoesNotRequirePhysicalColumnMetadata) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    const auto column = create_commit_tso_column(2);
    auto opts = commit_tso_read_options();
    ASSERT_NO_FATAL_FAILURE(expect_tso_rows(segment, *column, opts, {42, 42}));
}

TEST_F(SegmentIteratorExprZonemapTest, CompoundExprUsesCommitTsoBeforeDataIteratorCreation) {
    _tablet_schema = make_commit_tso_tablet_schema();
    for (bool physical_first : {false, true}) {
        for (int64_t threshold : {42, 43}) {
            SCOPED_TRACE(threshold);
            SCOPED_TRACE(physical_first);
            std::shared_ptr<Segment> segment;
            ASSERT_NO_FATAL_FAILURE(build_commit_tso_segment(&segment));
            if (physical_first) {
                std::shared_ptr<ColumnReader> physical;
                ASSERT_TRUE(segment->get_physical_column_reader(_tablet_schema->column(1),
                                                                &physical, &_stats)
                                    .ok());
            }
            TExprNode node;
            node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
            node.__set_node_type(TExprNodeType::COMPOUND_PRED);
            node.__set_opcode(TExprOpcode::COMPOUND_OR);
            node.__set_num_children(2);
            node.__set_is_nullable(false);
            auto expr = std::make_shared<VCompoundPred>(node);
            expr->add_child(std::make_shared<BigIntMaxAtLeastExpr>(1, threshold));
            expr->add_child(std::make_shared<IntMaxAtLeastExpr>(0, 1000));
            auto ctx = std::make_shared<VExprContext>(expr);
            ASSERT_NO_FATAL_FAILURE(prepare_expr_context(ctx));
            auto opts = commit_tso_read_options();
            opts.common_expr_ctxs_push_down = {ctx};
            std::unique_ptr<RowwiseIterator> iter;
            ASSERT_TRUE(segment->new_iterator(make_read_schema(_tablet_schema), opts, &iter).ok());
            EXPECT_EQ(threshold > 42, iter->empty());
        }
    }
}

TEST_F(SegmentIteratorExprZonemapTest, PageExprDoesNotReadPlaceholderZoneMaps) {
    _tablet_schema = make_commit_tso_tablet_schema();
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_commit_tso_segment(&segment));
    std::shared_ptr<ColumnReader> physical;
    ASSERT_TRUE(segment->get_physical_column_reader(_tablet_schema->column(1), &physical, &_stats)
                        .ok());
    SegmentIterator iter(segment, make_read_schema(_tablet_schema));
    iter._file_reader = segment->file_reader();
    iter._opts = commit_tso_read_options();
    auto ctx = std::make_shared<VExprContext>(std::make_shared<BigIntMaxAtLeastExpr>(1, 42));
    auto ranges = RowRanges::create_single(kCommitTsoRows);
    ASSERT_TRUE(iter._apply_expr_zonemap_to_row_ranges({ctx}, 0, &ranges).ok());
    EXPECT_EQ(kCommitTsoRows, ranges.count());
}

TEST_F(SegmentIteratorExprZonemapTest, CommitTsoIndexDoesNotOpenPhysicalIndexFile) {
    _tablet_schema = make_commit_tso_tablet_schema();
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_commit_tso_segment(&segment));
    std::unique_ptr<IndexIterator> iter;
    TabletIndex index;
    ASSERT_TRUE(segment->new_index_iterator(_tablet_schema->column(1), &index,
                                            commit_tso_read_options(), &iter)
                        .ok());
    EXPECT_EQ(nullptr, iter);
    EXPECT_EQ(nullptr, segment->_index_file_reader);
}

TEST_F(SegmentIteratorExprZonemapTest, StatisticsIteratorUsesLogicalCommitTso) {
    _tablet_schema = make_commit_tso_tablet_schema();
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_commit_tso_segment(&segment));
    auto schema = make_read_schema(_tablet_schema);
    auto opts = commit_tso_read_options();
    opts.push_down_agg_type_opt = TPushAggOp::MINMAX;
    std::unique_ptr<RowwiseIterator> iter;
    ASSERT_TRUE(segment->new_iterator(schema, opts, &iter).ok());
    auto block = schema->create_read_block();
    ASSERT_TRUE(iter->next_batch(&block).ok());
    ASSERT_EQ(2, block.rows());
    const auto& tsos = assert_cast<const ColumnInt64&>(*block.get_by_position(1).column);
    EXPECT_EQ(42, tsos.get_element(0));
    EXPECT_EQ(42, tsos.get_element(1));
}

TEST_F(SegmentIteratorExprZonemapTest, RowIdReadUsesLogicalCommitTso) {
    _tablet_schema = make_commit_tso_tablet_schema();
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_commit_tso_segment(&segment));
    TSlotDescriptor desc = TSlotDescriptorBuilder()
                                   .type(TYPE_BIGINT)
                                   .nullable(false)
                                   .column_name(COMMIT_TSO_COL)
                                   .column_pos(1)
                                   .build();
    desc.__set_col_unique_id(1);
    SlotDescriptor slot(desc);
    auto opts = commit_tso_read_options();
    ColumnIteratorUPtr iter;
    MutableColumnPtr dst = ColumnInt64::create();
    ASSERT_TRUE(
            segment->seek_and_read_by_rowid(*_tablet_schema, &slot, {1, 5}, dst, opts, iter).ok());
    ASSERT_EQ(2, dst->size());
    EXPECT_EQ(42, assert_cast<ColumnInt64&>(*dst).get_element(0));
    EXPECT_EQ(42, assert_cast<ColumnInt64&>(*dst).get_element(1));
}

} // namespace doris::segment_v2
