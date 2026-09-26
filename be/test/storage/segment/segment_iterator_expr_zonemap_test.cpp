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
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/comparison_predicate.h"
#include "storage/row_cursor.h"
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

class IntMaxAtLeastExpr final : public VExpr {
public:
    IntMaxAtLeastExpr(int column_id, int32_t threshold)
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
        return zone_map->max_value.get<TYPE_INT>() >= _threshold ? ZoneMapFilterResult::kMayMatch
                                                                 : ZoneMapFilterResult::kNoMatch;
    }

private:
    int _column_id;
    int32_t _threshold;
    std::string _expr_name = "int_max_at_least_expr";
};

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

// A non-null BIGINT hidden column keyed by name; ReadSchema derives version/tso/commit_tso
// ordinals from these names (see ReadSchema::_init_descriptors and storage/utils.h).
TabletColumnPtr create_hidden_bigint_column(int32_t id, const std::string& name) {
    auto column = std::make_shared<TabletColumn>();
    column->set_unique_id(id);
    column->set_name(name);
    column->set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
    column->set_is_key(false);
    column->set_is_nullable(false);
    column->set_length(8);
    column->set_index_length(8);
    column->set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);
    return column;
}

// int key + a single hidden BIGINT column named `hidden_col_name`, so the read schema exposes
// exactly one of version_ordinal()/tso_ordinal()/commit_tso_ordinal() at ordinal 1.
TabletSchemaSPtr make_hidden_column_tablet_schema(const std::string& hidden_col_name) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->append_column(*create_int_key(0, false));
    tablet_schema->append_column(*create_hidden_bigint_column(1, hidden_col_name));
    tablet_schema->set_storage_page_size(4096);
    return tablet_schema;
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

    void build_commit_tso_segment(std::shared_ptr<Segment>* segment) {
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
            row.mutable_field(1) = Field::create_field<TYPE_BIGINT>(0);
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

// placeholder_effective_value only reads schema ordinals + read_options, so it can run against any
// built segment. The tests below pair a hidden-column read schema with crafted read_options.
TEST_F(SegmentIteratorExprZonemapTest, PlaceholderEffectiveVersionColSingleVersion) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(make_hidden_column_tablet_schema(VERSION_COL));
    ASSERT_EQ(1, read_schema->version_ordinal());

    StorageReadOptions read_options;
    read_options.version = Version(7, 7);

    auto effective = segment->placeholder_effective_value(read_schema->version_ordinal(),
                                                          *read_schema, read_options);
    ASSERT_TRUE(effective.has_value());
    EXPECT_EQ(7, effective->get<TYPE_BIGINT>());
}

TEST_F(SegmentIteratorExprZonemapTest, PlaceholderEffectiveVersionColMultiVersion) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(make_hidden_column_tablet_schema(VERSION_COL));

    StorageReadOptions read_options;
    read_options.version = Version(2, 7);

    // A range (compaction) segment carries real on-disk values, so no placeholder substitution.
    auto effective = segment->placeholder_effective_value(read_schema->version_ordinal(),
                                                          *read_schema, read_options);
    EXPECT_FALSE(effective.has_value());
}

TEST_F(SegmentIteratorExprZonemapTest, PlaceholderEffectiveCommitTsoSingleVersion) {
    constexpr int64_t kCommitTso = 466872251335573505L;
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(make_hidden_column_tablet_schema(COMMIT_TSO_COL));
    ASSERT_EQ(1, read_schema->commit_tso_ordinal());

    StorageReadOptions read_options;
    read_options.version = Version(7, 7);
    read_options.commit_tso = TsoRange(kCommitTso, kCommitTso);

    auto effective = segment->placeholder_effective_value(read_schema->commit_tso_ordinal(),
                                                          *read_schema, read_options);
    ASSERT_TRUE(effective.has_value());
    EXPECT_EQ(kCommitTso, effective->get<TYPE_BIGINT>());
}

TEST_F(SegmentIteratorExprZonemapTest, PlaceholderEffectiveCommitTsoUnpublished) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(make_hidden_column_tablet_schema(COMMIT_TSO_COL));

    StorageReadOptions read_options;
    read_options.version = Version(7, 7);
    read_options.commit_tso =
            TsoRange(); // end_tso() == -1: pre-publish, on-disk 0 stays effective.

    auto effective = segment->placeholder_effective_value(read_schema->commit_tso_ordinal(),
                                                          *read_schema, read_options);
    EXPECT_FALSE(effective.has_value());
}

TEST_F(SegmentIteratorExprZonemapTest, PlaceholderEffectiveBinlogTso) {
    constexpr int64_t kCommitTso = 466872251335573505L;
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(make_hidden_column_tablet_schema(BINLOG_TSO_COL));
    ASSERT_EQ(1, read_schema->tso_ordinal());

    StorageReadOptions read_options;
    read_options.version = Version(7, 7);
    read_options.read_row_binlog = true;
    read_options.commit_tso = TsoRange(kCommitTso, kCommitTso);

    auto effective = segment->placeholder_effective_value(read_schema->tso_ordinal(), *read_schema,
                                                          read_options);
    ASSERT_TRUE(effective.has_value());
    EXPECT_EQ(kCommitTso, effective->get<TYPE_BIGINT>());

    // Pre-publish (end_tso() == -1) resolves the binlog tso placeholder to 0 rather than skipping.
    read_options.commit_tso = TsoRange();
    effective = segment->placeholder_effective_value(read_schema->tso_ordinal(), *read_schema,
                                                     read_options);
    ASSERT_TRUE(effective.has_value());
    EXPECT_EQ(0, effective->get<TYPE_BIGINT>());
}

TEST_F(SegmentIteratorExprZonemapTest, PlaceholderEffectiveBinlogTsoWithoutRowBinlog) {
    constexpr int64_t kCommitTso = 466872251335573505L;
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(make_hidden_column_tablet_schema(BINLOG_TSO_COL));

    StorageReadOptions read_options;
    read_options.version = Version(7, 7);
    read_options.read_row_binlog = false; // binlog tso is substituted only for row-binlog reads.
    read_options.commit_tso = TsoRange(kCommitTso, kCommitTso);

    auto effective = segment->placeholder_effective_value(read_schema->tso_ordinal(), *read_schema,
                                                          read_options);
    EXPECT_FALSE(effective.has_value());
}

TEST_F(SegmentIteratorExprZonemapTest, PlaceholderEffectivePlainColumnHasNoSubstitution) {
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    // Plain schema: no hidden columns, so every hidden-column ordinal is -1.
    auto read_schema = make_read_schema(make_tablet_schema());
    ASSERT_EQ(-1, read_schema->version_ordinal());
    ASSERT_EQ(-1, read_schema->commit_tso_ordinal());
    ASSERT_EQ(-1, read_schema->tso_ordinal());

    StorageReadOptions read_options;
    read_options.version = Version(7, 7);
    read_options.read_row_binlog = true;
    read_options.commit_tso = TsoRange(466872251335573505L, 466872251335573505L);

    auto effective = segment->placeholder_effective_value(/*cid=*/0, *read_schema, read_options);
    EXPECT_FALSE(effective.has_value());
}

} // namespace doris::segment_v2
