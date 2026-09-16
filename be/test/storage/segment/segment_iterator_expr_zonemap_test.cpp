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

#include <future>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/delete/delete_handler.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/iterator/vgeneric_iterators.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/comparison_predicate.h"
#include "storage/row_cursor.h"
#include "storage/rowset/rowset_meta.h"
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
constexpr int kRuntimeColumnRows = 8;
constexpr int kVersionCid = 1;
constexpr int kBinlogTimestampCid = 2;
constexpr int kCommitTsoCid = 3;
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

TabletSchemaSPtr make_agg_keys_tablet_schema() {
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->append_column(*create_int_key(0, false));
    tablet_schema->append_column(
            *create_int_value(1, FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM, false));
    tablet_schema->set_keys_type(KeysType::AGG_KEYS);
    tablet_schema->set_storage_page_size(4096);
    return tablet_schema;
}

TabletColumnPtr make_runtime_bigint_column(int32_t id, const std::string& name, bool is_nullable,
                                           const std::string& default_value = "") {
    auto column = std::make_shared<TabletColumn>();
    column->set_unique_id(id);
    column->set_name(name);
    column->set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
    column->set_is_key(false);
    column->set_is_nullable(is_nullable);
    column->set_length(8);
    column->set_index_length(8);
    column->set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);
    if (!default_value.empty()) {
        column->set_default_value(default_value);
    }
    return column;
}

TabletSchemaSPtr make_runtime_column_tablet_schema() {
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->append_column(*create_int_key(0, false));
    tablet_schema->append_column(*make_runtime_bigint_column(kVersionCid, VERSION_COL, false, "0"));
    tablet_schema->append_column(
            *make_runtime_bigint_column(kBinlogTimestampCid, BINLOG_TSO_COL, true));
    tablet_schema->append_column(
            *make_runtime_bigint_column(kCommitTsoCid, COMMIT_TSO_COL, false, "0"));
    tablet_schema->set_storage_page_size(4096);
    return tablet_schema;
}

TabletSchemaSPtr make_key_only_tablet_schema() {
    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->append_column(*create_int_key(0, false));
    tablet_schema->set_storage_page_size(4096);
    return tablet_schema;
}

TabletSchemaSPtr make_schema_with_added_default_column() {
    auto tablet_schema = make_key_only_tablet_schema();
    tablet_schema->append_column(*make_runtime_bigint_column(1, "added_value", false, "42"));
    return tablet_schema;
}

TabletSchemaSPtr make_schema_with_added_nullable_variant() {
    auto tablet_schema = make_key_only_tablet_schema();
    TabletColumn variant_column;
    variant_column.set_unique_id(1);
    variant_column.set_name("v");
    variant_column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    variant_column.set_is_key(false);
    variant_column.set_is_nullable(true);
    variant_column.set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);
    tablet_schema->append_column(std::move(variant_column));
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

    void build_runtime_column_segment(std::shared_ptr<Segment>* segment,
                                      bool write_runtime_columns = true) {
        const auto path = std::string(kTestDir) + "/runtime_column_segment.dat";
        auto fs = io::global_local_filesystem();
        io::FileWriterPtr file_writer;
        auto st = fs->create_file(path, &file_writer);
        ASSERT_TRUE(st.ok()) << st;

        VerticalSegmentWriterOptions opts;
        opts.num_rows_per_block = 4;
        // Write with a key-only schema to model an old segment created before the runtime system
        // columns were added. Segment::open still receives the current full schema below, so the
        // reader must synthesize VERSION/TSO from StorageReadOptions instead of reading data pages.
        const auto writer_schema =
                write_runtime_columns ? _tablet_schema : make_key_only_tablet_schema();
        TestVerticalSegmentWriter writer(file_writer.get(), 0, writer_schema, nullptr, nullptr,
                                         opts, nullptr);
        st = writer.init();
        ASSERT_TRUE(st.ok()) << st;

        RowCursor row;
        std::vector<Field> fields(writer_schema->num_columns(), Field(PrimitiveType::TYPE_NULL));
        st = row.init_scan_key(writer_schema, std::move(fields));
        ASSERT_TRUE(st.ok()) << st;
        for (int rid = 0; rid < kRuntimeColumnRows; ++rid) {
            row.mutable_field(0) = int_field(rid);
            if (write_runtime_columns) {
                row.mutable_field(kVersionCid) = Field::create_field<TYPE_BIGINT>(0);
                row.mutable_field(kBinlogTimestampCid) = Field();
                row.mutable_field(kCommitTsoCid) = Field::create_field<TYPE_BIGINT>(0);
            }
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
        ASSERT_EQ(kRuntimeColumnRows, (*segment)->num_rows());
    }

    void read_column(const std::shared_ptr<Segment>& segment, int32_t cid,
                     const StorageReadOptions& read_options, MutableColumnPtr* dst) {
        ColumnIteratorUPtr iter;
        auto st = segment->new_column_iterator(_tablet_schema->column(cid), &iter, &read_options);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_NE(nullptr, iter);

        auto file_reader = segment->file_reader();
        ColumnIteratorOptions iter_opts;
        iter_opts.stats = &_stats;
        iter_opts.file_reader = file_reader.get();
        iter_opts.io_ctx = read_options.io_ctx;
        st = iter->init(iter_opts);
        ASSERT_TRUE(st.ok()) << st;
        st = iter->seek_to_ordinal(0);
        ASSERT_TRUE(st.ok()) << st;

        *dst = Schema::get_data_type_ptr(_tablet_schema->column(cid))->create_column();
        size_t n = kRuntimeColumnRows;
        bool has_null = false;
        st = iter->next_batch(&n, *dst, &has_null);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(kRuntimeColumnRows, n);
        ASSERT_EQ(kRuntimeColumnRows, (*dst)->size());
    }

    void expect_bigint_values(const MutableColumnPtr& column, int64_t expected) {
        ASSERT_NE(nullptr, column.get());
        const IColumn* data_column = column.get();
        if (const auto* nullable = check_and_get_column<ColumnNullable>(data_column)) {
            for (size_t i = 0; i < nullable->size(); ++i) {
                EXPECT_FALSE(nullable->is_null_at(i));
            }
            data_column = &nullable->get_nested_column();
        }
        const auto* bigint_column = check_and_get_column<ColumnInt64>(data_column);
        ASSERT_NE(nullptr, bigint_column);
        for (size_t i = 0; i < bigint_column->size(); ++i) {
            EXPECT_EQ(expected, bigint_column->get_element(i));
        }
    }

    void expect_all_null(const MutableColumnPtr& column) {
        ASSERT_NE(nullptr, column.get());
        const auto* nullable = check_and_get_column<ColumnNullable>(column.get());
        ASSERT_NE(nullptr, nullable);
        for (size_t i = 0; i < nullable->size(); ++i) {
            EXPECT_TRUE(nullable->is_null_at(i));
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
    StorageReadOptions read_options(_stats);
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
    StorageReadOptions read_options(_stats);
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

TEST_F(SegmentIteratorExprZonemapTest, StatisticsIteratorFallsBackWithoutZoneMap) {
    // Case: MIN/MAX or MIX is requested for an AGG_KEYS value column without a zone map. The
    // segment must use its ordinary row iterator instead of an unusable statistics iterator.
    _tablet_schema = make_agg_keys_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    StorageReadOptions read_options(_stats);
    read_options.tablet_schema = _tablet_schema;
    std::shared_ptr<ColumnReader> value_reader;
    auto st = segment->get_column_reader_for_pruning(_tablet_schema->column(1), read_options,
                                                     &value_reader);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, value_reader);
    ASSERT_FALSE(value_reader->has_zone_map());

    for (auto agg_type : {TPushAggOp::MINMAX, TPushAggOp::MIX}) {
        SCOPED_TRACE(agg_type);
        read_options.push_down_agg_type_opt = agg_type;

        std::unique_ptr<RowwiseIterator> iter;
        st = segment->new_iterator(read_schema, read_options, &iter);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_NE(nullptr, iter);
        EXPECT_NE(nullptr, dynamic_cast<SegmentIterator*>(iter.get()));
    }
}

TEST_F(SegmentIteratorExprZonemapTest, CountStillUsesStatisticsIteratorWithoutZoneMap) {
    // Case: COUNT is requested for an AGG_KEYS value column without a zone map. COUNT needs only
    // row count/nullability, so the statistics iterator remains usable.
    _tablet_schema = make_agg_keys_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    StorageReadOptions read_options(_stats);
    read_options.tablet_schema = _tablet_schema;
    read_options.push_down_agg_type_opt = TPushAggOp::COUNT;

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    EXPECT_NE(nullptr, dynamic_cast<VStatisticsIterator*>(iter.get()));
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

TEST_F(SegmentIteratorExprZonemapTest, RuntimeColumnsUseCurrentReadOptions) {
    // Case: the same segment is read by successive requests with different VERSION and TSO
    // values. Runtime constants must be request-scoped even when column readers are cached.
    constexpr int64_t kCommitTso1 = 466872251335573505L;
    constexpr int64_t kCommitTso2 = kCommitTso1 + 100;
    _tablet_schema = make_runtime_column_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_runtime_column_segment(&segment));

    StorageReadOptions read_options(_stats);
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(0, 1);
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;

    // Before publish, physical readers may already be created and cached.
    MutableColumnPtr version_column;
    MutableColumnPtr binlog_timestamp_column;
    MutableColumnPtr commit_tso_column;
    ASSERT_NO_FATAL_FAILURE(read_column(segment, kVersionCid, read_options, &version_column));
    ASSERT_NO_FATAL_FAILURE(
            read_column(segment, kBinlogTimestampCid, read_options, &binlog_timestamp_column));
    ASSERT_NO_FATAL_FAILURE(read_column(segment, kCommitTsoCid, read_options, &commit_tso_column));
    ASSERT_NO_FATAL_FAILURE(expect_bigint_values(version_column, 0));
    ASSERT_NO_FATAL_FAILURE(expect_all_null(binlog_timestamp_column));
    ASSERT_NO_FATAL_FAILURE(expect_bigint_values(commit_tso_column, 0));

    read_options.version = Version(7, 7);
    read_options.commit_tso = TsoRange(kCommitTso1, kCommitTso1);
    read_options.read_row_binlog = true;
    ASSERT_NO_FATAL_FAILURE(read_column(segment, kVersionCid, read_options, &version_column));
    ASSERT_NO_FATAL_FAILURE(
            read_column(segment, kBinlogTimestampCid, read_options, &binlog_timestamp_column));
    ASSERT_NO_FATAL_FAILURE(read_column(segment, kCommitTsoCid, read_options, &commit_tso_column));
    ASSERT_NO_FATAL_FAILURE(expect_bigint_values(version_column, 7));
    ASSERT_NO_FATAL_FAILURE(expect_bigint_values(binlog_timestamp_column, kCommitTso1));
    ASSERT_NO_FATAL_FAILURE(expect_bigint_values(commit_tso_column, kCommitTso1));

    // A second read must observe the new request values instead of a cached constant reader.
    read_options.version = Version(9, 9);
    read_options.commit_tso = TsoRange(kCommitTso2, kCommitTso2);
    ASSERT_NO_FATAL_FAILURE(read_column(segment, kVersionCid, read_options, &version_column));
    ASSERT_NO_FATAL_FAILURE(
            read_column(segment, kBinlogTimestampCid, read_options, &binlog_timestamp_column));
    ASSERT_NO_FATAL_FAILURE(read_column(segment, kCommitTsoCid, read_options, &commit_tso_column));
    ASSERT_NO_FATAL_FAILURE(expect_bigint_values(version_column, 9));
    ASSERT_NO_FATAL_FAILURE(expect_bigint_values(binlog_timestamp_column, kCommitTso2));
    ASSERT_NO_FATAL_FAILURE(expect_bigint_values(commit_tso_column, kCommitTso2));
}

TEST_F(SegmentIteratorExprZonemapTest, RangeVersionUsesPhysicalValues) {
    // Case: a non-singleton requested version means this is not a point-in-time runtime constant.
    // Existing physical hidden-column values must win instead of using either range endpoint.
    constexpr int64_t kCommitTso = 466872251335573505L;
    _tablet_schema = make_runtime_column_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_runtime_column_segment(&segment));

    StorageReadOptions read_options(_stats);
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(7, 9);
    read_options.commit_tso = TsoRange(kCommitTso, kCommitTso);
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_options.read_row_binlog = true;

    MutableColumnPtr version_column;
    MutableColumnPtr binlog_timestamp_column;
    MutableColumnPtr commit_tso_column;
    ASSERT_NO_FATAL_FAILURE(read_column(segment, kVersionCid, read_options, &version_column));
    ASSERT_NO_FATAL_FAILURE(
            read_column(segment, kBinlogTimestampCid, read_options, &binlog_timestamp_column));
    ASSERT_NO_FATAL_FAILURE(read_column(segment, kCommitTsoCid, read_options, &commit_tso_column));
    ASSERT_NO_FATAL_FAILURE(expect_bigint_values(version_column, 0));
    ASSERT_NO_FATAL_FAILURE(expect_all_null(binlog_timestamp_column));
    ASSERT_NO_FATAL_FAILURE(expect_bigint_values(commit_tso_column, 0));
}

TEST_F(SegmentIteratorExprZonemapTest, MissingPhysicalRuntimeColumnsUseReadOptions) {
    // Case: an old segment predates the hidden VERSION/TSO columns. A singleton read request must
    // synthesize those values for every row from StorageReadOptions.
    constexpr int64_t kCommitTso = 466872251335573505L;
    _tablet_schema = make_runtime_column_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_runtime_column_segment(&segment, false));

    StorageReadOptions read_options(_stats);
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(7, 7);
    read_options.commit_tso = TsoRange(kCommitTso, kCommitTso);
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_options.read_row_binlog = true;

    MutableColumnPtr version_column;
    MutableColumnPtr binlog_timestamp_column;
    MutableColumnPtr commit_tso_column;
    ASSERT_NO_FATAL_FAILURE(read_column(segment, kVersionCid, read_options, &version_column));
    ASSERT_NO_FATAL_FAILURE(
            read_column(segment, kBinlogTimestampCid, read_options, &binlog_timestamp_column));
    ASSERT_NO_FATAL_FAILURE(read_column(segment, kCommitTsoCid, read_options, &commit_tso_column));
    ASSERT_NO_FATAL_FAILURE(expect_bigint_values(version_column, 7));
    ASSERT_NO_FATAL_FAILURE(expect_bigint_values(binlog_timestamp_column, kCommitTso));
    ASSERT_NO_FATAL_FAILURE(expect_bigint_values(commit_tso_column, kCommitTso));
}

TEST_F(SegmentIteratorExprZonemapTest,
       ConcurrentPointLookupReadsKeepRuntimeConstantsRequestScoped) {
    // Case: concurrent point lookups share one old segment but carry different commit TSOs. No
    // task may observe a constant reader created for another request.
    _tablet_schema = make_runtime_column_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_runtime_column_segment(&segment, false));

    constexpr int kTasks = 8;
    constexpr int kReadsPerTask = 32;
    auto slot_descriptor = TSlotDescriptorBuilder()
                                   .type(TYPE_BIGINT)
                                   .nullable(false)
                                   .column_name(COMMIT_TSO_COL)
                                   .column_pos(kCommitTsoCid)
                                   .build();
    slot_descriptor.__set_col_unique_id(kCommitTsoCid);
    SlotDescriptor commit_tso_slot(slot_descriptor);
    std::vector<std::future<std::string>> tasks;
    tasks.reserve(kTasks);
    for (int task_id = 0; task_id < kTasks; ++task_id) {
        tasks.emplace_back(std::async(std::launch::async, [&, task_id]() -> std::string {
            const int64_t expected_tso = 1000000 + task_id;
            for (int read = 0; read < kReadsPerTask; ++read) {
                OlapReaderStatistics stats;
                StorageReadOptions read_options(stats);
                read_options.tablet_schema = _tablet_schema;
                read_options.version = Version(task_id, task_id);
                read_options.commit_tso = TsoRange(expected_tso, expected_tso);
                read_options.io_ctx.reader_type = ReaderType::READER_QUERY;

                MutableColumnPtr dst = ColumnInt64::create();
                std::unique_ptr<ColumnIterator> iterator_hint;
                auto st = segment->seek_and_read_by_rowid(*_tablet_schema, &commit_tso_slot, {0},
                                                          dst, read_options, iterator_hint);
                if (!st.ok()) {
                    return st.to_string();
                }
                const auto* values = check_and_get_column<ColumnInt64>(dst.get());
                if (values == nullptr || values->size() != 1 ||
                    values->get_element(0) != expected_tso) {
                    return "runtime constant leaked across concurrent requests";
                }
            }
            return {};
        }));
    }

    for (auto& task : tasks) {
        EXPECT_TRUE(task.get().empty());
    }
}

TEST_F(SegmentIteratorExprZonemapTest, HiddenConstantsFeedStatisticsIterator) {
    // Case: hidden VERSION/TSO columns are absent physically but constant for this request. MIN/MAX
    // pushdown must consume their synthetic zone-map values through VStatisticsIterator.
    constexpr int64_t kCommitTso = 466872251335573505L;
    _tablet_schema = make_runtime_column_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_runtime_column_segment(&segment, false));
    auto read_schema = std::make_shared<ReadSchema>(
            project_columns_by_ordinal(_tablet_schema->columns(), {kVersionCid, kCommitTsoCid}));

    StorageReadOptions read_options(_stats);
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(7, 7);
    read_options.commit_tso = TsoRange(kCommitTso, kCommitTso);
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_options.push_down_agg_type_opt = TPushAggOp::MINMAX;

    std::unique_ptr<RowwiseIterator> iterator;
    auto st = segment->new_iterator(read_schema, read_options, &iterator);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, dynamic_cast<VStatisticsIterator*>(iterator.get()));

    Block block = read_schema->create_read_block();
    st = iterator->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(2, block.rows());
    const auto* version_values =
            check_and_get_column<ColumnInt64>(block.get_by_position(0).column.get());
    const auto* commit_tso_values =
            check_and_get_column<ColumnInt64>(block.get_by_position(1).column.get());
    ASSERT_NE(nullptr, version_values);
    ASSERT_NE(nullptr, commit_tso_values);
    EXPECT_EQ(7, version_values->get_element(0));
    EXPECT_EQ(7, version_values->get_element(1));
    EXPECT_EQ(kCommitTso, commit_tso_values->get_element(0));
    EXPECT_EQ(kCommitTso, commit_tso_values->get_element(1));
    EXPECT_TRUE(iterator->next_batch(&block).is<ErrorCode::END_OF_FILE>());
}

TEST_F(SegmentIteratorExprZonemapTest, MissingOrdinaryColumnUsesSchemaDefault) {
    // Case: an ordinary defaulted column is absent from an old segment. Physical pruning and value
    // reads must both resolve it to a ConstantColumnReader instead of reporting NOT_FOUND.
    _tablet_schema = make_schema_with_added_default_column();

    // The key-only writer models a segment created before `added_value` was added to the schema.
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_runtime_column_segment(&segment, false));

    StorageReadOptions read_options(_stats);
    read_options.tablet_schema = _tablet_schema;
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;

    std::shared_ptr<ColumnReader> pruning_reader;
    auto st = segment->get_column_reader_for_pruning(_tablet_schema->column(1), read_options,
                                                     &pruning_reader);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, pruning_reader);
    ASSERT_NE(nullptr, dynamic_cast<ConstantColumnReader*>(pruning_reader.get()));

    MutableColumnPtr added_value_column;
    ASSERT_NO_FATAL_FAILURE(read_column(segment, 1, read_options, &added_value_column));
    ASSERT_NO_FATAL_FAILURE(expect_bigint_values(added_value_column, 42));
}

TEST_F(SegmentIteratorExprZonemapTest, SchemaDefaultFeedsMinMaxStatisticsIterator) {
    // Case: MIN/MAX is pushed down for a column added after the segment was written. The statistics
    // iterator must return the schema default as both extrema without scanning physical rows.
    _tablet_schema = make_schema_with_added_default_column();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_runtime_column_segment(&segment, false));
    auto read_schema = std::make_shared<ReadSchema>(
            project_columns_by_ordinal(_tablet_schema->columns(), {1}));

    StorageReadOptions read_options(_stats);
    read_options.tablet_schema = _tablet_schema;
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_options.push_down_agg_type_opt = TPushAggOp::MINMAX;

    std::unique_ptr<RowwiseIterator> iterator;
    auto st = segment->new_iterator(read_schema, read_options, &iterator);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, dynamic_cast<VStatisticsIterator*>(iterator.get()));

    Block block = read_schema->create_read_block();
    st = iterator->next_batch(&block);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(2, block.rows());
    const auto* values = check_and_get_column<ColumnInt64>(block.get_by_position(0).column.get());
    ASSERT_NE(nullptr, values);
    EXPECT_EQ(42, values->get_element(0));
    EXPECT_EQ(42, values->get_element(1));
}

TEST_F(SegmentIteratorExprZonemapTest, DeletePredicateFiltersSchemaDefault) {
    // Case: DELETE WHERE targets the default of a column that is absent from an old segment. The
    // synthesized value must participate in delete evaluation and remove every matching old row.
    _tablet_schema = make_schema_with_added_default_column();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_runtime_column_segment(&segment, false));
    auto read_schema = make_read_schema(_tablet_schema);

    DeletePredicatePB delete_predicate;
    auto* predicate = delete_predicate.add_sub_predicates_v2();
    predicate->set_column_name("added_value");
    predicate->set_column_unique_id(1);
    predicate->set_op("=");
    predicate->set_cond_value("42");
    auto rowset_meta = std::make_shared<RowsetMeta>();
    rowset_meta->set_tablet_schema(_tablet_schema);
    rowset_meta->set_version(Version(2, 2));
    rowset_meta->set_delete_predicate(delete_predicate);

    DeleteHandler delete_handler;
    std::vector<TabletColumn> dropped_columns;
    auto st = delete_handler.init({rowset_meta}, /*version=*/100, read_schema, &dropped_columns);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(dropped_columns.empty());

    StorageReadOptions read_options(_stats);
    read_options.tablet_schema = _tablet_schema;
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
    delete_handler.get_delete_conditions_after_version(
            0, read_options.delete_condition_predicates.get(),
            &read_options.del_predicates_for_zone_map);

    std::unique_ptr<RowwiseIterator> iterator;
    st = segment->new_iterator(read_schema, read_options, &iterator);
    ASSERT_TRUE(st.ok()) << st;

    Block block = read_schema->create_read_block();
    st = iterator->next_batch(&block);
    if (st.ok()) {
        ASSERT_EQ(0, block.rows());
        st = iterator->next_batch(&block);
    }
    EXPECT_TRUE(st.is<ErrorCode::END_OF_FILE>()) << st;
}

TEST_F(SegmentIteratorExprZonemapTest, MissingVariantSeparatesValueAndPhysicalReaderSemantics) {
    // Case: a nullable VARIANT root is absent from an old segment. Physical metadata lookup must
    // report NOT_FOUND, while logical root and generated-child reads must synthesize NULL values.
    _tablet_schema = make_schema_with_added_nullable_variant();

    // The key-only writer models a segment created before nullable VARIANT root `v` was added.
    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_runtime_column_segment(&segment, false));

    StorageReadOptions read_options(_stats);
    read_options.tablet_schema = _tablet_schema;
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;

    std::shared_ptr<VariantColumnReader> physical_reader;
    auto st = segment->get_variant_root_reader(_tablet_schema->column(1), read_options,
                                               &physical_reader);
    ASSERT_TRUE(st.is<ErrorCode::NOT_FOUND>()) << st;
    ASSERT_EQ(nullptr, physical_reader);

    TabletColumn added_path;
    added_path.set_unique_id(-1);
    added_path.set_parent_unique_id(1);
    added_path.set_name("v.a");
    added_path.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    added_path.set_is_nullable(true);
    added_path.set_path_info(PathInData("v.a"));
    auto declared_path_type = added_path.get_vec_type();
    auto storage_path_type =
            segment->get_data_type_of(added_path, declared_path_type, read_options);
    ASSERT_TRUE(storage_path_type->equals(*declared_path_type));

    MutableColumnPtr value_column;
    ASSERT_NO_FATAL_FAILURE(read_column(segment, 1, read_options, &value_column));
    ASSERT_NO_FATAL_FAILURE(expect_all_null(value_column));

    // Reading a generated child path must use the missing root's logical NULL value. It must not
    // attempt to cast the ConstantColumnReader to VariantColumnReader or search sparse metadata.
    ColumnIteratorUPtr path_iterator;
    st = segment->new_column_iterator(added_path, &path_iterator, &read_options);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, path_iterator);
    ColumnIteratorOptions iterator_options;
    iterator_options.stats = &_stats;
    iterator_options.file_reader = segment->file_reader().get();
    iterator_options.io_ctx = read_options.io_ctx;
    st = path_iterator->init(iterator_options);
    ASSERT_TRUE(st.ok()) << st;

    MutableColumnPtr path_values = added_path.get_vec_type()->create_column();
    size_t rows = kRuntimeColumnRows;
    bool has_null = false;
    st = path_iterator->next_batch(&rows, path_values, &has_null);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(has_null);
    ASSERT_EQ(kRuntimeColumnRows, path_values->size());
    ASSERT_NO_FATAL_FAILURE(expect_all_null(path_values));
}

TEST_F(SegmentIteratorExprZonemapTest, NewIteratorPrunesCommitTsoByReadOptionValue) {
    constexpr int64_t kCommitTso = 466872251335573505L;
    _tablet_schema = make_runtime_column_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_runtime_column_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    StorageReadOptions read_options(_stats);
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(7, 7);
    read_options.commit_tso = TsoRange(kCommitTso, kCommitTso);
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_options.col_id_to_predicates.emplace(
            kCommitTsoCid, make_commit_tso_gt_predicate(kCommitTsoCid, kCommitTso));

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    EXPECT_TRUE(iter->empty());
    EXPECT_EQ(1, _stats.total_segment_number);
    EXPECT_EQ(1, _stats.filtered_segment_number);
}

} // namespace doris::segment_v2
