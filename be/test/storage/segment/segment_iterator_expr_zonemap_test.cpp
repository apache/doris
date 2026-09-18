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

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <algorithm>
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
#include "exprs/vslot_ref.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/predicate/block_column_predicate.h"
#include "storage/predicate/comparison_predicate.h"
#include "storage/read_time_hidden_column.h"
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

TabletSchemaSPtr make_version_tablet_schema(bool with_inverted_index = false) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    if (with_inverted_index) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
        tablet_schema->init_from_pb(schema_pb);
    }
    tablet_schema->append_column(*create_int_key(0, false));
    TabletColumn version_column;
    version_column.set_unique_id(1);
    version_column.set_name(VERSION_COL);
    version_column.set_type(FieldType::OLAP_FIELD_TYPE_BIGINT);
    version_column.set_is_key(false);
    version_column.set_is_nullable(false);
    version_column.set_is_bf_column(true);
    version_column.set_length(8);
    version_column.set_index_length(8);
    version_column.set_aggregation_method(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE);
    tablet_schema->append_column(version_column);
    if (with_inverted_index) {
        TabletIndexPB index_pb;
        index_pb.set_index_id(1);
        index_pb.set_index_name("version_idx");
        index_pb.set_index_type(IndexType::INVERTED);
        index_pb.add_col_unique_id(1);
        TabletIndex index;
        index.init_from_pb(index_pb);
        tablet_schema->append_index(std::move(index));
    }
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
    tablet_schema->append_column(tso_column);
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
    auto pred = std::make_shared<ComparisonPredicateBase<TYPE_BIGINT, PredicateType::EQ>>(
            column_id, VERSION_COL, Field::create_field<TYPE_BIGINT>(value));
    predicates->add_column_predicate(SingleColumnBlockPredicate::create_unique(pred));
    return predicates;
}

std::shared_ptr<ColumnPredicate> make_version_eq_column_predicate(int32_t column_id,
                                                                  int64_t value) {
    return std::make_shared<ComparisonPredicateBase<TYPE_BIGINT, PredicateType::EQ>>(
            column_id, VERSION_COL, Field::create_field<TYPE_BIGINT>(value));
}

// Read schema covers all tablet columns in order, so ordinal == tablet cid.
ReadSchemaSPtr make_read_schema(const TabletSchemaSPtr& tablet_schema) {
    return std::make_shared<ReadSchema>(tablet_schema->columns());
}

Block make_hidden_column_read_block(const ReadSchemaSPtr& read_schema) {
    Block block;
    block.insert({ColumnInt32::create(), std::make_shared<DataTypeInt32>(), "0"});
    const auto hidden_type = read_schema->data_type(1);
    block.insert({hidden_type->create_column(), hidden_type, read_schema->column(1)->name()});
    return block;
}

} // namespace

class SegmentIteratorExprZonemapTest : public testing::Test {
protected:
    void SetUp() override {
        _previous_searcher_cache = ExecEnv::GetInstance()->get_inverted_index_searcher_cache();
        _previous_query_cache = ExecEnv::GetInstance()->get_inverted_index_query_cache();
        constexpr int64_t kCacheLimit = 1024 * 1024;
        _inverted_index_searcher_cache = std::unique_ptr<InvertedIndexSearcherCache>(
                InvertedIndexSearcherCache::create_global_instance(kCacheLimit, 1));
        _inverted_index_query_cache = std::unique_ptr<InvertedIndexQueryCache>(
                InvertedIndexQueryCache::create_global_cache(kCacheLimit, 1));
        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(
                _inverted_index_searcher_cache.get());
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_inverted_index_query_cache.get());

        auto st = io::global_local_filesystem()->delete_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kTestDir);
        ASSERT_TRUE(st.ok()) << st;
        _tablet_schema = make_tablet_schema();
    }

    void TearDown() override {
        ExecEnv::GetInstance()->set_inverted_index_searcher_cache(_previous_searcher_cache);
        ExecEnv::GetInstance()->set_inverted_index_query_cache(_previous_query_cache);
        _inverted_index_searcher_cache.reset();
        _inverted_index_query_cache.reset();
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

    void create_index_file_writer(const io::FileSystemSPtr& fs, const std::string& path,
                                  std::unique_ptr<IndexFileWriter>* index_file_writer) {
        if (!_tablet_schema->has_inverted_index()) {
            return;
        }
        const std::string index_path_prefix {
                InvertedIndexDescriptor::get_index_file_path_prefix(path)};
        const std::string index_path =
                InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix);
        io::FileWriterPtr index_writer;
        auto st = fs->create_file(index_path, &index_writer);
        ASSERT_TRUE(st.ok()) << st;
        *index_file_writer = std::make_unique<IndexFileWriter>(
                fs, index_path_prefix, kRowsetId.to_string(), 0, InvertedIndexStorageFormatPB::V2,
                std::move(index_writer));
    }

    void close_index_file_writer(IndexFileWriter* index_file_writer) {
        if (index_file_writer == nullptr) {
            return;
        }
        auto st = index_file_writer->begin_close();
        ASSERT_TRUE(st.ok()) << st;
        st = index_file_writer->finish_close();
        ASSERT_TRUE(st.ok()) << st;
    }

    void build_hidden_bigint_segment(const std::string& file_name, int num_rows,
                                     uint32_t num_rows_per_block, const Field& hidden_value,
                                     std::shared_ptr<Segment>* segment) {
        const auto path = std::string(kTestDir) + "/" + file_name;
        auto fs = io::global_local_filesystem();
        io::FileWriterPtr file_writer;
        auto st = fs->create_file(path, &file_writer);
        ASSERT_TRUE(st.ok()) << st;

        std::unique_ptr<IndexFileWriter> index_file_writer;
        ASSERT_NO_FATAL_FAILURE(create_index_file_writer(fs, path, &index_file_writer));

        VerticalSegmentWriterOptions opts;
        opts.num_rows_per_block = num_rows_per_block;
        TestVerticalSegmentWriter writer(file_writer.get(), 0, _tablet_schema, nullptr, nullptr,
                                         opts, index_file_writer.get());
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
        st = writer.finalize_columns(&index_size);
        ASSERT_TRUE(st.ok()) << st;
        st = writer.finalize_footer(&file_size);
        ASSERT_TRUE(st.ok()) << st;
        st = file_writer->close();
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_NO_FATAL_FAILURE(close_index_file_writer(index_file_writer.get()));

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

    void assert_hidden_column_values(RowwiseIterator* iter, const StorageReadOptions& read_options,
                                     const ReadSchemaSPtr& read_schema, size_t expected_rows,
                                     int64_t expected_value) {
        auto st = iter->init(read_options);
        ASSERT_TRUE(st.ok()) << st;

        size_t total_rows = 0;
        auto block = make_hidden_column_read_block(read_schema);
        while (true) {
            st = iter->next_batch(&block);
            if (st.is<ErrorCode::END_OF_FILE>()) {
                break;
            }
            ASSERT_TRUE(st.ok()) << st;
            total_rows += block.rows();
            const auto* hidden_column = block.get_by_position(1).column.get();
            const ColumnInt64* hidden_values = nullptr;
            if (const auto* nullable = check_and_get_column<ColumnNullable>(hidden_column)) {
                EXPECT_TRUE(std::ranges::all_of(nullable->get_null_map_data(),
                                                [](uint8_t is_null) { return is_null == 0; }));
                hidden_values = assert_cast<const ColumnInt64*>(&nullable->get_nested_column());
            } else {
                hidden_values = assert_cast<const ColumnInt64*>(hidden_column);
            }
            EXPECT_TRUE(std::ranges::all_of(
                    hidden_values->get_data(),
                    [expected_value](int64_t value) { return value == expected_value; }));
            block.clear_column_data();
        }
        EXPECT_EQ(expected_rows, total_rows);
    }

    void assert_column_iterator_values(const std::shared_ptr<Segment>& segment,
                                       ColumnIterator* iter, size_t expected_rows,
                                       int64_t expected_value) {
        ColumnIteratorOptions iter_opts;
        iter_opts.stats = &_stats;
        iter_opts.file_reader = segment->file_reader().get();
        iter_opts.io_ctx.reader_type = ReaderType::READER_QUERY;
        auto st = iter->init(iter_opts);
        ASSERT_TRUE(st.ok()) << st;
        st = iter->seek_to_ordinal(0);
        ASSERT_TRUE(st.ok()) << st;

        MutableColumnPtr dst = ColumnVector<TYPE_BIGINT>::create();
        size_t rows = expected_rows;
        bool has_null = true;
        st = iter->next_batch(&rows, dst, &has_null);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_FALSE(has_null);
        ASSERT_EQ(expected_rows, dst->size());
        const auto* values = assert_cast<const ColumnInt64*>(dst.get());
        for (size_t i = 0; i < dst->size(); ++i) {
            EXPECT_EQ(expected_value, values->get_element(i));
        }
    }

    TabletSchemaSPtr _tablet_schema;
    OlapReaderStatistics _stats;
    RuntimeState _runtime_state;
    std::unique_ptr<InvertedIndexSearcherCache> _inverted_index_searcher_cache;
    std::unique_ptr<InvertedIndexQueryCache> _inverted_index_query_cache;
    InvertedIndexSearcherCache* _previous_searcher_cache = nullptr;
    InvertedIndexQueryCache* _previous_query_cache = nullptr;
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

TEST_F(SegmentIteratorExprZonemapTest, VersionPredicateSkipsPhysicalPageIndexes) {
    constexpr int64_t kVersion = 7;
    _tablet_schema = make_version_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_version_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    std::shared_ptr<ColumnReader> physical_reader;
    auto st = segment->get_column_reader(_tablet_schema->column(1), &physical_reader, &_stats);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, physical_reader);
    ASSERT_TRUE(physical_reader->has_bloom_filter_index(false));

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(kVersion, kVersion);
    read_options.block_row_max = 1024;
    read_options.col_id_to_predicates.emplace(1, make_version_eq_predicate(1, kVersion));

    std::unique_ptr<RowwiseIterator> iter;
    st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    ASSERT_FALSE(iter->empty());
    ASSERT_NO_FATAL_FAILURE(
            assert_hidden_column_values(iter.get(), read_options, read_schema, kNumRows, kVersion));
    EXPECT_EQ(0, _stats.rows_bf_filtered);
    EXPECT_EQ(0, _stats.rows_stats_filtered);
}

TEST_F(SegmentIteratorExprZonemapTest, VersionMinMaxFallsBackFromStatisticsIterator) {
    constexpr int64_t kVersion = 7;
    _tablet_schema = make_version_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_version_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(kVersion, kVersion);
    read_options.push_down_agg_type_opt = TPushAggOp::MINMAX;

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    EXPECT_NE(nullptr, dynamic_cast<SegmentIterator*>(iter.get()));
}

TEST_F(SegmentIteratorExprZonemapTest, ReplacesReadTimeVersionSuffix) {
    constexpr int64_t kVersion = 7;
    _tablet_schema = make_version_tablet_schema();
    // TabletSchemaPB may contain the hidden column without carrying its ordinal index.
    _tablet_schema->set_version_col_idx(-1);
    auto column = ColumnInt64::create();
    column->insert_value(123);
    column->insert_value(0);
    column->insert_value(0);

    const auto hidden_column =
            get_read_time_hidden_column(*_tablet_schema, _tablet_schema->column(1).unique_id());
    replace_suffix_with_read_time_hidden_column(hidden_column, Version(kVersion, kVersion),
                                                TsoRange(), false, 2, *column);

    ASSERT_EQ(3, column->size());
    EXPECT_EQ(123, column->get_element(0));
    EXPECT_EQ(kVersion, column->get_element(1));
    EXPECT_EQ(kVersion, column->get_element(2));
}

TEST_F(SegmentIteratorExprZonemapTest, VersionPredicateSkipsPhysicalInvertedIndex) {
    constexpr int64_t kVersion = 7;
    _tablet_schema = make_version_tablet_schema(true);

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_version_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    TQueryOptions query_options;
    query_options.__set_enable_inverted_index_query(true);
    query_options.__set_enable_inverted_index_query_cache(false);
    query_options.__set_enable_inverted_index_searcher_cache(false);
    _runtime_state.set_query_options(query_options);

    auto predicate = make_version_eq_column_predicate(1, kVersion);
    auto block_predicate = AndBlockColumnPredicate::create_shared();
    block_predicate->add_column_predicate(SingleColumnBlockPredicate::create_unique(predicate));

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(kVersion, kVersion);
    read_options.block_row_max = 1024;
    read_options.column_predicates = {predicate};
    read_options.col_id_to_predicates.emplace(1, std::move(block_predicate));

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    ASSERT_FALSE(iter->empty());
    ASSERT_NO_FATAL_FAILURE(
            assert_hidden_column_values(iter.get(), read_options, read_schema, kNumRows, kVersion));
    EXPECT_EQ(0, _stats.rows_inverted_index_filtered);
}

TEST_F(SegmentIteratorExprZonemapTest, ExprZonemapUsesReadTimeVersion) {
    constexpr int64_t kVersion = 7;
    _tablet_schema = make_version_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_version_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    auto expr_ctx = std::make_shared<VExprContext>(
            std::make_shared<BigIntEqualsExpr>(1, kVersion, 1, VERSION_COL));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_ctx));
    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(kVersion, kVersion);
    read_options.block_row_max = 1024;
    read_options.common_expr_ctxs_push_down = {expr_ctx};

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    ASSERT_FALSE(iter->empty());
    ASSERT_NO_FATAL_FAILURE(
            assert_hidden_column_values(iter.get(), read_options, read_schema, kNumRows, kVersion));
    EXPECT_EQ(0, _stats.expr_zonemap_filtered_segments);
    EXPECT_EQ(0, _stats.expr_zonemap_filtered_pages);
}

TEST_F(SegmentIteratorExprZonemapTest, ExprZonemapRejectsPhysicalVersionPlaceholder) {
    constexpr int64_t kVersion = 7;
    _tablet_schema = make_version_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_version_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    auto expr_ctx = std::make_shared<VExprContext>(
            std::make_shared<BigIntEqualsExpr>(1, 0, 1, VERSION_COL));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_ctx));
    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(kVersion, kVersion);
    read_options.common_expr_ctxs_push_down = {expr_ctx};

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    EXPECT_TRUE(iter->empty());
    EXPECT_EQ(1, _stats.filtered_segment_number);
    EXPECT_EQ(1, _stats.expr_zonemap_filtered_segments);
}

TEST_F(SegmentIteratorExprZonemapTest, ExprZonemapUsesReadTimeCommitTso) {
    constexpr int64_t kCommitTso = 466872251335573505L;
    _tablet_schema = make_commit_tso_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_commit_tso_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    auto expr_ctx = std::make_shared<VExprContext>(
            std::make_shared<BigIntEqualsExpr>(1, kCommitTso, 1, COMMIT_TSO_COL));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_ctx));
    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(7, 7);
    read_options.commit_tso = TsoRange(kCommitTso, kCommitTso);
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_options.block_row_max = 1024;
    read_options.common_expr_ctxs_push_down = {expr_ctx};

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    ASSERT_FALSE(iter->empty());
    ASSERT_NO_FATAL_FAILURE(assert_hidden_column_values(iter.get(), read_options, read_schema,
                                                        kCommitTsoRows, kCommitTso));
    EXPECT_EQ(0, _stats.expr_zonemap_filtered_segments);
    EXPECT_EQ(0, _stats.expr_zonemap_filtered_pages);
}

TEST_F(SegmentIteratorExprZonemapTest, ExprZonemapUsesReadTimeBinlogTso) {
    constexpr int64_t kCommitTso = 466872251335573505L;
    _tablet_schema = make_binlog_tso_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_binlog_tso_segment(&segment));
    auto read_schema = make_read_schema(_tablet_schema);

    auto expr_ctx = std::make_shared<VExprContext>(
            std::make_shared<BigIntEqualsExpr>(1, kCommitTso, 1, BINLOG_TSO_COL));
    ASSERT_NO_FATAL_FAILURE(prepare_expr_context(expr_ctx));
    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.runtime_state = &_runtime_state;
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(7, 7);
    read_options.commit_tso = TsoRange(kCommitTso, kCommitTso);
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;
    read_options.read_row_binlog = true;
    read_options.block_row_max = 1024;
    read_options.common_expr_ctxs_push_down = {expr_ctx};

    std::unique_ptr<RowwiseIterator> iter;
    auto st = segment->new_iterator(read_schema, read_options, &iter);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    ASSERT_FALSE(iter->empty());
    ASSERT_NO_FATAL_FAILURE(assert_hidden_column_values(iter.get(), read_options, read_schema,
                                                        kNumRows, kCommitTso));
    EXPECT_EQ(0, _stats.expr_zonemap_filtered_segments);
    EXPECT_EQ(0, _stats.expr_zonemap_filtered_pages);
}

TEST_F(SegmentIteratorExprZonemapTest, CommitTsoReaderIgnoresCachedPhysicalReader) {
    constexpr int64_t kCommitTso = 466872251335573505L;
    _tablet_schema = make_commit_tso_tablet_schema();

    std::shared_ptr<Segment> segment;
    ASSERT_NO_FATAL_FAILURE(build_commit_tso_segment(&segment));

    StorageReadOptions physical_options;
    physical_options.stats = &_stats;
    ColumnIteratorUPtr physical_iter;
    auto st = segment->new_column_iterator(_tablet_schema->column(1), &physical_iter,
                                           &physical_options);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, physical_iter);
    ASSERT_NO_FATAL_FAILURE(
            assert_column_iterator_values(segment, physical_iter.get(), kCommitTsoRows, 0));

    StorageReadOptions read_options;
    read_options.stats = &_stats;
    read_options.tablet_schema = _tablet_schema;
    read_options.version = Version(7, 7);
    read_options.commit_tso = TsoRange(kCommitTso, kCommitTso);
    read_options.io_ctx.reader_type = ReaderType::READER_QUERY;

    ColumnIteratorUPtr iter;
    st = segment->new_column_iterator(_tablet_schema->column(1), &iter, &read_options);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, iter);
    ASSERT_NO_FATAL_FAILURE(
            assert_column_iterator_values(segment, iter.get(), kCommitTsoRows, kCommitTso));
}

TEST_F(SegmentIteratorExprZonemapTest, CommitTsoReaderDoesNotPollutePhysicalReaderCache) {
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

    ColumnIteratorUPtr logical_iter;
    auto st = segment->new_column_iterator(_tablet_schema->column(1), &logical_iter, &read_options);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, logical_iter);
    ASSERT_NO_FATAL_FAILURE(
            assert_column_iterator_values(segment, logical_iter.get(), kCommitTsoRows, kCommitTso));

    StorageReadOptions physical_options;
    physical_options.stats = &_stats;
    ColumnIteratorUPtr physical_iter;
    st = segment->new_column_iterator(_tablet_schema->column(1), &physical_iter, &physical_options);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_NE(nullptr, physical_iter);
    ASSERT_NO_FATAL_FAILURE(
            assert_column_iterator_values(segment, physical_iter.get(), kCommitTsoRows, 0));
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

} // namespace doris::segment_v2
