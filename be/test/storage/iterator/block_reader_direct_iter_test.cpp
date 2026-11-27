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
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "storage/iterator/block_iterator_builder.h"
#include "storage/iterator/block_reader.h"
#include "storage/iterator/direct_iterator.h"
#include "storage/iterator/heap_merge_iterator.h"
#include "storage/iterator/merge_block_iterator.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/beta_rowset_reader.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/schema.h"
#include "storage/storage_engine.h"
#include "storage/tablet/base_tablet.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_reader.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {

constexpr uint32_t kMaxPathLen = 1024;

using Row = std::vector<int64_t>;

int64_t read_cell(const IColumn& column, size_t row) {
    const IColumn* col = &column;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*col)) {
        DORIS_CHECK(!nullable->is_null_at(row));
        col = &nullable->get_nested_column();
    }
    return col->get_int(row);
}

void append_row(MutableColumns& columns, const Row& row) {
    auto k = static_cast<int32_t>(row[0]);
    columns[0]->insert_data(reinterpret_cast<const char*>(&k), sizeof(k));
    columns[1]->insert_data(reinterpret_cast<const char*>(&row[1]), sizeof(row[1]));
}

std::vector<Row> rows_of(const Block& block) {
    std::vector<Row> rows;
    for (size_t r = 0; r < block.rows(); ++r) {
        rows.push_back({read_cell(*block.get_by_position(0).column, r),
                        read_cell(*block.get_by_position(1).column, r)});
    }
    return rows;
}

constexpr std::array kBatchSizes = {1, 2, 3, 100};

} // namespace

class BlockReaderDirectIterTest : public testing::Test {
protected:
    void SetUp() override {
        char buffer[kMaxPathLen];
        EXPECT_NE(getcwd(buffer, kMaxPathLen), nullptr);
        _absolute_dir = std::string(buffer) + "/ut_dir/block_reader_direct_iter_test";
        auto st = io::global_local_filesystem()->delete_directory(_absolute_dir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(_absolute_dir + "/tablet_path");
        ASSERT_TRUE(st.ok()) << st;
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        _saved_adaptive_batch_size = config::enable_adaptive_batch_size;
        config::enable_adaptive_batch_size = false;
        _saved_new_block_iterator = config::enable_new_block_iterator;
    }

    void TearDown() override {
        config::enable_adaptive_batch_size = _saved_adaptive_batch_size;
        config::enable_new_block_iterator = _saved_new_block_iterator;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        _engine = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    static const char* value_aggregation(KeysType keys_type) {
        switch (keys_type) {
        case KeysType::DUP_KEYS:
            return "NONE";
        case KeysType::AGG_KEYS:
            return "SUM";
        default:
            return "REPLACE";
        }
    }

    static TabletSchemaSPtr make_schema(KeysType keys_type, bool has_key = true) {
        TabletSchemaPB pb;
        pb.set_keys_type(keys_type);
        pb.set_num_short_key_columns(has_key ? 1 : 0);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_NONE);
        pb.set_next_column_unique_id(2);
        ColumnPB* k = pb.add_column();
        k->set_unique_id(0);
        k->set_name("k");
        k->set_type("INT");
        k->set_is_key(has_key);
        k->set_length(4);
        k->set_index_length(4);
        k->set_is_nullable(false);
        if (!has_key) {
            k->set_aggregation("NONE");
        }
        ColumnPB* v = pb.add_column();
        v->set_unique_id(1);
        v->set_name("v");
        v->set_type("BIGINT");
        v->set_is_key(false);
        v->set_length(8);
        v->set_index_length(8);
        v->set_is_nullable(false);
        v->set_aggregation(value_aggregation(keys_type));
        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    RowsetSharedPtr write_rowset(const TabletSchemaSPtr& schema,
                                 const std::vector<std::vector<Row>>& segments, int64_t version) {
        static int64_t next_rowset_id = 10000;
        RowsetWriterContext context;
        RowsetId rowset_id;
        rowset_id.init(next_rowset_id++);
        context.rowset_id = rowset_id;
        context.rowset_type = BETA_ROWSET;
        context.rowset_state = VISIBLE;
        context.tablet_schema = schema;
        context.tablet_path = _absolute_dir + "/tablet_path";
        context.version = Version(version, version);
        context.segments_overlap = segments.size() > 1 ? OVERLAPPING : NONOVERLAPPING;
        context.max_rows_per_segment = UINT32_MAX;
        auto res = RowsetFactory::create_rowset_writer(*_engine, context, false);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();

        for (const auto& segment : segments) {
            Block block = schema->create_storage_block();
            {
                auto columns_guard = block.mutate_columns_scoped();
                auto& columns = columns_guard.mutable_columns();
                for (const auto& row : segment) {
                    append_row(columns, row);
                }
            }
            EXPECT_TRUE(writer->add_block(&block).ok());
            EXPECT_TRUE(writer->flush().ok());
        }

        RowsetSharedPtr rowset;
        EXPECT_TRUE(writer->build(rowset).ok());
        EXPECT_EQ(segments.size(), rowset->num_segments());
        return rowset;
    }

    TabletSharedPtr create_tablet(const TabletSchema& schema, bool merge_on_write = false) {
        std::vector<TColumn> columns;
        std::unordered_map<uint32_t, uint32_t> col_ordinal_to_unique_id;
        const std::array types = {TPrimitiveType::INT, TPrimitiveType::BIGINT};
        for (size_t i = 0; i < schema.num_columns(); ++i) {
            const TabletColumn& column = schema.column(i);
            TColumn t_column;
            t_column.column_type.type = types[i];
            t_column.__set_column_name(column.name());
            t_column.__set_is_key(column.is_key());
            columns.push_back(t_column);
            col_ordinal_to_unique_id[static_cast<uint32_t>(i)] = column.unique_id();
        }
        TTabletSchema t_schema;
        t_schema.__set_short_key_column_count(schema.num_short_key_columns());
        t_schema.__set_schema_hash(3333);
        t_schema.__set_keys_type(schema.keys_type() == KeysType::UNIQUE_KEYS
                                         ? TKeysType::UNIQUE_KEYS
                                         : TKeysType::DUP_KEYS);
        t_schema.__set_storage_type(TStorageType::COLUMN);
        t_schema.__set_columns(columns);
        TabletMetaSharedPtr tablet_meta(new TabletMeta(
                1, 1, 1, 1, 1, 1, t_schema, 1, col_ordinal_to_unique_id, UniqueId(1, 2),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, merge_on_write));
        TabletSharedPtr tablet(new Tablet(*_engine, tablet_meta, nullptr));
        static_cast<void>(tablet->init());
        return tablet;
    }

    static TabletReader::ReaderParams make_params(const TabletSharedPtr& tablet,
                                                  const TabletSchemaSPtr& schema,
                                                  const std::vector<RowsetSharedPtr>& rowsets,
                                                  ReaderType reader_type) {
        TabletReader::ReaderParams params;
        params.tablet = tablet;
        params.tablet_schema = schema;
        params.reader_type = reader_type;
        params.version = Version(0, rowsets.back()->end_version());
        TabletReadSource read_source;
        for (const auto& rowset : rowsets) {
            RowsetReaderSharedPtr rs_reader;
            EXPECT_TRUE(rowset->create_reader(&rs_reader).ok());
            read_source.rs_splits.emplace_back(std::move(rs_reader));
        }
        read_source.fill_delete_predicates();
        params.set_read_source(std::move(read_source));
        params.read_schema = std::make_shared<ReadSchema>(schema->columns());
        return params;
    }

    static std::vector<Row> read(const TabletReader::ReaderParams& params, int batch_size,
                                 bool expect_new_iter) {
        BlockReader reader;
        reader.set_batch_size(batch_size);
        Status st = reader.init(params);
        EXPECT_TRUE(st.ok()) << st;
        EXPECT_EQ(reader._block_iter != nullptr, expect_new_iter);
        std::vector<Row> rows;
        bool eof = !st.ok();
        while (!eof) {
            Block block = params.read_schema->create_read_block();
            st = reader.next_block_with_aggregation(&block, &eof);
            EXPECT_TRUE(st.ok()) << st;
            if (!st.ok()) {
                break;
            }
            if (expect_new_iter) {
                EXPECT_EQ(block.rows() == 0, eof);
                EXPECT_LE(block.rows(), static_cast<size_t>(batch_size));
            }
            auto block_rows = rows_of(block);
            rows.insert(rows.end(), block_rows.begin(), block_rows.end());
        }
        return rows;
    }

    StorageEngine* _engine = nullptr;
    std::string _absolute_dir;
    bool _saved_adaptive_batch_size = false;
    bool _saved_new_block_iterator = true;
};

// DirectIterator reads rowsets in version order and segments in segment order without merging,
// while horizontal compaction of overlapping DUP_KEYS rowsets merges them by key with newer
// segment first on equal keys.
TEST_F(BlockReaderDirectIterTest, QueryReadsDirectlyAndCompactionMergesByKey) {
    auto schema = make_schema(KeysType::DUP_KEYS);
    auto tablet = create_tablet(*schema);
    std::vector<RowsetSharedPtr> rowsets = {
            write_rowset(schema, {{{1, 10}, {3, 30}}, {{2, 20}, {3, 31}}}, /*version=*/2),
            write_rowset(schema, {}, /*version=*/3),
            write_rowset(schema, {{{0, 40}, {3, 32}, {5, 50}}}, /*version=*/4),
    };
    const std::vector<Row> query_rows = {{1, 10}, {3, 30}, {2, 20}, {3, 31},
                                         {0, 40}, {3, 32}, {5, 50}};
    const std::vector<Row> expected_merged_new = {{0, 40}, {1, 10}, {2, 20}, {3, 32},
                                                  {3, 31}, {3, 30}, {5, 50}};

    for (bool new_block_iterator : {true, false}) {
        config::enable_new_block_iterator = new_block_iterator;
        for (int batch_size : kBatchSizes) {
            SCOPED_TRACE("new_block_iterator=" + std::to_string(new_block_iterator) +
                         " batch_size=" + std::to_string(batch_size));
            EXPECT_EQ(read(make_params(tablet, schema, rowsets, ReaderType::READER_QUERY),
                           batch_size, /*expect_new_iter=*/new_block_iterator),
                      query_rows);

            auto merged = read(
                    make_params(tablet, schema, rowsets, ReaderType::READER_CUMULATIVE_COMPACTION),
                    batch_size, /*expect_new_iter=*/new_block_iterator);
            EXPECT_TRUE(std::ranges::is_sorted(merged, {}, [](const Row& row) { return row[0]; }));
            if (new_block_iterator) {
                EXPECT_EQ(merged, expected_merged_new);
            }
        }
    }
}

// Non-overlapping DUP_KEYS compaction uses DirectIterator instead of building a merge heap, and
// keyless DUP_KEYS compaction reads rowsets in ascending version order.
TEST_F(BlockReaderDirectIterTest, NonOverlappingAndKeylessCompactionsReadDirectly) {
    config::enable_new_block_iterator = true;
    auto schema = make_schema(KeysType::DUP_KEYS);
    auto tablet = create_tablet(*schema);
    std::vector<RowsetSharedPtr> disjoint_rowsets = {
            write_rowset(schema, {{{1, 10}, {2, 20}}}, /*version=*/2),
            write_rowset(schema, {{{3, 30}, {4, 40}}}, /*version=*/3),
    };
    auto params =
            make_params(tablet, schema, disjoint_rowsets, ReaderType::READER_CUMULATIVE_COMPACTION);
    BlockReader reader;
    reader.set_batch_size(100);
    ASSERT_TRUE(reader.init(params).ok());
    EXPECT_NE(dynamic_cast<DirectIterator*>(reader._block_iter.get()), nullptr);

    auto keyless_schema = make_schema(KeysType::DUP_KEYS, /*has_key=*/false);
    auto keyless_tablet = create_tablet(*keyless_schema);
    std::vector<RowsetSharedPtr> keyless_rowsets = {
            write_rowset(keyless_schema, {{{5, 50}}}, /*version=*/2),
            write_rowset(keyless_schema, {{{1, 10}}}, /*version=*/3),
    };
    EXPECT_EQ(read(make_params(keyless_tablet, keyless_schema, keyless_rowsets,
                               ReaderType::READER_BASE_COMPACTION),
                   /*batch_size=*/1, /*expect_new_iter=*/true),
              (std::vector<Row> {{5, 50}, {1, 10}}));
}

TEST_F(BlockReaderDirectIterTest, PushesTheLimitDownToEverySegment) {
    auto schema = make_schema(KeysType::DUP_KEYS);
    auto tablet = create_tablet(*schema);
    std::vector<RowsetSharedPtr> rowsets = {
            write_rowset(schema, {{{1, 10}, {2, 20}, {3, 30}}, {{1, 11}, {2, 21}, {3, 31}}},
                         /*version=*/2),
            write_rowset(schema, {{{4, 40}, {5, 50}, {6, 60}}}, /*version=*/3),
    };

    for (bool new_block_iterator : {true, false}) {
        SCOPED_TRACE("new_block_iterator=" + std::to_string(new_block_iterator));
        config::enable_new_block_iterator = new_block_iterator;
        auto params = make_params(tablet, schema, rowsets, ReaderType::READER_QUERY);
        params.general_read_limit = 2;
        std::vector<RowsetReaderSharedPtr> rs_readers;
        for (const auto& rs_split : params.rs_splits) {
            rs_readers.push_back(rs_split.rs_reader);
        }

        BlockReader reader;
        reader.set_batch_size(100);
        ASSERT_TRUE(reader.init(params).ok());
        EXPECT_EQ(reader._block_iter != nullptr, new_block_iterator);
        for (const auto& rs_reader : rs_readers) {
            EXPECT_EQ(static_cast<BetaRowsetReader*>(rs_reader.get())->_topn_limit, 2U);
        }
        Block block = params.read_schema->create_read_block();
        bool eof = false;
        ASSERT_TRUE(reader.next_block_with_aggregation(&block, &eof).ok());
        EXPECT_FALSE(eof);
        EXPECT_EQ(rows_of(block), (std::vector<Row> {{1, 10}, {2, 20}}));
    }
}

TEST_F(BlockReaderDirectIterTest, ReleasesARowsetOnceItIsRead) {
    config::enable_new_block_iterator = true;
    auto schema = make_schema(KeysType::DUP_KEYS);
    auto tablet = create_tablet(*schema);
    std::vector<RowsetSharedPtr> rowsets = {
            write_rowset(schema, {{{1, 10}, {2, 20}}}, /*version=*/2),
            write_rowset(schema, {{{3, 30}, {4, 40}}}, /*version=*/3),
    };
    auto params = make_params(tablet, schema, rowsets, ReaderType::READER_QUERY);
    std::vector<std::weak_ptr<RowsetReader>> rs_readers;
    for (const auto& rs_split : params.rs_splits) {
        rs_readers.push_back(rs_split.rs_reader);
    }

    BlockReader reader;
    reader.set_batch_size(100);
    ASSERT_TRUE(reader.init(params).ok());
    params.rs_splits.clear();
    RuntimeProfile profile("BlockReaderDirectIterTest");
    bool eof = false;
    auto next_rows = [&]() {
        Block block = params.read_schema->create_read_block();
        EXPECT_TRUE(reader.next_block_with_aggregation(&block, &eof).ok());
        return rows_of(block);
    };

    EXPECT_EQ(next_rows(), (std::vector<Row> {{1, 10}, {2, 20}}));
    EXPECT_FALSE(rs_readers[0].expired());
    EXPECT_FALSE(rs_readers[1].expired());
    reader.update_profile(&profile);

    EXPECT_EQ(next_rows(), (std::vector<Row> {{3, 30}, {4, 40}}));
    EXPECT_TRUE(rs_readers[0].expired());
    EXPECT_FALSE(rs_readers[1].expired());

    EXPECT_TRUE(next_rows().empty());
    EXPECT_TRUE(eof);
    EXPECT_TRUE(rs_readers[1].expired());
    reader.update_profile(&profile);
}

namespace {

class FailingSegmentIterator final : public RowwiseIterator {
public:
    FailingSegmentIterator(const ReadSchema& schema, Status error)
            : _schema(schema), _error(std::move(error)) {}

    Status next_batch(Block* /*block*/) override { return _error; }
    const ReadSchema& schema() const override { return _schema; }

private:
    const ReadSchema& _schema;
    Status _error;
};

class StubRowsetReader final : public RowsetReader {
public:
    StubRowsetReader(ReadSchemaSPtr schema, Status open_status, Status batch_status)
            : _schema(std::move(schema)),
              _open_status(std::move(open_status)),
              _batch_status(std::move(batch_status)) {}

    Status init(RowsetReaderContext* /*read_context*/, const RowSetSplits& /*rs_splits*/) override {
        return Status::OK();
    }
    Status get_segment_iterators(RowsetReaderContext* /*read_context*/,
                                 std::vector<RowwiseIteratorUPtr>* out_iters,
                                 bool /*use_cache*/) override {
        RETURN_IF_ERROR(_open_status);
        out_iters->push_back(std::make_unique<FailingSegmentIterator>(*_schema, _batch_status));
        return Status::OK();
    }
    void reset_read_options() override {}
    Status next_batch(Block* /*block*/) override { return Status::NotSupported("stub"); }
    Status next_batch(BlockView* /*block_view*/) override { return Status::NotSupported("stub"); }
    Status next_batch(BlockWithSameBit* /*block_view*/) override {
        return Status::NotSupported("stub");
    }
    bool delete_flag() override { return false; }
    Version version() override { return {0, 0}; }
    RowsetSharedPtr rowset() override { return nullptr; }
    const ReadSchema& read_schema() const override { return *_schema; }
    int64_t filtered_rows() override { return 0; }
    uint64_t merged_rows() override { return 0; }
    RowsetTypePB type() const override { return BETA_ROWSET; }
    int64_t newest_write_timestamp() override { return 0; }
    void update_profile(RuntimeProfile* /*profile*/) override {}
    RowsetReaderSharedPtr clone() override { return nullptr; }
    void set_topn_limit(size_t /*topn_limit*/) override {}

private:
    ReadSchemaSPtr _schema;
    Status _open_status;
    Status _batch_status;
};

} // namespace

// DirectIterator propagates errors from opening a rowset's segment iterators, errors from a
// segment's next_batch(), and runtime query cancellation.
TEST_F(BlockReaderDirectIterTest, DirectIteratorPropagatesErrorAndCancellation) {
    config::enable_new_block_iterator = true;
    auto schema = make_schema(KeysType::DUP_KEYS);
    auto read_schema = std::make_shared<ReadSchema>(schema->columns());

    RowsetReaderContext ctx;
    OlapReaderStatistics stats;
    {
        std::vector<RowsetReaderSharedPtr> readers = {std::make_shared<StubRowsetReader>(
                read_schema, Status::IOError("open failed"), Status::OK())};
        DirectIterator iter(std::move(readers), &ctx, &stats);
        Block block = read_schema->create_read_block();
        bool eof = false;
        Status st = iter.next_block(&block, &eof);
        EXPECT_TRUE(st.is<ErrorCode::IO_ERROR>()) << st;
    }
    {
        std::vector<RowsetReaderSharedPtr> readers = {std::make_shared<StubRowsetReader>(
                read_schema, Status::OK(), Status::Corruption("bad segment"))};
        DirectIterator iter(std::move(readers), &ctx, &stats);
        Block block = read_schema->create_read_block();
        bool eof = false;
        Status st = iter.next_block(&block, &eof);
        EXPECT_TRUE(st.is<ErrorCode::CORRUPTION>()) << st;
    }
    {
        auto tablet = create_tablet(*schema);
        std::vector<RowsetSharedPtr> rowsets = {
                write_rowset(schema, {{{1, 10}, {2, 20}}}, /*version=*/2),
        };
        auto params = make_params(tablet, schema, rowsets, ReaderType::READER_QUERY);
        RuntimeState runtime_state;
        params.runtime_state = &runtime_state;

        BlockReader reader;
        reader.set_batch_size(1);
        ASSERT_TRUE(reader.init(params).ok());
        Block block = params.read_schema->create_read_block();
        bool eof = false;
        ASSERT_TRUE(reader.next_block_with_aggregation(&block, &eof).ok());
        EXPECT_EQ(block.rows(), 1U);

        runtime_state.cancel(Status::Cancelled("query cancelled"));
        Block next = params.read_schema->create_read_block();
        Status st = reader.next_block_with_aggregation(&next, &eof);
        EXPECT_TRUE(st.is<ErrorCode::CANCELLED>()) << st;
    }
}

// BlockIteratorBuilder::_validate_merge_schema rejects schemas whose key prefix exceeds the
// block columns, contains a non-key column, or references an out-of-range secondary sort column.
TEST_F(BlockReaderDirectIterTest, ValidateMergeSchemaRejectsInvalidContracts) {
    auto schema = make_schema(KeysType::DUP_KEYS);
    auto tablet = create_tablet(*schema);
    TabletReader::ReaderParams params;
    RowsetReaderContext ctx;
    OlapReaderStatistics stats;
    uint64_t merged_rows = 0;

    auto validate = [&](const ReadSchemaSPtr& rs, HeapMergeOrder order) {
        BlockIteratorBuilder builder(params, *tablet, rs, &ctx, &stats, &merged_rows,
                                     BlockBatchLimit(100));
        return builder._validate_merge_schema(order);
    };

    // Valid schema passes.
    auto valid_rs = std::make_shared<ReadSchema>(schema->columns());
    EXPECT_TRUE(validate(valid_rs, HeapMergeOrder::by_all_keys()).ok());

    // Compare column index out of range.
    Status st = validate(valid_rs,
                         HeapMergeOrder::by_all_keys_and_same_key_col(99, SortDirection::ASC));
    EXPECT_TRUE(st.is<ErrorCode::INTERNAL_ERROR>()) << st;
    EXPECT_NE(st.to_string().find("compare column index out of range"), std::string::npos) << st;

    // Non-key column in key prefix: put value column v before key column k.
    std::vector<TabletColumnPtr> reversed_cols = {
            std::make_shared<TabletColumn>(schema->column(1)),
            std::make_shared<TabletColumn>(schema->column(0)),
    };
    auto bad_prefix_rs = std::make_shared<ReadSchema>(std::move(reversed_cols));
    st = validate(bad_prefix_rs, HeapMergeOrder::by_all_keys());
    EXPECT_TRUE(st.is<ErrorCode::INTERNAL_ERROR>()) << st;
    EXPECT_NE(st.to_string().find("non-key column"), std::string::npos) << st;

    // Fewer block columns than key prefix.
    auto short_rs = std::make_shared<ReadSchema>(schema->columns());
    short_rs->_num_key_columns = short_rs->num_block_columns() + 1;
    st = validate(short_rs, HeapMergeOrder::by_all_keys());
    EXPECT_TRUE(st.is<ErrorCode::INTERNAL_ERROR>()) << st;
    EXPECT_NE(st.to_string().find("fewer columns than the key prefix"), std::string::npos) << st;
}

} // namespace doris
