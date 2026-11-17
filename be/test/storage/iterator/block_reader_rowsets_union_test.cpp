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

// With config::enable_new_block_iterator on, a DUP_KEYS query that does not ask for key order
// reads the union of its rowsets: BlockReader reads the rowsets one after another, each through its
// rowset reader, without VCollectIterator. These tests write rowsets, read them through BlockReader
// and check that
//   - a query returns every row, rowset by rowset in version order and segment by segment within
//     a rowset, also when a rowset is empty, the same rows as through VCollectIterator with the
//     config off;
//   - a pushed-down LIMIT reaches every rowset reader as a row budget of each of its segments, on
//     both paths;
//   - a rowset is released as soon as it is read;
//   - a compaction of the same rowsets still merges them by key through VCollectIterator, and only
//     DUP_KEYS queries that do not ask for key order read the union, only with the config on.

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
#include "storage/iterator/block_reader.h"
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

// The cells of one row: the key k (INT), then the value v (BIGINT).
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

// Blocks of 1, 2 and 3 rows end inside a segment; Blocks of 100 rows hold a whole segment.
constexpr std::array kBatchSizes = {1, 2, 3, 100};

} // namespace

class BlockReaderRowsetsUnionTest : public testing::Test {
protected:
    void SetUp() override {
        char buffer[kMaxPathLen];
        EXPECT_NE(getcwd(buffer, kMaxPathLen), nullptr);
        _absolute_dir = std::string(buffer) + "/ut_dir/block_reader_rowsets_union_test";
        auto st = io::global_local_filesystem()->delete_directory(_absolute_dir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(_absolute_dir + "/tablet_path");
        ASSERT_TRUE(st.ok()) << st;
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        _saved_adaptive_batch_size = config::enable_adaptive_batch_size;
        // Row-count limits only, so the Block boundaries of a read are deterministic.
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

    // `k INT` key, `v BIGINT` value, in a table of `keys_type`.
    static TabletSchemaSPtr make_schema(KeysType keys_type) {
        TabletSchemaPB pb;
        pb.set_keys_type(keys_type);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_NONE);
        pb.set_next_column_unique_id(2);
        ColumnPB* k = pb.add_column();
        k->set_unique_id(0);
        k->set_name("k");
        k->set_type("INT");
        k->set_is_key(true);
        k->set_length(4);
        k->set_index_length(4);
        k->set_is_nullable(false);
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

    // One rowset with one segment per entry of `segments`, declared OVERLAPPING when there are
    // several, like the rowset of one load that flushed several memtables. No entry makes an
    // empty rowset. Rows of a segment must be in key order.
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

    TabletSharedPtr create_tablet(const TabletSchema& schema) {
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
        t_schema.__set_keys_type(TKeysType::DUP_KEYS);
        t_schema.__set_storage_type(TStorageType::COLUMN);
        t_schema.__set_columns(columns);
        TabletMetaSharedPtr tablet_meta(new TabletMeta(
                1, 1, 1, 1, 1, 1, t_schema, 1, col_ordinal_to_unique_id, UniqueId(1, 2),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
        TabletSharedPtr tablet(new Tablet(*_engine, tablet_meta, nullptr));
        static_cast<void>(tablet->init());
        return tablet;
    }

    // The params of a read of `rowsets` by `reader_type`, as a scanner or a compaction fills them.
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

    // Read every row of `params` through BlockReader with `batch_size` rows per Block. A read of
    // the union must return rows with every OK and none with the end of the read.
    static std::vector<Row> read(const TabletReader::ReaderParams& params, int batch_size,
                                 bool expect_union) {
        BlockReader reader;
        reader.set_batch_size(batch_size);
        Status st = reader.init(params);
        EXPECT_TRUE(st.ok()) << st;
        EXPECT_EQ(reader._rowsets_union != nullptr, expect_union);
        std::vector<Row> rows;
        bool eof = !st.ok();
        while (!eof) {
            Block block = params.read_schema->create_read_block();
            st = reader.next_block_with_aggregation(&block, &eof);
            EXPECT_TRUE(st.ok()) << st;
            if (!st.ok()) {
                break;
            }
            if (expect_union) {
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

// A query reads the rowsets in version order and the segments of a rowset in their order, through
// the union as through VCollectIterator: the overlapping segments of a rowset are not merged, and
// an empty rowset gives no rows. A compaction of the same rowsets merges them by key.
TEST_F(BlockReaderRowsetsUnionTest, QueryReadsTheRowsetsOneAfterAnother) {
    auto schema = make_schema(KeysType::DUP_KEYS);
    auto tablet = create_tablet(*schema);
    std::vector<RowsetSharedPtr> rowsets = {
            write_rowset(schema, {{{1, 10}, {3, 30}}, {{2, 20}, {3, 31}}}, /*version=*/2),
            write_rowset(schema, {}, /*version=*/3),
            write_rowset(schema, {{{0, 40}, {5, 50}}}, /*version=*/4),
    };
    const std::vector<Row> union_rows = {{1, 10}, {3, 30}, {2, 20}, {3, 31}, {0, 40}, {5, 50}};

    auto sorted_union_rows = union_rows;
    std::ranges::sort(sorted_union_rows);

    for (bool new_block_iterator : {true, false}) {
        config::enable_new_block_iterator = new_block_iterator;
        for (int batch_size : kBatchSizes) {
            SCOPED_TRACE("new_block_iterator=" + std::to_string(new_block_iterator) +
                         " batch_size=" + std::to_string(batch_size));
            EXPECT_EQ(read(make_params(tablet, schema, rowsets, ReaderType::READER_QUERY),
                           batch_size, /*expect_union=*/new_block_iterator),
                      union_rows);

            auto merged = read(
                    make_params(tablet, schema, rowsets, ReaderType::READER_CUMULATIVE_COMPACTION),
                    batch_size, /*expect_union=*/false);
            EXPECT_TRUE(std::ranges::is_sorted(merged, {}, [](const Row& row) { return row[0]; }));
            std::ranges::sort(merged);
            EXPECT_EQ(merged, sorted_union_rows);
        }
    }
}

// A pushed-down LIMIT is a row budget of every segment: every rowset reader gets it for its
// segments, and the first segment caps its first read at it, though the Block could hold all its
// rows; the same through the union and through VCollectIterator. Like the scanner, the read stops
// once the LIMIT is reached.
TEST_F(BlockReaderRowsetsUnionTest, PushesTheLimitDownToEverySegment) {
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
        EXPECT_EQ(reader._rowsets_union != nullptr, new_block_iterator);
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

// The union holds a rowset reader, and with it the rowset, only until the rowset is read: once a
// scanner has dropped its own references after init, a rowset is released when the read moves on
// to the next one.
TEST_F(BlockReaderRowsetsUnionTest, ReleasesARowsetOnceItIsRead) {
    // VCollectIterator holds its last rowset until the reader is destroyed.
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
    // As OlapScanner::_open_impl does.
    params.rs_splits.clear();
    RuntimeProfile profile("BlockReaderRowsetsUnionTest");
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

// Only a DUP_KEYS query that does not ask for key order reads the union, and only with
// config::enable_new_block_iterator on. A key TopN or a changelog MIN_DELTA or DETAIL scan sets
// read_orderby_key, and compactions and the other key types merge.
TEST_F(BlockReaderRowsetsUnionTest, OnlyDupQueriesWithoutKeyOrderReadTheUnion) {
    struct Case {
        KeysType keys_type;
        ReaderType reader_type;
        bool read_orderby_key;
        bool unions_rowsets;
    };
    const std::array cases = {
            Case {.keys_type = KeysType::DUP_KEYS,
                  .reader_type = ReaderType::READER_QUERY,
                  .read_orderby_key = false,
                  .unions_rowsets = true},
            Case {.keys_type = KeysType::DUP_KEYS,
                  .reader_type = ReaderType::READER_QUERY,
                  .read_orderby_key = true,
                  .unions_rowsets = false},
            Case {.keys_type = KeysType::DUP_KEYS,
                  .reader_type = ReaderType::READER_CUMULATIVE_COMPACTION,
                  .read_orderby_key = false,
                  .unions_rowsets = false},
            Case {.keys_type = KeysType::DUP_KEYS,
                  .reader_type = ReaderType::READER_BASE_COMPACTION,
                  .read_orderby_key = false,
                  .unions_rowsets = false},
            Case {.keys_type = KeysType::DUP_KEYS,
                  .reader_type = ReaderType::READER_ALTER_TABLE,
                  .read_orderby_key = false,
                  .unions_rowsets = false},
            Case {.keys_type = KeysType::DUP_KEYS,
                  .reader_type = ReaderType::READER_CHECKSUM,
                  .read_orderby_key = false,
                  .unions_rowsets = false},
            Case {.keys_type = KeysType::UNIQUE_KEYS,
                  .reader_type = ReaderType::READER_QUERY,
                  .read_orderby_key = false,
                  .unions_rowsets = false},
            Case {.keys_type = KeysType::AGG_KEYS,
                  .reader_type = ReaderType::READER_QUERY,
                  .read_orderby_key = false,
                  .unions_rowsets = false},
    };
    for (bool new_block_iterator : {true, false}) {
        config::enable_new_block_iterator = new_block_iterator;
        for (const auto& c : cases) {
            SCOPED_TRACE("new_block_iterator=" + std::to_string(new_block_iterator) +
                         " keys_type=" + std::to_string(static_cast<int>(c.keys_type)) +
                         " reader_type=" + std::to_string(static_cast<int>(c.reader_type)) +
                         " read_orderby_key=" + std::to_string(c.read_orderby_key));
            BlockReader reader;
            reader._tablet_schema = make_schema(c.keys_type);
            TabletReader::ReaderParams params;
            params.reader_type = c.reader_type;
            params.read_orderby_key = c.read_orderby_key;
            EXPECT_EQ(reader._unions_rowsets(params), new_block_iterator && c.unions_rowsets);
        }
    }
}

} // namespace doris
