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

// One load that flushes several memtables writes a rowset whose segments overlap on keys, so the
// same key can sit in two segments of one rowset. For an AGG_KEYS read that folds the rows of a
// key, BlockReader merges every segment of such a rowset together with the other rowsets in one
// VHeapMergeIterator, and AggKeyBlockIterator folds the merged rows. These tests write such
// rowsets, read them through BlockReader as a query and as a compaction, with Block row limits
// that fall inside key groups, and check the rows that come back:
//   - the row of the segment flushed later is the newer row of its key;
//   - a key group that a Block row limit falls inside is still folded into one row;
//   - rowsets whose keys ascend from one to the next are read one after another.
// A query that pushes ORDER BY a key prefix LIMIT down to storage on a DUP_KEYS or merge-on-write
// table reads the rowsets one after another, the segments of each merged by the ORDER BY columns,
// and keeps the first rows: other tests read such rowsets as that query and check that the first
// rows by the ORDER BY columns come back, also rows of different keys equal on those columns.

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
#include "storage/iterator/block_reader.h"
#include "storage/iterators.h"
#include "storage/olap_common.h"
#include "storage/rowset/beta_rowset.h"
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
#include "util/uid_util.h"

namespace doris {

namespace {

constexpr uint32_t kMaxPathLen = 1024;

// One tablet column of a test schema. Every cell is an int64_t in the tests.
struct ColumnSpec {
    ColumnSpec(std::string name_, std::string type_, bool is_key_ = false,
               std::string aggregation_ = "")
            : name(std::move(name_)),
              type(std::move(type_)),
              is_key(is_key_),
              aggregation(std::move(aggregation_)) {}

    std::string name;
    std::string type; // INT or BIGINT
    bool is_key;
    std::string aggregation; // value columns only
};

using Row = std::vector<int64_t>;

int column_length(const std::string& type) {
    if (type == "INT") {
        return 4;
    }
    DORIS_CHECK_EQ(type, "BIGINT");
    return 8;
}

TKeysType::type thrift_keys_type(KeysType keys_type) {
    switch (keys_type) {
    case KeysType::DUP_KEYS:
        return TKeysType::DUP_KEYS;
    case KeysType::UNIQUE_KEYS:
        return TKeysType::UNIQUE_KEYS;
    case KeysType::AGG_KEYS:
        return TKeysType::AGG_KEYS;
    default:
        DORIS_CHECK(false) << "unexpected test keys type " << static_cast<int>(keys_type);
        return TKeysType::AGG_KEYS;
    }
}

TPrimitiveType::type thrift_type(FieldType type) {
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_INT:
        return TPrimitiveType::INT;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        return TPrimitiveType::BIGINT;
    default:
        DORIS_CHECK(false) << "unexpected test column type " << static_cast<int>(type);
        return TPrimitiveType::INVALID_TYPE;
    }
}

void append_cell(IColumn& column, FieldType type, int64_t value) {
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_INT: {
        auto v = static_cast<int32_t>(value);
        column.insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
        return;
    }
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        column.insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
        return;
    default:
        DORIS_CHECK(false) << "unexpected test column type " << static_cast<int>(type);
    }
}

int64_t read_cell(const IColumn& column, size_t row) {
    const IColumn* col = &column;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*col)) {
        DORIS_CHECK(!nullable->is_null_at(row));
        col = &nullable->get_nested_column();
    }
    return col->get_int(row);
}

std::string trace(ReaderType reader_type, int batch_size) {
    return "reader_type=" + std::to_string(static_cast<int>(reader_type)) +
           " batch_size=" + std::to_string(batch_size);
}

// A query and a compaction both read through BlockReader, with Blocks of 1, 2 and 3 rows whose row
// limits fall inside key groups and with Blocks that hold the whole merge.
constexpr std::array kReaderTypes = {ReaderType::READER_QUERY,
                                     ReaderType::READER_CUMULATIVE_COMPACTION};
constexpr std::array kBatchSizes = {1, 2, 3, 100};

// The first `limit` of `rows` sorted, descending when `descending`.
std::vector<Row> first_rows(std::vector<Row> rows, size_t limit, bool descending) {
    std::sort(rows.begin(), rows.end());
    if (descending) {
        std::reverse(rows.begin(), rows.end());
    }
    rows.resize(std::min(rows.size(), limit));
    return rows;
}

} // namespace

class BlockReaderOverlappingSegmentsTest : public testing::Test {
protected:
    void SetUp() override {
        char buffer[kMaxPathLen];
        EXPECT_NE(getcwd(buffer, kMaxPathLen), nullptr);
        _absolute_dir = std::string(buffer) + "/ut_dir/block_reader_overlapping_segments_test";
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

    // A schema of `keys_type`; column unique ids are the positions in `columns`.
    static TabletSchemaSPtr make_schema(const std::vector<ColumnSpec>& columns,
                                        KeysType keys_type = KeysType::AGG_KEYS) {
        TabletSchemaPB pb;
        pb.set_keys_type(keys_type);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_NONE);
        pb.set_next_column_unique_id(static_cast<int32_t>(columns.size()));
        for (size_t i = 0; i < columns.size(); ++i) {
            const auto& spec = columns[i];
            ColumnPB* column = pb.add_column();
            column->set_unique_id(static_cast<int32_t>(i));
            column->set_name(spec.name);
            column->set_type(spec.type);
            column->set_is_key(spec.is_key);
            column->set_length(column_length(spec.type));
            column->set_index_length(column_length(spec.type));
            column->set_is_nullable(false);
            if (!spec.aggregation.empty()) {
                column->set_aggregation(spec.aggregation);
            }
        }
        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // One rowset with one segment per entry of `segments`, declared OVERLAPPING when there are
    // several, like the rowset of one load that flushed several memtables: segment i + 1 was
    // flushed after segment i. With `segments_overlap` false it is NONOVERLAPPING, like the output
    // of a compaction: the keys must ascend from one segment to the next. Rows of a segment must
    // be in key order.
    RowsetSharedPtr write_rowset(const TabletSchemaSPtr& schema,
                                 const std::vector<std::vector<Row>>& segments, int64_t version,
                                 bool segments_overlap = true) {
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
        context.segments_overlap =
                segments.size() > 1 && segments_overlap ? OVERLAPPING : NONOVERLAPPING;
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
                    EXPECT_EQ(row.size(), columns.size());
                    for (size_t c = 0; c < columns.size(); ++c) {
                        append_cell(*columns[c], schema->column(c).type(), row[c]);
                    }
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
        for (size_t i = 0; i < schema.num_columns(); ++i) {
            const TabletColumn& column = schema.column(i);
            TColumn t_column;
            t_column.column_type.type = thrift_type(column.type());
            t_column.__set_column_name(column.name());
            t_column.__set_is_key(column.is_key());
            columns.push_back(t_column);
            col_ordinal_to_unique_id[static_cast<uint32_t>(i)] = column.unique_id();
        }
        TTabletSchema t_schema;
        t_schema.__set_short_key_column_count(schema.num_short_key_columns());
        t_schema.__set_schema_hash(3333);
        t_schema.__set_keys_type(thrift_keys_type(schema.keys_type()));
        t_schema.__set_storage_type(TStorageType::COLUMN);
        t_schema.__set_columns(columns);
        TabletMetaSharedPtr tablet_meta(
                new TabletMeta(1, 1, 1, 1, 1, 1, t_schema, 1, col_ordinal_to_unique_id,
                               UniqueId(1, 2), TTabletType::TABLET_TYPE_DISK,
                               TCompressionType::LZ4F, /*storage_policy_id=*/0, merge_on_write));
        TabletSharedPtr tablet(new Tablet(*_engine, tablet_meta, nullptr));
        static_cast<void>(tablet->init());
        return tablet;
    }

    // Read `rowsets` through BlockReader the way a scan or a compaction of `reader_type` with
    // `batch_size` rows per Block does, and return every row in output order. With `topn_limit`,
    // as a query that pushed ORDER BY the first key column LIMIT `topn_limit` down to storage,
    // descending when `descending`. With `segments`, only the segments [first, second) of each
    // rowset are read, as by one of the scanners a parallel scan splits a rowset among.
    static std::vector<Row> read(const TabletSharedPtr& tablet, const TabletSchemaSPtr& schema,
                                 const std::vector<RowsetSharedPtr>& rowsets,
                                 ReaderType reader_type, int batch_size, size_t topn_limit = 0,
                                 bool descending = false,
                                 std::pair<int64_t, int64_t> segments = {0, 0}) {
        TabletReader::ReaderParams params;
        params.tablet = tablet;
        params.tablet_schema = schema;
        params.reader_type = reader_type;
        params.version = Version(0, rowsets.back()->end_version());
        if (topn_limit > 0) {
            params.read_orderby_key = true;
            params.read_orderby_key_reverse = descending;
            params.read_orderby_key_num_prefix_columns = 1;
            params.read_orderby_key_limit = topn_limit;
        }
        TabletReadSource read_source;
        for (const auto& rowset : rowsets) {
            RowsetReaderSharedPtr rs_reader;
            EXPECT_TRUE(rowset->create_reader(&rs_reader).ok());
            read_source.rs_splits.emplace_back(std::move(rs_reader));
            read_source.rs_splits.back().segment_offsets = segments;
        }
        read_source.fill_delete_predicates();
        params.set_read_source(std::move(read_source));
        params.read_schema = std::make_shared<ReadSchema>(schema->columns());

        BlockReader reader;
        reader.set_batch_size(batch_size);
        Status st = reader.init(params);
        EXPECT_TRUE(st.ok()) << st;
        std::vector<Row> rows;
        bool eof = !st.ok();
        while (!eof) {
            Block block = params.read_schema->create_read_block();
            st = reader.next_block_with_aggregation(&block, &eof);
            EXPECT_TRUE(st.ok()) << st;
            if (!st.ok()) {
                break;
            }
            for (size_t r = 0; r < block.rows(); ++r) {
                Row row;
                for (size_t c = 0; c < block.columns(); ++c) {
                    row.push_back(read_cell(*block.get_by_position(c).column, r));
                }
                rows.push_back(std::move(row));
            }
        }
        return rows;
    }

    StorageEngine* _engine = nullptr;
    std::string _absolute_dir;
    bool _saved_adaptive_batch_size = false;
    bool _saved_new_block_iterator = true;
};

// AGG_KEYS: both overlapping segments of the lowest-version rowset hold keys 1..3, so its
// segment merge yields 1 1 2 2 3 3 and the row limits of Blocks of 1, 2 or 3 rows fall inside key
// groups. Every key must still come back once, with the SUM of all its rows and the REPLACE value
// of its newest row: the later segment within the rowset, the newer rowset when there is one. Two
// one-segment rowsets whose key ranges share key 2 are merged as well.
TEST_F(BlockReaderOverlappingSegmentsTest, AggKeysFoldEveryKeyGroupOnce) {
    // VCollectIterator still misreads segments that overlap on keys.
    config::enable_new_block_iterator = true;
    auto schema = make_schema(
            {{"k", "INT", true}, {"s", "BIGINT", false, "SUM"}, {"r", "INT", false, "REPLACE"}});
    auto tablet = create_tablet(*schema);
    auto overlapping = write_rowset(
            schema, {{{1, 1, 10}, {2, 2, 20}, {3, 3, 30}}, {{1, 10, 11}, {2, 20, 21}, {3, 30, 31}}},
            /*version=*/2);
    auto newer = write_rowset(schema, {{{2, 200, 22}, {3, 300, 32}}}, /*version=*/3);
    auto lower = write_rowset(schema, {{{1, 1, 10}, {2, 2, 20}}}, /*version=*/4);
    auto upper = write_rowset(schema, {{{2, 20, 21}, {3, 3, 30}}}, /*version=*/5);
    const std::vector<std::pair<std::vector<RowsetSharedPtr>, std::vector<Row>>> cases = {
            {{overlapping}, {{1, 11, 11}, {2, 22, 21}, {3, 33, 31}}},
            {{overlapping, newer}, {{1, 11, 11}, {2, 222, 22}, {3, 333, 32}}},
            {{lower, upper}, {{1, 1, 10}, {2, 22, 21}, {3, 3, 30}}},
    };

    for (const auto& [rowsets, expected] : cases) {
        for (ReaderType reader_type : kReaderTypes) {
            for (int batch_size : kBatchSizes) {
                SCOPED_TRACE("rowsets=" + std::to_string(rowsets.size()) + " " +
                             trace(reader_type, batch_size));
                EXPECT_EQ(read(tablet, schema, rowsets, reader_type, batch_size), expected);
            }
        }
    }
}

// AGG_KEYS: rowsets whose keys ascend from one rowset to the next, with no segments that overlap,
// are read one after another; every row comes back as it is, with or without the new block
// iterators.
TEST_F(BlockReaderOverlappingSegmentsTest, AggKeysReadRowsetsWithAscendingKeysInTurn) {
    auto schema = make_schema(
            {{"k", "INT", true}, {"s", "BIGINT", false, "SUM"}, {"r", "INT", false, "REPLACE"}});
    auto tablet = create_tablet(*schema);
    std::vector<RowsetSharedPtr> rowsets = {
            write_rowset(schema, {{{1, 1, 10}, {2, 2, 20}}}, /*version=*/2),
            write_rowset(schema, {{{3, 3, 30}, {4, 4, 40}}}, /*version=*/3),
    };

    for (bool new_block_iterator : {true, false}) {
        config::enable_new_block_iterator = new_block_iterator;
        for (ReaderType reader_type : kReaderTypes) {
            for (int batch_size : kBatchSizes) {
                SCOPED_TRACE("new_block_iterator=" + std::to_string(new_block_iterator) + " " +
                             trace(reader_type, batch_size));
                EXPECT_EQ(read(tablet, schema, rowsets, reader_type, batch_size),
                          (std::vector<Row> {{1, 1, 10}, {2, 2, 20}, {3, 3, 30}, {4, 4, 40}}));
            }
        }
    }
}

// AGG_KEYS: a NONOVERLAPPING rowset of several segments is read as one child, its segments one
// after another, and merges with a newer rowset that shares keys 2 and 3 with it.
TEST_F(BlockReaderOverlappingSegmentsTest, AggKeysMergeARowsetOfAscendingSegmentsWithANewerOne) {
    auto schema = make_schema(
            {{"k", "INT", true}, {"s", "BIGINT", false, "SUM"}, {"r", "INT", false, "REPLACE"}});
    auto tablet = create_tablet(*schema);
    std::vector<RowsetSharedPtr> rowsets = {
            write_rowset(schema, {{{1, 1, 10}, {2, 2, 20}}, {{3, 3, 30}, {4, 4, 40}}},
                         /*version=*/2, /*segments_overlap=*/false),
            write_rowset(schema, {{{2, 20, 21}, {3, 30, 31}}}, /*version=*/3),
    };

    for (bool new_block_iterator : {true, false}) {
        config::enable_new_block_iterator = new_block_iterator;
        for (ReaderType reader_type : kReaderTypes) {
            for (int batch_size : kBatchSizes) {
                SCOPED_TRACE("new_block_iterator=" + std::to_string(new_block_iterator) + " " +
                             trace(reader_type, batch_size));
                EXPECT_EQ(read(tablet, schema, rowsets, reader_type, batch_size),
                          (std::vector<Row> {{1, 1, 10}, {2, 22, 21}, {3, 33, 31}, {4, 4, 40}}));
            }
        }
    }
}

// AGG_KEYS: a NONOVERLAPPING rowset of several segments and a newer OVERLAPPING one are merged
// together: every key comes back once, with the SUM of all its rows and the REPLACE value of the
// newer rowset, from its later segment where both hold the key.
TEST_F(BlockReaderOverlappingSegmentsTest, AggKeysMergeAscendingAndOverlappingSegmentsTogether) {
    // VCollectIterator still misreads segments that overlap on keys.
    config::enable_new_block_iterator = true;
    auto schema = make_schema(
            {{"k", "INT", true}, {"s", "BIGINT", false, "SUM"}, {"r", "INT", false, "REPLACE"}});
    auto tablet = create_tablet(*schema);
    std::vector<RowsetSharedPtr> rowsets = {
            write_rowset(schema, {{{1, 1, 10}, {2, 2, 20}}, {{3, 3, 30}, {4, 4, 40}}},
                         /*version=*/2, /*segments_overlap=*/false),
            write_rowset(schema, {{{2, 20, 21}, {3, 30, 31}}, {{3, 300, 32}, {5, 500, 52}}},
                         /*version=*/3),
    };

    for (ReaderType reader_type : kReaderTypes) {
        for (int batch_size : kBatchSizes) {
            SCOPED_TRACE(trace(reader_type, batch_size));
            EXPECT_EQ(read(tablet, schema, rowsets, reader_type, batch_size),
                      (std::vector<Row> {
                              {1, 1, 10}, {2, 22, 21}, {3, 333, 32}, {4, 4, 40}, {5, 500, 52}}));
        }
    }
}

// DUP_KEYS with ORDER BY k1 LIMIT 4 pushed down to storage: an OVERLAPPING rowset whose segments
// interleave, a NONOVERLAPPING rowset of two segments and a one-segment rowset, read ascending and
// descending. The first 4 rows by k1 come back: with the new block iterators just those, else
// among up to twice as many.
TEST_F(BlockReaderOverlappingSegmentsTest, DupKeysTopNReturnsTheFirstRowsByTheOrderByColumns) {
    auto schema = make_schema({{"k1", "INT", true}, {"k2", "INT", true}, {"v", "BIGINT"}},
                              KeysType::DUP_KEYS);
    auto tablet = create_tablet(*schema);
    std::vector<RowsetSharedPtr> rowsets = {
            write_rowset(
                    schema,
                    {{{1, 1, 10}, {3, 1, 30}, {5, 1, 50}}, {{2, 1, 20}, {4, 1, 40}, {6, 1, 60}}},
                    /*version=*/2),
            write_rowset(schema, {{{0, 1, 0}, {7, 1, 70}}, {{8, 1, 80}, {9, 1, 90}}},
                         /*version=*/3, /*segments_overlap=*/false),
            write_rowset(schema, {{{2, 2, 22}, {10, 1, 100}}}, /*version=*/4),
    };

    for (bool new_block_iterator : {true, false}) {
        config::enable_new_block_iterator = new_block_iterator;
        for (bool descending : {false, true}) {
            for (int batch_size : kBatchSizes) {
                SCOPED_TRACE("new_block_iterator=" + std::to_string(new_block_iterator) +
                             " descending=" + std::to_string(descending) + " " +
                             trace(ReaderType::READER_QUERY, batch_size));
                auto rows = read(tablet, schema, rowsets, ReaderType::READER_QUERY, batch_size,
                                 /*topn_limit=*/4, descending);
                if (new_block_iterator) {
                    EXPECT_EQ(rows.size(), 4);
                }
                EXPECT_EQ(first_rows(rows, 4, descending),
                          descending ? (std::vector<Row> {
                                               {10, 1, 100}, {9, 1, 90}, {8, 1, 80}, {7, 1, 70}})
                                     : (std::vector<Row> {
                                               {0, 1, 0}, {1, 1, 10}, {2, 1, 20}, {2, 2, 22}}));
            }
        }
    }
}

// DUP_KEYS with ORDER BY k1 LIMIT pushed down to storage, on the scanner that a parallel scan gives
// segments 1 and 2 of an OVERLAPPING rowset, or segment 1 of a NONOVERLAPPING one: the first rows
// of those segments come back, not the smaller k1 of segment 0.
TEST_F(BlockReaderOverlappingSegmentsTest, DupKeysTopNReadsTheSegmentsOfItsSplit) {
    auto schema = make_schema({{"k1", "INT", true}, {"k2", "INT", true}, {"v", "BIGINT"}},
                              KeysType::DUP_KEYS);
    auto tablet = create_tablet(*schema);
    auto overlapping = write_rowset(
            schema, {{{1, 1, 10}, {4, 1, 40}}, {{2, 1, 20}, {5, 1, 50}}, {{3, 1, 30}, {6, 1, 60}}},
            /*version=*/2);
    auto ascending = write_rowset(schema, {{{1, 1, 10}}, {{2, 1, 20}, {3, 1, 30}}, {{4, 1, 40}}},
                                  /*version=*/3, /*segments_overlap=*/false);

    for (bool new_block_iterator : {true, false}) {
        config::enable_new_block_iterator = new_block_iterator;
        for (int batch_size : kBatchSizes) {
            SCOPED_TRACE("new_block_iterator=" + std::to_string(new_block_iterator) + " " +
                         trace(ReaderType::READER_QUERY, batch_size));
            auto rows = read(tablet, schema, {overlapping}, ReaderType::READER_QUERY, batch_size,
                             /*topn_limit=*/3, /*descending=*/false, /*segments=*/ {1, 3});
            EXPECT_EQ(first_rows(rows, 3, false),
                      (std::vector<Row> {{2, 1, 20}, {3, 1, 30}, {5, 1, 50}}));
            rows = read(tablet, schema, {overlapping}, ReaderType::READER_QUERY, batch_size,
                        /*topn_limit=*/2, /*descending=*/true, /*segments=*/ {1, 3});
            EXPECT_EQ(first_rows(rows, 2, true), (std::vector<Row> {{6, 1, 60}, {5, 1, 50}}));
            rows = read(tablet, schema, {ascending}, ReaderType::READER_QUERY, batch_size,
                        /*topn_limit=*/1, /*descending=*/false, /*segments=*/ {1, 2});
            EXPECT_EQ(first_rows(rows, 1, false), (std::vector<Row> {{2, 1, 20}}));
        }
    }
}

// Merge-on-write UNIQUE_KEYS with ORDER BY k1 LIMIT 4 pushed down to storage: the segments of an
// OVERLAPPING rowset hold different keys with the same k1, and the first 4 rows by k1 all come
// back, ascending and descending.
TEST_F(BlockReaderOverlappingSegmentsTest, MergeOnWriteTopNKeepsRowsEqualOnTheOrderByColumns) {
    // VCollectIterator merges such segments by k1 and keeps one row of each k1.
    config::enable_new_block_iterator = true;
    auto schema = make_schema({{"k1", "INT", true}, {"k2", "INT", true}, {"v", "BIGINT"}},
                              KeysType::UNIQUE_KEYS);
    auto tablet = create_tablet(*schema, /*merge_on_write=*/true);
    std::vector<RowsetSharedPtr> rowsets = {write_rowset(
            schema, {{{1, 1, 10}, {2, 1, 20}, {3, 1, 30}}, {{1, 2, 11}, {2, 2, 21}, {3, 2, 31}}},
            /*version=*/2)};

    for (bool descending : {false, true}) {
        for (int batch_size : kBatchSizes) {
            SCOPED_TRACE("descending=" + std::to_string(descending) + " " +
                         trace(ReaderType::READER_QUERY, batch_size));
            auto rows = read(tablet, schema, rowsets, ReaderType::READER_QUERY, batch_size,
                             /*topn_limit=*/4, descending);
            EXPECT_EQ(rows.size(), 4);
            EXPECT_EQ(
                    first_rows(rows, 4, descending),
                    descending
                            ? (std::vector<Row> {{3, 2, 31}, {3, 1, 30}, {2, 2, 21}, {2, 1, 20}})
                            : (std::vector<Row> {{1, 1, 10}, {1, 2, 11}, {2, 1, 20}, {2, 2, 21}}));
        }
    }
}

} // namespace doris
