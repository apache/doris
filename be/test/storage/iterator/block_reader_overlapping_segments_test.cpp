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
// same key can sit in two segments of one rowset. Reading such a rowset takes two merges: the
// rowset reader merges its segments (VMergeIterator over LazyInitSegmentIterators) and marks
// every row whose key repeats the row before it, then the rowsets are merged and
// hands every row with its same-key bit to the BlockReader fold. These tests write such rowsets
// for AGG_KEYS, merge-on-read UNIQUE_KEYS and sequence-map UNIQUE_KEYS tables, read them through
// BlockReader as a query and as a compaction, with Block sizes that cut key groups in two, and
// check the rows that come back:
//   - the row of the segment flushed later is the newer row of its key;
//   - a key group cut by a Block boundary is still folded into one row;
//   - a sequence-map table still sees every row of a key, so every column group takes the row
//     with its own largest sequence value.

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>
#include <unistd.h>

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
    std::string type; // INT, BIGINT or TINYINT
    bool is_key;
    std::string aggregation; // AGG_KEYS value columns only
};

using Row = std::vector<int64_t>;
// Sequence map of a UNIQUE_KEYS schema: each sequence column with the one value column it governs.
using SequenceMapSpec = std::vector<std::pair<std::string, std::string>>;

int column_length(const std::string& type) {
    if (type == "INT") {
        return 4;
    }
    if (type == "BIGINT") {
        return 8;
    }
    DORIS_CHECK_EQ(type, "TINYINT");
    return 1;
}

TPrimitiveType::type thrift_type(FieldType type) {
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_INT:
        return TPrimitiveType::INT;
    case FieldType::OLAP_FIELD_TYPE_BIGINT:
        return TPrimitiveType::BIGINT;
    case FieldType::OLAP_FIELD_TYPE_TINYINT:
        return TPrimitiveType::TINYINT;
    default:
        DORIS_CHECK(false) << "unexpected test column type " << static_cast<int>(type);
        return TPrimitiveType::INVALID_TYPE;
    }
}

TKeysType::type thrift_keys_type(KeysType keys_type) {
    switch (keys_type) {
    case KeysType::UNIQUE_KEYS:
        return TKeysType::UNIQUE_KEYS;
    case KeysType::AGG_KEYS:
        return TKeysType::AGG_KEYS;
    default:
        DORIS_CHECK(false) << "unexpected keys type " << static_cast<int>(keys_type);
        return TKeysType::DUP_KEYS;
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
    case FieldType::OLAP_FIELD_TYPE_TINYINT: {
        auto v = static_cast<int8_t>(value);
        column.insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
        return;
    }
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

// A query and a compaction both read through BlockReader, with Blocks of 1, 2 and 3 rows that cut
// key groups in two and with Blocks that hold the whole merge.
constexpr std::array kReaderTypes = {ReaderType::READER_QUERY,
                                     ReaderType::READER_CUMULATIVE_COMPACTION};
constexpr std::array kBatchSizes = {1, 2, 3, 100};

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
    }

    void TearDown() override {
        config::enable_adaptive_batch_size = _saved_adaptive_batch_size;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        _engine = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    // Column unique ids are the positions in `columns`.
    static TabletSchemaSPtr make_schema(KeysType keys_type, const std::vector<ColumnSpec>& columns,
                                        const SequenceMapSpec& sequence_map = {}) {
        TabletSchemaPB pb;
        pb.set_keys_type(keys_type);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_NONE);
        pb.set_next_column_unique_id(static_cast<int32_t>(columns.size()));
        auto uid_of = [&](const std::string& name) -> int32_t {
            for (size_t i = 0; i < columns.size(); ++i) {
                if (columns[i].name == name) {
                    return static_cast<int32_t>(i);
                }
            }
            DORIS_CHECK(false) << "unknown column " << name;
            return -1;
        };
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
        for (const auto& [sequence_column, value_column] : sequence_map) {
            ColumnGroupPB* group = pb.mutable_seq_map()->add_cg();
            group->set_sequence_column(uid_of(sequence_column));
            group->add_columns_in_group(uid_of(value_column));
        }
        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    // One rowset with one segment per entry of `segments`, declared OVERLAPPING when there are
    // several, like the rowset of one load that flushed several memtables: segment i + 1 was
    // flushed after segment i. Rows of a segment must be in key order.
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

    // A merge-on-read tablet of `schema`.
    TabletSharedPtr create_tablet(const TabletSchema& schema) {
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
        TabletMetaSharedPtr tablet_meta(new TabletMeta(
                1, 1, 1, 1, 1, 1, t_schema, 1, col_ordinal_to_unique_id, UniqueId(1, 2),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0,
                /*enable_unique_key_merge_on_write=*/false));
        TabletSharedPtr tablet(new Tablet(*_engine, tablet_meta, nullptr));
        static_cast<void>(tablet->init());
        return tablet;
    }

    // Read `rowsets` through BlockReader the way a scan or a compaction of `reader_type` with
    // `batch_size` rows per Block does, and return every row in output order.
    static std::vector<Row> read(const TabletSharedPtr& tablet, const TabletSchemaSPtr& schema,
                                 const std::vector<RowsetSharedPtr>& rowsets,
                                 ReaderType reader_type, int batch_size) {
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
};

// Merge-on-read UNIQUE_KEYS without a sequence column: keys 1 and 3 sit in both overlapping
// segments of one rowset, and the rows of segment 1 are the newer ones that a read keeps.
TEST_F(BlockReaderOverlappingSegmentsTest, UniqueKeysKeepTheRowOfTheLaterSegment) {
    auto schema = make_schema(KeysType::UNIQUE_KEYS,
                              {{"k", "INT", true}, {"v", "INT"}, {DELETE_SIGN, "TINYINT"}});
    auto tablet = create_tablet(*schema);
    std::vector<RowsetSharedPtr> rowsets = {
            write_rowset(schema, {{{1, 10, 0}, {2, 20, 0}, {3, 30, 0}}, {{1, 11, 0}, {3, 31, 0}}},
                         /*version=*/2),
    };

    for (ReaderType reader_type : kReaderTypes) {
        for (int batch_size : kBatchSizes) {
            SCOPED_TRACE(trace(reader_type, batch_size));
            EXPECT_EQ(read(tablet, schema, rowsets, reader_type, batch_size),
                      (std::vector<Row> {{1, 11, 0}, {2, 20, 0}, {3, 31, 0}}));
        }
    }
}

// AGG_KEYS: both overlapping segments of the lowest-version rowset hold keys 1..3, so its
// segment merge yields 1 1 2 2 3 3 and Blocks of 1, 2 or 3 rows cut key groups in two. Every key
// must still come back once, with the SUM of all its rows and the REPLACE value of its newest
// row: the later segment within the rowset, the newer rowset when there is one.
TEST_F(BlockReaderOverlappingSegmentsTest, AggKeysFoldEveryKeyGroupOnce) {
    auto schema = make_schema(
            KeysType::AGG_KEYS,
            {{"k", "INT", true}, {"s", "BIGINT", false, "SUM"}, {"r", "INT", false, "REPLACE"}});
    auto tablet = create_tablet(*schema);
    auto overlapping = write_rowset(
            schema, {{{1, 1, 10}, {2, 2, 20}, {3, 3, 30}}, {{1, 10, 11}, {2, 20, 21}, {3, 30, 31}}},
            /*version=*/2);
    auto newer = write_rowset(schema, {{{2, 200, 22}, {3, 300, 32}}}, /*version=*/3);
    const std::vector<std::pair<std::vector<RowsetSharedPtr>, std::vector<Row>>> cases = {
            {{overlapping}, {{1, 11, 11}, {2, 22, 21}, {3, 33, 31}}},
            {{overlapping, newer}, {{1, 11, 11}, {2, 222, 22}, {3, 333, 32}}},
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

// UNIQUE_KEYS with the sequence map {s1 -> v1, s2 -> v2}: every column group takes the values of
// the row with its own largest sequence value, so the read must see every row of a key. Key 1
// sits in both overlapping segments of one rowset and, in the second case, in a newer rowset too:
// s1 is largest in segment 0, s2 in segment 1 or in the newer rowset, so no single row holds the
// result.
TEST_F(BlockReaderOverlappingSegmentsTest, SequenceMapMergesEveryColumnGroup) {
    auto schema = make_schema(KeysType::UNIQUE_KEYS,
                              {{"k", "INT", true},
                               {"v1", "INT"},
                               {"s1", "BIGINT"},
                               {"v2", "INT"},
                               {"s2", "BIGINT"},
                               {DELETE_SIGN, "TINYINT"}},
                              {{"s1", "v1"}, {"s2", "v2"}});
    ASSERT_TRUE(schema->has_seq_map());
    auto tablet = create_tablet(*schema);
    auto overlapping = write_rowset(
            schema, {{{1, 10, 5, 100, 1, 0}, {2, 20, 1, 200, 1, 0}}, {{1, 11, 3, 101, 7, 0}}},
            /*version=*/2);
    auto newer = write_rowset(schema, {{{1, 12, 4, 102, 9, 0}}}, /*version=*/3);
    const std::vector<std::pair<std::vector<RowsetSharedPtr>, std::vector<Row>>> cases = {
            {{overlapping}, {{1, 10, 5, 101, 7, 0}, {2, 20, 1, 200, 1, 0}}},
            {{overlapping, newer}, {{1, 10, 5, 102, 9, 0}, {2, 20, 1, 200, 1, 0}}},
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

} // namespace doris
