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

#include "storage/iterator/unique_key_block_iterator.h"

#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <array>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/cast_set.h"
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
#include "storage/olap_define.h"
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

namespace doris {

namespace {

constexpr int64_t kNull = std::numeric_limits<int64_t>::min();
constexpr uint32_t kMaxPathLen = 1024;

using Row = std::vector<int64_t>;

void insert_cell(IColumn& column, const TabletColumn& schema_col, int64_t value) {
    if (auto* nullable = typeid_cast<ColumnNullable*>(&column)) {
        if (value == kNull) {
            nullable->insert_default();
            return;
        }
        nullable->get_null_map_data().push_back(0);
        insert_cell(nullable->get_nested_column(), schema_col, value);
        return;
    }
    if (schema_col.type() == FieldType::OLAP_FIELD_TYPE_TINYINT) {
        auto v = static_cast<int8_t>(value);
        column.insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    } else if (schema_col.type() == FieldType::OLAP_FIELD_TYPE_INT) {
        auto v = static_cast<int32_t>(value);
        column.insert_data(reinterpret_cast<const char*>(&v), sizeof(v));
    } else {
        column.insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
    }
}

int64_t read_cell(const IColumn& column, size_t row) {
    const IColumn* col = &column;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(*col)) {
        if (nullable->is_null_at(row)) {
            return kNull;
        }
        col = &nullable->get_nested_column();
    }
    return col->get_int(row);
}

std::vector<Row> rows_of(const Block& block) {
    std::vector<Row> rows;
    for (size_t r = 0; r < block.rows(); ++r) {
        Row row;
        for (size_t c = 0; c < block.columns(); ++c) {
            row.push_back(read_cell(*block.get_by_position(c).column, r));
        }
        rows.push_back(std::move(row));
    }
    return rows;
}

class FakeMergedInput final : public RowwiseIterator {
public:
    FakeMergedInput(ReadSchemaSPtr schema, std::vector<std::vector<Row>> batches,
                    Status end_status = Status::EndOfFile("done"))
            : _schema(std::move(schema)),
              _batches(std::move(batches)),
              _end_status(std::move(end_status)) {}

    Status init(const StorageReadOptions& /*opts*/) override { return Status::OK(); }

    Status next_batch(Block* block) override {
        if (_pos >= _batches.size()) {
            return _end_status;
        }
        {
            auto guard = block->mutate_columns_scoped();
            auto& cols = guard.mutable_columns();
            for (const auto& row : _batches[_pos]) {
                for (size_t c = 0; c < row.size(); ++c) {
                    insert_cell(*cols[c], *_schema->column(cast_set<uint32_t>(c)), row[c]);
                }
            }
        }
        last_first_col = block->get_by_position(0).column.get();
        ++_pos;
        return Status::OK();
    }

    const ReadSchema& schema() const override { return *_schema; }

    const IColumn* last_first_col = nullptr;

private:
    ReadSchemaSPtr _schema;
    std::vector<std::vector<Row>> _batches;
    Status _end_status;
    size_t _pos = 0;
};

TabletSchemaSPtr make_unique_schema(bool with_seq, bool with_delete_sign) {
    TabletSchemaPB pb;
    pb.set_keys_type(KeysType::UNIQUE_KEYS);
    pb.set_num_short_key_columns(1);
    pb.set_num_rows_per_row_block(1024);
    pb.set_compress_kind(COMPRESS_NONE);
    int32_t next_uid = 0;

    auto add_col = [&](const std::string& name, const std::string& type, int len, bool is_key,
                       bool nullable) {
        ColumnPB* c = pb.add_column();
        c->set_unique_id(next_uid++);
        c->set_name(name);
        c->set_type(type);
        c->set_is_key(is_key);
        c->set_length(len);
        c->set_index_length(len);
        c->set_is_nullable(nullable);
        if (!is_key) {
            c->set_aggregation("REPLACE");
        }
    };

    add_col("k", "INT", 4, true, false);
    add_col("v", "BIGINT", 8, false, false);
    if (with_delete_sign) {
        add_col(DELETE_SIGN, "TINYINT", 1, false, false);
    }
    if (with_seq) {
        pb.set_sequence_col_idx(cast_set<int32_t>(pb.column_size()));
        add_col(SEQUENCE_COL, "BIGINT", 8, false, true);
    }
    pb.set_next_column_unique_id(next_uid);
    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(pb);
    return schema;
}

} // namespace

// Keys spanning across Block boundaries, including an entire Block whose rows all repeat the
// previous Block's last key, are deduplicated to the first row of each key, while a Block with no
// duplicate keys passes its columns through without copying.
TEST(UniqueKeyBlockIteratorUnitTest, DedupsAcrossBlockBoundariesAndPassesDistinctBlocksWhole) {
    auto tablet_schema = make_unique_schema(/*with_seq=*/false, /*with_delete_sign=*/false);
    auto read_schema = std::make_shared<ReadSchema>(tablet_schema->columns());

    auto fake = std::make_unique<FakeMergedInput>(read_schema, std::vector<std::vector<Row>> {
                                                                       {{1, 10}, {2, 20}},
                                                                       {{2, 19}, {2, 18}},
                                                                       {{2, 17}, {3, 30}, {3, 29}},
                                                               });
    const auto* raw_fake = fake.get();
    uint64_t merged_rows = 0;
    UniqueKeyBlockIterator iter(std::move(fake), read_schema, &merged_rows);

    Block b1 = read_schema->create_read_block();
    bool eof = false;
    ASSERT_TRUE(iter.next_block(&b1, &eof).ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(b1.get_by_position(0).column.get(), raw_fake->last_first_col);
    EXPECT_EQ(rows_of(b1), (std::vector<Row> {{1, 10}, {2, 20}}));
    EXPECT_EQ(merged_rows, 0U);

    // Batch 2 ({{2,19},{2,18}}) is wholly dropped, so next_block advances straight to Batch 3.
    Block b2 = read_schema->create_read_block();
    ASSERT_TRUE(iter.next_block(&b2, &eof).ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(rows_of(b2), (std::vector<Row> {{3, 30}}));
    EXPECT_EQ(merged_rows, 4U);

    Block b3 = read_schema->create_read_block();
    ASSERT_TRUE(iter.next_block(&b3, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(b3.rows(), 0U);
}

TEST(UniqueKeyBlockIteratorUnitTest, PropagatesInputError) {
    auto tablet_schema = make_unique_schema(/*with_seq=*/false, /*with_delete_sign=*/false);
    auto read_schema = std::make_shared<ReadSchema>(tablet_schema->columns());
    auto fake = std::make_unique<FakeMergedInput>(read_schema, std::vector<std::vector<Row>> {},
                                                  Status::InternalError("boom"));
    uint64_t merged_rows = 0;
    UniqueKeyBlockIterator iter(std::move(fake), read_schema, &merged_rows);
    Block block = read_schema->create_read_block();
    bool eof = false;
    Status st = iter.next_block(&block, &eof);
    EXPECT_FALSE(st.ok());
}

class BlockReaderUniqueKeyIterTest : public testing::Test {
protected:
    void SetUp() override {
        char buffer[kMaxPathLen];
        EXPECT_NE(getcwd(buffer, kMaxPathLen), nullptr);
        _absolute_dir = std::string(buffer) + "/ut_dir/block_reader_unique_key_iter_test";
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        ASSERT_TRUE(io::global_local_filesystem()
                            ->create_directory(_absolute_dir + "/tablet_path")
                            .ok());
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

    RowsetSharedPtr write_rowset(const TabletSchemaSPtr& schema,
                                 const std::vector<std::vector<Row>>& segments, int64_t version) {
        static int64_t next_rowset_id = 20000;
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
                auto guard = block.mutate_columns_scoped();
                auto& cols = guard.mutable_columns();
                for (const auto& row : segment) {
                    for (size_t c = 0; c < row.size(); ++c) {
                        insert_cell(*cols[c], schema->column(c), row[c]);
                    }
                }
            }
            EXPECT_TRUE(writer->add_block(&block).ok());
            EXPECT_TRUE(writer->flush().ok());
        }
        RowsetSharedPtr rowset;
        EXPECT_TRUE(writer->build(rowset).ok());
        return rowset;
    }

    TabletSharedPtr create_tablet(const TabletSchema& schema) {
        std::vector<TColumn> columns;
        std::unordered_map<uint32_t, uint32_t> col_ordinal_to_unique_id;
        for (size_t i = 0; i < schema.num_columns(); ++i) {
            const TabletColumn& column = schema.column(i);
            TColumn t_column;
            if (column.type() == FieldType::OLAP_FIELD_TYPE_TINYINT) {
                t_column.column_type.type = TPrimitiveType::TINYINT;
            } else if (column.type() == FieldType::OLAP_FIELD_TYPE_INT) {
                t_column.column_type.type = TPrimitiveType::INT;
            } else {
                t_column.column_type.type = TPrimitiveType::BIGINT;
            }
            t_column.__set_column_name(column.name());
            t_column.__set_is_key(column.is_key());
            t_column.__set_is_allow_null(column.is_nullable());
            columns.push_back(t_column);
            col_ordinal_to_unique_id[static_cast<uint32_t>(i)] = column.unique_id();
        }
        TTabletSchema t_schema;
        t_schema.__set_short_key_column_count(schema.num_short_key_columns());
        t_schema.__set_schema_hash(4444);
        t_schema.__set_keys_type(TKeysType::UNIQUE_KEYS);
        t_schema.__set_storage_type(TStorageType::COLUMN);
        t_schema.__set_columns(columns);
        if (schema.has_sequence_col()) {
            t_schema.__set_sequence_col_idx(schema.sequence_col_idx());
        }
        TabletMetaSharedPtr tablet_meta(new TabletMeta(
                2, 2, 2, 2, 2, 2, t_schema, 1, col_ordinal_to_unique_id, UniqueId(3, 4),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
        TabletSharedPtr tablet(new Tablet(*_engine, tablet_meta, nullptr));
        static_cast<void>(tablet->init());
        return tablet;
    }

    static std::pair<std::vector<Row>, uint64_t> read_all(
            const TabletSharedPtr& tablet, const TabletSchemaSPtr& schema,
            const std::vector<RowsetSharedPtr>& rowsets, ReaderType reader_type, int batch_size) {
        TabletReader::ReaderParams params;
        params.tablet = tablet;
        params.tablet_schema = schema;
        params.reader_type = reader_type;
        params.version = Version(0, rowsets.back()->end_version());
        TabletReadSource read_source;
        for (const auto& rs : rowsets) {
            RowsetReaderSharedPtr rs_reader;
            EXPECT_TRUE(rs->create_reader(&rs_reader).ok());
            read_source.rs_splits.emplace_back(std::move(rs_reader));
        }
        read_source.fill_delete_predicates();
        params.set_read_source(std::move(read_source));
        params.read_schema = std::make_shared<ReadSchema>(schema->columns());

        BlockReader reader;
        reader.set_batch_size(batch_size);
        Status st = reader.init(params);
        EXPECT_TRUE(st.ok()) << st;
        std::vector<Row> out;
        bool eof = false;
        while (!eof) {
            Block block = params.read_schema->create_read_block();
            st = reader.next_block_with_aggregation(&block, &eof);
            EXPECT_TRUE(st.ok()) << st;
            auto r = rows_of(block);
            out.insert(out.end(), r.begin(), r.end());
        }
        return {out, reader.merged_rows()};
    }

    StorageEngine* _engine = nullptr;
    std::string _absolute_dir;
    bool _saved_adaptive_batch_size = false;
    bool _saved_new_block_iterator = true;
};

// Overlapping segments in one rowset plus a newer rowset, with nullable sequence column and
// delete_sign: highest non-NULL sequence wins, equal/NULL sequences pick the newer segment/rowset,
// merged_rows counts dropped versions, and base compaction / checksum prune delete_sign=1 rows.
TEST_F(BlockReaderUniqueKeyIterTest, KeepsWinningRowBySequenceAndSegmentOrderAndFiltersDeleteSign) {
    config::enable_new_block_iterator = true;
    auto schema = make_unique_schema(/*with_seq=*/true, /*with_delete_sign=*/true);
    auto tablet = create_tablet(*schema);
    // Columns: k, v, __DORIS_DELETE_SIGN__, __DORIS_SEQUENCE_COL__
    // k=1: seg0 seq=5 v=10 vs seg1 seq=3 v=11 -> seq=5 (v=10) wins!
    // k=2: seg0 seq=NULL v=20 vs seg1 seq=NULL v=21 -> newer segment seg1 (v=21) wins!
    // k=3: rs2 seq=10 v=30 del=0 vs rs3 seq=20 v=31 del=1 -> seq=20 (del=1) wins, pruned in base compaction!
    // k=4: rs2 seq=NULL v=40 vs rs3 seq=1 v=41 -> non-NULL seq=1 (v=41) beats NULL!
    std::vector<RowsetSharedPtr> rowsets = {
            write_rowset(schema,
                         {{{1, 10, 0, 5}, {2, 20, 0, kNull}, {3, 30, 0, 10}, {4, 40, 0, kNull}},
                          {{1, 11, 0, 3}, {2, 21, 0, kNull}}},
                         /*version=*/2),
            write_rowset(schema, {{{3, 31, 1, 20}, {4, 41, 0, 1}}}, /*version=*/3),
    };

    for (int batch_size : {1, 2, 3, 100}) {
        SCOPED_TRACE("batch_size=" + std::to_string(batch_size));
        auto [cumu_rows, cumu_merged] = read_all(
                tablet, schema, rowsets, ReaderType::READER_CUMULATIVE_COMPACTION, batch_size);
        EXPECT_EQ(cumu_merged, 4U);
        EXPECT_EQ(cumu_rows, (std::vector<Row> {
                                     {1, 10, 0, 5},
                                     {2, 21, 0, kNull},
                                     {3, 31, 1, 20},
                                     {4, 41, 0, 1},
                             }));

        auto [base_rows, base_merged] =
                read_all(tablet, schema, rowsets, ReaderType::READER_BASE_COMPACTION, batch_size);
        EXPECT_EQ(base_merged, 4U);
        EXPECT_EQ(base_rows, (std::vector<Row> {
                                     {1, 10, 0, 5},
                                     {2, 21, 0, kNull},
                                     {4, 41, 0, 1},
                             }));
    }
}

namespace {

std::vector<std::weak_ptr<RowsetReader>> attach_rowsets_with_weak_refs(
        const std::vector<RowsetSharedPtr>& rowsets, TabletReader::ReaderParams* params) {
    TabletReadSource read_source;
    std::vector<std::weak_ptr<RowsetReader>> weak_readers;
    for (const auto& rs : rowsets) {
        RowsetReaderSharedPtr rs_reader;
        DORIS_CHECK(rs->create_reader(&rs_reader).ok());
        weak_readers.push_back(rs_reader);
        read_source.rs_splits.emplace_back(std::move(rs_reader));
    }
    read_source.fill_delete_predicates();
    params->set_read_source(std::move(read_source));
    return weak_readers;
}

std::vector<Row> next_block_rows(BlockReader* reader, const ReadSchema& read_schema, bool* eof) {
    Block block = read_schema.create_read_block();
    DORIS_CHECK(reader->next_block_with_aggregation(&block, eof).ok());
    return rows_of(block);
}

std::vector<Row> drain_remaining_rows(BlockReader* reader, const ReadSchema& read_schema,
                                      bool* eof) {
    std::vector<Row> out;
    while (!*eof) {
        auto part = next_block_rows(reader, read_schema, eof);
        out.insert(out.end(), part.begin(), part.end());
    }
    return out;
}

} // namespace

TEST_F(BlockReaderUniqueKeyIterTest, ReleasesMergedRowsetReadersAsSegmentsReachEof) {
    config::enable_new_block_iterator = true;
    auto schema = make_unique_schema(/*with_seq=*/false, /*with_delete_sign=*/false);
    auto tablet = create_tablet(*schema);
    // Overlapping key ranges force VHeapMergeIterator: rs0 has keys {1, 2}, rs1 has keys {1, 3, 4, 5}.
    std::vector<RowsetSharedPtr> rowsets = {
            write_rowset(schema, {{{1, 10}, {2, 20}}}, /*version=*/2),
            write_rowset(schema, {{{1, 11}, {3, 30}, {4, 40}, {5, 50}}}, /*version=*/3),
    };

    TabletReader::ReaderParams params;
    params.tablet = tablet;
    params.tablet_schema = schema;
    params.reader_type = ReaderType::READER_QUERY;
    params.version = Version(0, 3);
    params.read_schema = std::make_shared<ReadSchema>(schema->columns());
    auto weak_readers = attach_rowsets_with_weak_refs(rowsets, &params);
    auto expired_state = [&]() {
        return std::pair<bool, bool> {weak_readers[0].expired(), weak_readers[1].expired()};
    };

    BlockReader reader;
    reader.set_batch_size(2);
    ASSERT_TRUE(reader.init(params).ok());
    params.rs_splits.clear();
    EXPECT_EQ(expired_state(), (std::pair<bool, bool> {false, false}));

    bool eof = false;
    // Batch 1 emits {1,11} from rs1 and stops before rs0's whole block so batch 2 can move it.
    EXPECT_EQ(next_block_rows(&reader, *params.read_schema, &eof), (std::vector<Row> {{1, 11}}));
    EXPECT_EQ(expired_state(), (std::pair<bool, bool> {false, false}));

    // Batch 2 moves rs0's {1,10},{2,20}, drops duplicate key 1, and hits EOF on rs0 immediately.
    EXPECT_EQ(next_block_rows(&reader, *params.read_schema, &eof), (std::vector<Row> {{2, 20}}));
    EXPECT_EQ(expired_state(), (std::pair<bool, bool> {true, false}));

    EXPECT_EQ(drain_remaining_rows(&reader, *params.read_schema, &eof),
              (std::vector<Row> {{3, 30}, {4, 40}, {5, 50}}));
    EXPECT_EQ(expired_state(), (std::pair<bool, bool> {true, true}));
}

} // namespace doris
