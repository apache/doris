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

#include "storage/iterator/sequence_map_block_iterator.h"

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
    if (schema_col.type() == FieldType::OLAP_FIELD_TYPE_INT) {
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

// Schema: k INT KEY, u BIGINT (unmapped), v1 BIGINT, s1 BIGINT NULL, v2 BIGINT, s2 BIGINT NULL
// Sequence map: s1 (col 3) -> {v1 (col 2)}, s2 (col 5) -> {v2 (col 4)}.
TabletSchemaSPtr make_seq_map_schema() {
    TabletSchemaPB pb;
    pb.set_keys_type(KeysType::UNIQUE_KEYS);
    pb.set_num_short_key_columns(1);
    pb.set_num_rows_per_row_block(1024);
    pb.set_compress_kind(COMPRESS_NONE);
    pb.set_next_column_unique_id(6);

    auto add_col = [&](int32_t uid, const std::string& name, const std::string& type, int len,
                       bool is_key, bool nullable) {
        ColumnPB* c = pb.add_column();
        c->set_unique_id(uid);
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

    add_col(0, "k", "INT", 4, true, false);
    add_col(1, "u", "BIGINT", 8, false, false);
    add_col(2, "v1", "BIGINT", 8, false, false);
    add_col(3, "s1", "BIGINT", 8, false, true);
    add_col(4, "v2", "BIGINT", 8, false, false);
    add_col(5, "s2", "BIGINT", 8, false, true);

    ColumnGroupPB* g1 = pb.mutable_seq_map()->add_cg();
    g1->set_sequence_column(3);
    g1->add_columns_in_group(2);
    ColumnGroupPB* g2 = pb.mutable_seq_map()->add_cg();
    g2->set_sequence_column(5);
    g2->add_columns_in_group(4);

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(pb);
    return schema;
}

class FakeSeqMapInput final : public RowwiseIterator {
public:
    FakeSeqMapInput(ReadSchemaSPtr schema, std::vector<std::vector<Row>> batches)
            : _schema(std::move(schema)), _batches(std::move(batches)) {}

    Status init(const StorageReadOptions& /*opts*/) override { return Status::OK(); }

    Status next_batch(Block* block) override {
        if (_pos >= _batches.size()) {
            return Status::EndOfFile("done");
        }
        auto guard = block->mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        for (const auto& row : _batches[_pos++]) {
            for (size_t c = 0; c < row.size(); ++c) {
                insert_cell(*cols[c], *_schema->column(cast_set<uint32_t>(c)), row[c]);
            }
        }
        return Status::OK();
    }

    const ReadSchema& schema() const override { return *_schema; }

private:
    ReadSchemaSPtr _schema;
    std::vector<std::vector<Row>> _batches;
    size_t _pos = 0;
};

} // namespace

// Chained key boundaries ([1,2], [2,3], [3,3]) with row limit 1: bounds batch size, keeps newest
// unmapped column `u`, selects max sequence per group (with NULL smallest and newer winning ties),
// and counts merged rows accurately.
TEST(SequenceMapBlockIteratorUnitTest, FoldsSequenceGroupsAcrossChainedBlockBoundaries) {
    auto tablet_schema = make_seq_map_schema();
    auto read_schema = std::make_shared<ReadSchema>(tablet_schema->columns());
    ASSERT_TRUE(read_schema
                        ->init_from_tablet_schema(*tablet_schema, /*merge_by_seq_map=*/true,
                                                  /*map_changelog_cols=*/false)
                        .ok());

    // Newest row of each key arrives first:
    // k=1: single row {1, 100, 10, 5, 20, 1}
    // k=2 (split across batch 0 and batch 1):
    //   newest: {2, 200, 21, 5, 22, kNull}
    //   older:  {2, 299, 23, 5, 24, 1} -> s1 ties (keep 21), s2=1 > NULL (replace with 24, 1), u stays 200
    // k=3 (split across batch 1 and batch 2):
    //   newest: {3, 300, 31, 2, 32, 9}
    //   older:  {3, 399, 33, 8, 34, 3} -> s1=8 > 2 (replace with 33, 8), s2=3 < 9 (keep 32, 9)
    auto fake = std::make_unique<FakeSeqMapInput>(
            read_schema, std::vector<std::vector<Row>> {
                                 {{1, 100, 10, 5, 20, 1}, {2, 200, 21, 5, 22, kNull}},
                                 {{2, 299, 23, 5, 24, 1}, {3, 300, 31, 2, 32, 9}},
                                 {{3, 399, 33, 8, 34, 3}},
                         });
    uint64_t merged_rows = 0;
    SequenceMapBlockIterator iter(std::move(fake), BlockBatchLimit(1), read_schema, &merged_rows);

    std::vector<Row> all_rows;
    bool eof = false;
    while (!eof) {
        Block block = read_schema->create_read_block();
        ASSERT_TRUE(iter.next_block(&block, &eof).ok());
        EXPECT_EQ(block.rows() == 0, eof);
        auto r = rows_of(block);
        all_rows.insert(all_rows.end(), r.begin(), r.end());
    }
    EXPECT_EQ(merged_rows, 2U);
    EXPECT_EQ(all_rows, (std::vector<Row> {
                                {1, 100, 10, 5, 20, 1},
                                {2, 200, 21, 5, 24, 1},
                                {3, 300, 33, 8, 32, 9},
                        }));
}

class BlockReaderSequenceMapIterTest : public testing::Test {
protected:
    void SetUp() override {
        char buffer[kMaxPathLen];
        EXPECT_NE(getcwd(buffer, kMaxPathLen), nullptr);
        _absolute_dir = std::string(buffer) + "/ut_dir/block_reader_seq_map_iter_test";
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
        static int64_t next_rowset_id = 30000;
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
            t_column.column_type.type = column.type() == FieldType::OLAP_FIELD_TYPE_INT
                                                ? TPrimitiveType::INT
                                                : TPrimitiveType::BIGINT;
            t_column.__set_column_name(column.name());
            t_column.__set_is_key(column.is_key());
            t_column.__set_is_allow_null(column.is_nullable());
            columns.push_back(t_column);
            col_ordinal_to_unique_id[static_cast<uint32_t>(i)] = column.unique_id();
        }
        TTabletSchema t_schema;
        t_schema.__set_short_key_column_count(schema.num_short_key_columns());
        t_schema.__set_schema_hash(5555);
        t_schema.__set_keys_type(TKeysType::UNIQUE_KEYS);
        t_schema.__set_storage_type(TStorageType::COLUMN);
        t_schema.__set_columns(columns);
        TabletMetaSharedPtr tablet_meta(new TabletMeta(
                3, 3, 3, 3, 3, 3, t_schema, 1, col_ordinal_to_unique_id, UniqueId(5, 6),
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
        if (!st.ok()) {
            return {{}, 0};
        }
        std::vector<Row> out;
        bool eof = false;
        while (!eof) {
            Block block = params.read_schema->create_read_block();
            EXPECT_TRUE(reader.next_block_with_aggregation(&block, &eof).ok());
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

// Overlapping segments within a single rowset hold winning sequence values in different segments
// (seg0 wins group 1, seg1 wins group 2); SequenceMapBlockIterator keeps both winning groups
// instead of dropping seg0 whole-row inside the rowset reader.
TEST_F(BlockReaderSequenceMapIterTest, MergesSequenceGroupsAcrossOverlappingSegmentsAndRowsets) {
    config::enable_new_block_iterator = true;
    auto schema = make_seq_map_schema();
    auto tablet = create_tablet(*schema);
    std::vector<RowsetSharedPtr> rowsets = {
            write_rowset(schema,
                         {
                                 {{1, 10, 100, 10, 200, 1}, {2, 20, 210, 5, 220, 5}},
                                 {{1, 11, 101, 2, 201, 20}, {2, 21, 211, 5, 221, 3}},
                         },
                         /*version=*/2),
            write_rowset(schema, {{{2, 22, 212, 4, 222, 9}}}, /*version=*/3),
    };

    for (ReaderType reader_type :
         {ReaderType::READER_QUERY, ReaderType::READER_CUMULATIVE_COMPACTION}) {
        for (int batch_size : {1, 2, 3, 100}) {
            SCOPED_TRACE("reader_type=" + std::to_string(static_cast<int>(reader_type)) +
                         " batch_size=" + std::to_string(batch_size));
            auto [out, merged] = read_all(tablet, schema, rowsets, reader_type, batch_size);
            EXPECT_EQ(merged, 3U);
            EXPECT_EQ(out, (std::vector<Row> {
                                   {1, 11, 100, 10, 201, 20},
                                   {2, 22, 211, 5, 222, 9},
                           }));
        }
    }
}

} // namespace doris
