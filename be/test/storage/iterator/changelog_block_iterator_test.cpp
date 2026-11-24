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

#include "storage/iterator/changelog_block_iterator.h"

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

#include "common/config.h"
#include "common/status.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/changelog.h"
#include "storage/changelog_types.h"
#include "storage/iterator/block_reader.h"
#include "storage/iterator/changelog_block_reader_utils.h"
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

// Columns: 0=k(BIGINT key), 1=v(BIGINT NULL), 2=__DORIS_CHANGELOG_BEFORE_v__(BIGINT NULL),
//          3=__DORIS_CHANGELOG_TSO__(BIGINT), 4=__DORIS_CHANGELOG_LSN__(BIGINT),
//          5=__DORIS_CHANGELOG_OP__(BIGINT)
using Row = std::vector<int64_t>;

void insert_cell(IColumn& column, int64_t value) {
    if (auto* nullable = typeid_cast<ColumnNullable*>(&column)) {
        if (value == kNull) {
            nullable->insert_default();
            return;
        }
        nullable->get_null_map_data().push_back(0);
        insert_cell(nullable->get_nested_column(), value);
        return;
    }
    column.insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
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

TabletSchemaSPtr make_changelog_schema() {
    TabletSchemaPB pb;
    pb.set_keys_type(KeysType::DUP_KEYS);
    pb.set_num_short_key_columns(1);
    pb.set_num_rows_per_row_block(1024);
    pb.set_compress_kind(COMPRESS_NONE);
    pb.set_next_column_unique_id(6);

    auto add_col = [&](int32_t uid, const std::string& name, bool is_key, bool nullable) {
        ColumnPB* c = pb.add_column();
        c->set_unique_id(uid);
        c->set_name(name);
        c->set_type("BIGINT");
        c->set_is_key(is_key);
        c->set_length(8);
        c->set_index_length(8);
        c->set_is_nullable(nullable);
        if (!is_key) {
            c->set_aggregation("NONE");
        }
    };

    add_col(0, "k", true, false);
    add_col(1, "v", false, true);
    add_col(2, changelog::build_before_column_name("v"), false, true);
    add_col(3, CHANGELOG_TSO_COL, false, false);
    add_col(4, CHANGELOG_LSN_COL, false, false);
    add_col(5, CHANGELOG_OP_COL, false, false);

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(pb);
    return schema;
}

class FakeBlockInput final : public BlockIterator {
public:
    FakeBlockInput(ReadSchemaSPtr schema, std::vector<std::vector<Row>> batches)
            : _schema(std::move(schema)), _batches(std::move(batches)) {}

    Status next_block(Block* block, bool* eof) override {
        if (_pos >= _batches.size()) {
            *eof = true;
            return Status::OK();
        }
        *eof = false;
        auto guard = block->mutate_columns_scoped();
        auto& cols = guard.mutable_columns();
        for (const auto& row : _batches[_pos++]) {
            for (size_t c = 0; c < row.size(); ++c) {
                insert_cell(*cols[c], row[c]);
            }
        }
        return Status::OK();
    }

private:
    ReadSchemaSPtr _schema;
    std::vector<std::vector<Row>> _batches;
    size_t _pos = 0;
};

} // namespace

// Exercises MIN_DELTA across input Block boundaries and batch_size=1 UPDATE pair carry-over:
// - k=1: APPEND(v=10, tso=1) | UPDATE(10->20, tso=2) -> INSERT(v=20, tso=2)
// - k=2: APPEND(v=30, tso=3) + DELETE(before=30, tso=4) -> SKIP
// - k=3: UPDATE(40->50, tso=5) + UPDATE(50->40, tso=6) -> SKIP (no-op update)
// - k=4: UPDATE(60->70, tso=7) -> UPDATE_BEFORE(v=60) + parked UPDATE_AFTER(v=70)
// - k=5: UPDATE(80->90, tso=8) + DELETE(before=90, tso=9) -> DELETE(v=80, tso=9)
TEST(ChangelogBlockIteratorUnitTest, MinDeltaFoldsGroupsAndParksUpdateAfterAtRowLimit) {
    auto tablet_schema = make_changelog_schema();
    auto read_schema = std::make_shared<ReadSchema>(tablet_schema->columns());
    ASSERT_TRUE(read_schema
                        ->init_from_tablet_schema(*tablet_schema, /*merge_by_seq_map=*/false,
                                                  /*map_changelog_cols=*/true)
                        .ok());

    const auto A = static_cast<int64_t>(ChangelogOp::APPEND);
    const auto U = static_cast<int64_t>(ChangelogOp::UPDATE);
    const auto D = static_cast<int64_t>(ChangelogOp::DELETE);

    auto input = std::make_unique<FakeBlockInput>(
            read_schema, std::vector<std::vector<Row>> {
                                 {{1, 10, kNull, 1, 1, A}},
                                 {{1, 20, 10, 2, 2, U}, {2, 30, kNull, 3, 3, A}},
                                 {{2, kNull, 30, 4, 4, D},
                                  {3, 50, 40, 5, 5, U},
                                  {3, 40, 50, 6, 6, U},
                                  {4, 70, 60, 7, 7, U},
                                  {5, 90, 80, 8, 8, U},
                                  {5, kNull, 90, 9, 9, D}},
                         });
    ChangelogMinDeltaBlockIterator iter(std::move(input), /*block_row_max=*/1, read_schema);

    std::vector<Row> out;
    bool eof = false;
    while (!eof) {
        Block block = read_schema->create_read_block();
        ASSERT_TRUE(iter.next_block(&block, &eof).ok());
        EXPECT_EQ(block.rows() == 0, eof);
        if (!eof) {
            EXPECT_EQ(block.rows(), 1U);
        }
        auto r = rows_of(block);
        out.insert(out.end(), r.begin(), r.end());
    }

    const auto INS = static_cast<int64_t>(changelog::StreamChangeOp::INSERT);
    const auto DEL = static_cast<int64_t>(changelog::StreamChangeOp::DELETE);
    const auto UB = static_cast<int64_t>(changelog::StreamChangeOp::UPDATE_BEFORE);
    const auto UA = static_cast<int64_t>(changelog::StreamChangeOp::UPDATE_AFTER);
    ASSERT_EQ(out.size(), 4U);
    EXPECT_EQ((Row {out[0][0], out[0][1], out[0][3], out[0][5]}), (Row {1, 20, 2, INS}));
    EXPECT_EQ((Row {out[1][0], out[1][1], out[1][3], out[1][5]}), (Row {4, 60, 7, UB}));
    EXPECT_EQ((Row {out[2][0], out[2][1], out[2][3], out[2][5]}), (Row {4, 70, 7, UA}));
    EXPECT_EQ((Row {out[3][0], out[3][1], out[3][3], out[3][5]}), (Row {5, 80, 9, DEL}));
}

// When every key group folds to SKIP, next_block returns an empty Block with *eof = true.
TEST(ChangelogBlockIteratorUnitTest, MinDeltaAllSkippedReturnsEmptyEof) {
    auto tablet_schema = make_changelog_schema();
    auto read_schema = std::make_shared<ReadSchema>(tablet_schema->columns());
    ASSERT_TRUE(read_schema->init_from_tablet_schema(*tablet_schema, false, true).ok());

    const auto A = static_cast<int64_t>(ChangelogOp::APPEND);
    const auto D = static_cast<int64_t>(ChangelogOp::DELETE);
    auto input = std::make_unique<FakeBlockInput>(
            read_schema,
            std::vector<std::vector<Row>> {{{1, 10, kNull, 1, 1, A}, {1, kNull, 10, 2, 2, D}}});
    ChangelogMinDeltaBlockIterator iter(std::move(input), /*block_row_max=*/16, read_schema);

    Block block = read_schema->create_read_block();
    bool eof = false;
    ASSERT_TRUE(iter.next_block(&block, &eof).ok());
    EXPECT_TRUE(eof);
    EXPECT_EQ(block.rows(), 0U);
}

class BlockReaderChangelogIterTest : public testing::Test {
protected:
    void SetUp() override {
        char buffer[kMaxPathLen];
        EXPECT_NE(getcwd(buffer, kMaxPathLen), nullptr);
        _absolute_dir = std::string(buffer) + "/ut_dir/block_reader_changelog_iter_test";
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
                                 const std::vector<std::vector<Row>>& segments, int64_t version,
                                 int64_t commit_tso) {
        static int64_t next_rowset_id = 40000;
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
                        insert_cell(*cols[c], row[c]);
                    }
                }
            }
            EXPECT_TRUE(writer->add_block(&block).ok());
            EXPECT_TRUE(writer->flush().ok());
        }
        RowsetSharedPtr rowset;
        EXPECT_TRUE(writer->build(rowset).ok());
        rowset->make_visible(Version(version, version), commit_tso);
        return rowset;
    }

    TabletSharedPtr create_tablet(const TabletSchema& schema) {
        std::vector<TColumn> columns;
        std::unordered_map<uint32_t, uint32_t> col_ordinal_to_unique_id;
        for (size_t i = 0; i < schema.num_columns(); ++i) {
            const TabletColumn& column = schema.column(i);
            TColumn t_column;
            t_column.column_type.type = TPrimitiveType::BIGINT;
            t_column.__set_column_name(column.name());
            t_column.__set_is_key(column.is_key());
            t_column.__set_is_allow_null(column.is_nullable());
            columns.push_back(t_column);
            col_ordinal_to_unique_id[static_cast<uint32_t>(i)] = column.unique_id();
        }
        TTabletSchema t_schema;
        t_schema.__set_short_key_column_count(schema.num_short_key_columns());
        t_schema.__set_schema_hash(6666);
        t_schema.__set_keys_type(TKeysType::DUP_KEYS);
        t_schema.__set_storage_type(TStorageType::COLUMN);
        t_schema.__set_columns(columns);
        TabletMetaSharedPtr tablet_meta(new TabletMeta(
                4, 4, 4, 4, 4, 4, t_schema, 1, col_ordinal_to_unique_id, UniqueId(7, 8),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F));
        TabletSharedPtr tablet(new Tablet(*_engine, tablet_meta, nullptr));
        static_cast<void>(tablet->init());
        return tablet;
    }

    static std::vector<Row> read_changelog(const TabletSharedPtr& tablet,
                                           const TabletSchemaSPtr& schema,
                                           const std::vector<RowsetSharedPtr>& rowsets,
                                           TChangelogScanType::type scan_type, int batch_size) {
        TabletReader::ReaderParams params;
        params.tablet = tablet;
        params.tablet_schema = schema;
        params.reader_type = ReaderType::READER_QUERY;
        params.read_changelog = true;
        params.read_orderby_key = true;
        params.force_key_ordered_read = true;
        params.changelog_scan_type = scan_type;
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
        Status init_st = reader.init(params);
        EXPECT_TRUE(init_st.ok()) << init_st;
        if (!init_st.ok()) {
            return {};
        }
        std::vector<Row> out;
        bool eof = false;
        while (!eof) {
            Block block = params.read_schema->create_read_block();
            EXPECT_TRUE(reader.next_block_with_aggregation(&block, &eof).ok());
            auto r = rows_of(block);
            out.insert(out.end(), r.begin(), r.end());
        }
        return out;
    }

    StorageEngine* _engine = nullptr;
    std::string _absolute_dir;
    bool _saved_adaptive_batch_size = false;
    bool _saved_new_block_iterator = true;
};

// Overlapping segments in one rowset hold the same (key, TSO) (seg0 APPEND 10, seg1 UPDATE 10->20),
// followed by a newer rowset with UPDATE 20->30 at higher TSO: older segment comes first on ties,
// and both MIN_DELTA and DETAIL produce the expected net and detailed changes across batch sizes.
TEST_F(BlockReaderChangelogIterTest, MergesByKeyAndTsoOldestFirstForMinDeltaAndDetail) {
    config::enable_new_block_iterator = true;
    auto schema = make_changelog_schema();
    auto tablet = create_tablet(*schema);
    const auto A = static_cast<int64_t>(ChangelogOp::APPEND);
    const auto U = static_cast<int64_t>(ChangelogOp::UPDATE);

    std::vector<RowsetSharedPtr> rowsets = {
            write_rowset(schema,
                         {
                                 {{1, 10, kNull, 5, 1, A}},
                                 {{1, 20, 10, 5, 2, U}},
                         },
                         /*version=*/2, /*commit_tso=*/5),
            write_rowset(schema, {{{1, 30, 20, 8, 3, U}}}, /*version=*/3, /*commit_tso=*/8),
    };

    const auto INS = static_cast<int64_t>(changelog::StreamChangeOp::INSERT);
    const auto UB = static_cast<int64_t>(changelog::StreamChangeOp::UPDATE_BEFORE);
    const auto UA = static_cast<int64_t>(changelog::StreamChangeOp::UPDATE_AFTER);

    for (int batch_size : {1, 2, 3, 100}) {
        SCOPED_TRACE("batch_size=" + std::to_string(batch_size));
        auto min_delta =
                read_changelog(tablet, schema, rowsets, TChangelogScanType::MIN_DELTA, batch_size);
        ASSERT_EQ(min_delta.size(), 1U);
        EXPECT_EQ((Row {min_delta[0][0], min_delta[0][1], min_delta[0][3], min_delta[0][5]}),
                  (Row {1, 30, 8, INS}));

        auto detail =
                read_changelog(tablet, schema, rowsets, TChangelogScanType::DETAIL, batch_size);
        ASSERT_EQ(detail.size(), 5U);
        EXPECT_EQ((Row {detail[0][1], detail[0][5]}), (Row {10, INS}));
        EXPECT_EQ((Row {detail[1][1], detail[1][5]}), (Row {10, UB}));
        EXPECT_EQ((Row {detail[2][1], detail[2][5]}), (Row {20, UA}));
        EXPECT_EQ((Row {detail[3][1], detail[3][5]}), (Row {20, UB}));
        EXPECT_EQ((Row {detail[4][1], detail[4][5]}), (Row {30, UA}));
    }
}

// Multiple consecutive intermediate UPDATEs for one key (both inside a single Block and spanning
// across Block boundaries) skip copying middle rows while preserving first-before and last-after.
TEST(ChangelogBlockIteratorUnitTest, MinDeltaSkipsIntermediateUpdatesWithinAndAcrossBlocks) {
    auto tablet_schema = make_changelog_schema();
    auto read_schema = std::make_shared<ReadSchema>(tablet_schema->columns());
    ASSERT_TRUE(read_schema->init_from_tablet_schema(*tablet_schema, false, true).ok());

    const auto U = static_cast<int64_t>(ChangelogOp::UPDATE);
    auto input = std::make_unique<FakeBlockInput>(
            read_schema, std::vector<std::vector<Row>> {
                                 {{1, 20, 10, 1, 1, U}, {1, 30, 20, 2, 2, U}, {1, 40, 30, 3, 3, U}},
                                 {{1, 50, 40, 4, 4, U}},
                                 {{1, 60, 50, 5, 5, U}, {2, 200, 100, 6, 6, U}},
                         });
    ChangelogMinDeltaBlockIterator iter(std::move(input), /*block_row_max=*/16, read_schema);

    Block block = read_schema->create_read_block();
    bool eof = false;
    ASSERT_TRUE(iter.next_block(&block, &eof).ok());
    EXPECT_FALSE(eof);
    const auto UB = static_cast<int64_t>(changelog::StreamChangeOp::UPDATE_BEFORE);
    const auto UA = static_cast<int64_t>(changelog::StreamChangeOp::UPDATE_AFTER);
    auto out = rows_of(block);
    ASSERT_EQ(out.size(), 4U);
    EXPECT_EQ((Row {out[0][0], out[0][1], out[0][5]}), (Row {1, 10, UB}));
    EXPECT_EQ((Row {out[1][0], out[1][1], out[1][5]}), (Row {1, 60, UA}));
    EXPECT_EQ((Row {out[2][0], out[2][1], out[2][5]}), (Row {2, 100, UB}));
    EXPECT_EQ((Row {out[3][0], out[3][1], out[3][5]}), (Row {2, 200, UA}));
}

} // namespace doris
