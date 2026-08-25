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

// Producer-side coverage for horizontal (non-vertical) row-binlog compaction.
//
// With enable_vertical_compaction=false a row-binlog cumulative compaction that takes the
// real merge path runs Merger::vmerge_rowsets. Two things must hold for downstream
// MIN_DELTA reads to be correct:
//   1. The merged output must be globally (key, TSO)-ordered, not a UNION of the inputs.
//      A UNION would write key1@TSO1..keyN@TSO1 then key1@TSO2..keyN@TSO2, so a key's
//      change chain is no longer consecutive.
//   2. When a forced segment boundary splits a key range across output segments (the same
//      user key lands in more than one segment), the output rowset meta must be OVERLAPPING
//      so BetaRowsetReader::is_merge_iterator() picks a merge iterator.
//
// This test drives the real Merger::vmerge_rowsets on a row-binlog tablet with two input
// rowsets that share every key under different TSOs, forces a multi-segment output via
// RowsetWriterContext::max_rows_per_segment, and asserts both properties. A negative case
// on a plain (non-binlog) DUP_KEYS tablet proves the fix is gated on the row-binlog path.

#include <gen_cpp/olap_common.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/merger.h"
#include "storage/olap_common.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/rowset_reader_context.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/schema.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "util/uid_util.h"

namespace doris {
using namespace ErrorCode;

namespace {
constexpr uint32_t kMaxPathLen = 1024;
constexpr char kTestDir[] = "/row_binlog_vmerge_test";
} // namespace

class RowBinlogVmergeCompactionTest : public testing::Test {
protected:
    void SetUp() override {
        char buffer[kMaxPathLen];
        EXPECT_NE(getcwd(buffer, kMaxPathLen), nullptr);
        _absolute_dir = std::string(buffer) + kTestDir;
        auto st = io::global_local_filesystem()->delete_directory(_absolute_dir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(_absolute_dir);
        ASSERT_TRUE(st.ok()) << st;
        EXPECT_TRUE(io::global_local_filesystem()
                            ->create_directory(_absolute_dir + "/tablet_path")
                            .ok());
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        _engine = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    // Row-binlog read schema: single leading key column, a value column, and the binlog TSO
    // column (marked via binlog_tso_col_idx so Level1Iterator uses it as the merge sequence
    // column). DUP_KEYS matches the FE-generated hidden row-binlog table, which keeps every
    // change event instead of deduplicating by user key.
    TabletSchemaSPtr create_row_binlog_schema() {
        TabletSchemaPB pb;
        pb.set_keys_type(DUP_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_NONE);
        pb.set_next_column_unique_id(4);

        ColumnPB* key = pb.add_column();
        key->set_unique_id(0);
        key->set_name("k");
        key->set_type("INT");
        key->set_is_key(true);
        key->set_length(4);
        key->set_index_length(4);
        key->set_is_nullable(false);

        ColumnPB* val = pb.add_column();
        val->set_unique_id(1);
        val->set_name("v");
        val->set_type("INT");
        val->set_is_key(false);
        val->set_length(4);
        val->set_is_nullable(false);

        // The binlog TSO column must be BIGINT; SegmentIterator::_update_tso_col_if_needed
        // asserts OLAP_FIELD_TYPE_BIGINT for it.
        ColumnPB* tso = pb.add_column();
        tso->set_unique_id(2);
        tso->set_name(BINLOG_TSO_COL);
        tso->set_type("BIGINT");
        tso->set_is_key(false);
        tso->set_length(8);
        tso->set_is_nullable(false);
        tso->set_visible(false);
        pb.set_binlog_tso_col_idx(2);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    TabletSchemaSPtr create_plain_dup_schema() {
        TabletSchemaPB pb;
        pb.set_keys_type(DUP_KEYS);
        pb.set_num_short_key_columns(1);
        pb.set_num_rows_per_row_block(1024);
        pb.set_compress_kind(COMPRESS_NONE);
        pb.set_next_column_unique_id(3);

        ColumnPB* key = pb.add_column();
        key->set_unique_id(0);
        key->set_name("k");
        key->set_type("INT");
        key->set_is_key(true);
        key->set_length(4);
        key->set_index_length(4);
        key->set_is_nullable(false);

        ColumnPB* val = pb.add_column();
        val->set_unique_id(1);
        val->set_name("v");
        val->set_type("INT");
        val->set_is_key(false);
        val->set_length(4);
        val->set_is_nullable(false);

        auto schema = std::make_shared<TabletSchema>();
        schema->init_from_pb(pb);
        return schema;
    }

    RowsetWriterContext create_rowset_writer_context(const TabletSchemaSPtr& tablet_schema,
                                                     const SegmentsOverlapPB& overlap,
                                                     uint32_t max_rows_per_segment, Version version,
                                                     bool enable_binlog) {
        static int64_t inc_id = 0;
        RowsetWriterContext context;
        RowsetId rowset_id;
        rowset_id.init(++inc_id);
        context.rowset_id = rowset_id;
        context.rowset_type = BETA_ROWSET;
        context.rowset_state = VISIBLE;
        context.tablet_schema = tablet_schema;
        context.tablet_path = _absolute_dir + "/tablet_path";
        context.version = version;
        context.segments_overlap = overlap;
        context.max_rows_per_segment = max_rows_per_segment;
        if (enable_binlog) {
            context.write_binlog_opt().enable = true;
        }
        return context;
    }

    // Build a single-segment input rowset. Each (key, val, tso) tuple becomes one row; every
    // input rowset shares the same keys but a distinct tso so the two rowsets overlap on keys.
    RowsetSharedPtr create_input_rowset(const TabletSchemaSPtr& tablet_schema, bool with_tso,
                                        const std::vector<std::tuple<int, int, int>>& rows,
                                        int64_t version) {
        auto context = create_rowset_writer_context(tablet_schema, NONOVERLAPPING, UINT32_MAX,
                                                    {version, version}, /*enable_binlog=*/false);
        auto res = RowsetFactory::create_rowset_writer(*_engine, context, false);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto writer = std::move(res).value();

        Block block = tablet_schema->create_storage_block();
        auto columns = std::move(block).mutate_columns();
        for (const auto& [k, v, tso] : rows) {
            columns[0]->insert_data((const char*)&k, sizeof(k));
            columns[1]->insert_data((const char*)&v, sizeof(v));
            if (with_tso) {
                int64_t tso64 = tso;
                columns[2]->insert_data((const char*)&tso64, sizeof(tso64));
            }
        }
        block.set_columns(std::move(columns));
        EXPECT_TRUE(writer->add_block(&block).ok());
        EXPECT_TRUE(writer->flush().ok());

        RowsetSharedPtr rowset;
        EXPECT_EQ(Status::OK(), writer->build(rowset));
        return rowset;
    }

    TabletSharedPtr create_tablet(const TabletSchema& tablet_schema, bool row_binlog_role) {
        std::vector<TColumn> cols;
        std::unordered_map<uint32_t, uint32_t> col_ordinal_to_unique_id;
        for (auto i = 0; i < tablet_schema.num_columns(); i++) {
            const TabletColumn& column = tablet_schema.column(i);
            TColumn col;
            col.column_type.type = TPrimitiveType::INT;
            col.__set_column_name(column.name());
            col.__set_is_key(column.is_key());
            cols.push_back(col);
            col_ordinal_to_unique_id[i] = column.unique_id();
        }

        TTabletSchema t_tablet_schema;
        t_tablet_schema.__set_short_key_column_count(tablet_schema.num_short_key_columns());
        t_tablet_schema.__set_schema_hash(3333);
        t_tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
        t_tablet_schema.__set_storage_type(TStorageType::COLUMN);
        t_tablet_schema.__set_columns(cols);
        TabletMetaSharedPtr tablet_meta(new TabletMeta(
                1, 1, 1, 1, 1, 1, t_tablet_schema, 1, col_ordinal_to_unique_id, UniqueId(1, 2),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0, false));
        if (row_binlog_role) {
            tablet_meta->set_tablet_role(TabletRolePB::TABLET_ROLE_ROW_BINLOG);
        }

        TabletSharedPtr tablet(new Tablet(*_engine, tablet_meta, nullptr));
        static_cast<void>(tablet->init());
        return tablet;
    }

    // Read the output rowset in physical (segment) order. need_ordered_result=false makes the
    // reader concatenate segments as written, so the returned sequence reflects the on-disk
    // layout produced by the merge.
    std::vector<std::tuple<int, int, int>> read_all(const RowsetSharedPtr& rowset,
                                                    const TabletSchemaSPtr& tablet_schema,
                                                    bool with_tso) {
        std::vector<ColumnId> ordinals;
        for (uint32_t i = 0; i < tablet_schema->num_columns(); ++i) {
            ordinals.push_back(i);
        }
        auto read_schema = std::make_shared<ReadSchema>(
                project_columns_by_ordinal(tablet_schema->columns(), ordinals));

        RowsetReaderContext reader_context;
        reader_context.tablet_schema = tablet_schema;
        reader_context.need_ordered_result = false;
        reader_context.read_schema = read_schema;

        RowsetReaderSharedPtr reader;
        EXPECT_TRUE(rowset->create_reader(&reader).ok());
        EXPECT_TRUE(reader->init(&reader_context).ok());

        std::vector<std::tuple<int, int, int>> out;
        Status s;
        do {
            Block block = read_schema->create_read_block();
            s = reader->next_batch(&block);
            auto columns = block.get_columns_with_type_and_name();
            for (auto i = 0; i < block.rows(); i++) {
                int tso = with_tso ? static_cast<int>(columns[2].column->get_int(i)) : 0;
                out.emplace_back(static_cast<int>(columns[0].column->get_int(i)),
                                 static_cast<int>(columns[1].column->get_int(i)), tso);
            }
        } while (s.ok());
        EXPECT_TRUE(s.is<END_OF_FILE>()) << s;
        return out;
    }

    std::string _absolute_dir;
    StorageEngine* _engine = nullptr;
};

// A real (key, TSO) merge of two row-binlog rowsets that share every key must interleave the
// events by key then TSO, and a forced segment boundary that splits a key range must mark the
// output OVERLAPPING.
TEST_F(RowBinlogVmergeCompactionTest, HorizontalMergeIsKeyTsoOrderedAndOverlapping) {
    TabletSchemaSPtr schema = create_row_binlog_schema();
    TabletSharedPtr tablet = create_tablet(*schema, /*row_binlog_role=*/true);
    ASSERT_TRUE(tablet->is_row_binlog_tablet());

    // rs0: keys 1..3 at tso=10; rs1: keys 1..3 at tso=20. The two rowsets overlap on keys.
    auto rs0 = create_input_rowset(schema, /*with_tso=*/true,
                                   {{1, 100, 10}, {2, 200, 10}, {3, 300, 10}}, /*version=*/1);
    auto rs1 = create_input_rowset(schema, /*with_tso=*/true,
                                   {{1, 110, 20}, {2, 210, 20}, {3, 310, 20}}, /*version=*/2);

    // Output default is NONOVERLAPPING (matches compaction.cpp for the non-quick-merge path).
    // With the (key, TSO) merge the physical order is
    //   (1,100),(1,110),(2,200),(2,210),(3,300),(3,310)
    // so max_rows_per_segment=3 splits key 2 across two segments (seg0 max_key=2, seg1
    // min_key=2), making the segments genuinely overlap.
    auto ctx = create_rowset_writer_context(schema, NONOVERLAPPING, /*max_rows_per_segment=*/3,
                                            {0, rs1->end_version()}, /*enable_binlog=*/true);
    auto res = RowsetFactory::create_rowset_writer(*_engine, ctx, /*is_vertical=*/false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto writer = std::move(res).value();

    std::vector<RowsetReaderSharedPtr> input_rs_readers;
    for (auto& rowset : {rs0, rs1}) {
        RowsetReaderSharedPtr rs_reader;
        ASSERT_TRUE(rowset->create_reader(&rs_reader).ok());
        input_rs_readers.push_back(std::move(rs_reader));
    }

    Merger::Statistics stats;
    ASSERT_TRUE(Merger::vmerge_rowsets(tablet, ReaderType::READER_CUMULATIVE_COMPACTION, *schema,
                                       input_rs_readers, writer.get(), &stats)
                        .ok());
    RowsetSharedPtr out_rowset;
    ASSERT_EQ(Status::OK(), writer->build(out_rowset));

    // Fix (B): a multi-segment output whose segments share a boundary key is OVERLAPPING.
    EXPECT_GT(out_rowset->rowset_meta()->num_segments(), 1);
    EXPECT_EQ(OVERLAPPING, out_rowset->rowset_meta()->segments_overlap());
    EXPECT_TRUE(out_rowset->rowset_meta()->is_segments_overlapping());

    // Fix (A): the merged stream is globally (key, TSO)-ordered, so each key's two events are
    // consecutive with the earlier-TSO (lower version) row first. A buggy UNION would instead
    // produce (1,100),(2,200),(3,300),(1,110),(2,210),(3,310). The binlog TSO column itself is
    // rewritten to the rowset commit TSO on read (_update_tso_col_if_needed), so we assert on
    // (key, val) which carries the per-event identity.
    auto rows = read_all(out_rowset, schema, /*with_tso=*/false);
    std::vector<std::pair<int, int>> got;
    for (const auto& [k, v, tso] : rows) {
        got.emplace_back(k, v);
    }
    std::vector<std::pair<int, int>> expected = {{1, 100}, {1, 110}, {2, 200},
                                                 {2, 210}, {3, 300}, {3, 310}};
    EXPECT_EQ(expected, got);
}

// Guard: the same overlapping-input + forced-boundary layout on a plain (non row-binlog)
// DUP_KEYS tablet must NOT be marked OVERLAPPING, proving both fixes are gated on the
// row-binlog path.
TEST_F(RowBinlogVmergeCompactionTest, PlainDupMergeStaysNonOverlapping) {
    TabletSchemaSPtr schema = create_plain_dup_schema();
    TabletSharedPtr tablet = create_tablet(*schema, /*row_binlog_role=*/false);
    ASSERT_FALSE(tablet->is_row_binlog_tablet());

    auto rs0 =
            create_input_rowset(schema, /*with_tso=*/false, {{1, 100, 0}, {2, 200, 0}, {3, 300, 0}},
                                /*version=*/1);
    auto rs1 =
            create_input_rowset(schema, /*with_tso=*/false, {{1, 110, 0}, {2, 210, 0}, {3, 310, 0}},
                                /*version=*/2);

    auto ctx = create_rowset_writer_context(schema, NONOVERLAPPING, /*max_rows_per_segment=*/2,
                                            {0, rs1->end_version()}, /*enable_binlog=*/false);
    auto res = RowsetFactory::create_rowset_writer(*_engine, ctx, /*is_vertical=*/false);
    ASSERT_TRUE(res.has_value()) << res.error();
    auto writer = std::move(res).value();

    std::vector<RowsetReaderSharedPtr> input_rs_readers;
    for (auto& rowset : {rs0, rs1}) {
        RowsetReaderSharedPtr rs_reader;
        ASSERT_TRUE(rowset->create_reader(&rs_reader).ok());
        input_rs_readers.push_back(std::move(rs_reader));
    }

    Merger::Statistics stats;
    ASSERT_TRUE(Merger::vmerge_rowsets(tablet, ReaderType::READER_CUMULATIVE_COMPACTION, *schema,
                                       input_rs_readers, writer.get(), &stats)
                        .ok());
    RowsetSharedPtr out_rowset;
    ASSERT_EQ(Status::OK(), writer->build(out_rowset));

    EXPECT_NE(OVERLAPPING, out_rowset->rowset_meta()->segments_overlap());
    EXPECT_FALSE(out_rowset->rowset_meta()->is_segments_overlapping());
}

} // namespace doris
