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
#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <random>
#include <string>
#include <vector>

#include "common/config.h"
#include "io/fs/local_file_system.h"
#include "olap/cumulative_compaction.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/storage_engine.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_reader.h"
#include "olap/tablet_schema.h"
#include "runtime/exec_env.h"
#include "util/key_util.h"
#include "vec/olap/block_reader.h"

namespace doris {
static std::string kSegmentDir = "./ut_dir/segments_key_bounds_truncation_test";

class SegmentsKeyBoundsTruncationTest : public testing::Test {
private:
    StorageEngine* engine_ref = nullptr;
    std::string absolute_dir;
    std::unique_ptr<DataDir> data_dir;
    int cur_version {2};

public:
    void SetUp() override {
        auto st = io::global_local_filesystem()->delete_directory(kSegmentDir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(kSegmentDir);
        ASSERT_TRUE(st.ok()) << st;
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        engine_ref = engine.get();
        data_dir = std::make_unique<DataDir>(*engine_ref, kSegmentDir);
        ASSERT_TRUE(data_dir->update_capacity().ok());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kSegmentDir).ok());
        engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    void disable_segments_key_bounds_truncation() {
        config::segments_key_bounds_truncation_threshold = -1;
    }

    TabletSchemaSPtr create_schema(int varchar_length) {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(DUP_KEYS);
        tablet_schema_pb.set_num_short_key_columns(1);
        tablet_schema_pb.set_num_rows_per_row_block(1024);
        tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
        tablet_schema_pb.set_next_column_unique_id(4);

        ColumnPB* column_1 = tablet_schema_pb.add_column();
        column_1->set_unique_id(1);
        column_1->set_name("k1");
        column_1->set_type("VARCHAR");
        column_1->set_is_key(true);
        column_1->set_length(varchar_length);
        column_1->set_index_length(36);
        column_1->set_is_nullable(false);
        column_1->set_is_bf_column(false);

        ColumnPB* column_2 = tablet_schema_pb.add_column();
        column_2->set_unique_id(2);
        column_2->set_name("c1");
        column_2->set_type("INT");
        column_2->set_length(4);
        column_2->set_index_length(4);
        column_2->set_is_nullable(true);
        column_2->set_is_key(false);
        column_2->set_is_nullable(true);
        column_2->set_is_bf_column(false);

        tablet_schema->init_from_pb(tablet_schema_pb);
        return tablet_schema;
    }

    TabletSharedPtr create_tablet(const TabletSchema& tablet_schema,
                                  bool enable_unique_key_merge_on_write) {
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
        if (tablet_schema.keys_type() == UNIQUE_KEYS) {
            t_tablet_schema.__set_keys_type(TKeysType::UNIQUE_KEYS);
        } else if (tablet_schema.keys_type() == DUP_KEYS) {
            t_tablet_schema.__set_keys_type(TKeysType::DUP_KEYS);
        } else if (tablet_schema.keys_type() == AGG_KEYS) {
            t_tablet_schema.__set_keys_type(TKeysType::AGG_KEYS);
        }
        t_tablet_schema.__set_storage_type(TStorageType::COLUMN);
        t_tablet_schema.__set_columns(cols);
        TabletMetaSharedPtr tablet_meta {std::make_shared<TabletMeta>(
                2, 2, 2, 2, 2, 2, t_tablet_schema, 2, col_ordinal_to_unique_id, UniqueId(1, 2),
                TTabletType::TABLET_TYPE_DISK, TCompressionType::LZ4F, 0,
                enable_unique_key_merge_on_write)};

        TabletSharedPtr tablet {std::make_shared<Tablet>(*engine_ref, tablet_meta, data_dir.get())};
        EXPECT_TRUE(tablet->init().ok());
        return tablet;
    }

    RowsetWriterContext create_rowset_writer_context(TabletSchemaSPtr tablet_schema,
                                                     const SegmentsOverlapPB& overlap,
                                                     uint32_t max_rows_per_segment,
                                                     Version version) {
        RowsetWriterContext rowset_writer_context;
        rowset_writer_context.rowset_id = engine_ref->next_rowset_id();
        rowset_writer_context.rowset_type = BETA_ROWSET;
        rowset_writer_context.rowset_state = VISIBLE;
        rowset_writer_context.tablet_schema = tablet_schema;
        rowset_writer_context.tablet_path = kSegmentDir;
        rowset_writer_context.version = version;
        rowset_writer_context.segments_overlap = overlap;
        rowset_writer_context.max_rows_per_segment = max_rows_per_segment;
        return rowset_writer_context;
    }

    void create_and_init_rowset_reader(Rowset* rowset, RowsetReaderContext& context,
                                       RowsetReaderSharedPtr* result) {
        auto s = rowset->create_reader(result);
        EXPECT_TRUE(s.ok());
        EXPECT_TRUE(*result != nullptr);

        s = (*result)->init(&context);
        EXPECT_TRUE(s.ok());
    }

    std::vector<vectorized::Block> generate_blocks(
            TabletSchemaSPtr tablet_schema, const std::vector<std::vector<std::string>>& data) {
        std::vector<vectorized::Block> ret;
        int const_value = 999;
        for (const auto& segment_rows : data) {
            vectorized::Block block = tablet_schema->create_block();
            auto columns = block.mutate_columns();
            for (const auto& row : segment_rows) {
                columns[0]->insert_data(row.data(), row.size());
                columns[1]->insert_data(reinterpret_cast<const char*>(&const_value),
                                        sizeof(const_value));
            }
            ret.emplace_back(std::move(block));
        }
        return ret;
    }

    std::vector<std::vector<std::string>> get_expected_key_bounds(
            const std::vector<std::vector<std::string>>& data) {
        std::vector<std::vector<std::string>> ret;
        for (const auto& rows : data) {
            auto& cur = ret.emplace_back();
            auto min_key = rows.front();
            auto max_key = rows.front();
            for (const auto& row : rows) {
                if (row < min_key) {
                    min_key = row;
                }
                if (row > max_key) {
                    max_key = row;
                }
            }

            // segments key bounds have marker
            min_key = std::string {KEY_NORMAL_MARKER} + min_key;
            max_key = std::string {KEY_NORMAL_MARKER} + max_key;

            cur.emplace_back(do_trunacte(min_key));
            cur.emplace_back(do_trunacte(max_key));
        }
        return ret;
    }

    RowsetSharedPtr create_rowset(TabletSchemaSPtr tablet_schema, SegmentsOverlapPB overlap,
                                  const std::vector<vectorized::Block> blocks, int64_t version,
                                  bool is_vertical) {
        auto writer_context = create_rowset_writer_context(tablet_schema, overlap, UINT32_MAX,
                                                           {version, version});
        auto res = RowsetFactory::create_rowset_writer(*engine_ref, writer_context, is_vertical);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto rowset_writer = std::move(res).value();

        uint32_t num_rows = 0;
        for (const auto& block : blocks) {
            num_rows += block.rows();
            EXPECT_TRUE(rowset_writer->add_block(&block).ok());
            EXPECT_TRUE(rowset_writer->flush().ok());
        }

        RowsetSharedPtr rowset;
        EXPECT_EQ(Status::OK(), rowset_writer->build(rowset));
        EXPECT_EQ(blocks.size(), rowset->rowset_meta()->num_segments());
        EXPECT_EQ(num_rows, rowset->rowset_meta()->num_rows());
        return rowset;
    }

    std::string do_trunacte(std::string key) {
        if (segments_key_bounds_truncation_enabled()) {
            auto threshold = config::segments_key_bounds_truncation_threshold;
            if (key.size() > threshold) {
                key.resize(threshold);
            }
        }
        return key;
    }

    bool segments_key_bounds_truncation_enabled() {
        return (config::segments_key_bounds_truncation_threshold > 0);
    }

    void check_key_bounds(const std::vector<std::vector<std::string>>& data,
                          const std::vector<KeyBoundsPB>& segments_key_bounds) {
        // 1. check size
        for (const auto& segments_key_bound : segments_key_bounds) {
            const auto& min_key = segments_key_bound.min_key();
            const auto& max_key = segments_key_bound.max_key();

            if (segments_key_bounds_truncation_enabled()) {
                EXPECT_LE(min_key.size(), config::segments_key_bounds_truncation_threshold);
                EXPECT_LE(max_key.size(), config::segments_key_bounds_truncation_threshold);
            }
        }

        // 2. check content
        auto expected_key_bounds = get_expected_key_bounds(data);
        for (std::size_t i = 0; i < expected_key_bounds.size(); i++) {
            const auto& min_key = segments_key_bounds[i].min_key();
            const auto& max_key = segments_key_bounds[i].max_key();

            EXPECT_EQ(min_key, expected_key_bounds[i][0]);
            EXPECT_EQ(max_key, expected_key_bounds[i][1]);
            std::cout << fmt::format("min_key={}, size={}\nmax_key={}, size={}\n",
                                     hexdump(min_key.data(), min_key.size()), min_key.size(),
                                     hexdump(max_key.data(), max_key.size()), max_key.size());
        }
    }

    std::vector<RowsetSharedPtr> create_rowsets(TabletSchemaSPtr tablet_schema,
                                                const std::vector<std::vector<std::string>>& data,
                                                const std::vector<int64_t>& truncate_lengths = {}) {
        std::vector<RowsetSharedPtr> rowsets;
        for (size_t i {0}; i < data.size(); i++) {
            const auto rows = data[i];
            if (!truncate_lengths.empty()) {
                config::segments_key_bounds_truncation_threshold = truncate_lengths[i];
            }
            std::vector<std::vector<std::string>> rowset_data {rows};
            auto blocks = generate_blocks(tablet_schema, rowset_data);
            RowsetSharedPtr rowset =
                    create_rowset(tablet_schema, NONOVERLAPPING, blocks, cur_version++, false);

            std::vector<KeyBoundsPB> segments_key_bounds;
            rowset->rowset_meta()->get_segments_key_bounds(&segments_key_bounds);
            for (const auto& segments_key_bound : segments_key_bounds) {
                const auto& min_key = segments_key_bound.min_key();
                const auto& max_key = segments_key_bound.max_key();

                LOG(INFO) << fmt::format(
                        "\n==== rowset_id={}, segment_key_bounds_truncated={} ====\nmin_key={}, "
                        "size={}\nmax_key={}, size={}\n",
                        rowset->rowset_id().to_string(), rowset->is_segments_key_bounds_truncated(),
                        min_key, min_key.size(), max_key, max_key.size());
            }

            rowsets.push_back(rowset);
            RowsetReaderSharedPtr rs_reader;
            EXPECT_TRUE(rowset->create_reader(&rs_reader));
        }
        for (std::size_t i {0}; i < truncate_lengths.size(); i++) {
            EXPECT_EQ((truncate_lengths[i] > 0), rowsets[i]->is_segments_key_bounds_truncated());
        }
        return rowsets;
    }

    TabletReader::ReaderParams create_reader_params(
            TabletSchemaSPtr tablet_schema, const std::vector<std::vector<std::string>>& data,
            const std::vector<int64_t>& truncate_lengths = {}) {
        TabletReader::ReaderParams reader_params;
        std::vector<RowsetSharedPtr> rowsets =
                create_rowsets(tablet_schema, data, truncate_lengths);
        std::vector<RowSetSplits> rs_splits;
        for (size_t i {0}; i < rowsets.size(); i++) {
            RowsetReaderSharedPtr rs_reader;
            EXPECT_TRUE(rowsets[i]->create_reader(&rs_reader));
            RowSetSplits rs_split;
            rs_split.rs_reader = rs_reader;
            rs_splits.emplace_back(rs_split);
        }
        reader_params.rs_splits = std::move(rs_splits);
        return reader_params;
    }
};

TEST_F(SegmentsKeyBoundsTruncationTest, CompareFuncTest) {
    // test `Slice::lhs_is_strictly_less_than_rhs`
    // enumerating all possible combinations
    // this test is reduntant, n = 3 is enough
    constexpr int n = 8;
    std::vector<std::string> datas;
    for (int l = 1; l <= n; l++) {
        for (int x = 0; x < (1 << l); x++) {
            datas.emplace_back(fmt::format("{:0{width}b}", x, fmt::arg("width", l)));
        }
    }
    std::cout << "datas.size()=" << datas.size() << "\n";

    int count1 {0}, count2 {0}, total {0};
    for (size_t i = 0; i < datas.size(); i++) {
        for (size_t j = 0; j < datas.size(); j++) {
            Slice X {datas[i]};
            Slice Y {datas[j]};
            for (int l1 = 0; l1 <= n; l1++) {
                bool X_is_truncated = (l1 != 0);
                Slice a {X};
                if (X_is_truncated && X.get_size() >= l1) {
                    a.truncate(l1);
                }
                for (int l2 = 0; l2 <= n; l2++) {
                    bool Y_is_truncated = (l2 != 0);
                    Slice b {Y};
                    if (Y_is_truncated && Y.get_size() >= l2) {
                        b.truncate(l2);
                    }

                    bool res1 = Slice::lhs_is_strictly_less_than_rhs(a, X_is_truncated, b,
                                                                     Y_is_truncated);
                    bool res2 = (X.compare(Y) < 0);
                    ++total;
                    if (res1 && res2) {
                        ++count1;
                    }
                    if (res2) {
                        ++count2;
                    }
                    EXPECT_FALSE(res1 && !res2) << fmt::format(
                            "X={}, a={}, l1={}, Y={}, b={}, l2={}, res1={}, res2={}", X.to_string(),
                            a.to_string(), l1, Y.to_string(), b.to_string(), l2, res1, res2);
                }
            }
        }
    }
    std::cout << fmt::format("count1={}, count2={}, count1/count2={}, total={}\n", count1, count2,
                             double(count1) / count2, total);
}

TEST_F(SegmentsKeyBoundsTruncationTest, BasicTruncationTest) {
    {
        // 1. don't do segments key bounds truncation when the config is off
        config::segments_key_bounds_truncation_threshold = -1;

        auto tablet_schema = create_schema(100);
        std::vector<std::vector<std::string>> data {{std::string(2, 'x'), std::string(3, 'y')},
                                                    {std::string(4, 'a'), std::string(15, 'b')},
                                                    {std::string(18, 'c'), std::string(5, 'z')},
                                                    {std::string(20, '0'), std::string(22, '1')}};
        auto blocks = generate_blocks(tablet_schema, data);
        RowsetSharedPtr rowset = create_rowset(tablet_schema, NONOVERLAPPING, blocks, 2, false);

        auto rowset_meta = rowset->rowset_meta();
        EXPECT_EQ(false, rowset_meta->is_segments_key_bounds_truncated());
        std::vector<KeyBoundsPB> segments_key_bounds;
        rowset_meta->get_segments_key_bounds(&segments_key_bounds);
        EXPECT_EQ(segments_key_bounds.size(), data.size());
        check_key_bounds(data, segments_key_bounds);
    }

    {
        // 2. do segments key bounds truncation when the config is on
        config::segments_key_bounds_truncation_threshold = 10;

        auto tablet_schema = create_schema(100);
        std::vector<std::vector<std::string>> data {{std::string(2, 'x'), std::string(3, 'y')},
                                                    {std::string(4, 'a'), std::string(15, 'b')},
                                                    {std::string(18, 'c'), std::string(5, 'z')},
                                                    {std::string(20, '0'), std::string(22, '1')}};
        auto blocks = generate_blocks(tablet_schema, data);
        RowsetSharedPtr rowset = create_rowset(tablet_schema, NONOVERLAPPING, blocks, 2, false);

        auto rowset_meta = rowset->rowset_meta();
        EXPECT_EQ(true, rowset_meta->is_segments_key_bounds_truncated());
        std::vector<KeyBoundsPB> segments_key_bounds;
        rowset_meta->get_segments_key_bounds(&segments_key_bounds);
        EXPECT_EQ(segments_key_bounds.size(), data.size());
        check_key_bounds(data, segments_key_bounds);
    }

    {
        // 3. segments_key_bounds_truncated should be set to false if no actual truncation happend
        config::segments_key_bounds_truncation_threshold = 100;

        auto tablet_schema = create_schema(100);
        std::vector<std::vector<std::string>> data {{std::string(2, 'x'), std::string(3, 'y')},
                                                    {std::string(4, 'a'), std::string(15, 'b')},
                                                    {std::string(18, 'c'), std::string(5, 'z')},
                                                    {std::string(20, '0'), std::string(22, '1')}};
        auto blocks = generate_blocks(tablet_schema, data);
        RowsetSharedPtr rowset = create_rowset(tablet_schema, NONOVERLAPPING, blocks, 2, false);

        auto rowset_meta = rowset->rowset_meta();
        EXPECT_EQ(false, rowset_meta->is_segments_key_bounds_truncated());
    }
}

TEST_F(SegmentsKeyBoundsTruncationTest, BlockReaderJudgeFuncTest) {
    auto tablet_schema = create_schema(100);

    {
        // all rowsets are truncated to same size
        // keys are distinctable from any index
        std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbb"},
                                                    {"cccccc", "dddddd"},
                                                    {"eeeeeee", "fffffff"},
                                                    {"xxxxxxx", "yyyyyyyy"}};
        {
            disable_segments_key_bounds_truncation();
            TabletReader::ReaderParams read_params = create_reader_params(tablet_schema, data);
            vectorized::BlockReader block_reader;
            EXPECT_FALSE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }

        {
            config::segments_key_bounds_truncation_threshold = 3;
            TabletReader::ReaderParams read_params = create_reader_params(tablet_schema, data);
            vectorized::BlockReader block_reader;
            // can still determine that segments are non ascending after truncation
            EXPECT_FALSE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }
    }

    {
        // all rowsets are truncated to same size
        // keys are distinctable from any index before truncation
        // some keys are not comparable after truncation
        std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbb"},
                                                    {"cccccccccccc", "ccdddddddd"},
                                                    {"cceeeeeeee", "fffffff"},
                                                    {"xxxxxxx", "yyyyyyyy"}};
        {
            disable_segments_key_bounds_truncation();
            TabletReader::ReaderParams read_params = create_reader_params(tablet_schema, data);
            vectorized::BlockReader block_reader;
            EXPECT_FALSE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }

        {
            config::segments_key_bounds_truncation_threshold = 6;
            TabletReader::ReaderParams read_params = create_reader_params(tablet_schema, data);
            vectorized::BlockReader block_reader;
            EXPECT_FALSE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }

        {
            config::segments_key_bounds_truncation_threshold = 3;
            TabletReader::ReaderParams read_params = create_reader_params(tablet_schema, data);
            vectorized::BlockReader block_reader;
            // can not determine wether rowset 2 and rowset 3 are mono ascending
            EXPECT_TRUE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }
    }

    {
        // all rowsets are truncated to same size
        // keys are not mono ascending before truncation
        std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbb"},
                                                    {"bbbbb", "cccccccc"},
                                                    {"cccccccc", "xxxxxxx"},
                                                    {"xxxxxxx", "yyyyyyyy"}};
        {
            disable_segments_key_bounds_truncation();
            TabletReader::ReaderParams read_params = create_reader_params(tablet_schema, data);
            vectorized::BlockReader block_reader;
            EXPECT_TRUE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }

        {
            config::segments_key_bounds_truncation_threshold = 3;
            TabletReader::ReaderParams read_params = create_reader_params(tablet_schema, data);
            vectorized::BlockReader block_reader;
            EXPECT_TRUE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }
    }

    {
        // some rowsets are truncated, some are not
        std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbbccccccc"},
                                                    {"bbbbbbddddddd", "dddddd"}};
        {
            TabletReader::ReaderParams read_params =
                    create_reader_params(tablet_schema, data, {-1, 9});
            vectorized::BlockReader block_reader;
            EXPECT_FALSE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }

        {
            TabletReader::ReaderParams read_params =
                    create_reader_params(tablet_schema, data, {-1, 4});
            vectorized::BlockReader block_reader;
            EXPECT_TRUE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }

        {
            TabletReader::ReaderParams read_params =
                    create_reader_params(tablet_schema, data, {9, -1});
            vectorized::BlockReader block_reader;
            EXPECT_FALSE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }

        {
            TabletReader::ReaderParams read_params =
                    create_reader_params(tablet_schema, data, {4, -1});
            vectorized::BlockReader block_reader;
            EXPECT_TRUE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }
    }

    {
        // some rowsets are truncated, some are not, truncated lengths may be different
        {
            std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbbbb"},
                                                        {"ccccccccc", "dddddd"},
                                                        {"eeeeeee", "ffffffggggg"},
                                                        {"ffffffhhhhhh", "hhhhhhh"},
                                                        {"iiiiiiii", "jjjjjjjjj"}};
            TabletReader::ReaderParams read_params =
                    create_reader_params(tablet_schema, data, {4, 5, 4, -1, 6});
            vectorized::BlockReader block_reader;
            EXPECT_TRUE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }
        {
            std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbbbb"},
                                                        {"ccccccccc", "dddddd"},
                                                        {"eeeeeee", "ffffffggggg"},
                                                        {"ffffffhhhhhh", "hhhhhhh"},
                                                        {"iiiiiiii", "jjjjjjjjj"}};
            TabletReader::ReaderParams read_params =
                    create_reader_params(tablet_schema, data, {4, 5, 8, -1, 6});
            vectorized::BlockReader block_reader;
            EXPECT_FALSE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }

        {
            std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbbbb"},
                                                        {"ccccccccc", "dddddd"},
                                                        {"eeeeeee", "ffffffggggg"},
                                                        {"ffffffhhhhhh", "hhhhhhh"},
                                                        {"iiiiiiii", "jjjjjjjjj"}};
            TabletReader::ReaderParams read_params =
                    create_reader_params(tablet_schema, data, {4, 5, -1, 4, 6});
            vectorized::BlockReader block_reader;
            EXPECT_TRUE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }
        {
            std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbbbb"},
                                                        {"ccccccccc", "dddddd"},
                                                        {"eeeeeee", "ffffffggggg"},
                                                        {"ffffffhhhhhh", "hhhhhhh"},
                                                        {"iiiiiiii", "jjjjjjjjj"}};
            TabletReader::ReaderParams read_params =
                    create_reader_params(tablet_schema, data, {4, 5, -1, 8, 6});
            vectorized::BlockReader block_reader;
            EXPECT_FALSE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }

        {
            std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbbbb"},
                                                        {"ccccccccc", "dddddd"},
                                                        {"eeeeeee", "ffffffggggg"},
                                                        {"ffffffhhhhhh", "hhhhhhh"},
                                                        {"iiiiiiii", "jjjjjjjjj"}};
            TabletReader::ReaderParams read_params =
                    create_reader_params(tablet_schema, data, {4, 5, 8, 4, 6});
            vectorized::BlockReader block_reader;
            EXPECT_TRUE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }
        {
            std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbbbb"},
                                                        {"ccccccccc", "dddddd"},
                                                        {"eeeeeee", "ffffffggggg"},
                                                        {"ffffffhhhhhh", "hhhhhhh"},
                                                        {"iiiiiiii", "jjjjjjjjj"}};
            TabletReader::ReaderParams read_params =
                    create_reader_params(tablet_schema, data, {4, 5, 4, 8, 6});
            vectorized::BlockReader block_reader;
            EXPECT_TRUE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }
        {
            std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbbbb"},
                                                        {"ccccccccc", "dddddd"},
                                                        {"eeeeeee", "ffffffggggg"},
                                                        {"ffffffhhhhhh", "hhhhhhh"},
                                                        {"iiiiiiii", "jjjjjjjjj"}};
            TabletReader::ReaderParams read_params =
                    create_reader_params(tablet_schema, data, {4, 5, 8, 9, 6});
            vectorized::BlockReader block_reader;
            EXPECT_FALSE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }
        {
            std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbbbb"},
                                                        {"ccccccccc", "dddddd"},
                                                        {"eeeeeee", "ffffffggggg"},
                                                        {"ffffffhhhhhh", "hhhhhhh"},
                                                        {"iiiiiiii", "jjjjjjjjj"}};
            TabletReader::ReaderParams read_params =
                    create_reader_params(tablet_schema, data, {4, 5, 3, 4, 6});
            vectorized::BlockReader block_reader;
            EXPECT_TRUE(block_reader._rowsets_not_mono_asc_disjoint(read_params));
        }
    }
}

TEST_F(SegmentsKeyBoundsTruncationTest, OrderedCompactionTest) {
    auto tablet_schema = create_schema(100);
    config::enable_ordered_data_compaction = true;
    config::ordered_data_compaction_min_segment_size = 1;

    {
        disable_segments_key_bounds_truncation();
        TabletSharedPtr tablet = create_tablet(*tablet_schema, false);
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());
        std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbcccccc"},
                                                    {"bbbbbddddddd", "dddddd"},
                                                    {"eeeeeee", "fffffffff"},
                                                    {"gggggggg", "hhhhhhh"},
                                                    {"iiiiiiii", "jjjjjjjjj"}};
        auto input_rowsets = create_rowsets(tablet_schema, data);
        CumulativeCompaction cu_compaction(*engine_ref, tablet);
        cu_compaction._input_rowsets = std::move(input_rowsets);
        EXPECT_TRUE(cu_compaction.handle_ordered_data_compaction());
        EXPECT_EQ(cu_compaction._input_rowsets.size(), data.size());
    }

    {
        TabletSharedPtr tablet = create_tablet(*tablet_schema, false);
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());
        std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbcccccc"},
                                                    {"bbbbbddddddd", "dddddd"},
                                                    {"eeeeeee", "fffffffff"},
                                                    {"gggggggg", "hhhhhhh"},
                                                    {"iiiiiiii", "jjjjjjjjj"}};
        auto input_rowsets = create_rowsets(tablet_schema, data, {4, 4, 4, 4, 4});
        CumulativeCompaction cu_compaction(*engine_ref, tablet);
        cu_compaction._input_rowsets = std::move(input_rowsets);
        EXPECT_FALSE(cu_compaction.handle_ordered_data_compaction());
    }

    {
        TabletSharedPtr tablet = create_tablet(*tablet_schema, false);
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());
        std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbcccccc"},
                                                    {"bbbbbddddddd", "dddddd"},
                                                    {"eeeeeee", "fffffffff"},
                                                    {"gggggggg", "hhhhhhh"},
                                                    {"iiiiiiii", "jjjjjjjjj"}};
        auto input_rowsets = create_rowsets(tablet_schema, data, {4, 8, 4, 4, 4});
        CumulativeCompaction cu_compaction(*engine_ref, tablet);
        cu_compaction._input_rowsets = std::move(input_rowsets);
        EXPECT_FALSE(cu_compaction.handle_ordered_data_compaction());
    }

    {
        TabletSharedPtr tablet = create_tablet(*tablet_schema, false);
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());
        std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbcccccc"},
                                                    {"bbbbbddddddd", "dddddd"},
                                                    {"eeeeeee", "fffffffff"},
                                                    {"gggggggg", "hhhhhhh"},
                                                    {"iiiiiiii", "jjjjjjjjj"}};
        auto input_rowsets = create_rowsets(tablet_schema, data, {8, 4, 4, 4, 4});
        CumulativeCompaction cu_compaction(*engine_ref, tablet);
        cu_compaction._input_rowsets = std::move(input_rowsets);
        EXPECT_FALSE(cu_compaction.handle_ordered_data_compaction());
    }

    {
        TabletSharedPtr tablet = create_tablet(*tablet_schema, false);
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());
        std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbcccccc"},
                                                    {"bbbbbddddddd", "dddddd"},
                                                    {"eeeeeee", "fffffffff"},
                                                    {"gggggggg", "hhhhhhh"},
                                                    {"iiiiiiii", "jjjjjjjjj"}};
        auto input_rowsets = create_rowsets(tablet_schema, data, {8, 9, 4, 4, 4});
        CumulativeCompaction cu_compaction(*engine_ref, tablet);
        cu_compaction._input_rowsets = std::move(input_rowsets);
        EXPECT_TRUE(cu_compaction.handle_ordered_data_compaction());
        EXPECT_EQ(cu_compaction._input_rowsets.size(), data.size());
    }

    {
        TabletSharedPtr tablet = create_tablet(*tablet_schema, false);
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());
        std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbcccccc"},
                                                    {"bbbbbddddddd", "dddddd"},
                                                    {"eeeeeee", "fffffffff"},
                                                    {"gggggggg", "hhhhhhh"},
                                                    {"iiiiiiii", "jjjjjjjjj"}};
        auto input_rowsets = create_rowsets(tablet_schema, data, {8, -1, 4, 4, 4});
        CumulativeCompaction cu_compaction(*engine_ref, tablet);
        cu_compaction._input_rowsets = std::move(input_rowsets);
        EXPECT_TRUE(cu_compaction.handle_ordered_data_compaction());
        EXPECT_EQ(cu_compaction._input_rowsets.size(), data.size());
    }

    {
        TabletSharedPtr tablet = create_tablet(*tablet_schema, false);
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());
        std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbcccccc"},
                                                    {"bbbbbddddddd", "dddddd"},
                                                    {"eeeeeee", "fffffffff"},
                                                    {"gggggggg", "hhhhhhh"},
                                                    {"iiiiiiii", "jjjjjjjjj"}};
        auto input_rowsets = create_rowsets(tablet_schema, data, {-1, 9, 4, 4, 4});
        CumulativeCompaction cu_compaction(*engine_ref, tablet);
        cu_compaction._input_rowsets = std::move(input_rowsets);
        EXPECT_TRUE(cu_compaction.handle_ordered_data_compaction());
        EXPECT_EQ(cu_compaction._input_rowsets.size(), data.size());
    }

    {
        TabletSharedPtr tablet = create_tablet(*tablet_schema, false);
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());
        std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbcccccc"},
                                                    {"bbbbbddddddd", "dddddd"},
                                                    {"eeeeeee", "fffffffff"},
                                                    {"gggggggg", "hhhhhhh"},
                                                    {"iiiiiiii", "jjjjjjjjj"}};
        auto input_rowsets = create_rowsets(tablet_schema, data, {-1, 4, 4, 4, 4});
        CumulativeCompaction cu_compaction(*engine_ref, tablet);
        cu_compaction._input_rowsets = std::move(input_rowsets);
        EXPECT_FALSE(cu_compaction.handle_ordered_data_compaction());
    }

    {
        TabletSharedPtr tablet = create_tablet(*tablet_schema, false);
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());
        std::vector<std::vector<std::string>> data {{"aaaaaaaaa", "bbbbbcccccc"},
                                                    {"bbbbbddddddd", "dddddd"},
                                                    {"eeeeeee", "fffffffff"},
                                                    {"gggggggg", "hhhhhhh"},
                                                    {"iiiiiiii", "jjjjjjjjj"}};
        auto input_rowsets = create_rowsets(tablet_schema, data, {4, -1, 4, 4, 4});
        CumulativeCompaction cu_compaction(*engine_ref, tablet);
        cu_compaction._input_rowsets = std::move(input_rowsets);
        EXPECT_FALSE(cu_compaction.handle_ordered_data_compaction());
    }
}
} // namespace doris
