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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_common.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <tuple>
#include <unordered_map>

#include "common/status.h"
#include "cpp/sync_point.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "io/io_common.h"
#include "io/rate_limiter_singleton.h"
#include "json2pb/json_to_pb.h"
#include "olap/base_compaction.h"
#include "olap/compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/delete_handler.h"
#include "olap/merger.h"
#include "olap/options.h"
#include "olap/rowid_conversion.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "runtime/exec_env.h"
#include "util/uid_util.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"

namespace doris {
using namespace ErrorCode;

static const uint32_t MAX_PATH_LEN = 1024;
static StorageEngine* engine_ref = nullptr;

class RateLimiterTest : public testing::TestWithParam<std::tuple<KeysType, bool, bool, bool, int>> {
protected:
    void SetUp() override {
        config::max_runnings_transactions_per_txn_map = 500;
        config::tablet_map_shard_size = 1;
        config::txn_map_shard_size = 1;
        config::txn_shard_size = 1;
        config::min_file_descriptor_number = 1000;
        config::base_compaction_dup_key_max_file_size_mbytes = 1024;
        config::mow_base_compaction_max_compaction_score = 200;
        config::base_compaction_max_compaction_score = 20;

        rate_limit_use_byte_total =
                doris::io::RateLimiterSingleton::getInstance()->GetTotalBytesThrough();

        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        absolute_dir = std::string(buffer) + kTestDir;
        auto st = io::global_local_filesystem()->delete_directory(absolute_dir);
        ASSERT_TRUE(st.ok()) << st;
        st = io::global_local_filesystem()->create_directory(absolute_dir);
        ASSERT_TRUE(st.ok()) << st;
        EXPECT_TRUE(io::global_local_filesystem()
                            ->create_directory(absolute_dir + "/tablet_path")
                            .ok());
        EXPECT_TRUE(io::global_local_filesystem()
                            ->create_directory(absolute_dir + "/data/1/1/1/")
                            .ok());
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        engine_ref = engine.get();
        _data_dir = std::make_unique<DataDir>(*engine_ref, absolute_dir, 100000000);
        static_cast<void>(_data_dir->init());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
    }

    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(absolute_dir).ok());
        engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    TabletSchemaSPtr create_schema(KeysType keys_type = DUP_KEYS) {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(keys_type);
        tablet_schema_pb.set_num_short_key_columns(1);
        tablet_schema_pb.set_num_rows_per_row_block(1024);
        tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
        tablet_schema_pb.set_next_column_unique_id(4);

        ColumnPB* column_1 = tablet_schema_pb.add_column();
        column_1->set_unique_id(1);
        column_1->set_name("c1");
        column_1->set_type("INT");
        column_1->set_is_key(true);
        column_1->set_length(4);
        column_1->set_index_length(4);
        column_1->set_is_nullable(false);
        column_1->set_is_bf_column(false);

        ColumnPB* column_2 = tablet_schema_pb.add_column();
        column_2->set_unique_id(2);
        column_2->set_name("c2");
        column_2->set_type("INT");
        column_2->set_length(4);
        column_2->set_index_length(4);
        column_2->set_is_nullable(true);
        column_2->set_is_key(false);
        column_2->set_is_nullable(false);
        column_2->set_is_bf_column(false);

        // unique table must contains the DELETE_SIGN column
        if (keys_type == UNIQUE_KEYS) {
            ColumnPB* column_3 = tablet_schema_pb.add_column();
            column_3->set_unique_id(3);
            column_3->set_name(DELETE_SIGN);
            column_3->set_type("TINYINT");
            column_3->set_length(1);
            column_3->set_index_length(1);
            column_3->set_is_nullable(false);
            column_3->set_is_key(false);
            column_3->set_is_nullable(false);
            column_3->set_is_bf_column(false);
        }

        tablet_schema->init_from_pb(tablet_schema_pb);
        return tablet_schema;
    }

    RowsetWriterContext create_rowset_writer_context(TabletSchemaSPtr tablet_schema,
                                                     const SegmentsOverlapPB& overlap,
                                                     uint32_t max_rows_per_segment,
                                                     Version version) {
        // FIXME(plat1ko): If `inc_id` set to 1000, and run `VerticalCompactionTest` before `TestRowIdConversion`,
        //  will `TestRowIdConversion` will fail. There may be some strange global states here.
        static int64_t inc_id = 0;
        RowsetWriterContext rowset_writer_context;
        RowsetId rowset_id;
        rowset_id.init(inc_id);
        rowset_writer_context.rowset_id = rowset_id;
        rowset_writer_context.rowset_type = BETA_ROWSET;
        rowset_writer_context.rowset_state = VISIBLE;
        rowset_writer_context.tablet_schema = tablet_schema;
        rowset_writer_context.tablet_path = absolute_dir + "/tablet_path";
        rowset_writer_context.version = version;
        rowset_writer_context.segments_overlap = overlap;
        rowset_writer_context.max_rows_per_segment = max_rows_per_segment;
        inc_id++;
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

    RowsetSharedPtr create_rowset(
            TabletSchemaSPtr tablet_schema, const SegmentsOverlapPB& overlap,
            std::vector<std::vector<std::tuple<int64_t, int64_t>>> rowset_data, int64_t version,
            bool is_vertical_merger) {
        if (overlap == NONOVERLAPPING) {
            for (auto i = 1; i < rowset_data.size(); i++) {
                auto& last_seg_data = rowset_data[i - 1];
                auto& cur_seg_data = rowset_data[i];
                int64_t last_seg_max = std::get<0>(last_seg_data[last_seg_data.size() - 1]);
                int64_t cur_seg_min = std::get<0>(cur_seg_data[0]);
                EXPECT_LT(last_seg_max, cur_seg_min);
            }
        }
        auto writer_context = create_rowset_writer_context(tablet_schema, overlap, UINT32_MAX,
                                                           {version, version});
        auto res = RowsetFactory::create_rowset_writer(*engine_ref, writer_context,
                                                       is_vertical_merger);
        EXPECT_TRUE(res.has_value()) << res.error();
        auto rowset_writer = std::move(res).value();

        uint32_t num_rows = 0;
        for (int i = 0; i < rowset_data.size(); ++i) {
            vectorized::Block block = tablet_schema->create_block();
            auto columns = block.mutate_columns();
            for (int rid = 0; rid < rowset_data[i].size(); ++rid) {
                int32_t c1 = std::get<0>(rowset_data[i][rid]);
                int32_t c2 = std::get<1>(rowset_data[i][rid]);
                columns[0]->insert_data((const char*)&c1, sizeof(c1));
                columns[1]->insert_data((const char*)&c2, sizeof(c2));

                if (tablet_schema->keys_type() == UNIQUE_KEYS) {
                    uint8_t num = 0;
                    columns[2]->insert_data((const char*)&num, sizeof(num));
                }
                num_rows++;
            }
            auto s = rowset_writer->add_block(&block);
            EXPECT_TRUE(s.ok());
            s = rowset_writer->flush();
            EXPECT_TRUE(s.ok());
        }

        RowsetSharedPtr rowset;
        EXPECT_EQ(Status::OK(), rowset_writer->build(rowset));
        EXPECT_EQ(rowset_data.size(), rowset->rowset_meta()->num_segments());
        EXPECT_EQ(num_rows, rowset->rowset_meta()->num_rows());
        return rowset;
    }

    void init_rs_meta(RowsetMetaSharedPtr& rs_meta, int64_t start, int64_t end) {
        std::string json_rowset_meta = R"({
            "rowset_id": 540081,
            "tablet_id": 15673,
            "partition_id": 10000,
            "tablet_schema_hash": 567997577,
            "rowset_type": "BETA_ROWSET",
            "rowset_state": "VISIBLE",
            "empty": false
        })";
        RowsetMetaPB rowset_meta_pb;
        json2pb::JsonToProtoMessage(json_rowset_meta, &rowset_meta_pb);
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        rs_meta->init_from_pb(rowset_meta_pb);
    }

    RowsetSharedPtr create_delete_predicate(const TabletSchemaSPtr& schema,
                                            DeletePredicatePB del_pred, int64_t version) {
        RowsetMetaSharedPtr rsm(new RowsetMeta());
        init_rs_meta(rsm, version, version);
        RowsetId id;
        id.init(version);
        rsm->set_rowset_id(id);
        rsm->set_delete_predicate(std::move(del_pred));
        rsm->set_tablet_schema(schema);
        return std::make_shared<BetaRowset>(schema, rsm, "");
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
        }
        t_tablet_schema.__set_storage_type(TStorageType::COLUMN);
        t_tablet_schema.__set_columns(cols);
        TabletMetaSharedPtr tablet_meta(
                new TabletMeta(1, 1, 1, 1, 1, 1, t_tablet_schema, 1, col_ordinal_to_unique_id,
                               UniqueId(1, 2), TTabletType::TABLET_TYPE_DISK,
                               TCompressionType::LZ4F, 0, enable_unique_key_merge_on_write));

        TabletSharedPtr tablet(new Tablet(*engine_ref, tablet_meta, _data_dir.get(),
                                          CUMULATIVE_SIZE_BASED_POLICY));
        static_cast<void>(tablet->init());
        return tablet;
    }

    void check_rate_limiter(KeysType keys_type, bool enable_unique_key_merge_on_write,
                            uint32_t num_input_rowset, uint32_t num_segments,
                            uint32_t rows_per_segment, const SegmentsOverlapPB& overlap,
                            bool has_delete_handler, bool is_vertical_merger,
                            int compaction_flag = 0) {
        DorisMetrics::instance()->local_compaction_read_bytes_total->set_value(0);
        DorisMetrics::instance()->local_compaction_write_bytes_total->set_value(0);
        DorisMetrics::instance()->real_read_byte_local_total->set_value(0);

        if (is_vertical_merger) {
            config::enable_vertical_compaction = true;
        } else {
            config::enable_vertical_compaction = false;
        }

        TabletSchemaSPtr tablet_schema = create_schema(keys_type);
        TabletSharedPtr tablet = create_tablet(*tablet_schema, enable_unique_key_merge_on_write);

        // generate input data
        std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>> input_data;
        generate_input_data(10, num_segments, rows_per_segment, overlap, input_data);

        // create input rowset
        SegmentsOverlapPB new_overlap = overlap;
        for (auto i = 0; i < 10; i++) {
            if (overlap == OVERLAP_UNKNOWN) {
                if (i == 0) {
                    new_overlap = NONOVERLAPPING;
                } else {
                    new_overlap = OVERLAPPING;
                }
            }
            RowsetSharedPtr rowset =
                    create_rowset(tablet_schema, new_overlap, input_data[i], i, is_vertical_merger);
            tablet->_rs_version_map.emplace(rowset->version(), rowset);
        }
        if (has_delete_handler) {
            // delete data with key < 1000
            std::vector<TCondition> conditions;
            TCondition condition;
            condition.column_name = tablet_schema->column(0).name();
            condition.condition_op = "<";
            condition.condition_values.clear();
            condition.condition_values.push_back("1000");
            conditions.push_back(condition);

            DeletePredicatePB del_pred;
            Status st =
                    DeleteHandler::generate_delete_predicate(*tablet_schema, conditions, &del_pred);
            ASSERT_TRUE(st.ok()) << st;
            RowsetSharedPtr rowset =
                    create_delete_predicate(tablet_schema, del_pred, num_input_rowset);
            tablet->_rs_version_map.emplace(rowset->version(), rowset);
        }

        if (compaction_flag == 0) {
            tablet->_cumulative_point = 11;
            BaseCompaction compaction(*engine_ref, tablet);
            auto st = compaction.prepare_compact();
            EXPECT_TRUE(st.ok());
            st = compaction.execute_compact();
            EXPECT_TRUE(st.ok());
        } else if (compaction_flag == 1) {
            tablet->_cumulative_point = 2;
            CumulativeCompaction compaction(*engine_ref, tablet);
            auto st = compaction.prepare_compact();
            EXPECT_TRUE(st.ok());
            st = compaction.execute_compact();
            EXPECT_TRUE(st.ok());
        }

        EXPECT_EQ(DorisMetrics::instance()->local_compaction_write_bytes_total->value() +
                          DorisMetrics::instance()->real_read_byte_local_total->value(),
                  doris::io::RateLimiterSingleton::getInstance()->GetTotalBytesThrough() -
                          rate_limit_use_byte_total);
        rate_limit_use_byte_total =
                doris::io::RateLimiterSingleton::getInstance()->GetTotalBytesThrough();
    }
    // if overlap == NONOVERLAPPING, all rowsets are non overlapping;
    // if overlap == OVERLAPPING, all rowsets are overlapping;
    // if overlap == OVERLAP_UNKNOWN, the first rowset is non overlapping, the
    // others are overlaping.
    void generate_input_data(
            uint32_t num_input_rowset, uint32_t num_segments, uint32_t rows_per_segment,
            const SegmentsOverlapPB& overlap,
            std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>>& input_data) {
        EXPECT_GE(rows_per_segment, 10);
        EXPECT_GE(num_segments * rows_per_segment, 500);
        bool is_overlap = false;
        for (auto i = 0; i < num_input_rowset; i++) {
            if (overlap == OVERLAPPING) {
                is_overlap = true;
            } else if (overlap == NONOVERLAPPING) {
                is_overlap = false;
            } else {
                if (i == 0) {
                    is_overlap = false;
                } else {
                    is_overlap = true;
                }
            }
            std::vector<std::vector<std::tuple<int64_t, int64_t>>> rowset_data;
            for (auto j = 0; j < num_segments; j++) {
                std::vector<std::tuple<int64_t, int64_t>> segment_data;
                for (auto n = 0; n < rows_per_segment; n++) {
                    int64_t c1 = j * rows_per_segment + n;
                    // There are 500 rows of data overlap between rowsets
                    if (i > 0) {
                        if (is_overlap) {
                            // There are 500 rows of data overlap between rowsets
                            c1 -= 500;
                        } else {
                            ++c1;
                        }
                    }
                    if (is_overlap && j > 0) {
                        // There are 10 rows of data overlap between segments
                        c1 += j * rows_per_segment - 10;
                    }
                    int64_t c2 = c1 + 1;
                    segment_data.emplace_back(c1, c2);
                }
                rowset_data.emplace_back(segment_data);
            }
            input_data.emplace_back(rowset_data);
        }
    }

private:
    const std::string kTestDir = "/ut_dir/rate";
    std::string absolute_dir;
    std::unique_ptr<DataDir> _data_dir;
    size_t rate_limit_use_byte_total;
};

INSTANTIATE_TEST_SUITE_P(
        TestRateLimiterMultiParameters, RateLimiterTest,
        ::testing::ValuesIn(std::vector<std::tuple<KeysType, bool, bool, bool, int>> {
                // Parameters: data_type, enable_unique_key_merge_on_write, has_delete_handler, is_vertical_merger
                {DUP_KEYS, false, false, false, 0},    {DUP_KEYS, false, false, false, 1},
                {UNIQUE_KEYS, false, false, false, 0}, {UNIQUE_KEYS, false, false, false, 1},
                {UNIQUE_KEYS, true, false, false, 0},  {UNIQUE_KEYS, true, false, false, 1},
                {DUP_KEYS, false, true, false, 0},     {DUP_KEYS, false, true, false, 1},
                {UNIQUE_KEYS, false, true, false, 0},  {UNIQUE_KEYS, false, true, false, 1},
                {UNIQUE_KEYS, true, true, false, 0},   {UNIQUE_KEYS, true, true, false, 1},
                {UNIQUE_KEYS, false, false, true, 0},  {UNIQUE_KEYS, false, false, true, 1},
                {UNIQUE_KEYS, true, false, true, 0},   {UNIQUE_KEYS, true, false, true, 1},
                {DUP_KEYS, false, true, true, 0},      {DUP_KEYS, false, true, true, 1},
                {UNIQUE_KEYS, false, true, true, 0},   {UNIQUE_KEYS, false, true, true, 1},
                {UNIQUE_KEYS, true, true, true, 0},    {UNIQUE_KEYS, true, true, true, 1}}));

TEST_P(RateLimiterTest, RateTest) {
    auto [keys_type, enable_unique_key_merge_on_write, has_delete_handler, is_vertical_merger,
          compaction_flag] = GetParam();

    // if num_input_rowset = 2, VCollectIterator::Level1Iterator::_merge = flase
    // if num_input_rowset = 3, VCollectIterator::Level1Iterator::_merge = true
    for (auto num_input_rowset = 2; num_input_rowset <= 3; num_input_rowset++) {
        uint32_t rows_per_segment = 4567;
        //         // RowsetReader: SegmentIterator
        {
            uint32_t num_segments = 1;
            SegmentsOverlapPB overlap = NONOVERLAPPING;
            check_rate_limiter(keys_type, enable_unique_key_merge_on_write, num_input_rowset,
                               num_segments, rows_per_segment, overlap, has_delete_handler,
                               is_vertical_merger, compaction_flag);
        }
        // RowsetReader: VMergeIterator
        {
            uint32_t num_segments = 2;
            SegmentsOverlapPB overlap = OVERLAPPING;
            check_rate_limiter(keys_type, enable_unique_key_merge_on_write, num_input_rowset,
                               num_segments, rows_per_segment, overlap, has_delete_handler,
                               is_vertical_merger);
        }
        // RowsetReader: VUnionIterator
        {
            uint32_t num_segments = 2;
            SegmentsOverlapPB overlap = NONOVERLAPPING;
            check_rate_limiter(keys_type, enable_unique_key_merge_on_write, num_input_rowset,
                               num_segments, rows_per_segment, overlap, has_delete_handler,
                               is_vertical_merger);
        }
        // RowsetReader: VUnionIterator + VMergeIterator
        {
            uint32_t num_segments = 2;
            SegmentsOverlapPB overlap = OVERLAP_UNKNOWN;
            check_rate_limiter(keys_type, enable_unique_key_merge_on_write, num_input_rowset,
                               num_segments, rows_per_segment, overlap, has_delete_handler,
                               is_vertical_merger);
        }
    }
}

} // namespace doris
