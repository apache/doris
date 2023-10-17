
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
#include <glog/logging.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>
#include <unistd.h>

#include <memory>
#include <ostream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gtest/gtest_pred_impl.h"
#include "gutil/stringprintf.h"
#include "io/fs/local_file_system.h"
#include "olap/cumulative_compaction.h"
#include "olap/data_dir.h"
#include "olap/delete_handler.h"
#include "olap/field.h"
#include "olap/olap_common.h"
#include "olap/options.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/schema.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "runtime/exec_env.h"
#include "util/uid_util.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"

namespace doris {
using namespace ErrorCode;
namespace vectorized {

static const uint32_t MAX_PATH_LEN = 1024;
static StorageEngine* k_engine = nullptr;

class OrderedDataCompactionTest : public ::testing::Test {
protected:
    void SetUp() override {
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        absolute_dir = std::string(buffer) + kTestDir;
        EXPECT_TRUE(io::global_local_filesystem()->delete_and_create_directory(absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()
                            ->create_directory(absolute_dir + "/tablet_path")
                            .ok());

        _data_dir = std::make_unique<DataDir>(absolute_dir);
        static_cast<void>(_data_dir->update_capacity());
        doris::EngineOptions options;
        k_engine = new StorageEngine(options);
        ExecEnv::GetInstance()->set_storage_engine(k_engine);
        config::enable_ordered_data_compaction = true;
        config::ordered_data_compaction_min_segment_size = 10;
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(absolute_dir).ok());
        if (k_engine != nullptr) {
            k_engine->stop();
            delete k_engine;
            k_engine = nullptr;
            ExecEnv::GetInstance()->set_storage_engine(nullptr);
        }
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

    TabletSchemaSPtr create_agg_schema() {
        TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
        TabletSchemaPB tablet_schema_pb;
        tablet_schema_pb.set_keys_type(KeysType::AGG_KEYS);
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
        column_2->set_aggregation("SUM");

        tablet_schema->init_from_pb(tablet_schema_pb);
        return tablet_schema;
    }

    void create_rowset_writer_context(TabletSchemaSPtr tablet_schema, const std::string& rowset_dir,
                                      const SegmentsOverlapPB& overlap,
                                      uint32_t max_rows_per_segment,
                                      RowsetWriterContext* rowset_writer_context) {
        static int64_t inc_id = 1000;
        RowsetId rowset_id;
        rowset_id.init(inc_id);
        rowset_writer_context->rowset_id = rowset_id;
        rowset_writer_context->rowset_type = BETA_ROWSET;
        rowset_writer_context->data_dir = _data_dir.get();
        rowset_writer_context->rowset_state = VISIBLE;
        rowset_writer_context->tablet_schema = tablet_schema;
        rowset_writer_context->rowset_dir = rowset_dir;
        rowset_writer_context->version = Version(inc_id, inc_id);
        rowset_writer_context->segments_overlap = overlap;
        rowset_writer_context->max_rows_per_segment = max_rows_per_segment;
        inc_id++;
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
            TabletSchemaSPtr tablet_schema, TabletSharedPtr tablet,
            const SegmentsOverlapPB& overlap,
            std::vector<std::vector<std::tuple<int64_t, int64_t>>> rowset_data) {
        RowsetWriterContext writer_context;
        if (overlap == NONOVERLAPPING) {
            for (auto i = 1; i < rowset_data.size(); i++) {
                auto& last_seg_data = rowset_data[i - 1];
                auto& cur_seg_data = rowset_data[i];
                int64_t last_seg_max = std::get<0>(last_seg_data[last_seg_data.size() - 1]);
                int64_t cur_seg_min = std::get<0>(cur_seg_data[0]);
                EXPECT_LT(last_seg_max, cur_seg_min);
            }
        }
        create_rowset_writer_context(tablet_schema, tablet->tablet_path(), overlap, UINT32_MAX,
                                     &writer_context);

        std::unique_ptr<RowsetWriter> rowset_writer;
        Status s = RowsetFactory::create_rowset_writer(writer_context, true, &rowset_writer);
        EXPECT_TRUE(s.ok());

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
            s = rowset_writer->add_block(&block);
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

    void init_rs_meta(RowsetMetaSharedPtr& pb1, int64_t start, int64_t end) {
        std::string json_rowset_meta = R"({
            "rowset_id": 540085,
            "tablet_id": 15674,
            "txn_id": 4045,
            "tablet_schema_hash": 567997588,
            "rowset_type": "BETA_ROWSET",
            "rowset_state": "VISIBLE",
            "start_version": 2,
            "end_version": 2,
            "num_rows": 3929,
            "total_disk_size": 84699,
            "data_disk_size": 84464,
            "index_disk_size": 235,
            "empty": false,
            "load_id": {
                "hi": -5350970832824939812,
                "lo": -6717994719194512122
            },
            "creation_time": 1553765670
        })";

        RowsetMetaPB rowset_meta_pb;
        json2pb::JsonToProtoMessage(json_rowset_meta, &rowset_meta_pb);
        rowset_meta_pb.set_start_version(start);
        rowset_meta_pb.set_end_version(end);
        rowset_meta_pb.set_creation_time(10000);

        pb1->init_from_pb(rowset_meta_pb);
    }

    void add_delete_predicate(TabletSharedPtr tablet, DeletePredicatePB& del_pred,
                              int64_t version) {
        RowsetMetaSharedPtr rsm(new RowsetMeta());
        init_rs_meta(rsm, version, version);
        RowsetId id;
        id.init(version * 1000);
        rsm->set_rowset_id(id);
        rsm->set_delete_predicate(del_pred);
        rsm->set_tablet_schema(tablet->tablet_schema());
        RowsetSharedPtr rowset = std::make_shared<BetaRowset>(tablet->tablet_schema(), "", rsm);
        static_cast<void>(tablet->add_rowset(rowset));
    }

    TabletSharedPtr create_tablet(const TabletSchema& tablet_schema,
                                  bool enable_unique_key_merge_on_write, int64_t version,
                                  bool has_delete_handler) {
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
        TabletMetaSharedPtr tablet_meta(
                new TabletMeta(2, 2, 2, 2, 2, 2, t_tablet_schema, 2, col_ordinal_to_unique_id,
                               UniqueId(1, 2), TTabletType::TABLET_TYPE_DISK,
                               TCompressionType::LZ4F, 0, enable_unique_key_merge_on_write));

        TabletSharedPtr tablet(new Tablet(tablet_meta, _data_dir.get()));
        static_cast<void>(tablet->init());
        if (has_delete_handler) {
            // delete data with key < 1000
            std::vector<TCondition> conditions;
            TCondition condition;
            condition.column_name = tablet_schema.column(0).name();
            condition.condition_op = "<";
            condition.condition_values.clear();
            condition.condition_values.push_back("100");
            conditions.push_back(condition);

            DeletePredicatePB del_pred;
            Status st =
                    DeleteHandler::generate_delete_predicate(tablet_schema, conditions, &del_pred);
            EXPECT_EQ(Status::OK(), st);
            add_delete_predicate(tablet, del_pred, version);
        }
        return tablet;
    }

    // all rowset's data are non overlappint
    void generate_input_data(
            uint32_t num_input_rowset, uint32_t num_segments, uint32_t rows_per_segment,
            std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>>& input_data) {
        static int data = 0;
        for (auto i = 0; i < num_input_rowset; i++) {
            std::vector<std::vector<std::tuple<int64_t, int64_t>>> rowset_data;
            for (auto j = 0; j < num_segments; j++) {
                std::vector<std::tuple<int64_t, int64_t>> segment_data;
                for (auto n = 0; n < rows_per_segment; n++) {
                    int64_t c1 = data;
                    int64_t c2 = data + 1;
                    ++data;
                    segment_data.emplace_back(c1, c2);
                }
                rowset_data.emplace_back(segment_data);
            }
            input_data.emplace_back(rowset_data);
        }
    }

    void block_create(TabletSchemaSPtr tablet_schema, vectorized::Block* block) {
        block->clear();
        Schema schema(tablet_schema);
        const auto& column_ids = schema.column_ids();
        for (size_t i = 0; i < schema.num_column_ids(); ++i) {
            auto column_desc = schema.column(column_ids[i]);
            auto data_type = Schema::get_data_type_ptr(*column_desc);
            EXPECT_TRUE(data_type != nullptr);
            auto column = data_type->create_column();
            block->insert(vectorized::ColumnWithTypeAndName(std::move(column), data_type,
                                                            column_desc->name()));
        }
    }

private:
    const std::string kTestDir = "/ut_dir/vertical_compaction_test";
    string absolute_dir;
    std::unique_ptr<DataDir> _data_dir;
};

TEST_F(OrderedDataCompactionTest, test_01) {
    auto num_input_rowset = 5;
    auto num_segments = 2;
    auto rows_per_segment = 100;
    std::vector<std::vector<std::vector<std::tuple<int64_t, int64_t>>>> input_data;
    generate_input_data(num_input_rowset, num_segments, rows_per_segment, input_data);
    for (auto rs_id = 0; rs_id < input_data.size(); rs_id++) {
        for (auto s_id = 0; s_id < input_data[rs_id].size(); s_id++) {
            for (auto row_id = 0; row_id < input_data[rs_id][s_id].size(); row_id++) {
                LOG(INFO) << "input data: " << std::get<0>(input_data[rs_id][s_id][row_id]) << " "
                          << std::get<1>(input_data[rs_id][s_id][row_id]);
            }
        }
    }

    TabletSchemaSPtr tablet_schema = create_schema();
    TabletSharedPtr tablet = create_tablet(*tablet_schema, false, 10000, false);
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(tablet->tablet_path()).ok());
    // create input rowset
    vector<RowsetSharedPtr> input_rowsets;
    SegmentsOverlapPB new_overlap = NONOVERLAPPING;
    for (auto i = 0; i < num_input_rowset; i++) {
        RowsetSharedPtr rowset = create_rowset(tablet_schema, tablet, new_overlap, input_data[i]);
        input_rowsets.push_back(rowset);
    }
    //auto end_version = input_rowsets.back()->end_version();
    CumulativeCompaction cu_compaction(tablet);
    cu_compaction.set_input_rowset(input_rowsets);
    EXPECT_EQ(cu_compaction.handle_ordered_data_compaction(), true);
    for (int i = 0; i < 100; ++i) {
        LOG(INFO) << "stop";
    }

    RowsetSharedPtr out_rowset = cu_compaction.output_rowset();

    // create output rowset reader
    RowsetReaderContext reader_context;
    reader_context.tablet_schema = tablet_schema;
    reader_context.need_ordered_result = false;
    std::vector<uint32_t> return_columns = {0, 1};
    reader_context.return_columns = &return_columns;
    RowsetReaderSharedPtr output_rs_reader;
    LOG(INFO) << "create rowset reader in test";
    create_and_init_rowset_reader(out_rowset.get(), reader_context, &output_rs_reader);

    // read output rowset data
    vectorized::Block output_block;
    std::vector<std::tuple<int64_t, int64_t>> output_data;
    Status s = Status::OK();
    do {
        block_create(tablet_schema, &output_block);
        s = output_rs_reader->next_block(&output_block);
        auto columns = output_block.get_columns_with_type_and_name();
        EXPECT_EQ(columns.size(), 2);
        for (auto i = 0; i < output_block.rows(); i++) {
            output_data.emplace_back(columns[0].column->get_int(i), columns[1].column->get_int(i));
        }
    } while (s == Status::OK());
    EXPECT_EQ(Status::Error<END_OF_FILE>(""), s);
    EXPECT_EQ(out_rowset->rowset_meta()->num_rows(), output_data.size());
    EXPECT_EQ(output_data.size(), num_input_rowset * num_segments * rows_per_segment);
    std::vector<uint32_t> segment_num_rows;
    EXPECT_TRUE(output_rs_reader->get_segment_num_rows(&segment_num_rows).ok());
    // check vertical compaction result
    for (auto id = 0; id < output_data.size(); id++) {
        LOG(INFO) << "output data: " << std::get<0>(output_data[id]) << " "
                  << std::get<1>(output_data[id]);
    }
    int dst_id = 0;
    for (auto rs_id = 0; rs_id < input_data.size(); rs_id++) {
        for (auto s_id = 0; s_id < input_data[rs_id].size(); s_id++) {
            for (auto row_id = 0; row_id < input_data[rs_id][s_id].size(); row_id++) {
                LOG(INFO) << "input data: " << std::get<0>(input_data[rs_id][s_id][row_id]) << " "
                          << std::get<1>(input_data[rs_id][s_id][row_id]);
                EXPECT_EQ(std::get<0>(input_data[rs_id][s_id][row_id]),
                          std::get<0>(output_data[dst_id]));
                EXPECT_EQ(std::get<1>(input_data[rs_id][s_id][row_id]),
                          std::get<1>(output_data[dst_id]));
                dst_id++;
            }
        }
    }
}

} // namespace vectorized
} // namespace doris
