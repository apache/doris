// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.
//
// #include <gtest/gtest.h>
//
// #include "olap/cumulative_compaction.h"
// #include "olap/rowset/beta_rowset_writer.h"
// #include "olap/rowset/rowset_factory.h"
// #include "olap/storage_engine.h"
//
// namespace doris {
//
// using namespace doris::vectorized;
//
// constexpr static uint32_t MAX_PATH_LEN = 1024;
// constexpr static std::string_view dest_dir = "./ut_dir/variant_index_test";
// constexpr static std::string_view tmp_dir = "./ut_dir/tmp";
// static int64_t inc_id = 1000;
//
// struct DataRow {
//     std::string value;
// };
//
// class VariantCompactionTest : public ::testing::Test {
// protected:
//     void SetUp() override {
//         // absolute dir
//         char buffer[MAX_PATH_LEN];
//         EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
//         _curreent_dir = std::string(buffer);
//         _absolute_dir = _curreent_dir + std::string(dest_dir);
//         EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
//         EXPECT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());
//
//         // tmp dir
//         EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
//         EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp_dir).ok());
//         std::vector<StorePath> paths;
//         paths.emplace_back(std::string(tmp_dir), 1024000000);
//         auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
//         EXPECT_TRUE(tmp_file_dirs->init().ok());
//         ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));
//
//         // storage engine
//         doris::EngineOptions options;
//         auto engine = std::make_unique<StorageEngine>(options);
//         _engine_ref = engine.get();
//         _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
//         static_cast<void>(_data_dir->update_capacity());
//         ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
//
//         // tablet_schema
//         TabletSchemaPB schema_pb;
//         schema_pb.set_keys_type(KeysType::DUP_KEYS);
//         schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);
//
//         construct_column(schema_pb.add_column(), nullptr, 10000, "", 0, "INT", "key");
//         construct_column(schema_pb.add_column(), nullptr, 10003, "", 1, "VARIANT", "value");
//
//         _tablet_schema.reset(new TabletSchema);
//         _tablet_schema->init_from_pb(schema_pb);
//
//         // tablet
//         TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));
//
//         _tablet.reset(new Tablet(*_engine_ref, tablet_meta, _data_dir.get()));
//         EXPECT_TRUE(_tablet->init().ok());
//     }
//     void TearDown() override {
//         EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
//         EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
//         EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
//         _engine_ref = nullptr;
//         ExecEnv::GetInstance()->set_storage_engine(nullptr);
//     }
//
//     void construct_column(ColumnPB* column_pb, TabletIndexPB* tablet_index, int64_t index_id,
//                           const std::string& index_name, int32_t col_unique_id,
//                           const std::string& column_type, const std::string& column_name) {
//         column_pb->set_unique_id(col_unique_id);
//         column_pb->set_name(column_name);
//         column_pb->set_type(column_type);
//         column_pb->set_is_key(false);
//         column_pb->set_is_nullable(true);
//         if (tablet_index) {
//             tablet_index->set_index_id(index_id);
//             tablet_index->set_index_name(index_name);
//             tablet_index->set_index_type(IndexType::INVERTED);
//             tablet_index->add_col_unique_id(col_unique_id);
//         }
//     }
//
//     RowsetWriterContext rowset_writer_context() {
//         RowsetWriterContext context;
//         RowsetId rowset_id;
//         rowset_id.init(inc_id);
//         context.rowset_id = rowset_id;
//         context.rowset_type = BETA_ROWSET;
//         context.data_dir = _data_dir.get();
//         context.rowset_state = VISIBLE;
//         context.tablet_schema = _tablet_schema;
//         context.tablet_path = _tablet->tablet_path();
//         context.version = Version(inc_id, inc_id);
//         context.max_rows_per_segment = 200;
//         inc_id++;
//         return context;
//     }
//
//     VariantCompactionTest() = default;
//     ~VariantCompactionTest() override = default;
//
// private:
//     TabletSchemaSPtr _tablet_schema = nullptr;
//     StorageEngine* _engine_ref = nullptr;
//     std::unique_ptr<DataDir> _data_dir = nullptr;
//     TabletSharedPtr _tablet = nullptr;
//     std::string _absolute_dir;
//     std::string _curreent_dir;
// };
//
// std::vector<DataRow> read_data(const std::string file_name) {
//     std::ifstream file(file_name);
//     EXPECT_TRUE(file.is_open());
//
//     std::string line;
//     std::vector<DataRow> data;
//
//     while (std::getline(file, line)) {
//         DataRow row;
//         row.value = line;
//         data.emplace_back(std::move(row));
//     }
//
//     file.close();
//     return data;
// }
//
// TEST_F(VariantCompactionTest, write_index_test) {
//     EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
//     EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
//     // dense -> sparse
//     // sparse -> dense
//     std::string data_file1 =
//             _curreent_dir + "/be/test/olap/rowset/segment_v2/variant/data/variant1.json";
//     std::string data_file2 =
//             _curreent_dir + "/be/test/olap/rowset/segment_v2/variant/data/variant2.json";
//
//     std::vector<std::vector<DataRow>> data;
//     data.emplace_back(read_data(data_file1));
//     data.emplace_back(read_data(data_file2));
//
//     std::vector<RowsetSharedPtr> rowsets(data.size());
//     for (int i = 0; i < data.size(); i++) {
//         const auto& res =
//                 RowsetFactory::create_rowset_writer(*_engine_ref, rowset_writer_context(), false);
//         EXPECT_TRUE(res.has_value()) << res.error();
//         const auto& rowset_writer = res.value();
//
//         Block block = _tablet_schema->create_block();
//         auto columns = block.mutate_columns();
//         int rowid = 0;
//         for (const auto& row : data[i]) {
//             vectorized::Field k(rowid++);
//             vectorized::Field v(row.value);
//             columns[0]->insert(k);
//             columns[1]->insert(v);
//         }
//         EXPECT_TRUE(rowset_writer->add_block(&block).ok());
//         EXPECT_TRUE(rowset_writer->flush().ok());
//         // const auto& dst_writer = dynamic_cast<BaseBetaRowsetWriter*>(rowset_writer.get());
//
//         EXPECT_TRUE(rowset_writer->build(rowsets[i]).ok());
//         EXPECT_TRUE(_tablet->add_rowset(rowsets[i]).ok());
//         EXPECT_TRUE(rowsets[i]->num_segments() == 5);
//
//         // check segment meta info
//     }
//
//     CumulativeCompaction compaction(*_engine_ref, _tablet);
//     compaction._input_rowsets = std::move(rowsets);
//     Status st = compaction.build_basic_info();
//     EXPECT_TRUE(st.ok()) << st.to_string();
//
//     std::vector<RowsetReaderSharedPtr> input_rs_readers;
//     input_rs_readers.reserve(compaction._input_rowsets.size());
//     for (auto& rowset : compaction._input_rowsets) {
//         RowsetReaderSharedPtr rs_reader;
//         EXPECT_TRUE(rowset->create_reader(&rs_reader).ok());
//         input_rs_readers.push_back(std::move(rs_reader));
//     }
//
//     RowsetWriterContext ctx;
//     EXPECT_TRUE(compaction.construct_output_rowset_writer(ctx).ok());
//
//     compaction._stats.rowid_conversion = compaction._rowid_conversion.get();
//     EXPECT_TRUE(Merger::vertical_merge_rowsets(_tablet, compaction.compaction_type(),
//                                                *(compaction._cur_tablet_schema), input_rs_readers,
//                                                compaction._output_rs_writer.get(), 100000, 5,
//                                                &compaction._stats)
//                         .ok());
//     const auto& dst_writer =
//             dynamic_cast<BaseBetaRowsetWriter*>(compaction._output_rs_writer.get());
//
//     st = compaction._output_rs_writer->build(compaction._output_rowset);
//     EXPECT_TRUE(st.ok()) << st.to_string();
//
//     EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);
//
//     const auto& output_rowset = compaction._output_rowset;
//
//     // check segment file
//     for (int seg_id = 0; seg_id < output_rowset->num_segments(); seg_id++) {
//         // meta
//     }
// }
//
// } // namespace doris
//