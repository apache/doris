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

#include <gtest/gtest.h>

#include "olap/cumulative_compaction.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_factory.h"
#include "olap/rowset/segment_v2/inverted_index_file_reader.h"
#include "olap/storage_engine.h"

namespace doris {

using namespace doris::vectorized;

constexpr static uint32_t MAX_PATH_LEN = 1024;
constexpr static std::string_view dest_dir = "./ut_dir/inverted_index_test";
constexpr static std::string_view tmp_dir = "./ut_dir/tmp";
static int64_t inc_id = 1000;

struct DataRow {
    int key;
    std::string word;
    std::string url;
    int num;
};

class IndexCompactionTest : public ::testing::Test {
protected:
    void SetUp() override {
        // absolute dir
        char buffer[MAX_PATH_LEN];
        EXPECT_NE(getcwd(buffer, MAX_PATH_LEN), nullptr);
        _curreent_dir = std::string(buffer);
        _absolute_dir = _curreent_dir + std::string(dest_dir);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(_absolute_dir).ok());

        // tmp dir
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->create_directory(tmp_dir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(std::string(tmp_dir), 1024000000);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        EXPECT_TRUE(tmp_file_dirs->init().ok());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        // storage engine
        doris::EngineOptions options;
        auto engine = std::make_unique<StorageEngine>(options);
        _engine_ref = engine.get();
        _data_dir = std::make_unique<DataDir>(*_engine_ref, _absolute_dir);
        static_cast<void>(_data_dir->update_capacity());
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));

        // tablet_schema
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(KeysType::DUP_KEYS);
        schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V2);

        construct_column(schema_pb.add_column(), schema_pb.add_index(), 10000, "key_index", 0,
                         "INT", "key");
        construct_column(schema_pb.add_column(), schema_pb.add_index(), 10001, "v1_index", 1,
                         "STRING", "v1");
        construct_column(schema_pb.add_column(), schema_pb.add_index(), 10002, "v2_index", 2,
                         "STRING", "v2", true);
        construct_column(schema_pb.add_column(), schema_pb.add_index(), 10003, "v3_index", 3, "INT",
                         "v3");

        _tablet_schema.reset(new TabletSchema);
        _tablet_schema->init_from_pb(schema_pb);

        // tablet
        TabletMetaSharedPtr tablet_meta(new TabletMeta(_tablet_schema));

        _tablet.reset(new Tablet(*_engine_ref, tablet_meta, _data_dir.get()));
        EXPECT_TRUE(_tablet->init().ok());
        config::inverted_index_compaction_enable = true;
    }
    void TearDown() override {
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_absolute_dir).ok());
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(tmp_dir).ok());
        _engine_ref = nullptr;
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
    }

    void construct_column(ColumnPB* column_pb, TabletIndexPB* tablet_index, int64_t index_id,
                          const std::string& index_name, int32_t col_unique_id,
                          const std::string& column_type, const std::string& column_name,
                          bool parser = false) {
        column_pb->set_unique_id(col_unique_id);
        column_pb->set_name(column_name);
        column_pb->set_type(column_type);
        column_pb->set_is_key(false);
        column_pb->set_is_nullable(true);
        tablet_index->set_index_id(index_id);
        tablet_index->set_index_name(index_name);
        tablet_index->set_index_type(IndexType::INVERTED);
        tablet_index->add_col_unique_id(col_unique_id);
        if (parser) {
            auto* properties = tablet_index->mutable_properties();
            (*properties)[INVERTED_INDEX_PARSER_KEY] = INVERTED_INDEX_PARSER_UNICODE;
        }
    }

    RowsetWriterContext rowset_writer_context() {
        RowsetWriterContext context;
        RowsetId rowset_id;
        rowset_id.init(inc_id);
        context.rowset_id = rowset_id;
        context.rowset_type = BETA_ROWSET;
        context.data_dir = _data_dir.get();
        context.rowset_state = VISIBLE;
        context.tablet_schema = _tablet_schema;
        context.tablet_path = _tablet->tablet_path();
        context.version = Version(inc_id, inc_id);
        context.max_rows_per_segment = 200;
        inc_id++;
        return context;
    }

    IndexCompactionTest() = default;
    ~IndexCompactionTest() override = default;

private:
    TabletSchemaSPtr _tablet_schema = nullptr;
    StorageEngine* _engine_ref = nullptr;
    std::unique_ptr<DataDir> _data_dir = nullptr;
    TabletSharedPtr _tablet = nullptr;
    std::string _absolute_dir;
    std::string _curreent_dir;
};

std::vector<DataRow> read_data(const std::string file_name) {
    std::ifstream file(file_name);
    EXPECT_TRUE(file.is_open());

    std::string line;
    std::vector<DataRow> data;

    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string item;
        DataRow row;
        EXPECT_TRUE(std::getline(ss, item, ','));
        row.key = std::stoi(item);
        EXPECT_TRUE(std::getline(ss, item, ','));
        row.word = item;
        EXPECT_TRUE(std::getline(ss, item, ','));
        row.url = item;
        EXPECT_TRUE(std::getline(ss, item, ','));
        row.num = std::stoi(item);
        data.emplace_back(std::move(row));
    }

    file.close();
    return data;
}

bool query_bkd(const TabletIndex* index,
               std::shared_ptr<InvertedIndexFileReader>& inverted_index_file_reader,
               const std::vector<int>& query_data, const std::vector<int>& query_result) {
    const auto& idx_reader = BkdIndexReader::create_shared(index, inverted_index_file_reader);
    const auto& index_searcher_builder = std::make_unique<BKDIndexSearcherBuilder>();
    auto dir = inverted_index_file_reader->open(index);
    EXPECT_TRUE(dir.has_value());
    auto searcher_result = index_searcher_builder->get_index_searcher(dir.value().release());
    EXPECT_TRUE(searcher_result.has_value());
    auto bkd_searcher = std::get_if<BKDIndexSearcherPtr>(&searcher_result.value());
    EXPECT_TRUE(bkd_searcher != nullptr);
    idx_reader->_type_info = get_scalar_type_info((FieldType)(*bkd_searcher)->type);
    EXPECT_TRUE(idx_reader->_type_info != nullptr);
    idx_reader->_value_key_coder = get_key_coder(idx_reader->_type_info->type());

    for (int i = 0; i < query_data.size(); i++) {
        vectorized::Field param_value = Int32(query_data[i]);
        std::unique_ptr<segment_v2::InvertedIndexQueryParamFactory> query_param = nullptr;
        EXPECT_TRUE(segment_v2::InvertedIndexQueryParamFactory::create_query_value(
                            PrimitiveType::TYPE_INT, &param_value, query_param)
                            .ok());
        auto result = std::make_shared<roaring::Roaring>();
        EXPECT_TRUE(idx_reader
                            ->invoke_bkd_query(query_param->get_value(),
                                               InvertedIndexQueryType::EQUAL_QUERY, *bkd_searcher,
                                               result)
                            .ok());
        EXPECT_EQ(query_result[i], result->cardinality()) << query_data[i];
    }
    return true;
}

bool query_string(const TabletIndex* index,
                  std::shared_ptr<InvertedIndexFileReader>& inverted_index_file_reader,
                  const std::string& column_name, const std::vector<std::string>& query_data,
                  const std::vector<int>& query_result) {
    const auto& idx_reader =
            StringTypeInvertedIndexReader::create_shared(index, inverted_index_file_reader);
    const auto& index_searcher_builder = std::make_unique<FulltextIndexSearcherBuilder>();
    auto dir = inverted_index_file_reader->open(index);
    EXPECT_TRUE(dir.has_value());
    auto searcher_result = index_searcher_builder->get_index_searcher(dir.value().release());
    EXPECT_TRUE(searcher_result.has_value());
    auto string_searcher = std::get_if<FulltextIndexSearcherPtr>(&searcher_result.value());
    EXPECT_TRUE(string_searcher != nullptr);
    std::wstring column_name_ws = StringUtil::string_to_wstring(column_name);

    for (int i = 0; i < query_data.size(); i++) {
        TQueryOptions queryOptions;
        auto query = QueryFactory::create(InvertedIndexQueryType::EQUAL_QUERY, *string_searcher,
                                          queryOptions, nullptr);
        EXPECT_TRUE(query != nullptr);
        InvertedIndexQueryInfo query_info;
        query_info.field_name = column_name_ws;
        query_info.terms.emplace_back(query_data[i]);
        query->add(query_info);
        auto result = std::make_shared<roaring::Roaring>();
        query->search(*result);
        EXPECT_EQ(query_result[i], result->cardinality()) << query_data[i];
    }
    return true;
}

bool query_fulltext(const TabletIndex* index,
                    std::shared_ptr<InvertedIndexFileReader>& inverted_index_file_reader,
                    const std::string& column_name, const std::vector<std::string>& query_data,
                    const std::vector<int>& query_result) {
    const auto& idx_reader = FullTextIndexReader::create_shared(index, inverted_index_file_reader);
    const auto& index_searcher_builder = std::make_unique<FulltextIndexSearcherBuilder>();
    auto dir = inverted_index_file_reader->open(index);
    EXPECT_TRUE(dir.has_value());
    auto searcher_result = index_searcher_builder->get_index_searcher(dir.value().release());
    EXPECT_TRUE(searcher_result.has_value());
    auto string_searcher = std::get_if<FulltextIndexSearcherPtr>(&searcher_result.value());
    EXPECT_TRUE(string_searcher != nullptr);
    std::wstring column_name_ws = StringUtil::string_to_wstring(column_name);

    for (int i = 0; i < query_data.size(); i++) {
        TQueryOptions queryOptions;
        auto query = QueryFactory::create(InvertedIndexQueryType::MATCH_ANY_QUERY, *string_searcher,
                                          queryOptions, nullptr);
        EXPECT_TRUE(query != nullptr);
        InvertedIndexQueryInfo query_info;
        query_info.field_name = column_name_ws;
        query_info.terms.emplace_back(query_data[i]);
        query->add(query_info);
        auto result = std::make_shared<roaring::Roaring>();
        query->search(*result);
        EXPECT_EQ(query_result[i], result->cardinality()) << query_data[i];
    }
    return true;
}

TEST_F(IndexCompactionTest, write_index_test) {
    EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_tablet->tablet_path()).ok());
    EXPECT_TRUE(io::global_local_filesystem()->create_directory(_tablet->tablet_path()).ok());
    std::string data_file1 =
            _curreent_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data1.csv";
    std::string data_file2 =
            _curreent_dir + "/be/test/olap/rowset/segment_v2/inverted_index/data/data2.csv";

    std::vector<std::vector<DataRow>> data;
    data.emplace_back(read_data(data_file1));
    data.emplace_back(read_data(data_file2));

    std::vector<RowsetSharedPtr> rowsets(data.size());
    for (int i = 0; i < data.size(); i++) {
        const auto& res =
                RowsetFactory::create_rowset_writer(*_engine_ref, rowset_writer_context(), false);
        EXPECT_TRUE(res.has_value()) << res.error();
        const auto& rowset_writer = res.value();

        Block block = _tablet_schema->create_block();
        auto columns = block.mutate_columns();
        for (const auto& row : data[i]) {
            vectorized::Field key = Int32(row.key);
            vectorized::Field v1(row.word);
            vectorized::Field v2(row.url);
            vectorized::Field v3 = Int32(row.num);
            columns[0]->insert(key);
            columns[1]->insert(v1);
            columns[2]->insert(v2);
            columns[3]->insert(v3);
        }
        EXPECT_TRUE(rowset_writer->add_block(&block).ok());
        EXPECT_TRUE(rowset_writer->flush().ok());
        const auto& dst_writer = dynamic_cast<BaseBetaRowsetWriter*>(rowset_writer.get());

        // inverted index file writer
        for (const auto& [seg_id, idx_file_writer] : dst_writer->_idx_files.get_file_writers()) {
            EXPECT_TRUE(idx_file_writer->_closed);
        }

        EXPECT_TRUE(rowset_writer->build(rowsets[i]).ok());
        EXPECT_TRUE(_tablet->add_rowset(rowsets[i]).ok());
        EXPECT_TRUE(rowsets[i]->num_segments() == 5);

        // check rowset meta and file
        for (int seg_id = 0; seg_id < rowsets[i]->num_segments(); seg_id++) {
            const auto& index_info = rowsets[i]->_rowset_meta->inverted_index_file_info(seg_id);
            EXPECT_TRUE(index_info.has_index_size());
            const auto& fs = rowsets[i]->_rowset_meta->fs();
            const auto& file_name = fmt::format("{}/{}_{}.idx", rowsets[i]->tablet_path(),
                                                rowsets[i]->rowset_id().to_string(), seg_id);
            int64_t file_size = 0;
            EXPECT_TRUE(fs->file_size(file_name, &file_size).ok());
            EXPECT_EQ(index_info.index_size(), file_size);

            const auto& seg_path = rowsets[i]->segment_path(seg_id);
            EXPECT_TRUE(seg_path.has_value());
            const auto& index_file_path_prefix =
                    InvertedIndexDescriptor::get_index_file_path_prefix(seg_path.value());
            auto inverted_index_file_reader = std::make_shared<InvertedIndexFileReader>(
                    fs, std::string(index_file_path_prefix),
                    _tablet_schema->get_inverted_index_storage_format(), index_info);
            EXPECT_TRUE(inverted_index_file_reader->init().ok());
            const auto& dirs = inverted_index_file_reader->get_all_directories();
            EXPECT_TRUE(dirs.has_value());
            EXPECT_EQ(dirs.value().size(), 4);
        }
    }

    CumulativeCompaction compaction(*_engine_ref, _tablet);
    compaction._input_rowsets = std::move(rowsets);
    compaction.build_basic_info();

    std::vector<RowsetReaderSharedPtr> input_rs_readers;
    input_rs_readers.reserve(compaction._input_rowsets.size());
    for (auto& rowset : compaction._input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        EXPECT_TRUE(rowset->create_reader(&rs_reader).ok());
        input_rs_readers.push_back(std::move(rs_reader));
    }

    RowsetWriterContext ctx;
    EXPECT_TRUE(compaction.construct_output_rowset_writer(ctx).ok());

    // col word
    EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(1));
    // col url
    EXPECT_TRUE(ctx.columns_to_do_index_compaction.contains(2));

    compaction._stats.rowid_conversion = compaction._rowid_conversion.get();
    EXPECT_TRUE(Merger::vertical_merge_rowsets(_tablet, compaction.compaction_type(),
                                               *(compaction._cur_tablet_schema), input_rs_readers,
                                               compaction._output_rs_writer.get(), 100000, 5,
                                               &compaction._stats)
                        .ok());
    const auto& dst_writer =
            dynamic_cast<BaseBetaRowsetWriter*>(compaction._output_rs_writer.get());
    for (const auto& [seg_id, idx_file_writer] : dst_writer->_idx_files.get_file_writers()) {
        EXPECT_FALSE(idx_file_writer->_closed);
    }
    auto st = compaction.do_inverted_index_compaction();
    EXPECT_TRUE(st.ok()) << st.to_string();

    st = compaction._output_rs_writer->build(compaction._output_rowset);
    EXPECT_TRUE(st.ok()) << st.to_string();

    for (const auto& [seg_id, idx_file_writer] : dst_writer->_idx_files.get_file_writers()) {
        EXPECT_TRUE(idx_file_writer->_closed);
    }
    EXPECT_TRUE(compaction._output_rowset->num_segments() == 1);

    const auto& output_rowset = compaction._output_rowset;

    // check rowset meta and file
    for (int seg_id = 0; seg_id < output_rowset->num_segments(); seg_id++) {
        // meta
        const auto& index_info = output_rowset->_rowset_meta->inverted_index_file_info(seg_id);
        EXPECT_TRUE(index_info.has_index_size());
        const auto& fs = output_rowset->_rowset_meta->fs();
        const auto& file_name = fmt::format("{}/{}_{}.idx", output_rowset->tablet_path(),
                                            output_rowset->rowset_id().to_string(), seg_id);
        int64_t file_size = 0;
        EXPECT_TRUE(fs->file_size(file_name, &file_size).ok());
        EXPECT_EQ(index_info.index_size(), file_size);

        // file
        const auto& seg_path = output_rowset->segment_path(seg_id);
        EXPECT_TRUE(seg_path.has_value());
        const auto& index_file_path_prefix =
                InvertedIndexDescriptor::get_index_file_path_prefix(seg_path.value());
        auto inverted_index_file_reader = std::make_shared<InvertedIndexFileReader>(
                fs, std::string(index_file_path_prefix),
                _tablet_schema->get_inverted_index_storage_format(), index_info);
        EXPECT_TRUE(inverted_index_file_reader->init().ok());
        const auto& dirs = inverted_index_file_reader->get_all_directories();
        EXPECT_TRUE(dirs.has_value());
        EXPECT_EQ(dirs.value().size(), 4);

        // read col key
        const auto& key = _tablet_schema->column_by_uid(0);
        const auto* key_index = _tablet_schema->inverted_index(key);
        EXPECT_TRUE(key_index != nullptr);
        std::vector<int> query_data {99, 66, 56, 87, 85, 96, 20000};
        std::vector<int> query_result {21, 25, 22, 18, 14, 18, 0};
        EXPECT_TRUE(query_bkd(key_index, inverted_index_file_reader, query_data, query_result));

        // read col v3
        const auto& v3_column = _tablet_schema->column_by_uid(3);
        const auto* v3_index = _tablet_schema->inverted_index(v3_column);
        EXPECT_TRUE(v3_index != nullptr);
        std::vector<int> query_data3 {99, 66, 56, 87, 85, 96, 10000};
        std::vector<int> query_result3 {12, 20, 25, 23, 16, 24, 0};
        EXPECT_TRUE(query_bkd(v3_index, inverted_index_file_reader, query_data3, query_result3));

        // read col v1
        const auto& v1_column = _tablet_schema->column_by_uid(1);
        const auto* v1_index = _tablet_schema->inverted_index(v1_column);
        EXPECT_TRUE(v1_index != nullptr);
        std::vector<std::string> query_data1 {"good", "maybe", "great", "null"};
        std::vector<int> query_result1 {197, 191, 194, 0};
        EXPECT_TRUE(query_string(v1_index, inverted_index_file_reader, "1", query_data1,
                                 query_result1));

        // read col v2
        const auto& v2_column = _tablet_schema->column_by_uid(2);
        const auto* v2_index = _tablet_schema->inverted_index(v2_column);
        EXPECT_TRUE(v2_index != nullptr);
        std::vector<std::string> query_data2 {"musicstream.com", "http", "https", "null"};
        std::vector<int> query_result2 {191, 799, 1201, 0};
        EXPECT_TRUE(query_fulltext(v2_index, inverted_index_file_reader, "2", query_data2,
                                   query_result2));
    }
}

} // namespace doris
