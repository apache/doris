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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

// Add CLucene RAM Directory header
#include <olap/field.h>

#include <cstdint>
#include <memory>

#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/ann_index/ann_index.h"
#include "olap/rowset/segment_v2/ann_index/ann_index_writer.h"
#include "olap/rowset/segment_v2/ann_index/ann_search_params.h"
#include "olap/rowset/segment_v2/ann_index/faiss_ann_index.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "vector_search_utils.h"

using namespace doris::vector_search_utils;

namespace doris {

class AnnIndexTest : public testing::Test {
protected:
    void SetUp() override {
        // Create a tmp_file_dirs, this will be used by
        const std::string testDir = "./ut_dir/AnnIndexTest";
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(testDir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(testDir).ok());
        std::vector<StorePath> paths;
        paths.emplace_back(testDir, 1024);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        ASSERT_TRUE(tmp_file_dirs->init());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        _ram_dir = std::make_shared<segment_v2::DorisRAMFSDirectory>();
        auto fs = io::global_local_filesystem();
        _index_file_writer = std::make_unique<MockIndexFileWriter>(fs);
        _index_meta = std::make_unique<MockTabletIndex>();
        _tablet_column_array = std::make_unique<MockTabletColumn>();
        _tablet_column_float = std::make_unique<MockTabletColumn>();

        EXPECT_CALL(*_tablet_column_array, type())
                .WillRepeatedly(testing::Return(FieldType::OLAP_FIELD_TYPE_ARRAY));
        EXPECT_CALL(*_tablet_column_array, get_sub_column(0))
                .WillOnce(testing::ReturnRef(*_tablet_column_float));
        EXPECT_CALL(*_tablet_column_float, type())
                .WillOnce(testing::Return(FieldType::OLAP_FIELD_TYPE_FLOAT));

        Field field(*_tablet_column_array);

        EXPECT_CALL(*_index_file_writer, open(_index_meta.get()))
                .WillOnce(testing::Return(_ram_dir));

        std::map<std::string, std::string> properties = {
                {segment_v2::AnnIndexColumnWriter::INDEX_TYPE, "hnsw"},
                {segment_v2::AnnIndexColumnWriter::DIM, "10"},
                {segment_v2::AnnIndexColumnWriter::MAX_DEGREE, "32"}};

        EXPECT_CALL(*_index_meta, properties()).WillOnce(testing::ReturnRef(properties));
        _ann_index_col_writer = std::make_unique<segment_v2::AnnIndexColumnWriter>(
                _index_file_writer.get(), _index_meta.get());
        EXPECT_TRUE(_ann_index_col_writer->init().ok());
    }

    void TearDown() override {}

    std::unique_ptr<segment_v2::IndexColumnWriter> _ann_index_col_writer;
    std::shared_ptr<segment_v2::DorisFSDirectory> _ram_dir;
    std::unique_ptr<MockTabletColumn> _tablet_column_array;
    std::unique_ptr<MockTabletColumn> _tablet_column_float;
    std::unique_ptr<MockIndexFileWriter> _index_file_writer;
    std::unique_ptr<MockTabletIndex> _index_meta;
};

TEST_F(AnnIndexTest, SmokeTest) {
    segment_v2::AnnIndexColumnWriter* ann_index_writer =
            dynamic_cast<segment_v2::AnnIndexColumnWriter*>(_ann_index_col_writer.get());
    ASSERT_NE(ann_index_writer, nullptr);

    // Add some dummy data
    /*
    [0,1,2,3,4,5,6,7,8,9]
    [10,11,12,13,14,15,16,17,18,19]
    [20,21,22,23,24,25,26,27,28,29]
    [30,31,32,33,34,35,36,37,38,39]
    [40,41,42,43,44,45,46,47,48,49]
    [50,...]
    */
    std::unique_ptr<float[]> data(new float[10 * 10]);
    for (int i = 0; i < 10 * 10; ++i) {
        data[i] = static_cast<float>(i);
    }

    // Create offsets_data with size num_rows + 1; last entry is total elements
    std::unique_ptr<size_t[]> offsets_data(new size_t[11]);
    size_t* offsets = offsets_data.get();

    for (int i = 0; i < 10; ++i) {
        offsets[i] = i * 10; // start offset of each row
    }
    offsets[10] = 10 * 10; // terminal offset

    auto st = ann_index_writer->add_array_values(
            0, data.get(), nullptr, reinterpret_cast<uint8_t*>(offsets_data.get()), 10);
    EXPECT_TRUE(st.ok()) << st.to_string();

    ASSERT_TRUE(ann_index_writer->finish().ok());

    // Read the index file
    auto index2 = std::make_unique<segment_v2::FaissVectorIndex>();
    // Step 6: Load the index
    auto load_status = index2->load(_ram_dir.get());
    ASSERT_TRUE(load_status.ok()) << load_status.to_string();
    // [0,1,2,3,4,5,6,7,8,9]
    std::unique_ptr<float[]> query_vec(new float[10]);
    for (int i = 0; i < 10; ++i) {
        query_vec[i] = static_cast<float>(i);
    }

    // Use HNSW search parameters for FAISS HNSW top-N search
    segment_v2::HNSWSearchParameters params;
    params.ef_search = 64; // reasonable default for test
    // TopN search requires candidate roaring and rows_of_segment
    auto all_rows = std::make_unique<roaring::Roaring>();
    for (int i = 0; i < 10; ++i) all_rows->add(i);
    params.roaring = all_rows.get();
    params.rows_of_segment = 10;
    segment_v2::IndexSearchResult result;
    ASSERT_TRUE(index2->ann_topn_search(query_vec.get(), 1, params, result).ok());
    EXPECT_TRUE(result.roaring->cardinality() == 1);
    EXPECT_TRUE(result.roaring->contains(0));

    std::shared_ptr<segment_v2::IndexFileReader> index_file_reader =
            std::make_shared<MockIndexFileReader>();
    //     EXPECT_CALL(*index_file_reader, init(_, _));
    //     EXPECT_CALL(*index_file_reader, open(_, _))
    //     auto ann_index_reader =
    //             std::make_unique<segment_v2::AnnIndexReader>(_index_meta.get(), index_file_reader);

} // namespace doris

} // namespace doris