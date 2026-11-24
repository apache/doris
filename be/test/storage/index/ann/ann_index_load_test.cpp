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

#include <memory>
#include <vector>

#include "exprs/vectorized_fn_call.h"
#include "io/fs/local_file_system.h"
#include "storage/index/ann/ann_range_search_runtime.h"
#include "storage/index/ann/ann_topn_runtime.h"
#include "storage/index/ann/vector_search_utils.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/schema.h"
#include "storage/segment/column_reader.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_iterator.h"

namespace doris::segment_v2 {

using vector_search_utils::MockIndexFileReader;

class AnnIndexLoadTest : public VectorSearchTest, public testing::WithParamInterface<int> {
protected:
    void SetUp() override {
        VectorSearchTest::SetUp();
        _meta._index_id = 1;
        _meta._properties = {{"index_type", "hnsw"}, {"metric_type", "l2_distance"}, {"dim", "4"}};
        _file_reader = std::make_shared<MockIndexFileReader>();
        _reader = std::make_shared<AnnIndexReader>(&_meta, _file_reader);
    }

    void expect_open_failure() {
        EXPECT_CALL(*_file_reader, init(testing::_, testing::_))
                .WillOnce(testing::Return(Status::OK()));
        EXPECT_CALL(*_file_reader, open(testing::_, testing::_))
                .WillOnce(
                        [this](const TabletIndex*, const io::IOContext*)
                                -> Result<std::unique_ptr<DorisCompoundReader, DirectoryDeleter>> {
                            return ResultError(Status(GetParam(), "injected ANN open failure"));
                        });
    }

    void check_range_load_status(int load_code) {
        std::vector<std::unique_ptr<IndexIterator>> index_iterators(1);
        index_iterators[0] = std::make_unique<AnnIndexIterator>(_reader);
        std::vector<std::unique_ptr<ColumnIterator>> column_iterators;
        AnnRangeSearchRuntime runtime;
        runtime.is_ann_range_search = true;
        runtime.dim = 4;
        runtime.metric_type = AnnIndexMetric::L2;
        runtime.user_params.ann_index_candidate_rows_percent_threshold = 0;
        roaring::Roaring candidates;
        candidates.addRange(0, 10);
        const auto original_candidates = candidates;
        AnnIndexStats stats;
        AnnRangeSearchEvaluationResult result;
        VectorizedFnCall expr;
        const std::vector<ColumnId> idx_to_cid {0};
        auto status =
                expr.evaluate_ann_range_search(runtime, index_iterators, idx_to_cid,
                                               column_iterators, 10, candidates, stats, result);
        const bool missing = load_code == ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND;
        EXPECT_EQ(status.code(), missing ? ErrorCode::OK : load_code);
        EXPECT_FALSE(result.executed);
        EXPECT_EQ(stats.fall_back_brute_force_cnt, missing ? 1 : 0);
        EXPECT_EQ(candidates, original_candidates);
    }

    void check_topn_load_status(int load_code) {
        auto schema = std::make_shared<TabletSchema>();
        auto segment = std::make_shared<Segment>(0, RowsetId(), schema, InvertedIndexFileInfo());
        segment->_num_rows = 10;
        TabletColumn vector_column(OLAP_FIELD_AGGREGATION_NONE, FieldType::OLAP_FIELD_TYPE_ARRAY);
        vector_column.set_name("embedding");
        vector_column.add_sub_column(
                TabletColumn(OLAP_FIELD_AGGREGATION_NONE, FieldType::OLAP_FIELD_TYPE_FLOAT));
        schema->append_column(vector_column);
        auto read_schema = std::make_shared<Schema>(schema->columns(), std::vector<ColumnId> {0});
        OlapReaderStatistics stats;
        SegmentIterator iter(segment, read_schema);
        iter._opts.stats = &stats;
        iter._index_iterators.resize(1);
        iter._index_iterators[0] = std::make_unique<AnnIndexIterator>(_reader);
        iter._need_read_data_indices[0] = false;
        iter._row_bitmap.addRange(0, 10);
        const auto original_candidates = iter._row_bitmap;

        VExprContextSPtr order_ctx;
        ASSERT_TRUE(VExpr::create_expr_tree(read_from_json<TExpr>(_distance_function_call_thrift),
                                            order_ctx)
                            .ok());
        iter._ann_topn_runtime = std::make_shared<AnnTopNRuntime>(true, 5, order_ctx);
        iter._ann_topn_runtime->_src_column_idx = 0;
        iter._ann_topn_runtime->_metric_type = AnnIndexMetric::L2;
        iter._ann_topn_runtime->_user_params.ann_index_candidate_rows_percent_threshold = 0;

        auto status = iter._apply_ann_topn_predicate();
        const bool missing = load_code == ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND;
        EXPECT_EQ(status.code(), missing ? ErrorCode::OK : load_code);
        EXPECT_EQ(iter._need_read_data_indices[0], missing);
        EXPECT_EQ(stats.ann_fall_back_brute_force_cnt, missing ? 1 : 0);
        EXPECT_EQ(iter._row_bitmap, original_candidates);
    }

    TabletIndex _meta;
    std::shared_ptr<MockIndexFileReader> _file_reader;
    std::shared_ptr<AnnIndexReader> _reader;
};

TEST_P(AnnIndexLoadTest, OpenStatusReachesIterator) {
    expect_open_failure();
    AnnIndexIterator iterator(_reader);
    auto status = iterator.load_index();
    EXPECT_EQ(status.code(), GetParam());
    EXPECT_THAT(status.msg(), testing::HasSubstr("injected ANN open failure"));
    // A repeated load returns the stored failure instead of reporting success with a partial index.
    EXPECT_EQ(iterator.load_index().code(), GetParam());
    EXPECT_EQ(_reader->_vector_index, nullptr);
}

TEST_P(AnnIndexLoadTest, InitStatusIsPreserved) {
    EXPECT_CALL(*_file_reader, init(testing::_, testing::_))
            .WillOnce(testing::Return(Status(GetParam(), "injected ANN init failure")));
    EXPECT_CALL(*_file_reader, open(testing::_, testing::_)).Times(0);
    AnnIndexIterator iterator(_reader);
    auto status = iterator.load_index();
    EXPECT_EQ(status.code(), GetParam());
    EXPECT_EQ(status.msg(), "injected ANN init failure");
}

TEST_P(AnnIndexLoadTest, RangeFallsBackOnlyForMissingIndex) {
    expect_open_failure();
    check_range_load_status(GetParam());
}

TEST_P(AnnIndexLoadTest, TopNFallsBackOnlyForMissingIndex) {
    expect_open_failure();
    check_topn_load_status(GetParam());
}

// An injected BYPASS still propagates; a real empty container now returns FILE_NOT_FOUND.
INSTANTIATE_TEST_SUITE_P(LoadErrors, AnnIndexLoadTest,
                         testing::Values(ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND,
                                         ErrorCode::IO_ERROR, ErrorCode::MEM_LIMIT_EXCEEDED,
                                         ErrorCode::INVERTED_INDEX_FILE_CORRUPTED,
                                         ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                                         ErrorCode::INVERTED_INDEX_BYPASS, ErrorCode::NOT_FOUND));

TEST_F(AnnIndexLoadTest, EmptyV2ContainerFallsBackToScan) {
    auto fs = io::global_local_filesystem();
    const std::string directory = "./ut_dir/ann_empty_container";
    ASSERT_TRUE(fs->delete_directory(directory).ok());
    ASSERT_TRUE(fs->create_directory(directory).ok());
    const std::string prefix = directory + "/segment_0";
    const auto path = InvertedIndexDescriptor::get_index_file_path_v2(prefix);
    io::FileWriterPtr file_writer;
    ASSERT_TRUE(fs->create_file(path, &file_writer).ok());
    {
        // A skipped ANN build leaves no logical indexes in the real V2 writer.
        IndexFileWriter writer(fs, prefix, "empty_ann", 0, InvertedIndexStorageFormatPB::V2,
                               std::move(file_writer), false);
        ASSERT_TRUE(writer.begin_close().ok());
        ASSERT_TRUE(writer.finish_close().ok());
    }
    int64_t size = -1;
    ASSERT_TRUE(fs->file_size(path, &size).ok());
    ASSERT_EQ(size, 0);

    _reader = std::make_shared<AnnIndexReader>(
            &_meta,
            std::make_shared<IndexFileReader>(fs, prefix, InvertedIndexStorageFormatPB::V2));
    check_range_load_status(ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    _reader = std::make_shared<AnnIndexReader>(
            &_meta,
            std::make_shared<IndexFileReader>(fs, prefix, InvertedIndexStorageFormatPB::V2));
    check_topn_load_status(ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    ASSERT_TRUE(fs->delete_directory(directory).ok());
}

TEST_F(AnnIndexLoadTest, NullReaderIsNotMissingIndex) {
    AnnIndexIterator iterator(nullptr);
    EXPECT_TRUE(iterator.load_index().is<ErrorCode::INDEX_INVALID_PARAMETERS>());
}

TEST_F(AnnIndexLoadTest, MissingInternalFaissFileIsNotMissingIndex) {
    for (const auto* type : {"hnsw", "ivf", "ivf_on_disk"}) {
        SCOPED_TRACE(type);
        _meta._properties["index_type"] = type;
        auto reader = std::make_shared<AnnIndexReader>(&_meta, _file_reader);
        EXPECT_CALL(*_file_reader, init(testing::_, testing::_))
                .WillOnce(testing::Return(Status::OK()));
        EXPECT_CALL(*_file_reader, open(testing::_, testing::_))
                .WillOnce(
                        [this](const TabletIndex*, const io::IOContext*)
                                -> Result<std::unique_ptr<DorisCompoundReader, DirectoryDeleter>> {
                            // The logical index opens successfully, but its directory lacks ann.faiss.
                            lucene::store::Directory* directory = _ram_dir.get();
                            return std::unique_ptr<DorisCompoundReader, DirectoryDeleter>(
                                    new DorisCompoundReader(directory->openInput("index_file"),
                                                            EntriesType {}));
                        });
        auto status = reader->load_index(nullptr);
        EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>()) << status;
    }
}

} // namespace doris::segment_v2
