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
#include "storage/index/ann/ann_range_search_runtime.h"
#include "storage/index/ann/ann_topn_runtime.h"
#include "storage/index/ann/vector_search_utils.h"
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

    int expected_code() const {
        return GetParam() == ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND
                       ? ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND
                       : GetParam();
    }

    TabletIndex _meta;
    std::shared_ptr<MockIndexFileReader> _file_reader;
    std::shared_ptr<AnnIndexReader> _reader;
};

TEST_P(AnnIndexLoadTest, OpenStatusReachesIterator) {
    expect_open_failure();
    AnnIndexIterator iterator(_reader);
    auto status = iterator.load_index();
    EXPECT_EQ(status.code(), expected_code());
    EXPECT_THAT(status.msg(), testing::HasSubstr("injected ANN open failure"));
    // A repeated load returns the stored failure instead of reporting success with a partial index.
    EXPECT_EQ(iterator.load_index().code(), expected_code());
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
    auto status = expr.evaluate_ann_range_search(runtime, index_iterators, column_iterators, 10,
                                                 candidates, stats, false, result);
    const bool missing = expected_code() == ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND;
    EXPECT_EQ(status.code(), missing ? ErrorCode::OK : expected_code());
    EXPECT_FALSE(result.executed);
    EXPECT_EQ(stats.fall_back_brute_force_cnt, missing ? 1 : 0);
    EXPECT_EQ(candidates, original_candidates);
}

TEST_P(AnnIndexLoadTest, TopNFallsBackOnlyForMissingIndex) {
    expect_open_failure();
    auto schema = std::make_shared<TabletSchema>();
    auto segment = std::make_shared<Segment>(0, RowsetId(), schema, InvertedIndexFileInfo());
    segment->_num_rows = 10;
    auto read_schema = std::make_shared<ReadSchema>(std::vector<TabletColumnPtr> {});
    OlapReaderStatistics stats;
    StorageReadOptions opts(stats);
    SegmentIterator iter(segment, read_schema, opts);
    iter._index_iterators.resize(1);
    iter._index_iterators[0] = std::make_unique<AnnIndexIterator>(_reader);
    iter._column_states.resize(1);
    iter._column_states[0].need_read_data = false;
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
    const bool missing = expected_code() == ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND;
    EXPECT_EQ(status.code(), missing ? ErrorCode::OK : expected_code());
    EXPECT_EQ(iter._column_states[0].need_read_data, missing);
    EXPECT_EQ(stats.ann_fall_back_brute_force_cnt, missing ? 1 : 0);
    EXPECT_EQ(iter._row_bitmap, original_candidates);
}

INSTANTIATE_TEST_SUITE_P(LoadErrors, AnnIndexLoadTest,
                         testing::Values(ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND,
                                         ErrorCode::INVERTED_INDEX_SNII_NOT_FOUND,
                                         ErrorCode::IO_ERROR, ErrorCode::MEM_LIMIT_EXCEEDED,
                                         ErrorCode::INVERTED_INDEX_FILE_CORRUPTED,
                                         ErrorCode::INVERTED_INDEX_CLUCENE_ERROR,
                                         ErrorCode::INVERTED_INDEX_BYPASS, ErrorCode::NOT_FOUND));

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
