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

#include "olap/collection_statistics.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/exception.h"
#include "gen_cpp/Exprs_types.h"
#include "io/fs/local_file_system.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/segment_v2/inverted_index/query/query_info.h"
#include "olap/tablet_schema.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vliteral.h"
#include "vec/exprs/vslot_ref.h"

namespace doris {

namespace collection_statistics {

class MockVExpr : public vectorized::VExpr {
public:
    MockVExpr(TExprNodeType::type node_type) : _mock_node_type(node_type) {}

    TExprNodeType::type node_type() const override { return _mock_node_type; }

    Status execute(vectorized::VExprContext* context, vectorized::Block* block,
                   int32_t* result_column_id) override {
        return Status::OK();
    }

    Status prepare(RuntimeState* state, const RowDescriptor& desc,
                   vectorized::VExprContext* context) override {
        return Status::OK();
    }

    Status open(RuntimeState* state, vectorized::VExprContext* context,
                FunctionContext::FunctionStateScope scope) override {
        return Status::OK();
    }

    void close(vectorized::VExprContext* context,
               FunctionContext::FunctionStateScope scope) override {}

    const std::string& expr_name() const override {
        static std::string name = "mock_expr";
        return name;
    }

    std::string debug_string() const override { return "MockVExpr"; }

private:
    TExprNodeType::type _mock_node_type;
};

class MockVSlotRef : public vectorized::VSlotRef {
public:
    MockVSlotRef(const std::string& column_name) : _column_name(column_name) {}

    const std::string& column_name() const override { return _column_name; }
    const std::string& expr_name() const override { return _column_name; }
    std::string debug_string() const override { return "MockVSlotRef: " + _column_name; }

private:
    std::string _column_name;
};

class MockVLiteral : public vectorized::VLiteral {
public:
    MockVLiteral(const std::string& value) : _value(value) {}

    std::string value() const override { return _value; }
    const std::string& expr_name() const override { return _value; }
    std::string debug_string() const override { return "MockVLiteral: " + _value; }

private:
    std::string _value;
};

class MockRowsetMeta : public RowsetMeta {
public:
    MockRowsetMeta() : RowsetMeta() { _fs = io::global_local_filesystem(); }

    io::FileSystemSPtr fs() override { return _fs; }

private:
    io::FileSystemSPtr _fs;
};

class MockRowset : public Rowset {
public:
    MockRowset(TabletSchemaSPtr schema, RowsetMetaSharedPtr rowset_meta)
            : Rowset(schema, rowset_meta, "/mock/tablet/path") {
        _num_segments = 0;
    }

    Status create_reader(std::shared_ptr<RowsetReader>* result) override {
        return Status::NotSupported("MockRowset::create_reader not implemented");
    }

    Status remove() override { return Status::OK(); }

    Status link_files_to(const std::string& dir, RowsetId new_rowset_id, size_t start_seg_id,
                         std::set<int64_t>* without_index_uids) override {
        return Status::OK();
    }

    Status copy_files_to(const std::string& dir, const RowsetId& new_rowset_id) override {
        return Status::OK();
    }

    Status remove_old_files(std::vector<std::string>* files_to_remove) override {
        return Status::OK();
    }

    Status check_file_exist() override { return Status::OK(); }

    Status upload_to(const StorageResource& dest_fs, const RowsetId& new_rowset_id) override {
        return Status::OK();
    }

    Status get_inverted_index_size(int64_t* index_size) override {
        *index_size = 0;
        return Status::OK();
    }

    void clear_inverted_index_cache() override {}

    Status init() override { return Status::OK(); }

    void do_close() override {}

    Status check_current_rowset_segment() override { return Status::OK(); }

    int64_t num_segments() const override { return _num_segments; }

    Result<std::string> segment_path(int64_t seg_id) override {
        if (_segment_paths.find(seg_id) != _segment_paths.end()) {
            return _segment_paths.at(seg_id);
        }
        return ResultError(Status::InternalError("Segment path not found"));
    }

    void set_segment_path(int64_t seg_id, const std::string& path) {
        _segment_paths[seg_id] = path;
    }

    void set_num_segments(int64_t num) { _num_segments = num; }

private:
    int64_t _num_segments;
    std::map<int64_t, std::string> _segment_paths;
};

class MockRowsetReader : public RowsetReader {
public:
    MockRowsetReader(std::shared_ptr<MockRowset> rowset) : _rowset(rowset) {}

    Status init(RowsetReaderContext* read_context, const RowSetSplits& rs_splits) override {
        return Status::OK();
    }

    Status get_segment_iterators(RowsetReaderContext* read_context,
                                 std::vector<RowwiseIteratorUPtr>* out_iters,
                                 bool use_cache = false) override {
        return Status::OK();
    }

    void reset_read_options() override {}

    Status next_block(vectorized::Block* block) override {
        return Status::NotSupported("MockRowsetReader::next_block not implemented");
    }

    Status next_block_view(vectorized::BlockView* block_view) override {
        return Status::NotSupported("MockRowsetReader::next_block_view not implemented");
    }

    bool delete_flag() override { return false; }

    Version version() override { return Version(1, 1); }

    RowsetSharedPtr rowset() override { return _rowset; }

    int64_t filtered_rows() override { return 0; }

    uint64_t merged_rows() override { return 0; }

    RowsetTypePB type() const override { return BETA_ROWSET; }

    int64_t newest_write_timestamp() override { return 0; }

    void update_profile(RuntimeProfile* profile) override {}

    RowsetReaderSharedPtr clone() override { return std::make_shared<MockRowsetReader>(_rowset); }

    void set_topn_limit(size_t topn_limit) override {}

private:
    std::shared_ptr<MockRowset> _rowset;
};

} // namespace collection_statistics

class CollectionStatisticsTest : public ::testing::Test {
protected:
    void SetUp() override {
        stats_ = std::make_unique<CollectionStatistics>();
        test_dir_ = "./collection_statistics_test_" +
                    std::to_string(::testing::UnitTest::GetInstance()->random_seed());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(test_dir_).ok());
    }

    void TearDown() override {
        stats_.reset();
        (void)io::global_local_filesystem()->delete_directory(test_dir_);
    }

    TabletSchemaSPtr create_tablet_schema_with_inverted_index() {
        auto tablet_schema = std::make_shared<TabletSchema>();

        TabletColumn column;
        column.set_unique_id(1);
        column.set_name("content");
        column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
        tablet_schema->append_column(column);

        TabletIndex index;
        index._index_id = 1;
        index._index_type = IndexType::INVERTED;
        index._col_unique_ids.push_back(1);
        std::map<std::string, std::string> properties;
        properties["parser"] = "standard";
        index._properties = properties;

        tablet_schema->append_index(std::move(index));

        return tablet_schema;
    }

    vectorized::VExprContextSPtrs create_match_expr_contexts(
            const std::string& search_term = "search term") {
        vectorized::VExprContextSPtrs contexts;

        auto match_expr =
                std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
        auto slot_ref = std::make_shared<collection_statistics::MockVSlotRef>("content");
        auto literal = std::make_shared<collection_statistics::MockVLiteral>(search_term);

        match_expr->_children.push_back(slot_ref);
        match_expr->_children.push_back(literal);

        auto context = std::make_shared<vectorized::VExprContext>(match_expr);
        contexts.push_back(context);

        return contexts;
    }
    std::vector<RowSetSplits> create_mock_rowset_splits(int num_segments = 1) {
        std::vector<RowSetSplits> splits;

        auto rowset_meta = std::make_shared<collection_statistics::MockRowsetMeta>();
        auto rowset = std::make_shared<collection_statistics::MockRowset>(
                create_tablet_schema_with_inverted_index(), rowset_meta);
        rowset->set_num_segments(num_segments);

        for (int i = 0; i < num_segments; ++i) {
            rowset->set_segment_path(i, test_dir_ + "/segment_" + std::to_string(i) + ".dat");
        }

        auto reader = std::make_shared<collection_statistics::MockRowsetReader>(rowset);

        RowSetSplits split(reader);
        splits.push_back(split);

        return splits;
    }

    std::unique_ptr<CollectionStatistics> stats_;
    std::string test_dir_;
};

TEST_F(CollectionStatisticsTest, CollectWithEmptyRowsetSplits) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();
    auto expr_contexts = create_match_expr_contexts();

    std::vector<RowSetSplits> empty_splits;

    auto status = stats_->collect(empty_splits, tablet_schema, expr_contexts);
    EXPECT_TRUE(status.ok()) << status.msg();
}

TEST_F(CollectionStatisticsTest, CollectWithEmptyExpressions) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();
    vectorized::VExprContextSPtrs empty_contexts;

    std::vector<RowSetSplits> empty_splits;

    auto status = stats_->collect(empty_splits, tablet_schema, empty_contexts);
    EXPECT_TRUE(status.ok()) << status.msg();
}

TEST_F(CollectionStatisticsTest, CollectWithNonMatchExpression) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();

    vectorized::VExprContextSPtrs contexts;
    auto non_match_expr =
            std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::BINARY_PRED);
    auto context = std::make_shared<vectorized::VExprContext>(non_match_expr);
    contexts.push_back(context);

    std::vector<RowSetSplits> empty_splits;

    auto status = stats_->collect(empty_splits, tablet_schema, contexts);
    EXPECT_TRUE(status.ok()) << status.msg();
}

TEST_F(CollectionStatisticsTest, CollectWithMultipleMatchExpressions) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();

    vectorized::VExprContextSPtrs contexts;

    auto match_expr1 =
            std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto slot_ref1 = std::make_shared<collection_statistics::MockVSlotRef>("content");
    auto literal1 = std::make_shared<collection_statistics::MockVLiteral>("term1");
    match_expr1->_children.push_back(slot_ref1);
    match_expr1->_children.push_back(literal1);
    contexts.push_back(std::make_shared<vectorized::VExprContext>(match_expr1));

    auto match_expr2 =
            std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto slot_ref2 = std::make_shared<collection_statistics::MockVSlotRef>("content");
    auto literal2 = std::make_shared<collection_statistics::MockVLiteral>("term2");
    match_expr2->_children.push_back(slot_ref2);
    match_expr2->_children.push_back(literal2);
    contexts.push_back(std::make_shared<vectorized::VExprContext>(match_expr2));

    std::vector<RowSetSplits> empty_splits;

    auto status = stats_->collect(empty_splits, tablet_schema, contexts);
    EXPECT_TRUE(status.ok()) << status.msg();
}

TEST_F(CollectionStatisticsTest, CollectWithNestedExpressions) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();

    vectorized::VExprContextSPtrs contexts;

    auto and_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::BINARY_PRED);

    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto slot_ref = std::make_shared<collection_statistics::MockVSlotRef>("content");
    auto literal = std::make_shared<collection_statistics::MockVLiteral>("nested term");
    match_expr->_children.push_back(slot_ref);
    match_expr->_children.push_back(literal);

    auto other_expr =
            std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::BINARY_PRED);

    and_expr->_children.push_back(match_expr);
    and_expr->_children.push_back(other_expr);

    contexts.push_back(std::make_shared<vectorized::VExprContext>(and_expr));

    std::vector<RowSetSplits> empty_splits;

    auto status = stats_->collect(empty_splits, tablet_schema, contexts);
    EXPECT_TRUE(status.ok()) << status.msg();
}

TEST_F(CollectionStatisticsTest, CollectWithMockRowsetSplits) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();
    auto expr_contexts = create_match_expr_contexts();

    auto splits = create_mock_rowset_splits(2);

    auto status = stats_->collect(splits, tablet_schema, expr_contexts);

    EXPECT_FALSE(status.ok());
}

TEST_F(CollectionStatisticsTest, CollectWithEmptySegments) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();
    auto expr_contexts = create_match_expr_contexts();

    auto splits = create_mock_rowset_splits(0);

    auto status = stats_->collect(splits, tablet_schema, expr_contexts);
    EXPECT_TRUE(status.ok()) << status.msg();
}

TEST_F(CollectionStatisticsTest, CollectWithMultipleRowsetSplits) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();
    auto expr_contexts = create_match_expr_contexts();

    std::vector<RowSetSplits> splits;

    for (int i = 0; i < 3; ++i) {
        auto rowset_meta = std::make_shared<collection_statistics::MockRowsetMeta>();
        auto rowset =
                std::make_shared<collection_statistics::MockRowset>(tablet_schema, rowset_meta);
        rowset->set_num_segments(0);

        auto reader = std::make_shared<collection_statistics::MockRowsetReader>(rowset);

        RowSetSplits split(reader);
        splits.push_back(split);
    }

    auto status = stats_->collect(splits, tablet_schema, expr_contexts);
    EXPECT_TRUE(status.ok()) << status.msg();
}

class TestableCollectionStatistics : public CollectionStatistics {
public:
    void set_total_num_docs(uint64_t num_docs) { _total_num_docs = num_docs; }

    void set_total_num_tokens(const std::wstring& field_name, uint64_t num_tokens) {
        _total_num_tokens[field_name] = num_tokens;
    }

    void set_term_doc_freq(const std::wstring& field_name, const std::wstring& term,
                           uint64_t freq) {
        _term_doc_freqs[field_name][term] = freq;
    }
};

class CollectionStatisticsDetailedTest : public ::testing::Test {
protected:
    void SetUp() override { stats_ = std::make_unique<TestableCollectionStatistics>(); }

    void TearDown() override { stats_.reset(); }

    std::unique_ptr<TestableCollectionStatistics> stats_;
};

TEST_F(CollectionStatisticsDetailedTest, GetStatisticsWithValidData) {
    std::wstring field_name = L"test_field";
    std::wstring term = L"test_term";

    stats_->set_total_num_docs(1000);
    stats_->set_total_num_tokens(field_name, 5000);
    stats_->set_term_doc_freq(field_name, term, 100);

    EXPECT_EQ(stats_->get_doc_num(), 1000);
    EXPECT_EQ(stats_->get_total_term_cnt_by_col(field_name), 5000);
    EXPECT_EQ(stats_->get_term_doc_freq_by_col(field_name, term), 100);

    float expected_avg_dl = 5000.0f / 1000.0f;
    EXPECT_FLOAT_EQ(stats_->get_or_calculate_avg_dl(field_name), expected_avg_dl);

    float expected_idf = std::log(1 + (1000 - 100 + 0.5) / (100 + 0.5));
    EXPECT_FLOAT_EQ(stats_->get_or_calculate_idf(field_name, term), expected_idf);
}

TEST_F(CollectionStatisticsDetailedTest, GetStatisticsThrowsWhenDataNotExists) {
    std::wstring nonexistent_field = L"nonexistent";
    std::wstring nonexistent_term = L"nonexistent";

    // Test exceptions for missing data
    EXPECT_THROW(stats_->get_doc_num(), Exception);
    EXPECT_THROW(stats_->get_total_term_cnt_by_col(nonexistent_field), Exception);
    EXPECT_THROW(stats_->get_term_doc_freq_by_col(nonexistent_field, nonexistent_term), Exception);
    EXPECT_THROW(stats_->get_or_calculate_avg_dl(nonexistent_field), Exception);
    EXPECT_THROW(stats_->get_or_calculate_idf(nonexistent_field, nonexistent_term), Exception);
}

TEST_F(CollectionStatisticsDetailedTest, CachingMechanismWorks) {
    std::wstring field_name = L"test_field";
    std::wstring term = L"test_term";

    stats_->set_total_num_docs(1000);
    stats_->set_total_num_tokens(field_name, 5000);
    stats_->set_term_doc_freq(field_name, term, 100);

    float first_avg_dl = stats_->get_or_calculate_avg_dl(field_name);
    float first_idf = stats_->get_or_calculate_idf(field_name, term);

    stats_->set_total_num_docs(2000);
    stats_->set_total_num_tokens(field_name, 10000);
    stats_->set_term_doc_freq(field_name, term, 200);

    float second_avg_dl = stats_->get_or_calculate_avg_dl(field_name);
    float second_idf = stats_->get_or_calculate_idf(field_name, term);

    EXPECT_FLOAT_EQ(first_avg_dl, second_avg_dl);
    EXPECT_FLOAT_EQ(first_idf, second_idf);
}

TEST_F(CollectionStatisticsDetailedTest, HandlesZeroValuesCorrectly) {
    std::wstring field_name = L"test_field";
    std::wstring term = L"test_term";

    stats_->set_total_num_docs(0);
    EXPECT_THROW(stats_->get_doc_num(), Exception);

    stats_->set_total_num_docs(100);
    stats_->set_total_num_tokens(field_name, 0);
    stats_->set_term_doc_freq(field_name, term, 0);

    EXPECT_EQ(stats_->get_total_term_cnt_by_col(field_name), 0);
    EXPECT_EQ(stats_->get_term_doc_freq_by_col(field_name, term), 0);
    EXPECT_FLOAT_EQ(stats_->get_or_calculate_avg_dl(field_name), 0.0f);
}

TEST_F(CollectionStatisticsDetailedTest, IdfCalculationWithDifferentFrequencies) {
    std::wstring field_name = L"test_field";
    std::wstring common_term = L"common_term";
    std::wstring rare_term = L"rare_term";

    stats_->set_total_num_docs(1000);
    stats_->set_term_doc_freq(field_name, common_term, 500);
    stats_->set_term_doc_freq(field_name, rare_term, 10);

    float common_idf = stats_->get_or_calculate_idf(field_name, common_term);
    float rare_idf = stats_->get_or_calculate_idf(field_name, rare_term);

    EXPECT_GT(rare_idf, common_idf);
    EXPECT_GT(common_idf, 0);
    EXPECT_GT(rare_idf, 0);
}

} // namespace doris