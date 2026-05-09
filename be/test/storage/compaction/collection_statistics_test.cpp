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

#include "storage/compaction/collection_statistics.h"

#include <gen_cpp/Exprs_types.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "common/exception.h"
#include "core/data_type/data_type_string.h"
#include "exec/common/variant_util.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "io/fs/local_file_system.h"
#include "storage/compaction/collection_statistics.cpp"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

namespace collection_statistics {

class MockVExpr : public VExpr {
public:
    MockVExpr(TExprNodeType::type node_type) : _mock_node_type(node_type) {
        if (node_type == TExprNodeType::MATCH_PRED) {
            _opcode = TExprOpcode::MATCH_PHRASE;
        }
    }

    TExprNodeType::type node_type() const override { return _mock_node_type; }

    Status execute(VExprContext* context, Block* block, int32_t* result_column_id) const override {
        return Status::OK();
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        return Status::OK();
    }

    Status prepare(RuntimeState* state, const RowDescriptor& desc, VExprContext* context) override {
        return Status::OK();
    }

    Status open(RuntimeState* state, VExprContext* context,
                FunctionContext::FunctionStateScope scope) override {
        return Status::OK();
    }

    void close(VExprContext* context, FunctionContext::FunctionStateScope scope) override {}

    const std::string& expr_name() const override {
        static std::string name = "mock_expr";
        return name;
    }

    std::string debug_string() const override { return "MockVExpr"; }

private:
    TExprNodeType::type _mock_node_type;
};

class MockVSlotRef : public VSlotRef {
public:
    MockVSlotRef(const std::string& column_name, SlotId slot_id)
            : _column_name(column_name), _slot_id(slot_id) {
        _node_type = TExprNodeType::SLOT_REF;
    }

    const std::string& column_name() const override { return _column_name; }
    const std::string& expr_name() const override { return _column_name; }
    std::string debug_string() const override { return "MockVSlotRef: " + _column_name; }
    SlotId slot_id() const override { return _slot_id; }

private:
    std::string _column_name;
    SlotId _slot_id;
};

class MockVLiteral : public VLiteral {
public:
    MockVLiteral(const std::string& value) : _value(value) {}

    std::string value() const override { return _value; }
    std::string value(const DataTypeSerDe::FormatOptions& options) const override { return _value; }
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

    Status next_batch(Block* block) override {
        return Status::NotSupported("MockRowsetReader::next_batch not implemented");
    }

    Status next_batch(BlockView* block_view) override {
        return Status::NotSupported("MockRowsetReader::next_batch not implemented");
    }

    Status next_batch(BlockWithSameBit* block_view) override {
        return Status::NotSupported("MockRowsetReader::next_batch not implemented");
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
        runtime_state_ = std::make_shared<MockRuntimeState>();
        runtime_state_->_mock_desc_tbl->add_slot_descriptor(SlotId(1), 1001);
        test_dir_ = "./collection_statistics_test_" +
                    std::to_string(::testing::UnitTest::GetInstance()->random_seed());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(test_dir_).ok());
    }

    void TearDown() override {
        stats_.reset();
        runtime_state_.reset();
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
        properties["support_phrase"] = "true";
        index._properties = properties;

        tablet_schema->append_index(std::move(index));

        return tablet_schema;
    }

    VExprContextSPtrs create_match_expr_contexts(const std::string& search_term = "search term") {
        VExprContextSPtrs contexts;

        auto match_expr =
                std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
        auto slot_ref = std::make_shared<collection_statistics::MockVSlotRef>("content", SlotId(1));
        auto literal = std::make_shared<collection_statistics::MockVLiteral>(search_term);

        match_expr->_children.push_back(slot_ref);
        match_expr->_children.push_back(literal);

        auto context = std::make_shared<VExprContext>(match_expr);
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
    std::shared_ptr<MockRuntimeState> runtime_state_;
    std::string test_dir_;
};

TEST_F(CollectionStatisticsTest, CollectWithEmptyRowsetSplits) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();
    auto expr_contexts = create_match_expr_contexts();

    std::vector<RowSetSplits> empty_splits;

    auto status = stats_->collect(runtime_state_.get(), empty_splits, tablet_schema, expr_contexts,
                                  nullptr);
    EXPECT_TRUE(status.ok()) << status.msg();
}

TEST_F(CollectionStatisticsTest, CollectWithEmptyExpressions) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();
    VExprContextSPtrs empty_contexts;

    std::vector<RowSetSplits> empty_splits;

    auto status = stats_->collect(runtime_state_.get(), empty_splits, tablet_schema, empty_contexts,
                                  nullptr);
    EXPECT_TRUE(status.ok()) << status.msg();
}

TEST_F(CollectionStatisticsTest, CollectWithNonMatchExpression) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();

    VExprContextSPtrs contexts;
    auto non_match_expr =
            std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::BINARY_PRED);
    auto context = std::make_shared<VExprContext>(non_match_expr);
    contexts.push_back(context);

    std::vector<RowSetSplits> empty_splits;

    auto status =
            stats_->collect(runtime_state_.get(), empty_splits, tablet_schema, contexts, nullptr);
    EXPECT_TRUE(status.ok()) << status.msg();
}

TEST_F(CollectionStatisticsTest, CollectWithMultipleMatchExpressions) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();

    VExprContextSPtrs contexts;

    auto match_expr1 =
            std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto slot_ref1 = std::make_shared<collection_statistics::MockVSlotRef>("content", SlotId(1));
    auto literal1 = std::make_shared<collection_statistics::MockVLiteral>("term1");
    match_expr1->_children.push_back(slot_ref1);
    match_expr1->_children.push_back(literal1);
    contexts.push_back(std::make_shared<VExprContext>(match_expr1));

    auto match_expr2 =
            std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto slot_ref2 = std::make_shared<collection_statistics::MockVSlotRef>("content", SlotId(1));
    auto literal2 = std::make_shared<collection_statistics::MockVLiteral>("term2");
    match_expr2->_children.push_back(slot_ref2);
    match_expr2->_children.push_back(literal2);
    contexts.push_back(std::make_shared<VExprContext>(match_expr2));

    std::vector<RowSetSplits> empty_splits;

    auto status =
            stats_->collect(runtime_state_.get(), empty_splits, tablet_schema, contexts, nullptr);
    EXPECT_TRUE(status.ok()) << status.msg();
}

TEST_F(CollectionStatisticsTest, CollectWithNestedExpressions) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();

    VExprContextSPtrs contexts;

    auto and_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::BINARY_PRED);

    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto slot_ref = std::make_shared<collection_statistics::MockVSlotRef>("content", SlotId(1));
    auto literal = std::make_shared<collection_statistics::MockVLiteral>("nested term");
    match_expr->_children.push_back(slot_ref);
    match_expr->_children.push_back(literal);

    auto other_expr =
            std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::BINARY_PRED);

    and_expr->_children.push_back(match_expr);
    and_expr->_children.push_back(other_expr);

    contexts.push_back(std::make_shared<VExprContext>(and_expr));

    std::vector<RowSetSplits> empty_splits;

    auto status =
            stats_->collect(runtime_state_.get(), empty_splits, tablet_schema, contexts, nullptr);
    EXPECT_TRUE(status.ok()) << status.msg();
}

TEST_F(CollectionStatisticsTest, CollectWithMockRowsetSplits) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();
    auto expr_contexts = create_match_expr_contexts();

    auto splits = create_mock_rowset_splits(2);

    auto status =
            stats_->collect(runtime_state_.get(), splits, tablet_schema, expr_contexts, nullptr);

    EXPECT_TRUE(status.ok());
}

TEST_F(CollectionStatisticsTest, CollectWithEmptySegments) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();
    auto expr_contexts = create_match_expr_contexts();

    auto splits = create_mock_rowset_splits(0);

    auto status =
            stats_->collect(runtime_state_.get(), splits, tablet_schema, expr_contexts, nullptr);
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

    auto status =
            stats_->collect(runtime_state_.get(), splits, tablet_schema, expr_contexts, nullptr);
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

TEST_F(CollectionStatisticsTest, CollectWithCastWrappedSlotRef) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();

    VExprContextSPtrs contexts;

    // match_pred(left: CAST(slot_ref), right: literal)
    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto cast_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::CAST_EXPR);
    auto slot_ref = std::make_shared<collection_statistics::MockVSlotRef>("content", SlotId(1));
    auto literal = std::make_shared<collection_statistics::MockVLiteral>("cast term");

    cast_expr->_children.push_back(slot_ref);
    match_expr->_children.push_back(cast_expr);
    match_expr->_children.push_back(literal);

    contexts.push_back(std::make_shared<VExprContext>(match_expr));

    std::vector<RowSetSplits> empty_splits;
    auto status =
            stats_->collect(runtime_state_.get(), empty_splits, tablet_schema, contexts, nullptr);
    EXPECT_TRUE(status.ok()) << status.msg();
}

TEST_F(CollectionStatisticsTest, CollectWithDoubleCastWrappedSlotRef) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();

    VExprContextSPtrs contexts;

    // match_pred(left: CAST(CAST(slot_ref)), right: literal)
    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto outer_cast = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::CAST_EXPR);
    auto inner_cast = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::CAST_EXPR);
    auto slot_ref = std::make_shared<collection_statistics::MockVSlotRef>("content", SlotId(1));
    auto literal = std::make_shared<collection_statistics::MockVLiteral>("double cast term");

    inner_cast->_children.push_back(slot_ref);
    outer_cast->_children.push_back(inner_cast);
    match_expr->_children.push_back(outer_cast);
    match_expr->_children.push_back(literal);

    contexts.push_back(std::make_shared<VExprContext>(match_expr));

    std::vector<RowSetSplits> empty_splits;
    auto status =
            stats_->collect(runtime_state_.get(), empty_splits, tablet_schema, contexts, nullptr);
    EXPECT_TRUE(status.ok()) << status.msg();
}

// Regression for AIR-36: match score collection must resolve indexes for
// variant sub-columns whose indexes live in _path_set_info_map (typed paths or
// inherited sub-column indexes). The previous simple lookup using
// inverted_indexs(col_unique_id, suffix_path) missed those indexes.
TEST_F(CollectionStatisticsTest, ExtractCollectInfoForVariantSubcolumnIndex) {
    auto tablet_schema = std::make_shared<TabletSchema>();

    constexpr int32_t kVariantUid = 9001;

    TabletColumn variant_col;
    variant_col.set_unique_id(kVariantUid);
    variant_col.set_name("v");
    variant_col.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    tablet_schema->append_column(variant_col);

    TabletColumn sub_col;
    sub_col.set_unique_id(-1);
    sub_col.set_name("v.host");
    sub_col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    sub_col.set_parent_unique_id(kVariantUid);
    PathInData path("v.host");
    sub_col.set_path_info(path);
    tablet_schema->append_column(sub_col);

    auto sub_index = std::make_shared<TabletIndex>();
    TabletIndexPB index_pb;
    index_pb.set_index_id(2001);
    index_pb.set_index_name("variant_subcolumn_idx");
    index_pb.set_index_type(IndexType::INVERTED);
    index_pb.add_col_unique_id(kVariantUid);
    auto* props = index_pb.mutable_properties();
    (*props)["parser"] = "standard";
    (*props)["support_phrase"] = "true";
    sub_index->init_from_pb(index_pb);

    TabletSchema::PathsSetInfo path_set_info;
    TabletIndexes sub_indexes = {sub_index};
    path_set_info.subcolumn_indexes["host"] = sub_indexes;
    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> path_set_info_map;
    path_set_info_map[kVariantUid] = std::move(path_set_info);
    tablet_schema->set_path_set_info(std::move(path_set_info_map));

    EXPECT_TRUE(tablet_schema->inverted_indexs(kVariantUid, "host").empty());

    auto found = tablet_schema->inverted_indexs(tablet_schema->column(/*ordinal=*/1));
    ASSERT_EQ(found.size(), 1u);
    EXPECT_EQ(found[0]->index_name(), "variant_subcolumn_idx");

    constexpr int kSlotId = 42;
    runtime_state_->_mock_desc_tbl->add_slot_descriptor(SlotId(kSlotId), kVariantUid);

    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto slot_ref =
            std::make_shared<collection_statistics::MockVSlotRef>("v.host", SlotId(kSlotId));
    auto literal = std::make_shared<collection_statistics::MockVLiteral>("foo");
    match_expr->_children.push_back(slot_ref);
    match_expr->_children.push_back(literal);

    VExprContextSPtrs contexts;
    contexts.push_back(std::make_shared<VExprContext>(match_expr));

    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    auto status = stats_->extract_collect_info(runtime_state_.get(), contexts, tablet_schema,
                                               &collect_infos);
    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    auto it = collect_infos.find(StringHelper::to_wstring(std::to_string(kVariantUid) + ".v.host"));
    ASSERT_NE(it, collect_infos.end());
    ASSERT_NE(it->second.index_meta, nullptr);
    EXPECT_EQ(it->second.index_meta->index_name(), "variant_subcolumn_idx");
}

// Regression for score on a dynamic variant sub-column inherited from a plain
// parent variant inverted index (no field_pattern template). Matches the
// scan-time schema shape: _init_variant_columns materializes the accessed
// path as an extracted VARIANT placeholder, so neither inverted_indexs(column)
// nor generate_sub_column_info resolves the parent index. Collector clones
// the parent's non-field-pattern indexes with the variant path as suffix.
TEST_F(CollectionStatisticsTest, ExtractCollectInfoForVariantParentIndexWithoutTemplate) {
    auto tablet_schema = std::make_shared<TabletSchema>();

    constexpr int32_t kVariantUid = 9004;

    TabletColumn variant_col;
    variant_col.set_unique_id(kVariantUid);
    variant_col.set_name("v");
    variant_col.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    tablet_schema->append_column(variant_col);

    TabletColumn sub_col;
    sub_col.set_unique_id(-1);
    sub_col.set_name("v.key");
    sub_col.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    sub_col.set_parent_unique_id(kVariantUid);
    PathInData path("v.key");
    sub_col.set_path_info(path);
    tablet_schema->append_column(sub_col);

    TabletIndexPB index_pb;
    index_pb.set_index_id(2004);
    index_pb.set_index_name("variant_parent_idx");
    index_pb.set_index_type(IndexType::INVERTED);
    index_pb.add_col_unique_id(kVariantUid);
    auto* props = index_pb.mutable_properties();
    (*props)["parser"] = "english";
    (*props)["support_phrase"] = "true";

    TabletIndex index;
    index.init_from_pb(index_pb);
    tablet_schema->append_index(std::move(index));

    // Pre-conditions: column-aware lookup is empty (no inheritance pre-populated)
    // and generate_sub_column_info returns false (no field_pattern template).
    // The collector must still resolve through the VARIANT-placeholder branch.
    ASSERT_TRUE(tablet_schema->inverted_indexs(tablet_schema->column(/*ordinal=*/1)).empty());
    ASSERT_EQ(tablet_schema->inverted_indexs(kVariantUid).size(), 1u);
    TabletSchema::SubColumnInfo sub_column_info;
    ASSERT_FALSE(variant_util::generate_sub_column_info(*tablet_schema, kVariantUid, "key",
                                                        &sub_column_info));

    constexpr int kSlotId = 45;
    runtime_state_->_mock_desc_tbl->add_slot_descriptor(SlotId(kSlotId), kVariantUid, "v.key",
                                                        {"key"});

    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto cast_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::CAST_EXPR);
    cast_expr->_data_type = std::make_shared<DataTypeString>();
    auto slot_ref = std::make_shared<collection_statistics::MockVSlotRef>("v.key", SlotId(kSlotId));
    auto literal = std::make_shared<collection_statistics::MockVLiteral>("abc");
    cast_expr->_children.push_back(slot_ref);
    match_expr->_children.push_back(cast_expr);
    match_expr->_children.push_back(literal);

    VExprContextSPtrs contexts;
    contexts.push_back(std::make_shared<VExprContext>(match_expr));

    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    auto status = stats_->extract_collect_info(runtime_state_.get(), contexts, tablet_schema,
                                               &collect_infos);
    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    auto it = collect_infos.find(StringHelper::to_wstring(std::to_string(kVariantUid) + ".v.key"));
    ASSERT_NE(it, collect_infos.end());
    ASSERT_NE(it->second.index_meta, nullptr);
    ASSERT_NE(it->second.owned_index_meta, nullptr);
    EXPECT_EQ(it->second.index_meta->index_name(), "variant_parent_idx");
}

namespace {

// Build a sub-column template for the parent variant column. pattern_type has no
// public setter on TabletColumn, so construct through ColumnPB.
TabletColumn make_subcolumn_template(const std::string& pattern, PatternTypePB pattern_type) {
    ColumnPB column_pb;
    column_pb.set_unique_id(-1);
    column_pb.set_name(pattern);
    column_pb.set_type("STRING");
    column_pb.set_is_nullable(true);
    column_pb.set_pattern_type(pattern_type);

    TabletColumn templ;
    templ.init_from_pb(column_pb);
    return templ;
}

} // namespace

TEST_F(CollectionStatisticsTest, ExtractCollectInfoForVariantFieldPatternIndex) {
    auto tablet_schema = std::make_shared<TabletSchema>();

    constexpr int32_t kVariantUid = 9002;

    TabletColumn variant_col;
    variant_col.set_unique_id(kVariantUid);
    variant_col.set_name("meta");
    variant_col.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    TabletColumn host_template = make_subcolumn_template("host", PatternTypePB::MATCH_NAME);
    variant_col.add_sub_column(host_template);
    tablet_schema->append_column(variant_col);

    TabletColumn sub_col;
    sub_col.set_unique_id(-1);
    sub_col.set_name("meta.host");
    sub_col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    sub_col.set_parent_unique_id(kVariantUid);
    PathInData path("meta.host");
    sub_col.set_path_info(path);
    tablet_schema->append_column(sub_col);

    TabletIndexPB index_pb;
    index_pb.set_index_id(2002);
    index_pb.set_index_name("variant_field_pattern_idx");
    index_pb.set_index_type(IndexType::INVERTED);
    index_pb.add_col_unique_id(kVariantUid);
    auto* props = index_pb.mutable_properties();
    (*props)["parser"] = "standard";
    (*props)["support_phrase"] = "true";
    (*props)["field_pattern"] = "host";

    TabletIndex index;
    index.init_from_pb(index_pb);
    tablet_schema->append_index(std::move(index));

    ASSERT_TRUE(tablet_schema->inverted_indexs(tablet_schema->column(/*ordinal=*/1)).empty());
    ASSERT_EQ(tablet_schema->inverted_index_by_field_pattern(kVariantUid, "host").size(), 1u);

    constexpr int kSlotId = 43;
    runtime_state_->_mock_desc_tbl->add_slot_descriptor(SlotId(kSlotId), kVariantUid, "meta.host",
                                                        {"host"});

    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto slot_ref =
            std::make_shared<collection_statistics::MockVSlotRef>("meta.host", SlotId(kSlotId));
    auto literal = std::make_shared<collection_statistics::MockVLiteral>("alpha");
    match_expr->_children.push_back(slot_ref);
    match_expr->_children.push_back(literal);

    VExprContextSPtrs contexts;
    contexts.push_back(std::make_shared<VExprContext>(match_expr));

    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    auto status = stats_->extract_collect_info(runtime_state_.get(), contexts, tablet_schema,
                                               &collect_infos);
    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    auto it = collect_infos.find(
            StringHelper::to_wstring(std::to_string(kVariantUid) + ".meta.host"));
    ASSERT_NE(it, collect_infos.end());
    ASSERT_NE(it->second.index_meta, nullptr);
    ASSERT_NE(it->second.owned_index_meta, nullptr);
    EXPECT_EQ(it->second.index_meta->index_name(), "variant_field_pattern_idx");
}

// Regression: field_pattern="user.*" is registered under the pattern string,
// while the query slot resolves to column_paths=["user", "name"]. The fallback
// must match the parent variant's sub-column template first, then use the
// matched pattern to fetch the index, and collect under the actual Lucene field.
TEST_F(CollectionStatisticsTest, ExtractCollectInfoForVariantFieldPatternGlobIndex) {
    auto tablet_schema = std::make_shared<TabletSchema>();

    constexpr int32_t kVariantUid = 9003;

    TabletColumn variant_col;
    variant_col.set_unique_id(kVariantUid);
    variant_col.set_name("meta");
    variant_col.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    TabletColumn glob_template = make_subcolumn_template("user.*", PatternTypePB::MATCH_NAME_GLOB);
    variant_col.add_sub_column(glob_template);
    tablet_schema->append_column(variant_col);

    TabletColumn sub_col;
    sub_col.set_unique_id(-1);
    sub_col.set_name("meta.user.name");
    sub_col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    sub_col.set_parent_unique_id(kVariantUid);
    PathInData path("meta.user.name");
    sub_col.set_path_info(path);
    tablet_schema->append_column(sub_col);

    TabletIndexPB index_pb;
    index_pb.set_index_id(2003);
    index_pb.set_index_name("variant_field_pattern_glob_idx");
    index_pb.set_index_type(IndexType::INVERTED);
    index_pb.add_col_unique_id(kVariantUid);
    auto* props = index_pb.mutable_properties();
    (*props)["parser"] = "standard";
    (*props)["support_phrase"] = "true";
    (*props)["field_pattern"] = "user.*";

    TabletIndex index;
    index.init_from_pb(index_pb);
    tablet_schema->append_index(std::move(index));

    ASSERT_TRUE(tablet_schema->inverted_indexs(tablet_schema->column(/*ordinal=*/1)).empty());
    ASSERT_TRUE(tablet_schema->inverted_index_by_field_pattern(kVariantUid, "user.name").empty());
    ASSERT_EQ(tablet_schema->inverted_index_by_field_pattern(kVariantUid, "user.*").size(), 1u);
    TabletSchema::SubColumnInfo sub_column_info;
    ASSERT_TRUE(variant_util::generate_sub_column_info(*tablet_schema, kVariantUid, "user.name",
                                                       &sub_column_info));
    ASSERT_EQ(sub_column_info.indexes.size(), 1u);
    EXPECT_EQ(sub_column_info.column.suffix_path(), "meta.user.name");
    EXPECT_EQ(sub_column_info.indexes[0]->index_name(), "variant_field_pattern_glob_idx");

    constexpr int kSlotId = 44;
    runtime_state_->_mock_desc_tbl->add_slot_descriptor(SlotId(kSlotId), kVariantUid,
                                                        "meta.user.name", {"user", "name"});

    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto slot_ref = std::make_shared<collection_statistics::MockVSlotRef>("meta.user.name",
                                                                          SlotId(kSlotId));
    auto literal = std::make_shared<collection_statistics::MockVLiteral>("alice");
    match_expr->_children.push_back(slot_ref);
    match_expr->_children.push_back(literal);

    VExprContextSPtrs contexts;
    contexts.push_back(std::make_shared<VExprContext>(match_expr));

    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    auto status = stats_->extract_collect_info(runtime_state_.get(), contexts, tablet_schema,
                                               &collect_infos);
    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    auto it = collect_infos.find(
            StringHelper::to_wstring(std::to_string(kVariantUid) + ".meta.user.name"));
    ASSERT_NE(it, collect_infos.end());
    ASSERT_NE(it->second.index_meta, nullptr);
    ASSERT_NE(it->second.owned_index_meta, nullptr);
    EXPECT_EQ(it->second.index_meta->index_name(), "variant_field_pattern_glob_idx");
}

// E1: Match predicate whose left subtree contains no VSlotRef.
// find_slot_ref recurses through children; when it returns nullptr the
// collector reports INVERTED_INDEX_NOT_SUPPORTED.
// Calls MatchPredicateCollector::collect() directly so coverage attribution
// is not muddied by extract_collect_info's virtual-dispatch indirection.
TEST_F(CollectionStatisticsTest, CollectMissingSlotRefReturnsError) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    TabletColumn col;
    col.set_unique_id(1001);
    col.set_name("c");
    col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    tablet_schema->append_column(col);

    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto literal_left = std::make_shared<collection_statistics::MockVLiteral>("foo");
    auto literal_right = std::make_shared<collection_statistics::MockVLiteral>("bar");
    match_expr->_children.push_back(literal_left);
    match_expr->_children.push_back(literal_right);

    MatchPredicateCollector collector;
    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    auto status =
            collector.collect(runtime_state_.get(), tablet_schema, match_expr, &collect_infos);
    ASSERT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
    EXPECT_TRUE(status.msg().find("Cannot find slot reference") != std::string::npos);
}

// E2: SlotRef points to a slot_id absent from the runtime descriptor table.
TEST_F(CollectionStatisticsTest, CollectMissingSlotDescriptorReturnsError) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    TabletColumn col;
    col.set_unique_id(1002);
    col.set_name("c");
    col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    tablet_schema->append_column(col);

    constexpr int kAbsentSlotId = 99999;

    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto slot_ref =
            std::make_shared<collection_statistics::MockVSlotRef>("c", SlotId(kAbsentSlotId));
    auto literal = std::make_shared<collection_statistics::MockVLiteral>("v");
    match_expr->_children.push_back(slot_ref);
    match_expr->_children.push_back(literal);

    MatchPredicateCollector collector;
    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    auto status =
            collector.collect(runtime_state_.get(), tablet_schema, match_expr, &collect_infos);
    ASSERT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
    EXPECT_TRUE(status.msg().find("Cannot find slot descriptor") != std::string::npos);
}

// E3: SlotRef name does not exist in tablet_schema (field_index returns -1).
TEST_F(CollectionStatisticsTest, CollectUnknownColumnNameReturnsError) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    TabletColumn col;
    col.set_unique_id(1003);
    col.set_name("declared");
    col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    tablet_schema->append_column(col);

    constexpr int kSlotId = 50;
    runtime_state_->_mock_desc_tbl->add_slot_descriptor(SlotId(kSlotId), 1003, "missing", {});

    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto slot_ref =
            std::make_shared<collection_statistics::MockVSlotRef>("missing", SlotId(kSlotId));
    auto literal = std::make_shared<collection_statistics::MockVLiteral>("v");
    match_expr->_children.push_back(slot_ref);
    match_expr->_children.push_back(literal);

    MatchPredicateCollector collector;
    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    auto status =
            collector.collect(runtime_state_.get(), tablet_schema, match_expr, &collect_infos);
    ASSERT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
    EXPECT_TRUE(status.msg().find("Cannot find column index") != std::string::npos);
}

// I1 + L3 + O1: Plain string column with a direct inverted index.
// Direct hit produces a CollectInfo whose owned_index_meta is null
// (the meta lives in the schema and is not cloned).
TEST_F(CollectionStatisticsTest, CollectDirectIndexHitFromSchema) {
    auto tablet_schema = std::make_shared<TabletSchema>();

    constexpr int32_t kColUid = 1100;
    TabletColumn col;
    col.set_unique_id(kColUid);
    col.set_name("note");
    col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    tablet_schema->append_column(col);

    TabletIndexPB index_pb;
    index_pb.set_index_id(2100);
    index_pb.set_index_name("note_idx");
    index_pb.set_index_type(IndexType::INVERTED);
    index_pb.add_col_unique_id(kColUid);
    auto* props = index_pb.mutable_properties();
    (*props)["parser"] = "english";
    (*props)["support_phrase"] = "true";
    TabletIndex index;
    index.init_from_pb(index_pb);
    tablet_schema->append_index(std::move(index));

    constexpr int kSlotId = 60;
    runtime_state_->_mock_desc_tbl->add_slot_descriptor(SlotId(kSlotId), kColUid, "note", {});

    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto slot_ref = std::make_shared<collection_statistics::MockVSlotRef>("note", SlotId(kSlotId));
    auto literal = std::make_shared<collection_statistics::MockVLiteral>("hello world");
    match_expr->_children.push_back(slot_ref);
    match_expr->_children.push_back(literal);

    MatchPredicateCollector collector;
    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    auto status =
            collector.collect(runtime_state_.get(), tablet_schema, match_expr, &collect_infos);
    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    auto it = collect_infos.find(StringHelper::to_wstring(std::to_string(kColUid)));
    ASSERT_NE(it, collect_infos.end());
    EXPECT_NE(it->second.index_meta, nullptr);
    EXPECT_EQ(it->second.owned_index_meta, nullptr); // O1: schema-direct meta is not owned
    EXPECT_FALSE(it->second.term_infos.empty());
}

// I2: Plain string column with no index and not an extracted variant
// sub-column. Fallback path does not apply (column.is_extracted_column()
// is false). In BE_TEST builds the empty-index check is skipped, so
// collect returns OK with no CollectInfo emitted.
TEST_F(CollectionStatisticsTest, CollectNotExtractedColumnSkipsFallback) {
    auto tablet_schema = std::make_shared<TabletSchema>();

    constexpr int32_t kColUid = 1200;
    TabletColumn col;
    col.set_unique_id(kColUid);
    col.set_name("plain");
    col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    tablet_schema->append_column(col);
    // no index appended

    constexpr int kSlotId = 70;
    runtime_state_->_mock_desc_tbl->add_slot_descriptor(SlotId(kSlotId), kColUid, "plain", {});

    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto slot_ref = std::make_shared<collection_statistics::MockVSlotRef>("plain", SlotId(kSlotId));
    auto literal = std::make_shared<collection_statistics::MockVLiteral>("v");
    match_expr->_children.push_back(slot_ref);
    match_expr->_children.push_back(literal);

    MatchPredicateCollector collector;
    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    auto status =
            collector.collect(runtime_state_.get(), tablet_schema, match_expr, &collect_infos);
    ASSERT_TRUE(status.ok()) << status.msg();
    EXPECT_TRUE(collect_infos.empty());
}

// L1: Index whose properties do not request an analyzer
// (should_analyzer returns false). The matching index_meta is iterated
// but skipped before insertion.
TEST_F(CollectionStatisticsTest, CollectSkipsIndexWithoutAnalyzer) {
    auto tablet_schema = std::make_shared<TabletSchema>();

    constexpr int32_t kColUid = 1300;
    TabletColumn col;
    col.set_unique_id(kColUid);
    col.set_name("kw");
    col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    tablet_schema->append_column(col);

    TabletIndexPB index_pb;
    index_pb.set_index_id(2300);
    index_pb.set_index_name("kw_idx");
    index_pb.set_index_type(IndexType::INVERTED);
    index_pb.add_col_unique_id(kColUid);
    // No "parser" property -> should_analyzer returns false
    TabletIndex index;
    index.init_from_pb(index_pb);
    tablet_schema->append_index(std::move(index));

    constexpr int kSlotId = 80;
    runtime_state_->_mock_desc_tbl->add_slot_descriptor(SlotId(kSlotId), kColUid, "kw", {});

    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto slot_ref = std::make_shared<collection_statistics::MockVSlotRef>("kw", SlotId(kSlotId));
    auto literal = std::make_shared<collection_statistics::MockVLiteral>("v");
    match_expr->_children.push_back(slot_ref);
    match_expr->_children.push_back(literal);

    MatchPredicateCollector collector;
    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    auto status =
            collector.collect(runtime_state_.get(), tablet_schema, match_expr, &collect_infos);
    ASSERT_TRUE(status.ok()) << status.msg();
    EXPECT_TRUE(collect_infos.empty());
}

// L2: Index whose analyzer is set (should_analyzer returns true) but does
// not declare "support_phrase=true". MockVExpr drives MATCH_PHRASE opcode,
// so is_need_similarity_score returns false and the index is skipped.
TEST_F(CollectionStatisticsTest, CollectSkipsIndexWithoutSimilarityScore) {
    auto tablet_schema = std::make_shared<TabletSchema>();

    constexpr int32_t kColUid = 1350;
    TabletColumn col;
    col.set_unique_id(kColUid);
    col.set_name("body");
    col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    tablet_schema->append_column(col);

    TabletIndexPB index_pb;
    index_pb.set_index_id(2350);
    index_pb.set_index_name("body_idx");
    index_pb.set_index_type(IndexType::INVERTED);
    index_pb.add_col_unique_id(kColUid);
    auto* props = index_pb.mutable_properties();
    (*props)["parser"] = "english"; // should_analyzer == true
    // Intentionally omit "support_phrase" -> is_need_similarity_score == false
    TabletIndex index;
    index.init_from_pb(index_pb);
    tablet_schema->append_index(std::move(index));

    constexpr int kSlotId = 85;
    runtime_state_->_mock_desc_tbl->add_slot_descriptor(SlotId(kSlotId), kColUid, "body", {});

    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    auto slot_ref = std::make_shared<collection_statistics::MockVSlotRef>("body", SlotId(kSlotId));
    auto literal = std::make_shared<collection_statistics::MockVLiteral>("hello");
    match_expr->_children.push_back(slot_ref);
    match_expr->_children.push_back(literal);

    MatchPredicateCollector collector;
    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    auto status =
            collector.collect(runtime_state_.get(), tablet_schema, match_expr, &collect_infos);
    ASSERT_TRUE(status.ok()) << status.msg();
    EXPECT_TRUE(collect_infos.empty());
}

// L4: Two MATCH predicates on the same column produce CollectInfo entries
// keyed on the same field_name; the second insertion merges term_infos
// into the first entry.
TEST_F(CollectionStatisticsTest, CollectMergesTermsForSameFieldName) {
    auto tablet_schema = std::make_shared<TabletSchema>();

    constexpr int32_t kColUid = 1400;
    TabletColumn col;
    col.set_unique_id(kColUid);
    col.set_name("doc");
    col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    tablet_schema->append_column(col);

    TabletIndexPB index_pb;
    index_pb.set_index_id(2400);
    index_pb.set_index_name("doc_idx");
    index_pb.set_index_type(IndexType::INVERTED);
    index_pb.add_col_unique_id(kColUid);
    auto* props = index_pb.mutable_properties();
    (*props)["parser"] = "english";
    (*props)["support_phrase"] = "true";
    TabletIndex index;
    index.init_from_pb(index_pb);
    tablet_schema->append_index(std::move(index));

    constexpr int kSlotId = 90;
    runtime_state_->_mock_desc_tbl->add_slot_descriptor(SlotId(kSlotId), kColUid, "doc", {});

    auto build_match = [&](const std::string& term) {
        auto m = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
        auto s = std::make_shared<collection_statistics::MockVSlotRef>("doc", SlotId(kSlotId));
        auto l = std::make_shared<collection_statistics::MockVLiteral>(term);
        m->_children.push_back(s);
        m->_children.push_back(l);
        return m;
    };

    MatchPredicateCollector collector;
    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    auto first = collector.collect(runtime_state_.get(), tablet_schema, build_match("alpha"),
                                   &collect_infos);
    ASSERT_TRUE(first.ok()) << first.msg();
    auto second = collector.collect(runtime_state_.get(), tablet_schema, build_match("beta"),
                                    &collect_infos);
    ASSERT_TRUE(second.ok()) << second.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    auto it = collect_infos.find(StringHelper::to_wstring(std::to_string(kColUid)));
    ASSERT_NE(it, collect_infos.end());
    EXPECT_GE(it->second.term_infos.size(), 2u); // both "alpha" and "beta" present
}

// Test-only subclass that exposes the protected helpers of PredicateCollector.
class TestablePredicateCollector : public MatchPredicateCollector {
public:
    using MatchPredicateCollector::build_field_name;
    using MatchPredicateCollector::find_slot_ref;
};

// find_slot_ref: null shared_ptr returns nullptr (early-return branch).
TEST_F(CollectionStatisticsTest, FindSlotRefHandlesNullExpr) {
    TestablePredicateCollector collector;
    VExprSPtr null_expr;
    EXPECT_EQ(collector.find_slot_ref(null_expr), nullptr);
}

// find_slot_ref: when expr is a non-CAST wrapper containing a SLOT_REF in its
// children, the recursive descent finds the slot via the for-loop body.
TEST_F(CollectionStatisticsTest, FindSlotRefRecursesIntoChildren) {
    TestablePredicateCollector collector;
    auto wrapper = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::FUNCTION_CALL);
    auto slot_ref = std::make_shared<collection_statistics::MockVSlotRef>("c", SlotId(99));
    wrapper->_children.push_back(slot_ref);
    EXPECT_EQ(collector.find_slot_ref(wrapper), slot_ref.get());
}

// find_slot_ref: leaf non-slot (no children) returns nullptr after for-loop.
TEST_F(CollectionStatisticsTest, FindSlotRefReturnsNullForLeafNonSlot) {
    TestablePredicateCollector collector;
    auto literal = std::make_shared<collection_statistics::MockVLiteral>("x");
    EXPECT_EQ(collector.find_slot_ref(literal), nullptr);
}

// build_field_name: non-empty suffix is appended with a dot separator.
TEST_F(CollectionStatisticsTest, BuildFieldNameWithSuffix) {
    TestablePredicateCollector collector;
    EXPECT_EQ(collector.build_field_name(42, "a.b"), "42.a.b");
}

// build_field_name: empty suffix returns just the unique id as string.
TEST_F(CollectionStatisticsTest, BuildFieldNameWithoutSuffix) {
    TestablePredicateCollector collector;
    EXPECT_EQ(collector.build_field_name(42, ""), "42");
}

TEST(TermInfoComparerTest, OrdersByTermAndDedups) {
    using doris::TermInfoComparer;
    using doris::segment_v2::TermInfo;

    std::set<TermInfo, TermInfoComparer> terms;

    TermInfo t1;
    t1.term = std::string("banana");
    t1.position = 2;

    TermInfo t2;
    t2.term = std::string("apple");
    t2.position = 10;

    TermInfo t3;
    t3.term = std::string("cherry");
    t3.position = 1;

    TermInfo dup;
    dup.term = std::string("banana");
    dup.position = 100;

    terms.insert(t1);
    terms.insert(t2);
    terms.insert(t3);
    terms.insert(dup);

    std::vector<std::string> ordered;
    ordered.reserve(terms.size());
    for (const auto& t : terms) {
        ordered.push_back(t.get_single_term());
    }

    EXPECT_EQ(terms.size(), 3u);
    EXPECT_THAT(ordered, ::testing::ElementsAre("apple", "banana", "cherry"));
}

} // namespace doris
