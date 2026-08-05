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

#include "storage/index/inverted/similarity/collection_statistics.h"

#include <gen_cpp/Exprs_types.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <atomic>
#include <barrier>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/exception.h"
#include "core/data_type/data_type_string.h"
#include "exec/common/variant_util.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vsearch.h"
#include "exprs/vslot_ref.h"
#include "io/fs/local_file_system.h"
#include "runtime/exec_env.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_writer.h"
#include "storage/index/inverted/common_grams/common_grams_key_codec.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/similarity/collection_statistics.cpp"
#include "storage/index/snii/query/bm25_scorer.h"
#include "storage/index/snii/snii_doris_adapter.h"
#include "storage/index/snii/stats/snii_stats_provider.h"
#include "storage/index/snii/writer/snii_compound_writer.h"
#include "storage/index/snii_query_test_util.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/slice.h"

namespace doris {

namespace collection_statistics {

class MockVExpr : public VExpr {
public:
    MockVExpr(TExprNodeType::type node_type) : _mock_node_type(node_type) {
        if (node_type == TExprNodeType::MATCH_PRED) {
            _opcode = TExprOpcode::MATCH_PHRASE;
            InvertedIndexAnalyzerConfig config;
            config.parser_type = InvertedIndexParserType::PARSER_STANDARD;
            config.stop_words = "none";
            _analyzer_ctx = std::make_shared<InvertedIndexAnalyzerCtx>();
            _analyzer_ctx->analyzer_provider =
                    segment_v2::inverted_index::InvertedIndexAnalyzer::create_analyzer_provider(
                            &config);
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

    const InvertedIndexAnalyzerCtx* query_analyzer_ctx() const override {
        return _analyzer_ctx.get();
    }

    void set_analyzer_ctx(InvertedIndexAnalyzerCtxSPtr analyzer_ctx) {
        _analyzer_ctx = std::move(analyzer_ctx);
    }

    void set_opcode(TExprOpcode::type opcode) { _opcode = opcode; }

private:
    TExprNodeType::type _mock_node_type;
    InvertedIndexAnalyzerCtxSPtr _analyzer_ctx;
};

class FixedFingerprintAnalyzerProvider final : public segment_v2::inverted_index::AnalyzerProvider {
public:
    FixedFingerprintAnalyzerProvider(std::shared_ptr<lucene::analysis::Analyzer> analyzer,
                                     std::string fingerprint)
            : _analyzer(std::move(analyzer)), _fingerprint(std::move(fingerprint)) {}

    std::shared_ptr<lucene::analysis::Analyzer> get_analyzer(
            segment_v2::inverted_index::AnalysisPurpose) const override {
        return _analyzer;
    }

    std::string_view base_analyzer_fingerprint() const override { return _fingerprint; }

private:
    std::shared_ptr<lucene::analysis::Analyzer> _analyzer;
    std::string _fingerprint;
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
        _segment_path_requests.push_back(seg_id);
        if (_segment_paths.find(seg_id) != _segment_paths.end()) {
            return _segment_paths.at(seg_id);
        }
        return ResultError(Status::InternalError("Segment path not found"));
    }

    void set_segment_path(int64_t seg_id, const std::string& path) {
        _segment_paths[seg_id] = path;
    }

    void set_num_segments(int64_t num) { _num_segments = num; }

    const std::vector<int64_t>& segment_path_requests() const { return _segment_path_requests; }

private:
    int64_t _num_segments;
    std::map<int64_t, std::string> _segment_paths;
    std::vector<int64_t> _segment_path_requests;
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

    void set_topn_limit(size_t limit) override {}

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

    VExprContextSPtrs create_match_expr_contexts(
            const std::string& search_term = "search term",
            const std::string& base_analyzer_fingerprint = "") {
        VExprContextSPtrs contexts;

        auto match_expr =
                std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
        if (!base_analyzer_fingerprint.empty()) {
            auto analyzer = match_expr->query_analyzer_ctx()->analyzer_provider->get_analyzer(
                    segment_v2::inverted_index::AnalysisPurpose::kPlainQuery);
            auto provider =
                    std::make_shared<collection_statistics::FixedFingerprintAnalyzerProvider>(
                            std::move(analyzer), base_analyzer_fingerprint);
            auto analyzer_ctx = std::make_shared<InvertedIndexAnalyzerCtx>();
            analyzer_ctx->analyzer_provider = std::move(provider);
            match_expr->set_analyzer_ctx(std::move(analyzer_ctx));
        }
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

    TabletSchemaSPtr create_legacy_v3_schema() {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::V3);
        auto tablet_schema = std::make_shared<TabletSchema>();
        tablet_schema->init_from_pb(schema_pb);

        TabletColumn column;
        column.set_unique_id(1);
        column.set_name("content");
        column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
        tablet_schema->append_column(column);

        TabletIndex index;
        index._index_id = 1;
        index._index_type = IndexType::INVERTED;
        index._col_unique_ids.push_back(1);
        index._properties["parser"] = "standard";
        index._properties["support_phrase"] = "true";
        tablet_schema->append_index(std::move(index));
        return tablet_schema;
    }

    TabletSchemaSPtr create_snii_schema(int64_t index_id = 1) {
        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(DUP_KEYS);
        schema_pb.set_inverted_index_storage_format(InvertedIndexStorageFormatPB::SNII);
        auto tablet_schema = std::make_shared<TabletSchema>();
        tablet_schema->init_from_pb(schema_pb);

        TabletColumn column;
        column.set_unique_id(1);
        column.set_name("content");
        column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
        tablet_schema->append_column(column);

        TabletIndex index;
        index._index_id = index_id;
        index._index_type = IndexType::INVERTED;
        index._col_unique_ids.push_back(1);
        index._properties["parser"] = "standard";
        index._properties["support_phrase"] = "true";
        tablet_schema->append_index(std::move(index));
        return tablet_schema;
    }

    Status write_legacy_v3_segment(const TabletSchemaSPtr& tablet_schema,
                                   const std::string& segment_path) {
        std::vector<StorePath> paths;
        paths.emplace_back(test_dir_, 1024);
        auto tmp_file_dirs = std::make_unique<segment_v2::TmpFileDirs>(paths);
        RETURN_IF_ERROR(tmp_file_dirs->init());
        ExecEnv::GetInstance()->set_tmp_file_dir(std::move(tmp_file_dirs));

        const std::string index_path_prefix {
                segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(segment_path)};
        io::FileWriterPtr compound_file;
        io::FileWriterOptions options;
        auto fs = io::global_local_filesystem();
        RETURN_IF_ERROR(fs->create_file(
                segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix),
                &compound_file, &options));

        segment_v2::IndexFileWriter file_writer(fs, index_path_prefix, "legacy_v3", 0,
                                                InvertedIndexStorageFormatPB::V3,
                                                std::move(compound_file));
        const auto index_metas = tablet_schema->inverted_indexs(1);
        DORIS_CHECK(index_metas.size() == 1);
        std::unique_ptr<segment_v2::IndexColumnWriter> column_writer;
        RETURN_IF_ERROR(segment_v2::IndexColumnWriter::create(
                &tablet_schema->column(0), &column_writer, &file_writer, index_metas[0]));
        std::vector<Slice> values {Slice("alpha beta")};
        RETURN_IF_ERROR(column_writer->add_values("content", values.data(), values.size()));
        RETURN_IF_ERROR(column_writer->finish());
        RETURN_IF_ERROR(file_writer.begin_close());
        return file_writer.finish_close();
    }

    Status write_snii_common_grams_segment(const std::string& segment_path,
                                           std::string base_analyzer_fingerprint,
                                           uint64_t scoring_token_count = 3) {
        const std::string index_path_prefix {
                segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(segment_path)};
        io::FileWriterPtr file_writer;
        io::FileWriterOptions options;
        auto fs = io::global_local_filesystem();
        RETURN_IF_ERROR(fs->create_file(
                segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix),
                &file_writer, &options));

        segment_v2::snii_doris::DorisSniiFileWriter adapter(file_writer.get());
        snii::writer::SniiCompoundWriter writer(&adapter);
        auto metadata = segment_v2::inverted_index::make_common_grams_segment_metadata(
                {.common_grams_dictionary_identity = "test-stopwords-v1",
                 .base_analyzer_fingerprint = std::move(base_analyzer_fingerprint),
                 .common_grams_fingerprint = "test-common-grams-v1"});
        metadata.scoring_doc_count = 2;
        metadata.scoring_token_count = scoring_token_count;

        snii::writer::TermPostings alpha;
        alpha.term = "alpha";
        alpha.docids = {0, 1};
        alpha.freqs = {1, 1};
        alpha.positions_flat = {0, 0};

        snii::writer::TermPostings beta;
        beta.term = "beta";
        beta.docids = {0};
        beta.freqs = {1};
        beta.positions_flat = {1};

        snii::writer::TermPostings gram;
        gram.term = DORIS_TRY(segment_v2::inverted_index::encode_common_gram("alpha", "beta"));
        gram.docids = {0};
        gram.freqs = {1};
        gram.positions_flat = {0};

        snii::writer::SniiIndexInput input;
        input.index_id = 1;
        input.config = snii::format::IndexConfig::kDocsPositionsScoring;
        input.doc_count = 2;
        input.encoded_norms = {snii::query::encode_norm(2), snii::query::encode_norm(1)};
        input.terms = {std::move(gram), std::move(alpha), std::move(beta)};
        std::ranges::sort(input.terms, {}, &snii::writer::TermPostings::term);
        input.common_grams_metadata = std::move(metadata);

        RETURN_IF_ERROR(writer.add_logical_index(input));
        RETURN_IF_ERROR(writer.finish());
        return file_writer->close(false);
    }

    Status write_plain_snii_scoring_segment(const std::string& segment_path) {
        const std::string index_path_prefix {
                segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(segment_path)};
        io::FileWriterPtr file_writer;
        io::FileWriterOptions options;
        auto fs = io::global_local_filesystem();
        RETURN_IF_ERROR(fs->create_file(
                segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(index_path_prefix),
                &file_writer, &options));

        segment_v2::snii_doris::DorisSniiFileWriter adapter(file_writer.get());
        snii::writer::SniiCompoundWriter writer(&adapter);

        snii::writer::TermPostings alpha;
        alpha.term = "alpha";
        alpha.docids = {0, 1};
        alpha.freqs = {1, 1};
        alpha.positions_flat = {0, 0};

        snii::writer::TermPostings beta;
        beta.term = "beta";
        beta.docids = {0};
        beta.freqs = {1};
        beta.positions_flat = {1};

        snii::writer::SniiIndexInput input;
        input.index_id = 1;
        input.config = snii::format::IndexConfig::kDocsPositionsScoring;
        input.doc_count = 2;
        input.encoded_norms = {snii::query::encode_norm(2), snii::query::encode_norm(1)};
        input.terms = {std::move(alpha), std::move(beta)};

        RETURN_IF_ERROR(writer.add_logical_index(input));
        RETURN_IF_ERROR(writer.finish());
        return file_writer->close(false);
    }

    VExprContextSPtrs create_search_contexts(const std::string& clause_type,
                                             const std::string& value) {
        TSearchClause clause;
        clause.clause_type = clause_type;
        clause.field_name = "content";
        clause.value = value;
        clause.__isset.field_name = true;
        clause.__isset.value = true;

        return create_search_contexts(std::move(clause));
    }

    VExprContextSPtrs create_search_contexts(TSearchClause root,
                                             std::vector<TSearchFieldBinding> field_bindings = {}) {
        TSearchParam search_param;
        search_param.root = std::move(root);
        search_param.field_bindings = std::move(field_bindings);

        TExprNode node;
        node.node_type = TExprNodeType::SEARCH_EXPR;
        TTypeNode type_node;
        type_node.type = TTypeNodeType::SCALAR;
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::BOOLEAN);
        type_node.__set_scalar_type(scalar_type);
        TTypeDesc type_desc;
        type_desc.types.push_back(type_node);
        node.__set_type(type_desc);
        node.search_param = std::move(search_param);
        node.__isset.search_param = true;

        return {std::make_shared<VExprContext>(VSearchExpr::create_shared(node))};
    }

    TabletSchemaSPtr create_tablet_schema_with_keyword_and_fulltext_indexes() {
        auto tablet_schema = std::make_shared<TabletSchema>();

        TabletColumn column;
        column.set_unique_id(1);
        column.set_name("content");
        column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
        tablet_schema->append_column(column);

        TabletIndex keyword_index;
        keyword_index._index_id = 10;
        keyword_index._index_type = IndexType::INVERTED;
        keyword_index._col_unique_ids.push_back(1);
        tablet_schema->append_index(std::move(keyword_index));

        TabletIndex fulltext_index;
        fulltext_index._index_id = 20;
        fulltext_index._index_type = IndexType::INVERTED;
        fulltext_index._col_unique_ids.push_back(1);
        fulltext_index._properties["parser"] = "standard";
        fulltext_index._properties["support_phrase"] = "true";
        tablet_schema->append_index(std::move(fulltext_index));

        return tablet_schema;
    }

    TabletSchemaSPtr create_array_tablet_schema_with_keyword_and_fulltext_indexes() {
        auto tablet_schema = std::make_shared<TabletSchema>();

        TabletColumn item;
        item.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
        TabletColumn column;
        column.set_unique_id(1);
        column.set_name("content");
        column.set_type(FieldType::OLAP_FIELD_TYPE_ARRAY);
        column.add_sub_column(item);
        tablet_schema->append_column(column);

        TabletIndex keyword_index;
        keyword_index._index_id = 10;
        keyword_index._index_type = IndexType::INVERTED;
        keyword_index._col_unique_ids.push_back(1);
        tablet_schema->append_index(std::move(keyword_index));

        TabletIndex fulltext_index;
        fulltext_index._index_id = 20;
        fulltext_index._index_type = IndexType::INVERTED;
        fulltext_index._col_unique_ids.push_back(1);
        fulltext_index._properties["parser"] = "standard";
        fulltext_index._properties["support_phrase"] = "true";
        tablet_schema->append_index(std::move(fulltext_index));

        return tablet_schema;
    }

    TabletSchemaSPtr create_tablet_schema_with_two_fulltext_indexes() {
        auto tablet_schema = std::make_shared<TabletSchema>();

        TabletColumn column;
        column.set_unique_id(1);
        column.set_name("content");
        column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
        tablet_schema->append_column(column);

        for (const auto& [index_id, parser] : {std::pair<int64_t, std::string> {10, "standard"},
                                               std::pair<int64_t, std::string> {20, "english"}}) {
            TabletIndex index;
            index._index_id = index_id;
            index._index_type = IndexType::INVERTED;
            index._col_unique_ids.push_back(1);
            index._properties["parser"] = parser;
            index._properties["support_phrase"] = "true";
            tablet_schema->append_index(std::move(index));
        }

        return tablet_schema;
    }

    VExprContextSPtrs create_reserved_exact_search_contexts() {
        return create_search_contexts(
                "EXACT", std::string(segment_v2::inverted_index::CG_V1_MARKER) + "user");
    }

    void expect_no_collected_tokens(const std::wstring& field_name) {
        EXPECT_THROW(stats_->get_total_term_cnt_by_col(field_name), Exception);
    }

    void expect_collected_tokens(const std::wstring& field_name, uint64_t token_count) {
        EXPECT_EQ(stats_->get_total_term_cnt_by_col(field_name), token_count);
    }

    void expect_no_collected_term(const std::wstring& field_name, const std::wstring& term) {
        EXPECT_THROW(stats_->get_term_doc_freq_by_col(field_name, term), Exception);
    }

    void expect_collected_term(const std::wstring& field_name, const std::wstring& term,
                               uint64_t doc_frequency) {
        EXPECT_EQ(stats_->get_term_doc_freq_by_col(field_name, term), doc_frequency);
    }

    struct SniiScoringFieldInput {
        SniiScoringFieldInput(
                std::wstring field_name,
                std::optional<segment_v2::inverted_index::CommonGramsSegmentMetadata> metadata,
                uint64_t index_doc_count, bool has_semantic_norms,
                std::optional<std::string> expected_base_analyzer_fingerprint = std::nullopt)
                : field_name(std::move(field_name)),
                  metadata(std::move(metadata)),
                  index_doc_count(index_doc_count),
                  physical_sum_total_term_freq(this->metadata ? this->metadata->scoring_token_count
                                                              : 0),
                  has_semantic_norms(has_semantic_norms),
                  expected_base_analyzer_fingerprint(
                          std::move(expected_base_analyzer_fingerprint)) {}

        std::wstring field_name;
        std::optional<segment_v2::inverted_index::CommonGramsSegmentMetadata> metadata;
        uint64_t index_doc_count = 0;
        uint64_t physical_sum_total_term_freq = 0;
        bool has_scoring_tier = true;
        bool has_positions = true;
        bool has_semantic_norms = false;
        std::optional<std::string> expected_base_analyzer_fingerprint;
    };

    Status stage_snii_fields_for_test(
            CollectionStatistics* statistics, const std::vector<SniiScoringFieldInput>& fields,
            CollectionStatistics::SniiScoringSegmentAccumulator* segment_accumulator) {
        for (const auto& field : fields) {
            segment_v2::inverted_index::PlainTermKeyVersion key_version;
            const std::string_view expected_base_analyzer_fingerprint =
                    field.expected_base_analyzer_fingerprint.has_value()
                            ? *field.expected_base_analyzer_fingerprint
                    : field.metadata.has_value()
                            ? std::string_view(field.metadata->base_analyzer_fingerprint)
                            : std::string_view();
            RETURN_IF_ERROR(statistics->admit_snii_scoring_segment(
                    field.field_name, field.metadata, expected_base_analyzer_fingerprint,
                    field.index_doc_count, field.physical_sum_total_term_freq,
                    field.has_scoring_tier, field.has_positions, field.has_semantic_norms,
                    &key_version, segment_accumulator));
        }
        return Status::OK();
    }

    Status admit_snii_fields_for_test(CollectionStatistics* statistics,
                                      const std::vector<SniiScoringFieldInput>& fields) {
        CollectionStatistics::SniiScoringSegmentAccumulator segment_accumulator;
        RETURN_IF_ERROR(stage_snii_fields_for_test(statistics, fields, &segment_accumulator));
        statistics->commit_snii_scoring_segment(std::move(segment_accumulator));
        return Status::OK();
    }

    Status admit_snii_segment_for_test(
            CollectionStatistics* statistics, const std::wstring& field_name,
            const std::optional<segment_v2::inverted_index::CommonGramsSegmentMetadata>& metadata,
            uint64_t index_doc_count, bool has_semantic_norms) {
        return admit_snii_fields_for_test(
                statistics, {{field_name, metadata, index_doc_count, has_semantic_norms}});
    }

    Status stage_snii_fields_then_file_not_found_for_test(
            CollectionStatistics* statistics,
            const std::vector<SniiScoringFieldInput>& fields_before_failure) {
        CollectionStatistics::SniiScoringSegmentAccumulator segment_accumulator;
        RETURN_IF_ERROR(stage_snii_fields_for_test(statistics, fields_before_failure,
                                                   &segment_accumulator));
        for (const auto& field : fields_before_failure) {
            add_term_doc_frequency(&segment_accumulator.term_doc_freqs, field.field_name, L"staged",
                                   1);
        }
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>(
                "simulated later field failure");
    }

    void expect_collected_stats(const std::wstring& field_name, uint64_t doc_count,
                                uint64_t token_count) {
        EXPECT_EQ(stats_->get_doc_num(), doc_count);
        expect_collected_tokens(field_name, token_count);
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

    EXPECT_TRUE(status.ok()) << status;
    expect_no_collected_tokens(L"1");
}

TEST_F(CollectionStatisticsTest, CollectWithEmptySegments) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();
    auto expr_contexts = create_match_expr_contexts();

    auto splits = create_mock_rowset_splits(0);

    auto status =
            stats_->collect(runtime_state_.get(), splits, tablet_schema, expr_contexts, nullptr);
    EXPECT_TRUE(status.ok()) << status.msg();
}

TEST_F(CollectionStatisticsTest, LegacyReservedTermUsesRawV3Namespace) {
    auto tablet_schema = create_legacy_v3_schema();
    const std::string segment_path = test_dir_ + "/legacy_v3_0.dat";
    ASSERT_TRUE(write_legacy_v3_segment(tablet_schema, segment_path).ok());

    auto rowset_meta = std::make_shared<collection_statistics::MockRowsetMeta>();
    auto rowset = std::make_shared<collection_statistics::MockRowset>(tablet_schema, rowset_meta);
    rowset->set_num_segments(1);
    rowset->set_segment_path(0, segment_path);
    auto reader = std::make_shared<collection_statistics::MockRowsetReader>(rowset);
    std::vector<RowSetSplits> splits {RowSetSplits(reader)};

    auto status = stats_->collect(runtime_state_.get(), splits, tablet_schema,
                                  create_reserved_exact_search_contexts(), nullptr);

    ASSERT_TRUE(status.ok()) << status;
    expect_collected_stats(L"1", 1, 2);
    expect_collected_term(L"1",
                          segment_v2::inverted_index::StringHelper::to_wstring(
                                  std::string(segment_v2::inverted_index::CG_V1_MARKER) + "user"),
                          0);
}

TEST_F(CollectionStatisticsTest, LegacyV3SegmentsUsePhysicalScoringStatistics) {
    auto tablet_schema = create_legacy_v3_schema();
    const std::string first_segment_path = test_dir_ + "/legacy_v3_0.dat";
    const std::string second_segment_path = test_dir_ + "/legacy_v3_1.dat";
    ASSERT_TRUE(write_legacy_v3_segment(tablet_schema, first_segment_path).ok());
    ASSERT_TRUE(write_legacy_v3_segment(tablet_schema, second_segment_path).ok());

    auto rowset_meta = std::make_shared<collection_statistics::MockRowsetMeta>();
    auto rowset = std::make_shared<collection_statistics::MockRowset>(tablet_schema, rowset_meta);
    rowset->set_num_segments(2);
    rowset->set_segment_path(0, first_segment_path);
    rowset->set_segment_path(1, second_segment_path);
    auto reader = std::make_shared<collection_statistics::MockRowsetReader>(rowset);
    std::vector<RowSetSplits> splits {RowSetSplits(reader)};

    auto status = stats_->collect(runtime_state_.get(), splits, tablet_schema,
                                  create_match_expr_contexts("alpha"), nullptr);

    ASSERT_TRUE(status.ok()) << status;
    expect_collected_stats(L"1", 2, 4);
    expect_collected_term(L"1", L"alpha", 2);
}

TEST_F(CollectionStatisticsTest, LegacyV3SkipsMissingSegmentAfterCollectingAvailableStatistics) {
    auto tablet_schema = create_legacy_v3_schema();
    const std::string first_segment_path = test_dir_ + "/legacy_v3_0.dat";
    ASSERT_TRUE(write_legacy_v3_segment(tablet_schema, first_segment_path).ok());

    auto rowset_meta = std::make_shared<collection_statistics::MockRowsetMeta>();
    auto rowset = std::make_shared<collection_statistics::MockRowset>(tablet_schema, rowset_meta);
    rowset->set_num_segments(2);
    rowset->set_segment_path(0, first_segment_path);
    rowset->set_segment_path(1, test_dir_ + "/missing_v3_1.dat");
    auto reader = std::make_shared<collection_statistics::MockRowsetReader>(rowset);
    std::vector<RowSetSplits> splits {RowSetSplits(reader)};

    auto status = stats_->collect(runtime_state_.get(), splits, tablet_schema,
                                  create_match_expr_contexts("alpha"), nullptr);

    ASSERT_TRUE(status.ok()) << status;
    expect_collected_stats(L"1", 1, 2);
    expect_collected_term(L"1", L"alpha", 1);
}

TEST_F(CollectionStatisticsTest, LegacyV3SkipsEmptySegmentAfterCollectingAvailableStatistics) {
    auto tablet_schema = create_legacy_v3_schema();
    const std::string first_segment_path = test_dir_ + "/legacy_v3_0.dat";
    const std::string empty_segment_path = test_dir_ + "/legacy_v3_empty_1.dat";
    ASSERT_TRUE(write_legacy_v3_segment(tablet_schema, first_segment_path).ok());

    const std::string empty_index_path =
            segment_v2::InvertedIndexDescriptor::get_index_file_path_v2(
                    segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(
                            empty_segment_path));
    io::FileWriterPtr empty_file;
    io::FileWriterOptions options;
    ASSERT_TRUE(io::global_local_filesystem()
                        ->create_file(empty_index_path, &empty_file, &options)
                        .ok());
    ASSERT_TRUE(empty_file->close(false).ok());

    auto rowset_meta = std::make_shared<collection_statistics::MockRowsetMeta>();
    auto rowset = std::make_shared<collection_statistics::MockRowset>(tablet_schema, rowset_meta);
    rowset->set_num_segments(2);
    rowset->set_segment_path(0, first_segment_path);
    rowset->set_segment_path(1, empty_segment_path);
    auto reader = std::make_shared<collection_statistics::MockRowsetReader>(rowset);
    std::vector<RowSetSplits> splits {RowSetSplits(reader)};

    const Status status = stats_->collect(runtime_state_.get(), splits, tablet_schema,
                                          create_match_expr_contexts("alpha"), nullptr);

    ASSERT_TRUE(status.ok()) << status;
    expect_collected_stats(L"1", 1, 2);
    expect_collected_term(L"1", L"alpha", 1);
}

TEST_F(CollectionStatisticsTest, SniiCommonGramsUsesSemanticScoringStatistics) {
    auto tablet_schema = create_snii_schema();
    auto expr_contexts = create_match_expr_contexts("alpha", "test-base-v1");
    const auto* analyzer_ctx = expr_contexts.front()->root()->query_analyzer_ctx();
    ASSERT_NE(analyzer_ctx, nullptr);
    ASSERT_NE(analyzer_ctx->analyzer_provider, nullptr);

    const std::string segment_path = test_dir_ + "/snii_common_grams_0.dat";
    auto write_status = write_snii_common_grams_segment(
            segment_path,
            std::string(analyzer_ctx->analyzer_provider->base_analyzer_fingerprint()));
    ASSERT_TRUE(write_status.ok()) << write_status;

    auto rowset_meta = std::make_shared<collection_statistics::MockRowsetMeta>();
    auto rowset = std::make_shared<collection_statistics::MockRowset>(tablet_schema, rowset_meta);
    rowset->set_num_segments(1);
    rowset->set_segment_path(0, segment_path);
    auto reader = std::make_shared<collection_statistics::MockRowsetReader>(rowset);
    std::vector<RowSetSplits> splits {RowSetSplits(reader)};

    auto status =
            stats_->collect(runtime_state_.get(), splits, tablet_schema, expr_contexts, nullptr);

    ASSERT_TRUE(status.ok()) << status;
    expect_collected_stats(L"1", 2, 3);
    expect_collected_term(L"1", L"alpha", 2);
}

TEST_F(CollectionStatisticsTest, SniiScoringLookupUsesCallerIoContext) {
    snii::snii_test::ScopedEnv force_nonresident_dict("SNII_DICT_RESIDENT_MAX", "0");
    auto tablet_schema = create_snii_schema();
    auto expr_contexts = create_match_expr_contexts("alpha", "test-base-v1");
    const auto* analyzer_ctx = expr_contexts.front()->root()->query_analyzer_ctx();
    ASSERT_NE(analyzer_ctx, nullptr);
    ASSERT_NE(analyzer_ctx->analyzer_provider, nullptr);

    const std::string segment_path = test_dir_ + "/snii_io_context_0.dat";
    ASSERT_TRUE(write_snii_common_grams_segment(
                        segment_path,
                        std::string(analyzer_ctx->analyzer_provider->base_analyzer_fingerprint()))
                        .ok());

    auto rowset_meta = std::make_shared<collection_statistics::MockRowsetMeta>();
    auto rowset = std::make_shared<collection_statistics::MockRowset>(tablet_schema, rowset_meta);
    rowset->set_num_segments(1);
    rowset->set_segment_path(0, segment_path);
    auto reader = std::make_shared<collection_statistics::MockRowsetReader>(rowset);
    std::vector<RowSetSplits> splits {RowSetSplits(reader)};

    const std::string index_path_prefix {
            segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(segment_path)};
    io::FileCacheStatistics open_stats;
    io::IOContext open_io_ctx;
    open_io_ctx.file_cache_stats = &open_stats;
    segment_v2::IndexFileReader file_reader(io::global_local_filesystem(), index_path_prefix,
                                            InvertedIndexStorageFormatPB::SNII);
    ASSERT_TRUE(file_reader.init(config::inverted_index_read_buffer_size, &open_io_ctx).ok());
    const auto index_metas = tablet_schema->inverted_indexs(1);
    ASSERT_EQ(index_metas.size(), 1);
    auto logical_reader = file_reader.open_snii_index(index_metas.front(), &open_io_ctx);
    ASSERT_TRUE(logical_reader.has_value()) << logical_reader.error();
    ASSERT_GT(open_stats.inverted_index_range_read_count, 0);

    io::FileCacheStatistics collect_stats;
    io::IOContext collect_io_ctx;
    collect_io_ctx.file_cache_stats = &collect_stats;
    const auto status = stats_->collect(runtime_state_.get(), splits, tablet_schema, expr_contexts,
                                        &collect_io_ctx);

    ASSERT_TRUE(status.ok()) << status;
    EXPECT_GT(collect_stats.inverted_index_range_read_count,
              open_stats.inverted_index_range_read_count);
}

TEST_F(CollectionStatisticsTest, SniiStatsProviderUsesSemanticCommonGramsTokenCount) {
    auto tablet_schema = create_snii_schema();
    auto expr_contexts = create_match_expr_contexts("alpha", "test-base-v1");
    const auto* analyzer_ctx = expr_contexts.front()->root()->query_analyzer_ctx();
    ASSERT_NE(analyzer_ctx, nullptr);
    ASSERT_NE(analyzer_ctx->analyzer_provider, nullptr);

    const std::string segment_path = test_dir_ + "/snii_semantic_stats_0.dat";
    auto write_status = write_snii_common_grams_segment(
            segment_path,
            std::string(analyzer_ctx->analyzer_provider->base_analyzer_fingerprint()));
    ASSERT_TRUE(write_status.ok()) << write_status;

    const std::string index_path_prefix {
            segment_v2::InvertedIndexDescriptor::get_index_file_path_prefix(segment_path)};
    segment_v2::IndexFileReader file_reader(io::global_local_filesystem(), index_path_prefix,
                                            InvertedIndexStorageFormatPB::SNII);
    ASSERT_TRUE(file_reader.init().ok());
    const auto index_metas = tablet_schema->inverted_indexs(1);
    ASSERT_EQ(index_metas.size(), 1);
    auto logical_reader = file_reader.open_snii_index(index_metas.front());
    ASSERT_TRUE(logical_reader.has_value()) << logical_reader.error();

    snii::stats::SniiStatsProvider provider;
    ASSERT_TRUE(snii::stats::SniiStatsProvider::open(logical_reader->get(), &provider).ok());
    EXPECT_EQ(provider.doc_count(), 2);
    EXPECT_EQ(provider.indexed_doc_count(), 2);
    EXPECT_EQ(provider.sum_total_term_freq(), 3);
    EXPECT_DOUBLE_EQ(provider.avgdl(), 1.5);
    EXPECT_TRUE(provider.has_norms());
}

TEST_F(CollectionStatisticsTest, SniiWriterRejectsMissingSemanticScoringMetadata) {
    const std::string segment_path = test_dir_ + "/snii_missing_scoring_metadata_0.dat";
    auto write_status = write_plain_snii_scoring_segment(segment_path);
    EXPECT_EQ(write_status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
}

TEST_F(CollectionStatisticsTest, SniiScoringRejectsMissingSegmentForWholeCollection) {
    auto tablet_schema = create_snii_schema();
    auto expr_contexts = create_match_expr_contexts("alpha", "test-base-v1");
    const auto* analyzer_ctx = expr_contexts.front()->root()->query_analyzer_ctx();
    ASSERT_NE(analyzer_ctx, nullptr);
    ASSERT_NE(analyzer_ctx->analyzer_provider, nullptr);

    const std::string first_segment_path = test_dir_ + "/snii_complete_0.dat";
    auto write_status = write_snii_common_grams_segment(
            first_segment_path,
            std::string(analyzer_ctx->analyzer_provider->base_analyzer_fingerprint()));
    ASSERT_TRUE(write_status.ok()) << write_status;

    auto rowset_meta = std::make_shared<collection_statistics::MockRowsetMeta>();
    auto rowset = std::make_shared<collection_statistics::MockRowset>(tablet_schema, rowset_meta);
    rowset->set_num_segments(2);
    rowset->set_segment_path(0, first_segment_path);
    rowset->set_segment_path(1, test_dir_ + "/missing_snii_1.dat");
    auto reader = std::make_shared<collection_statistics::MockRowsetReader>(rowset);
    std::vector<RowSetSplits> splits {RowSetSplits(reader)};

    auto status =
            stats_->collect(runtime_state_.get(), splits, tablet_schema, expr_contexts, nullptr);

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
}

TEST_F(CollectionStatisticsTest, SniiScoringRejectsMissingLogicalIndexForWholeCollection) {
    auto tablet_schema = create_snii_schema(/*index_id=*/2);
    auto expr_contexts = create_match_expr_contexts("alpha", "test-base-v1");
    const auto* analyzer_ctx = expr_contexts.front()->root()->query_analyzer_ctx();
    ASSERT_NE(analyzer_ctx, nullptr);
    ASSERT_NE(analyzer_ctx->analyzer_provider, nullptr);

    const std::string segment_path = test_dir_ + "/snii_missing_logical_index_0.dat";
    auto write_status = write_snii_common_grams_segment(
            segment_path,
            std::string(analyzer_ctx->analyzer_provider->base_analyzer_fingerprint()));
    ASSERT_TRUE(write_status.ok()) << write_status;

    auto rowset_meta = std::make_shared<collection_statistics::MockRowsetMeta>();
    auto rowset = std::make_shared<collection_statistics::MockRowset>(tablet_schema, rowset_meta);
    rowset->set_num_segments(1);
    rowset->set_segment_path(0, segment_path);
    auto reader = std::make_shared<collection_statistics::MockRowsetReader>(rowset);
    std::vector<RowSetSplits> splits {RowSetSplits(reader)};

    auto status =
            stats_->collect(runtime_state_.get(), splits, tablet_schema, expr_contexts, nullptr);

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
    expect_no_collected_tokens(L"1");
}

TEST_F(CollectionStatisticsTest, SniiWriterRejectsZeroSemanticTokensForNonemptyPostings) {
    auto status = write_snii_common_grams_segment(test_dir_ + "/snii_zero_semantic_tokens_0.dat",
                                                  "test-base-v1",
                                                  /*scoring_token_count=*/0);

    EXPECT_EQ(status.code(), ErrorCode::INVALID_ARGUMENT);
    EXPECT_THAT(status.msg(), ::testing::HasSubstr("zero semantic scoring tokens"));
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

segment_v2::inverted_index::CommonGramsSegmentMetadata complete_snii_scoring_metadata(
        std::string base_analyzer_fingerprint = "base-v1", uint64_t doc_count = 3,
        uint64_t token_count = 7) {
    using namespace segment_v2::inverted_index;
    segment_v2::inverted_index::CommonGramsSegmentMetadata metadata;
    metadata.plain_term_key_version = PlainTermKeyVersion::kEscapedV1;
    metadata.common_grams_coverage = CommonGramsCoverage::kComplete;
    metadata.common_grams_semantics_version = COMMON_GRAMS_SEMANTICS_VERSION_V1;
    metadata.common_grams_key_version = COMMON_GRAMS_KEY_VERSION_V1;
    metadata.common_grams_dictionary_identity = "builtin-stopwords:v1";
    metadata.base_analyzer_fingerprint = std::move(base_analyzer_fingerprint);
    metadata.common_grams_fingerprint = "common-grams-v1";
    metadata.scoring_coverage = ScoringCoverage::kComplete;
    metadata.scoring_stats_version = COMMON_GRAMS_SCORING_STATS_VERSION_V1;
    metadata.norm_semantics_version = COMMON_GRAMS_NORM_SEMANTICS_VERSION_V1;
    metadata.scoring_doc_count = doc_count;
    metadata.scoring_token_count = token_count;
    return metadata;
}

Result<SniiScoringSegmentStats> resolve_snii_scoring_segment_for_test(
        std::optional<segment_v2::inverted_index::CommonGramsSegmentMetadata> metadata,
        uint64_t physical_doc_count, bool has_norms) {
    const uint64_t physical_sum_total_term_freq = metadata ? metadata->scoring_token_count : 0;
    return resolve_snii_scoring_segment(metadata, physical_doc_count, physical_sum_total_term_freq,
                                        /*has_scoring_tier=*/true,
                                        /*has_positions=*/true, has_norms);
}

TEST(CollectionStatisticsCommonGramsTest, MissingMetadataWithoutPersistedProofRejectsScoring) {
    auto result = resolve_snii_scoring_segment_for_test(std::nullopt, 3, true);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
}

TEST(CollectionStatisticsCommonGramsTest, RawNoInternalMetadataWithoutScoringProofIsRejected) {
    segment_v2::inverted_index::CommonGramsSegmentMetadata metadata;
    metadata.plain_term_key_version =
            segment_v2::inverted_index::PlainTermKeyVersion::kRawNoInternal;
    metadata.common_grams_coverage = segment_v2::inverted_index::CommonGramsCoverage::kNone;
    metadata.base_analyzer_fingerprint = "base-v1";

    auto result = resolve_snii_scoring_segment_for_test(metadata, 3, true);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
}

TEST(CollectionStatisticsCommonGramsTest, ScoringCoverageNoneRejectsScoringAdmission) {
    auto metadata = complete_snii_scoring_metadata();
    metadata.scoring_coverage = segment_v2::inverted_index::ScoringCoverage::kNone;
    metadata.scoring_stats_version = 0;
    metadata.norm_semantics_version = 0;

    auto result = resolve_snii_scoring_segment_for_test(metadata, 3, true);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
}

TEST(CollectionStatisticsCommonGramsTest, IncompatibleScoringVersionsRejectScoringAdmission) {
    auto metadata = complete_snii_scoring_metadata();

    ++metadata.scoring_stats_version;
    auto scoring_version_result = resolve_snii_scoring_segment_for_test(metadata, 3, true);
    ASSERT_FALSE(scoring_version_result.has_value());
    EXPECT_EQ(scoring_version_result.error().code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);

    --metadata.scoring_stats_version;
    ++metadata.norm_semantics_version;
    auto norm_version_result = resolve_snii_scoring_segment_for_test(metadata, 3, true);
    ASSERT_FALSE(norm_version_result.has_value());
    EXPECT_EQ(norm_version_result.error().code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
}

TEST(CollectionStatisticsCommonGramsTest, ScoringDocCountMismatchRejectsScoringAdmission) {
    auto result = resolve_snii_scoring_segment_for_test(complete_snii_scoring_metadata(), 4, true);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::INVERTED_INDEX_FILE_CORRUPTED);
}

TEST(CollectionStatisticsCommonGramsTest, MissingSemanticNormsRejectScoringAdmission) {
    auto result = resolve_snii_scoring_segment_for_test(complete_snii_scoring_metadata(), 3, false);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::INVERTED_INDEX_FILE_CORRUPTED);
}

TEST(CollectionStatisticsCommonGramsTest, CompleteMetadataOnNonScoringTierIsCorruption) {
    auto metadata = complete_snii_scoring_metadata();
    auto result = resolve_snii_scoring_segment(metadata, 3, 7,
                                               /*has_scoring_tier=*/false,
                                               /*has_positions=*/true,
                                               /*has_semantic_norms=*/true);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::INVERTED_INDEX_FILE_CORRUPTED);
}

TEST(CollectionStatisticsCommonGramsTest, ZeroSemanticTokensWithPhysicalTermsIsCorruption) {
    auto metadata = complete_snii_scoring_metadata("base-v1", 3, 0);
    auto result = resolve_snii_scoring_segment(metadata, 3, 1,
                                               /*has_scoring_tier=*/true,
                                               /*has_positions=*/true,
                                               /*has_semantic_norms=*/true);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::INVERTED_INDEX_FILE_CORRUPTED);
}

TEST(CollectionStatisticsCommonGramsTest, EmptyPhysicalAndSemanticTokenCountsAreValid) {
    auto metadata = complete_snii_scoring_metadata("base-v1", 3, 0);
    auto result = resolve_snii_scoring_segment(metadata, 3, 0,
                                               /*has_scoring_tier=*/true,
                                               /*has_positions=*/true,
                                               /*has_semantic_norms=*/true);

    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->token_count, 0);
}

TEST(CollectionStatisticsCommonGramsTest, LegacyPhysicalScoringRequiresDocumentLengthNorms) {
    auto result = resolve_snii_scoring_segment_for_test(std::nullopt, 3, false);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
}

TEST(CollectionStatisticsCommonGramsTest, CompleteMetadataUsesSemanticDocAndTokenCounts) {
    auto result = resolve_snii_scoring_segment_for_test(complete_snii_scoring_metadata(), 3, true);

    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->doc_count, 3);
    EXPECT_EQ(result->token_count, 7);
    EXPECT_EQ(result->plain_term_key_version,
              segment_v2::inverted_index::PlainTermKeyVersion::kEscapedV1);
    EXPECT_EQ(result->base_analyzer_fingerprint, "base-v1");
}

TEST(CollectionStatisticsCommonGramsTest, CompletePlainMetadataIsExplicitSemanticProof) {
    auto metadata = complete_snii_scoring_metadata();
    metadata.plain_term_key_version =
            segment_v2::inverted_index::PlainTermKeyVersion::kRawNoInternal;
    metadata.common_grams_coverage = segment_v2::inverted_index::CommonGramsCoverage::kNone;
    metadata.common_grams_semantics_version = 0;
    metadata.common_grams_key_version = 0;
    metadata.common_grams_dictionary_identity.clear();
    metadata.common_grams_fingerprint.clear();

    auto result = resolve_snii_scoring_segment_for_test(metadata, 3, true);

    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result->doc_count, 3);
    EXPECT_EQ(result->token_count, 7);
}

TEST(CollectionStatisticsCommonGramsTest, PlainSemanticTokenCountMustEqualPhysicalCount) {
    auto metadata = complete_snii_scoring_metadata();
    metadata.plain_term_key_version =
            segment_v2::inverted_index::PlainTermKeyVersion::kRawNoInternal;
    metadata.common_grams_coverage = segment_v2::inverted_index::CommonGramsCoverage::kNone;
    metadata.common_grams_semantics_version = 0;
    metadata.common_grams_key_version = 0;
    metadata.common_grams_dictionary_identity.clear();
    metadata.common_grams_fingerprint.clear();

    auto result = resolve_snii_scoring_segment(metadata, 3, 8,
                                               /*has_scoring_tier=*/true,
                                               /*has_positions=*/true,
                                               /*has_semantic_norms=*/true);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::INVERTED_INDEX_FILE_CORRUPTED);
}

TEST(CollectionStatisticsCommonGramsTest, EmptySemanticFingerprintIsNotScoringProof) {
    auto metadata = complete_snii_scoring_metadata();
    metadata.base_analyzer_fingerprint.clear();

    auto result = resolve_snii_scoring_segment_for_test(metadata, 3, true);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error().code(), ErrorCode::INVERTED_INDEX_FILE_CORRUPTED);
}

TEST_F(CollectionStatisticsTest, MixedBaseFingerprintRejectsAndClearsWholeCollection) {
    auto first = complete_snii_scoring_metadata("base-v1", 3, 7);
    ASSERT_TRUE(admit_snii_segment_for_test(stats_.get(), L"1", first, 3, true).ok());
    EXPECT_FLOAT_EQ(stats_->get_or_calculate_avg_dl(L"1"), 7.0F / 3.0F);

    auto second = complete_snii_scoring_metadata("base-v2", 3, 5);
    auto status = admit_snii_segment_for_test(stats_.get(), L"1", second, 3, true);

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
    expect_no_collected_tokens(L"1");
    EXPECT_THROW(stats_->get_or_calculate_avg_dl(L"1"), Exception);
}

TEST_F(CollectionStatisticsTest, PersistedFingerprintMustMatchRequestAnalyzer) {
    auto metadata = complete_snii_scoring_metadata("persisted-base", 3, 7);
    auto status = admit_snii_fields_for_test(
            stats_.get(), {{L"1", metadata, 3, true, std::string("request-base")}});

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
    expect_no_collected_tokens(L"1");
    EXPECT_THROW(stats_->get_doc_num(), Exception);
}

TEST_F(CollectionStatisticsTest, CollectionStatisticsInstancesKeepAdmissionStateIsolated) {
    CollectionStatistics first;
    CollectionStatistics second;

    ASSERT_TRUE(admit_snii_segment_for_test(&first, L"1",
                                            complete_snii_scoring_metadata("base-a", 2, 6), 2, true)
                        .ok());
    ASSERT_TRUE(admit_snii_segment_for_test(
                        &second, L"1", complete_snii_scoring_metadata("base-b", 5, 25), 5, true)
                        .ok());

    EXPECT_FLOAT_EQ(first.get_or_calculate_avg_dl(L"1"), 3.0F);
    EXPECT_FLOAT_EQ(second.get_or_calculate_avg_dl(L"1"), 5.0F);
}

TEST_F(CollectionStatisticsTest, LegacyAndUnprovedRawNoInternalRejectWholeCollection) {
    ASSERT_TRUE(admit_snii_segment_for_test(stats_.get(), L"1",
                                            complete_snii_scoring_metadata("base-v1", 2, 6), 2,
                                            true)
                        .ok());

    segment_v2::inverted_index::CommonGramsSegmentMetadata plain_metadata;
    plain_metadata.plain_term_key_version =
            segment_v2::inverted_index::PlainTermKeyVersion::kRawNoInternal;
    plain_metadata.common_grams_coverage = segment_v2::inverted_index::CommonGramsCoverage::kNone;
    plain_metadata.base_analyzer_fingerprint = "base-v1";
    auto status = admit_snii_segment_for_test(stats_.get(), L"1", plain_metadata, 3, true);

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
    expect_no_collected_tokens(L"1");
    EXPECT_THROW(stats_->get_doc_num(), Exception);
}

TEST_F(CollectionStatisticsTest, LegacyAndCommonGramsMixRejectsWholeCollection) {
    ASSERT_TRUE(admit_snii_segment_for_test(stats_.get(), L"1",
                                            complete_snii_scoring_metadata("base-v1", 2, 6), 2,
                                            true)
                        .ok());

    auto status = admit_snii_segment_for_test(stats_.get(), L"1", std::nullopt, 3, true);

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
    expect_no_collected_tokens(L"1");
    EXPECT_THROW(stats_->get_or_calculate_avg_dl(L"1"), Exception);
}

TEST_F(CollectionStatisticsTest, ExplicitSemanticPlainAndCommonGramsSegmentsAccumulate) {
    auto plain_metadata = complete_snii_scoring_metadata("base-v1", 2, 6);
    plain_metadata.plain_term_key_version =
            segment_v2::inverted_index::PlainTermKeyVersion::kRawNoInternal;
    plain_metadata.common_grams_coverage = segment_v2::inverted_index::CommonGramsCoverage::kNone;
    plain_metadata.common_grams_semantics_version = 0;
    plain_metadata.common_grams_key_version = 0;
    plain_metadata.common_grams_dictionary_identity.clear();
    plain_metadata.common_grams_fingerprint.clear();

    ASSERT_TRUE(admit_snii_segment_for_test(stats_.get(), L"1", plain_metadata, 2, true).ok());
    ASSERT_TRUE(admit_snii_segment_for_test(stats_.get(), L"1",
                                            complete_snii_scoring_metadata("base-v1", 3, 7), 3,
                                            true)
                        .ok());

    expect_collected_stats(L"1", 5, 13);
    EXPECT_FLOAT_EQ(stats_->get_or_calculate_avg_dl(L"1"), 13.0F / 5.0F);
}

TEST_F(CollectionStatisticsTest, CommonGramsSegmentsAccumulateSemanticStatistics) {
    ASSERT_TRUE(admit_snii_segment_for_test(stats_.get(), L"1",
                                            complete_snii_scoring_metadata("base-v1", 2, 6), 2,
                                            true)
                        .ok());
    ASSERT_TRUE(admit_snii_segment_for_test(stats_.get(), L"1",
                                            complete_snii_scoring_metadata("base-v1", 3, 9), 3,
                                            true)
                        .ok());

    expect_collected_stats(L"1", 5, 15);
    EXPECT_FLOAT_EQ(stats_->get_or_calculate_avg_dl(L"1"), 3.0F);
}

TEST_F(CollectionStatisticsTest, MultiFieldSegmentsCommitAndAccumulateAtomically) {
    ASSERT_TRUE(admit_snii_fields_for_test(
                        stats_.get(),
                        {{L"1", complete_snii_scoring_metadata("field-1", 3, 7), 3, true},
                         {L"2", complete_snii_scoring_metadata("field-2", 3, 12), 3, true}})
                        .ok());
    ASSERT_TRUE(admit_snii_fields_for_test(
                        stats_.get(),
                        {{L"1", complete_snii_scoring_metadata("field-1", 2, 5), 2, true},
                         {L"2", complete_snii_scoring_metadata("field-2", 2, 8), 2, true}})
                        .ok());

    expect_collected_stats(L"1", 5, 12);
    expect_collected_tokens(L"2", 20);
    EXPECT_FLOAT_EQ(stats_->get_or_calculate_avg_dl(L"1"), 12.0F / 5.0F);
    EXPECT_FLOAT_EQ(stats_->get_or_calculate_avg_dl(L"2"), 4.0F);
}

TEST_F(CollectionStatisticsTest, MultiFieldSegmentDocCountsMustAgree) {
    auto status = admit_snii_fields_for_test(
            stats_.get(), {{L"1", complete_snii_scoring_metadata("field-1", 3, 7), 3, true},
                           {L"2", complete_snii_scoring_metadata("field-2", 4, 12), 4, true}});

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
    expect_no_collected_tokens(L"1");
    expect_no_collected_tokens(L"2");
    EXPECT_THROW(stats_->get_doc_num(), Exception);
}

TEST_F(CollectionStatisticsTest, LaterFieldFileNotFoundDoesNotPublishPartialSegment) {
    ASSERT_TRUE(admit_snii_segment_for_test(stats_.get(), L"1",
                                            complete_snii_scoring_metadata("field-1", 2, 6), 2,
                                            true)
                        .ok());

    auto status = stage_snii_fields_then_file_not_found_for_test(
            stats_.get(),
            {{L"2", complete_snii_scoring_metadata("staged-field-2", 3, 12), 3, true}});

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND);
    expect_collected_stats(L"1", 2, 6);
    expect_no_collected_tokens(L"2");
    expect_no_collected_term(L"2", L"staged");

    ASSERT_TRUE(admit_snii_segment_for_test(
                        stats_.get(), L"2",
                        complete_snii_scoring_metadata("different-field-2", 1, 4), 1, true)
                        .ok());
    expect_collected_tokens(L"2", 4);
}

TEST(CollectionStatisticsCommonGramsTest, DocFrequencySupportsLogicalAndPhysicalTermKeys) {
    std::unordered_map<std::wstring, std::unordered_map<std::wstring, uint64_t>>
            logical_frequencies;

    add_term_doc_frequency(&logical_frequencies, L"field", L"logical", 3);
    EXPECT_EQ(logical_frequencies[L"field"][L"logical"], 3);

    add_term_doc_frequency(&logical_frequencies, L"field", L"same", 5);
    EXPECT_EQ(logical_frequencies[L"field"][L"same"], 5);
}

TEST(CollectionStatisticsCommonGramsTest, PhysicalAliasesCannotMergeLogicalDocFrequencies) {
    using Frequencies =
            std::unordered_map<std::wstring, std::unordered_map<std::wstring, uint64_t>>;
    Frequencies logical_frequencies;

    const std::wstring alias = std::wstring(1, wchar_t {0x1e}) + L"G00000001:";
    add_term_doc_frequency(&logical_frequencies, L"field", L"\x1f", 3);
    add_term_doc_frequency(&logical_frequencies, L"field", alias, 5);

    EXPECT_EQ(logical_frequencies[L"field"][alias], 5);
}

TEST(CollectionStatisticsCommonGramsTest, UnrepresentablePlainTermRegistersZeroDocFrequency) {
    using Frequencies =
            std::unordered_map<std::wstring, std::unordered_map<std::wstring, uint64_t>>;
    Frequencies logical_frequencies;

    add_term_doc_frequency(&logical_frequencies, L"field", L"unrepresentable", 0);

    ASSERT_TRUE(logical_frequencies.contains(L"field"));
    EXPECT_TRUE(logical_frequencies.at(L"field").contains(L"unrepresentable"));
    EXPECT_EQ(logical_frequencies.at(L"field").at(L"unrepresentable"), 0);
}

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

TEST_F(CollectionStatisticsTest, MatchScoringUsesTextSemanticsForVariantParentIndexFallback) {
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
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(collect_infos.size(), 1U);
    auto it = collect_infos.find(StringHelper::to_wstring(std::to_string(kVariantUid) + ".v.key"));
    ASSERT_NE(it, collect_infos.end());
    ASSERT_NE(it->second.index_meta, nullptr);
    ASSERT_NE(it->second.owned_index_meta, nullptr);
    EXPECT_EQ(it->second.index_meta->index_id(), 2004);
    EXPECT_EQ(it->second.unique_terms, std::vector<std::string>({"abc"}));
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
    EXPECT_FALSE(it->second.unique_terms.empty());
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
    auto analyzer_ctx = std::make_shared<InvertedIndexAnalyzerCtx>();
    analyzer_ctx->analyzer_key = "none";
    analyzer_ctx->parser_type = InvertedIndexParserType::PARSER_NONE;
    match_expr->set_analyzer_ctx(std::move(analyzer_ctx));

    MatchPredicateCollector collector;
    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    auto status =
            collector.collect(runtime_state_.get(), tablet_schema, match_expr, &collect_infos);
    ASSERT_TRUE(status.ok()) << status.msg();
    EXPECT_TRUE(collect_infos.empty());
}

TEST_F(CollectionStatisticsTest, ExplicitNoneDoesNotSelectNormalizerIndexForScoring) {
    auto tablet_schema = std::make_shared<TabletSchema>();

    constexpr int32_t kColUid = 1325;
    TabletColumn col;
    col.set_unique_id(kColUid);
    col.set_name("normalized");
    col.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    tablet_schema->append_column(col);

    TabletIndexPB index_pb;
    index_pb.set_index_id(2325);
    index_pb.set_index_name("normalized_idx");
    index_pb.set_index_type(IndexType::INVERTED);
    index_pb.add_col_unique_id(kColUid);
    auto* props = index_pb.mutable_properties();
    (*props)[INVERTED_INDEX_NORMALIZER_NAME_KEY] = "lowercase";
    (*props)[INVERTED_INDEX_PARSER_PHRASE_SUPPORT_KEY] = "true";
    TabletIndex index;
    index.init_from_pb(index_pb);
    tablet_schema->append_index(std::move(index));

    constexpr int kSlotId = 82;
    runtime_state_->_mock_desc_tbl->add_slot_descriptor(SlotId(kSlotId), kColUid, "normalized", {});

    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    match_expr->_children.push_back(
            std::make_shared<collection_statistics::MockVSlotRef>("normalized", SlotId(kSlotId)));
    match_expr->_children.push_back(std::make_shared<collection_statistics::MockVLiteral>("ABC"));
    auto analyzer_ctx = std::make_shared<InvertedIndexAnalyzerCtx>();
    analyzer_ctx->analyzer_key = "none";
    analyzer_ctx->parser_type = InvertedIndexParserType::PARSER_NONE;
    match_expr->set_analyzer_ctx(std::move(analyzer_ctx));

    MatchPredicateCollector collector;
    std::unordered_map<std::wstring, CollectInfo> collect_infos;
    auto status =
            collector.collect(runtime_state_.get(), tablet_schema, match_expr, &collect_infos);

    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.msg().find("No inverted index found for analyzer 'none'"), std::string::npos)
            << status;
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

TEST_F(CollectionStatisticsTest, CollectPreservesLogicalClauseShapesForSameFieldName) {
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
    auto first = collector.collect(runtime_state_.get(), tablet_schema, build_match("alpha beta"),
                                   &collect_infos);
    ASSERT_TRUE(first.ok()) << first.msg();
    auto second = collector.collect(runtime_state_.get(), tablet_schema,
                                    build_match("alpha alpha beta"), &collect_infos);
    ASSERT_TRUE(second.ok()) << second.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    auto it = collect_infos.find(StringHelper::to_wstring(std::to_string(kColUid)));
    ASSERT_NE(it, collect_infos.end());
    ASSERT_EQ(it->second.unique_terms, std::vector<std::string>({"alpha", "beta"}));
    ASSERT_EQ(it->second.unique_term_slots.size(), 2u);
    EXPECT_EQ(it->second.unique_term_slots.at("alpha"), 0u);
    EXPECT_EQ(it->second.unique_term_slots.at("beta"), 1u);
    ASSERT_EQ(it->second.logical_scoring_leaves.size(), 2u);
    ASSERT_EQ(it->second.logical_scoring_leaves[0].clauses.size(), 2u);
    EXPECT_EQ(it->second.logical_scoring_leaves[0].clauses[0].df_slot, 0u);
    EXPECT_EQ(it->second.logical_scoring_leaves[0].clauses[0].position, 1);
    EXPECT_EQ(it->second.logical_scoring_leaves[0].clauses[1].df_slot, 1u);
    EXPECT_EQ(it->second.logical_scoring_leaves[0].clauses[1].position, 2);
    ASSERT_EQ(it->second.logical_scoring_leaves[1].clauses.size(), 3u);
    EXPECT_EQ(it->second.logical_scoring_leaves[1].clauses[0].df_slot, 0u);
    EXPECT_EQ(it->second.logical_scoring_leaves[1].clauses[0].position, 1);
    EXPECT_EQ(it->second.logical_scoring_leaves[1].clauses[1].df_slot, 0u);
    EXPECT_EQ(it->second.logical_scoring_leaves[1].clauses[1].position, 2);
    EXPECT_EQ(it->second.logical_scoring_leaves[1].clauses[2].df_slot, 1u);
    EXPECT_EQ(it->second.logical_scoring_leaves[1].clauses[2].position, 3);
}

TEST_F(CollectionStatisticsTest, CollectUsesMatchRequestAnalyzerProviderAndFingerprint) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();

    auto analyzer = segment_v2::inverted_index::InvertedIndexAnalyzer::create_builtin_analyzer(
            InvertedIndexParserType::PARSER_ENGLISH, "", INVERTED_INDEX_PARSER_FALSE, "none");
    auto provider = std::make_shared<collection_statistics::FixedFingerprintAnalyzerProvider>(
            std::move(analyzer), "request-base-v1");
    auto analyzer_ctx = std::make_shared<InvertedIndexAnalyzerCtx>();
    analyzer_ctx->analyzer_provider = provider;

    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    match_expr->set_analyzer_ctx(std::move(analyzer_ctx));
    auto slot_ref = std::make_shared<collection_statistics::MockVSlotRef>("content", SlotId(1));
    auto literal = std::make_shared<collection_statistics::MockVLiteral>("Alpha ALPHA");
    match_expr->_children.push_back(slot_ref);
    match_expr->_children.push_back(literal);

    MatchPredicateCollector collector;
    CollectInfoMap collect_infos;
    auto status =
            collector.collect(runtime_state_.get(), tablet_schema, match_expr, &collect_infos);

    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    const auto& collect_info = collect_infos.begin()->second;
    EXPECT_EQ(collect_info.expected_base_analyzer_fingerprint, "request-base-v1");
    ASSERT_EQ(collect_info.unique_terms, std::vector<std::string>({"Alpha", "ALPHA"}));
    ASSERT_EQ(collect_info.logical_scoring_leaves.size(), 1u);
    ASSERT_EQ(collect_info.logical_scoring_leaves[0].clauses.size(), 2u);
    EXPECT_EQ(collect_info.logical_scoring_leaves[0].clauses[0].df_slot, 0u);
    EXPECT_EQ(collect_info.logical_scoring_leaves[0].clauses[1].df_slot, 1u);
}

TEST_F(CollectionStatisticsTest, CollectPhrasePrefixExcludesScoringTail) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();
    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    match_expr->set_opcode(TExprOpcode::MATCH_PHRASE_PREFIX);
    match_expr->_children.push_back(
            std::make_shared<collection_statistics::MockVSlotRef>("content", SlotId(1)));
    match_expr->_children.push_back(
            std::make_shared<collection_statistics::MockVLiteral>("alpha beta gamma"));

    MatchPredicateCollector collector;
    CollectInfoMap collect_infos;
    auto status =
            collector.collect(runtime_state_.get(), tablet_schema, match_expr, &collect_infos);

    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    const auto& collect_info = collect_infos.begin()->second;
    EXPECT_EQ(collect_info.unique_terms, std::vector<std::string>({"alpha", "beta"}));
    ASSERT_EQ(collect_info.logical_scoring_leaves.size(), 1u);
    ASSERT_EQ(collect_info.logical_scoring_leaves[0].clauses.size(), 2u);
    EXPECT_EQ(collect_info.logical_scoring_leaves[0].clauses[0].position, 1);
    EXPECT_EQ(collect_info.logical_scoring_leaves[0].clauses[1].position, 2);
}

TEST_F(CollectionStatisticsTest, CollectSingleTermPhrasePrefixHasEmptyScoringLeaf) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();
    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    match_expr->set_opcode(TExprOpcode::MATCH_PHRASE_PREFIX);
    match_expr->_children.push_back(
            std::make_shared<collection_statistics::MockVSlotRef>("content", SlotId(1)));
    match_expr->_children.push_back(std::make_shared<collection_statistics::MockVLiteral>("alpha"));

    MatchPredicateCollector collector;
    CollectInfoMap collect_infos;
    auto status =
            collector.collect(runtime_state_.get(), tablet_schema, match_expr, &collect_infos);

    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    const auto& collect_info = collect_infos.begin()->second;
    EXPECT_TRUE(collect_info.unique_terms.empty());
    ASSERT_EQ(collect_info.logical_scoring_leaves.size(), 1u);
    EXPECT_TRUE(collect_info.logical_scoring_leaves[0].clauses.empty());
}

TEST_F(CollectionStatisticsTest, SearchMatchCollectsRawExecutionTerm) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();
    auto contexts = create_search_contexts("MATCH", "alpha beta");
    CollectInfoMap collect_infos;

    auto status = stats_->extract_collect_info(runtime_state_.get(), contexts, tablet_schema,
                                               &collect_infos);

    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    const auto& collect_info = collect_infos.begin()->second;
    EXPECT_EQ(collect_info.unique_terms, std::vector<std::string>({"alpha beta"}));
    ASSERT_EQ(collect_info.logical_scoring_leaves.size(), 1u);
    ASSERT_EQ(collect_info.logical_scoring_leaves[0].clauses.size(), 1u);
    EXPECT_EQ(collect_info.logical_scoring_leaves[0].clauses[0].df_slot, 0u);
}

TEST_F(CollectionStatisticsTest, MatchSelectsOnlyTheRuntimeAnalyzerIndex) {
    auto tablet_schema = create_tablet_schema_with_two_fulltext_indexes();
    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    match_expr->_children.push_back(
            std::make_shared<collection_statistics::MockVSlotRef>("content", SlotId(1)));
    match_expr->_children.push_back(
            std::make_shared<collection_statistics::MockVLiteral>("running quickly"));

    InvertedIndexAnalyzerConfig config;
    config.analyzer_name = "english";
    config.parser_type = InvertedIndexParserType::PARSER_ENGLISH;
    config.stop_words = "none";
    auto analyzer_ctx = std::make_shared<InvertedIndexAnalyzerCtx>();
    analyzer_ctx->analyzer_key = "english";
    analyzer_ctx->parser_type = InvertedIndexParserType::PARSER_ENGLISH;
    analyzer_ctx->analyzer_provider =
            segment_v2::inverted_index::InvertedIndexAnalyzer::create_analyzer_provider(&config);
    match_expr->set_analyzer_ctx(std::move(analyzer_ctx));

    MatchPredicateCollector collector;
    CollectInfoMap collect_infos;
    auto status =
            collector.collect(runtime_state_.get(), tablet_schema, match_expr, &collect_infos);

    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    const auto& collect_info = collect_infos.begin()->second;
    ASSERT_NE(collect_info.index_meta, nullptr);
    EXPECT_EQ(collect_info.index_meta->index_id(), 20);
    EXPECT_EQ(collect_info.logical_scoring_leaves.size(), 1u);
}

TEST_F(CollectionStatisticsTest, MatchArrayStringSelectsFulltextLeafIndex) {
    auto tablet_schema = create_array_tablet_schema_with_keyword_and_fulltext_indexes();
    auto match_expr = std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
    match_expr->_children.push_back(
            std::make_shared<collection_statistics::MockVSlotRef>("content", SlotId(1)));
    match_expr->_children.push_back(
            std::make_shared<collection_statistics::MockVLiteral>("alpha beta"));

    MatchPredicateCollector collector;
    CollectInfoMap collect_infos;
    auto status =
            collector.collect(runtime_state_.get(), tablet_schema, match_expr, &collect_infos);

    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    ASSERT_NE(collect_infos.begin()->second.index_meta, nullptr);
    EXPECT_EQ(collect_infos.begin()->second.index_meta->index_id(), 20);
}

TEST_F(CollectionStatisticsTest, SearchTermSelectsOnlyTheRuntimeFullTextIndex) {
    auto tablet_schema = create_tablet_schema_with_keyword_and_fulltext_indexes();
    auto contexts = create_search_contexts("TERM", "alpha beta");
    CollectInfoMap collect_infos;

    auto status = stats_->extract_collect_info(runtime_state_.get(), contexts, tablet_schema,
                                               &collect_infos);

    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    const auto& collect_info = collect_infos.begin()->second;
    ASSERT_NE(collect_info.index_meta, nullptr);
    EXPECT_EQ(collect_info.index_meta->index_id(), 20);
    EXPECT_EQ(collect_info.logical_scoring_leaves.size(), 1u);
}

TEST_F(CollectionStatisticsTest, SearchArrayStringSelectsFulltextLeafIndex) {
    auto tablet_schema = create_array_tablet_schema_with_keyword_and_fulltext_indexes();
    auto contexts = create_search_contexts("TERM", "alpha beta");
    CollectInfoMap collect_infos;

    auto status = stats_->extract_collect_info(runtime_state_.get(), contexts, tablet_schema,
                                               &collect_infos);

    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    ASSERT_NE(collect_infos.begin()->second.index_meta, nullptr);
    EXPECT_EQ(collect_infos.begin()->second.index_meta->index_id(), 20);
}

TEST_F(CollectionStatisticsTest, SearchExactIgnoresAnalyzedBindingHint) {
    auto tablet_schema = create_tablet_schema_with_keyword_and_fulltext_indexes();
    TSearchClause clause;
    clause.clause_type = "EXACT";
    clause.field_name = "content";
    clause.value = "running quickly";
    clause.__isset.field_name = true;
    clause.__isset.value = true;
    TSearchFieldBinding binding;
    binding.field_name = "content";
    binding.slot_index = 0;
    binding.index_properties["parser"] = "english";
    binding.index_properties["support_phrase"] = "true";
    binding.__isset.index_properties = true;
    CollectInfoMap collect_infos;

    auto status = stats_->extract_collect_info(
            runtime_state_.get(), create_search_contexts(std::move(clause), {std::move(binding)}),
            tablet_schema, &collect_infos);

    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    ASSERT_NE(collect_infos.begin()->second.index_meta, nullptr);
    EXPECT_EQ(collect_infos.begin()->second.index_meta->index_id(), 10);
    EXPECT_EQ(collect_infos.begin()->second.unique_terms,
              std::vector<std::string>({"running quickly"}));
}

TEST_F(CollectionStatisticsTest, SearchScoringRejectsMissingField) {
    auto tablet_schema = create_tablet_schema_with_inverted_index();
    TSearchClause clause;
    clause.clause_type = "TERM";
    clause.field_name = "missing";
    clause.value = "alpha";
    clause.__isset.field_name = true;
    clause.__isset.value = true;
    CollectInfoMap collect_infos;

    auto status = stats_->extract_collect_info(runtime_state_.get(),
                                               create_search_contexts(std::move(clause)),
                                               tablet_schema, &collect_infos);

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
    EXPECT_TRUE(collect_infos.empty());
}

TEST_F(CollectionStatisticsTest, SearchScoringRejectsMissingIndex) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    TabletColumn column;
    column.set_unique_id(1);
    column.set_name("content");
    column.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    tablet_schema->append_column(column);
    CollectInfoMap collect_infos;

    auto status = stats_->extract_collect_info(runtime_state_.get(),
                                               create_search_contexts("TERM", "alpha"),
                                               tablet_schema, &collect_infos);

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
    EXPECT_TRUE(collect_infos.empty());
}

TEST_F(CollectionStatisticsTest, SearchTypedVariantBindingSelectsItsAnalyzerIndex) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    constexpr int32_t kVariantUid = 9010;

    TabletColumn variant_column;
    variant_column.set_unique_id(kVariantUid);
    variant_column.set_name("v");
    variant_column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    tablet_schema->append_column(variant_column);

    TabletColumn subcolumn;
    subcolumn.set_unique_id(-1);
    subcolumn.set_name("v.host");
    subcolumn.set_type(FieldType::OLAP_FIELD_TYPE_STRING);
    subcolumn.set_parent_unique_id(kVariantUid);
    subcolumn.set_path_info(PathInData("v.host", true));
    tablet_schema->append_column(subcolumn);

    TabletSchema::PathsSetInfo path_set_info;
    TabletSchema::SubColumnInfo typed_path_info;
    typed_path_info.column = subcolumn;
    for (const auto& [index_id, parser] : {std::pair<int64_t, std::string> {3010, "standard"},
                                           std::pair<int64_t, std::string> {3020, "english"}}) {
        auto index = std::make_shared<TabletIndex>();
        TabletIndexPB index_pb;
        index_pb.set_index_id(index_id);
        index_pb.set_index_name(parser + "_variant_idx");
        index_pb.set_index_type(IndexType::INVERTED);
        index_pb.add_col_unique_id(kVariantUid);
        (*index_pb.mutable_properties())["parser"] = parser;
        (*index_pb.mutable_properties())["support_phrase"] = "true";
        index->init_from_pb(index_pb);
        typed_path_info.indexes.push_back(std::move(index));
    }
    path_set_info.typed_path_set.emplace("host", std::move(typed_path_info));
    std::unordered_map<int32_t, TabletSchema::PathsSetInfo> path_set_info_map;
    path_set_info_map.emplace(kVariantUid, std::move(path_set_info));
    tablet_schema->set_path_set_info(std::move(path_set_info_map));

    TSearchClause clause;
    clause.clause_type = "TERM";
    clause.field_name = "v.host";
    clause.value = "running";
    clause.__isset.field_name = true;
    clause.__isset.value = true;
    TSearchFieldBinding binding;
    binding.field_name = "v.host";
    binding.slot_index = 0;
    binding.is_variant_subcolumn = true;
    binding.__isset.is_variant_subcolumn = true;
    binding.parent_field_name = "v";
    binding.__isset.parent_field_name = true;
    binding.subcolumn_path = "host";
    binding.__isset.subcolumn_path = true;
    binding.index_properties["parser"] = "english";
    binding.index_properties["support_phrase"] = "true";
    binding.__isset.index_properties = true;
    CollectInfoMap collect_infos;

    auto status = stats_->extract_collect_info(
            runtime_state_.get(), create_search_contexts(std::move(clause), {std::move(binding)}),
            tablet_schema, &collect_infos);

    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    const auto& collect_info = collect_infos.begin()->second;
    ASSERT_NE(collect_info.index_meta, nullptr);
    EXPECT_EQ(collect_info.index_meta->index_id(), 3020);
    EXPECT_EQ(collect_info.unique_terms, std::vector<std::string>({"running"}));
}

TEST_F(CollectionStatisticsTest, SearchScoringUsesTextSemanticsForVariantParentIndexFallback) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    constexpr int32_t kVariantUid = 9015;

    TabletColumn variant_column;
    variant_column.set_unique_id(kVariantUid);
    variant_column.set_name("v");
    variant_column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    tablet_schema->append_column(variant_column);

    TabletIndex parent_index;
    parent_index._index_id = 3025;
    parent_index._index_type = IndexType::INVERTED;
    parent_index._col_unique_ids.push_back(kVariantUid);
    parent_index._properties["parser"] = "standard";
    parent_index._properties["support_phrase"] = "true";
    tablet_schema->append_index(std::move(parent_index));

    TSearchClause clause;
    clause.clause_type = "PHRASE";
    clause.field_name = "v.dynamic";
    clause.value = "alpha beta";
    clause.__isset.field_name = true;
    clause.__isset.value = true;
    TSearchFieldBinding binding;
    binding.field_name = "v.dynamic";
    binding.slot_index = 0;
    binding.is_variant_subcolumn = true;
    binding.__isset.is_variant_subcolumn = true;
    binding.parent_field_name = "v";
    binding.__isset.parent_field_name = true;
    binding.subcolumn_path = "dynamic";
    binding.__isset.subcolumn_path = true;
    binding.index_properties["parser"] = "standard";
    binding.index_properties["support_phrase"] = "true";
    binding.__isset.index_properties = true;
    CollectInfoMap collect_infos;

    auto status = stats_->extract_collect_info(
            runtime_state_.get(), create_search_contexts(std::move(clause), {std::move(binding)}),
            tablet_schema, &collect_infos);

    ASSERT_TRUE(status.ok()) << status;
    ASSERT_EQ(collect_infos.size(), 1U);
    auto it = collect_infos.find(
            StringHelper::to_wstring(std::to_string(kVariantUid) + ".v.dynamic"));
    ASSERT_NE(it, collect_infos.end());
    ASSERT_NE(it->second.index_meta, nullptr);
    ASSERT_NE(it->second.owned_index_meta, nullptr);
    EXPECT_EQ(it->second.index_meta->index_id(), 3025);
    EXPECT_EQ(it->second.unique_terms, std::vector<std::string>({"alpha", "beta"}));
}

TEST_F(CollectionStatisticsTest, SearchVariantFieldPatternKeepsSelectedMetadataAlive) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    constexpr int32_t kVariantUid = 9020;

    TabletColumn variant_column;
    variant_column.set_unique_id(kVariantUid);
    variant_column.set_name("meta");
    variant_column.set_type(FieldType::OLAP_FIELD_TYPE_VARIANT);
    auto subcolumn_template = make_subcolumn_template("user.*", PatternTypePB::MATCH_NAME_GLOB);
    variant_column.add_sub_column(subcolumn_template);
    tablet_schema->append_column(variant_column);

    TabletIndexPB index_pb;
    index_pb.set_index_id(3030);
    index_pb.set_index_name("variant_search_field_pattern_idx");
    index_pb.set_index_type(IndexType::INVERTED);
    index_pb.add_col_unique_id(kVariantUid);
    (*index_pb.mutable_properties())["parser"] = "standard";
    (*index_pb.mutable_properties())["support_phrase"] = "true";
    (*index_pb.mutable_properties())["field_pattern"] = "user.*";
    TabletIndex index;
    index.init_from_pb(index_pb);
    tablet_schema->append_index(std::move(index));

    TSearchClause clause;
    clause.clause_type = "PHRASE";
    clause.field_name = "meta.user.name";
    clause.value = "alice smith";
    clause.__isset.field_name = true;
    clause.__isset.value = true;
    TSearchFieldBinding binding;
    binding.field_name = "meta.user.name";
    binding.slot_index = 0;
    binding.is_variant_subcolumn = true;
    binding.__isset.is_variant_subcolumn = true;
    binding.parent_field_name = "meta";
    binding.__isset.parent_field_name = true;
    binding.subcolumn_path = "user.name";
    binding.__isset.subcolumn_path = true;
    binding.index_properties["parser"] = "standard";
    binding.index_properties["support_phrase"] = "true";
    binding.__isset.index_properties = true;
    CollectInfoMap collect_infos;

    auto status = stats_->extract_collect_info(
            runtime_state_.get(), create_search_contexts(std::move(clause), {std::move(binding)}),
            tablet_schema, &collect_infos);

    ASSERT_TRUE(status.ok()) << status.msg();
    ASSERT_EQ(collect_infos.size(), 1u);
    auto iter = collect_infos.find(
            StringHelper::to_wstring(std::to_string(kVariantUid) + ".meta.user.name"));
    ASSERT_NE(iter, collect_infos.end());
    ASSERT_NE(iter->second.index_meta, nullptr);
    ASSERT_NE(iter->second.owned_index_meta, nullptr);
    EXPECT_EQ(iter->second.index_meta->index_id(), 3030);
}

TEST_F(CollectionStatisticsTest, SearchScoringRejectsNumericBkdLeaf) {
    auto tablet_schema = std::make_shared<TabletSchema>();
    TabletColumn column;
    column.set_unique_id(2);
    column.set_name("number");
    column.set_type(FieldType::OLAP_FIELD_TYPE_INT);
    tablet_schema->append_column(column);
    TabletIndex index;
    index._index_id = 3040;
    index._index_type = IndexType::INVERTED;
    index._col_unique_ids.push_back(2);
    tablet_schema->append_index(std::move(index));
    TSearchClause clause;
    clause.clause_type = "TERM";
    clause.field_name = "number";
    clause.value = "42";
    clause.__isset.field_name = true;
    clause.__isset.value = true;
    CollectInfoMap collect_infos;

    auto status = stats_->extract_collect_info(runtime_state_.get(),
                                               create_search_contexts(std::move(clause)),
                                               tablet_schema, &collect_infos);

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
    EXPECT_TRUE(collect_infos.empty());
}

TEST_F(CollectionStatisticsTest, NestedSearchScoringIsRejected) {
    TSearchClause phrase;
    phrase.clause_type = "PHRASE";
    phrase.field_name = "content";
    phrase.value = "alpha beta";
    phrase.__isset.field_name = true;
    phrase.__isset.value = true;

    TSearchClause nested;
    nested.clause_type = "NESTED";
    nested.children.push_back(std::move(phrase));
    nested.__isset.children = true;

    CollectInfoMap collect_infos;
    auto status = stats_->extract_collect_info(
            runtime_state_.get(), create_search_contexts(std::move(nested)),
            create_tablet_schema_with_inverted_index(), &collect_infos);

    EXPECT_EQ(status.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
    EXPECT_TRUE(collect_infos.empty());
}

TEST_F(CollectionStatisticsTest, OneScoringFieldCannotSelectDifferentPhysicalIndexes) {
    auto tablet_schema = create_tablet_schema_with_two_fulltext_indexes();
    auto build_match = [](const std::string& analyzer_name, InvertedIndexParserType parser_type) {
        auto match_expr =
                std::make_shared<collection_statistics::MockVExpr>(TExprNodeType::MATCH_PRED);
        match_expr->_children.push_back(
                std::make_shared<collection_statistics::MockVSlotRef>("content", SlotId(1)));
        match_expr->_children.push_back(
                std::make_shared<collection_statistics::MockVLiteral>("alpha beta"));

        InvertedIndexAnalyzerConfig config;
        config.analyzer_name = analyzer_name;
        config.parser_type = parser_type;
        config.stop_words = "none";
        auto analyzer_ctx = std::make_shared<InvertedIndexAnalyzerCtx>();
        analyzer_ctx->analyzer_key = analyzer_name;
        analyzer_ctx->parser_type = parser_type;
        analyzer_ctx->analyzer_provider =
                segment_v2::inverted_index::InvertedIndexAnalyzer::create_analyzer_provider(
                        &config);
        match_expr->set_analyzer_ctx(std::move(analyzer_ctx));
        return match_expr;
    };

    MatchPredicateCollector collector;
    CollectInfoMap collect_infos;
    auto first = collector.collect(
            runtime_state_.get(), tablet_schema,
            build_match("standard", InvertedIndexParserType::PARSER_STANDARD), &collect_infos);
    ASSERT_TRUE(first.ok()) << first.msg();

    auto second = collector.collect(runtime_state_.get(), tablet_schema,
                                    build_match("english", InvertedIndexParserType::PARSER_ENGLISH),
                                    &collect_infos);

    EXPECT_EQ(second.code(), ErrorCode::INVERTED_INDEX_NOT_SUPPORTED);
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

} // namespace doris
