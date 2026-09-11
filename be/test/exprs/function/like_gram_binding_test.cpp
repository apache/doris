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

#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "exprs/function/like.h"
#include "exprs/vcompound_pred.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "gen_cpp/AgentService_types.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "runtime/runtime_state.h"
#include "storage/index/index_file_reader.h"
#include "storage/index/index_file_writer.h"
#include "storage/index/index_query_context.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/snii/snii_index_reader.h"
#include "storage/index/snii/snii_index_writer.h"
#include "storage/olap_common.h"
#include "storage/segment/column_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris {
namespace {

constexpr const char* kTestDir = "./ut_dir/like_gram_binding_test";
constexpr const char* kIndexPathPrefix = "./ut_dir/like_gram_binding_test/segment";
constexpr int64_t kIndexId = 6753801;

struct PatternEvaluation {
    ColumnPtr row_result;
    segment_v2::InvertedIndexResultBitmap candidates;
    bool has_exact_result = false;
};

class LikeGramBindingTest : public testing::Test {
protected:
    void SetUp() override {
        auto* exec_env = ExecEnv::GetInstance();
        _previous_policy_mgr = exec_env->index_policy_mgr();
        exec_env->_index_policy_mgr = &_policy_mgr;
        _previous_query_cache = exec_env->get_inverted_index_query_cache();
        _query_cache.reset(
                segment_v2::InvertedIndexQueryCache::create_global_cache(1024 * 1024, 1));
        exec_env->set_inverted_index_query_cache(_query_cache.get());

        TIndexPolicy tokenizer;
        tokenizer.id = 6753802;
        tokenizer.name = "like_binding_dense_tokenizer";
        tokenizer.type = TIndexPolicyType::TOKENIZER;
        tokenizer.properties = {{"type", "ngram"}, {"mode", "dense"}, {"min_gram", "3"}};
        TIndexPolicy analyzer;
        analyzer.id = 6753803;
        analyzer.name = "like_binding_dense_analyzer";
        analyzer.type = TIndexPolicyType::ANALYZER;
        analyzer.properties = {{"tokenizer", tokenizer.name}};
        _policy_mgr.apply_policy_changes({tokenizer, analyzer}, {});

        TabletIndexPB index_pb;
        index_pb.set_index_type(IndexType::INVERTED);
        index_pb.set_index_id(kIndexId);
        index_pb.set_index_name("like_binding_index");
        index_pb.add_col_unique_id(0);
        index_pb.mutable_properties()->insert({"analyzer", analyzer.name});
        _index_meta.init_from_pb(index_pb);

        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kTestDir).ok());
        auto status = write_indexes(kIndexPathPrefix, {&_index_meta});
        ASSERT_TRUE(status.ok()) << status;
        _file_reader = std::make_shared<segment_v2::IndexFileReader>(
                io::global_local_filesystem(), kIndexPathPrefix,
                InvertedIndexStorageFormatPB::SNII);
        status = _file_reader->init();
        ASSERT_TRUE(status.ok()) << status;
        auto reader = segment_v2::SniiIndexReader::create_shared(
                &_index_meta, _file_reader, segment_v2::InvertedIndexReaderType::FULLTEXT,
                _values.size(), /*column_is_array=*/false);
        _iterators.resize(1);
        status = reader->new_iterator(_iterators.data());
        ASSERT_TRUE(status.ok()) << status;

        TQueryOptions options;
        options.__set_query_type(TQueryType::SELECT);
        options.__set_enable_hyperscan_fallback(true);
        options.__set_enable_inverted_index_query_cache(false);
        options.__set_enable_inverted_index_searcher_cache(false);
        _runtime_state.set_query_options(options);
        _query_context = std::make_shared<segment_v2::IndexQueryContext>();
        _query_context->runtime_state = &_runtime_state;
        _query_context->stats = &_stats;
        _query_context->io_ctx = &_io_context;
        _iterators[0]->set_context(_query_context);
    }

    void TearDown() override {
        _iterators.clear();
        _file_reader.reset();
        auto* exec_env = ExecEnv::GetInstance();
        exec_env->set_inverted_index_query_cache(_previous_query_cache);
        _query_cache.reset();
        exec_env->_index_policy_mgr = _previous_policy_mgr;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    Status write_indexes(const std::string& path_prefix,
                         const std::vector<const TabletIndex*>& indexes) {
        io::FileWriterPtr file_writer;
        RETURN_IF_ERROR(
                io::global_local_filesystem()->create_file(path_prefix + ".idx", &file_writer));
        segment_v2::IndexFileWriter index_file_writer(
                io::global_local_filesystem(), path_prefix, "like_binding_rowset", 0,
                InvertedIndexStorageFormatPB::SNII, std::move(file_writer), true, kIndexId);
        std::vector<Slice> slices;
        for (const auto& value : _values) {
            slices.emplace_back(value);
        }
        for (const auto* index : indexes) {
            segment_v2::SniiIndexColumnWriter writer(&index_file_writer, index,
                                                     FieldType::OLAP_FIELD_TYPE_VARCHAR);
            RETURN_IF_ERROR(writer.init());
            RETURN_IF_ERROR(writer.add_values("p", slices.data(), slices.size()));
            RETURN_IF_ERROR(writer.finish());
        }
        RETURN_IF_ERROR(index_file_writer.begin_close());
        return index_file_writer.finish_close();
    }

    Status evaluate_pattern(const std::string& name, bool column_on_left,
                            const std::string& literal_value, PatternEvaluation* result) {
        const DataTypePtr string_type = std::make_shared<DataTypeString>();
        TFunction function;
        TFunctionName function_name;
        function_name.__set_function_name(name);
        function.__set_name(function_name);
        function.__set_binary_type(TFunctionBinaryType::BUILTIN);
        function.__set_arg_types({string_type->to_thrift(), string_type->to_thrift()});
        function.__set_ret_type(DataTypeUInt8().to_thrift());
        function.__set_has_var_args(false);
        TExprNode node;
        node.__set_node_type(TExprNodeType::FUNCTION_CALL);
        node.__set_type(DataTypeUInt8().to_thrift());
        node.__set_fn(function);
        node.__set_num_children(2);
        node.__set_is_nullable(false);
        auto expression = VectorizedFnCall::create_shared(node);
        auto slot = VSlotRef::create_shared(0, 0, 0, string_type, "p");
        auto literal = VLiteral::create_shared(string_type,
                                               Field::create_field<TYPE_STRING>(literal_value));
        if (column_on_left) {
            expression->add_child(slot);
            expression->add_child(literal);
        } else {
            expression->add_child(literal);
            expression->add_child(slot);
        }

        std::vector<IndexFieldNameAndTypePair> storage_types {{"p", string_type}};
        std::unordered_map<ColumnId, std::unordered_map<const VExpr*, bool>> index_status;
        segment_v2::ColumnIteratorOptions column_options;
        auto index_context = std::make_shared<IndexExecContext>(
                _iterators, storage_types, index_status, nullptr, nullptr, column_options);
        VExprContext context(expression);
        context.set_index_context(index_context);
        RETURN_IF_ERROR(context.prepare(&_runtime_state, RowDescriptor {}));
        RETURN_IF_ERROR(context.open(&_runtime_state));
        // SegmentIterator names the conjunct root as the consumer; without that the gram
        // push-down knows its result would be dropped and skips the index read altogether.
        context.set_approx_index_result_consumer(expression.get());

        auto column = ColumnString::create();
        for (const auto& value : _values) {
            column->insert_data(value.data(), value.size());
        }
        Block block;
        block.insert({std::move(column), string_type, "p"});
        // Establish the SQL result before any index result can affect expression execution.
        RETURN_IF_ERROR(context.execute(&block, result->row_result));
        RETURN_IF_ERROR(context.evaluate_inverted_index(_values.size()));
        result->has_exact_result = index_context->has_index_result_for_expr(expression.get());
        if (const auto* candidates =
                    index_context->get_approx_index_result_for_expr(expression.get())) {
            result->candidates = *candidates;
        }
        return Status::OK();
    }

    void check_selective_result(const PatternEvaluation& result) {
        ASSERT_FALSE(result.row_result->get_bool(0));
        ASSERT_TRUE(result.row_result->get_bool(1));
        ASSERT_FALSE(result.row_result->get_bool(2));
        EXPECT_FALSE(result.has_exact_result);
        ASSERT_FALSE(result.candidates.is_empty());
        EXPECT_TRUE(result.candidates.approximate());
        EXPECT_TRUE(result.candidates.get_data_bitmap()->contains(1))
                << "The gram index must preserve the TRUE row";
        EXPECT_FALSE(result.candidates.get_data_bitmap()->contains(0));
        EXPECT_FALSE(result.candidates.get_data_bitmap()->contains(2));
    }

    // Where the gram push-down is started from, and whether anybody will read its result back.
    enum class PushDownShape {
        kConsumedRoot,   // the LIKE call is the conjunct root, exactly what SegmentIterator does
        kUnconsumedRoot, // same tree, but the caller reads no approximate result (a virtual
                         // column projection, which only ever materializes exact results)
        kUnderNot,       // NOT(LIKE ...)
        kUnderOr,        // LIKE ... OR LIKE ...
    };

    struct PushDownProbe {
        int64_t index_reader_opens = 0; // readers opened to answer an index query
        bool has_candidates = false;    // an approximate result was stored for the LIKE call
    };

    // Opening a reader is the first thing any index query does, so this counts index reads: a
    // push-down that was skipped before it started leaves the counters where they were.
    int64_t index_reader_opens() const {
        return _stats.inverted_index_searcher_cache_hit + _stats.inverted_index_searcher_cache_miss;
    }

    Status probe_push_down(PushDownShape shape, PushDownProbe* probe) {
        const DataTypePtr string_type = std::make_shared<DataTypeString>();
        auto make_like = [&]() {
            TFunction function;
            TFunctionName function_name;
            function_name.__set_function_name("like");
            function.__set_name(function_name);
            function.__set_binary_type(TFunctionBinaryType::BUILTIN);
            function.__set_arg_types({string_type->to_thrift(), string_type->to_thrift()});
            function.__set_ret_type(DataTypeUInt8().to_thrift());
            function.__set_has_var_args(false);
            TExprNode node;
            node.__set_node_type(TExprNodeType::FUNCTION_CALL);
            node.__set_type(DataTypeUInt8().to_thrift());
            node.__set_fn(function);
            node.__set_num_children(2);
            node.__set_is_nullable(false);
            auto call = VectorizedFnCall::create_shared(node);
            call->add_child(VSlotRef::create_shared(0, 0, 0, string_type, "p"));
            call->add_child(VLiteral::create_shared(
                    string_type, Field::create_field<TYPE_STRING>(std::string("%abcdef%"))));
            return call;
        };

        auto like = make_like();
        VExprSPtr root = like;
        if (shape == PushDownShape::kUnderNot || shape == PushDownShape::kUnderOr) {
            const bool is_not = shape == PushDownShape::kUnderNot;
            TExprNode compound_node;
            compound_node.__set_node_type(TExprNodeType::COMPOUND_PRED);
            compound_node.__set_type(DataTypeUInt8().to_thrift());
            compound_node.__set_opcode(is_not ? TExprOpcode::COMPOUND_NOT
                                              : TExprOpcode::COMPOUND_OR);
            compound_node.__set_num_children(is_not ? 1 : 2);
            compound_node.__set_is_nullable(false);
            auto compound = VCompoundPred::create_shared(compound_node);
            compound->add_child(like);
            if (!is_not) {
                compound->add_child(make_like());
            }
            root = compound;
        }

        std::vector<IndexFieldNameAndTypePair> storage_types {{"p", string_type}};
        std::unordered_map<ColumnId, std::unordered_map<const VExpr*, bool>> index_status;
        segment_v2::ColumnIteratorOptions column_options;
        auto index_context = std::make_shared<IndexExecContext>(
                _iterators, storage_types, index_status, nullptr, nullptr, column_options);
        VExprContext context(root);
        context.set_index_context(index_context);
        RETURN_IF_ERROR(context.prepare(&_runtime_state, RowDescriptor {}));
        RETURN_IF_ERROR(context.open(&_runtime_state));
        if (shape != PushDownShape::kUnconsumedRoot) {
            // SegmentIterator names the conjunct root for every pushed-down conjunct, compound
            // ones included; only the virtual column loop names nothing.
            context.set_approx_index_result_consumer(root.get());
        }

        const int64_t before = index_reader_opens();
        RETURN_IF_ERROR(context.evaluate_inverted_index(_values.size()));
        probe->index_reader_opens = index_reader_opens() - before;
        probe->has_candidates =
                index_context->get_approx_index_result_for_expr(like.get()) != nullptr;
        return Status::OK();
    }

    const std::vector<std::string> _values {"%", "abcdef", "unrelated"};
    IndexPolicyMgr _policy_mgr;
    IndexPolicyMgr* _previous_policy_mgr = nullptr;
    segment_v2::InvertedIndexQueryCache* _previous_query_cache = nullptr;
    std::unique_ptr<segment_v2::InvertedIndexQueryCache> _query_cache;
    TabletIndex _index_meta;
    std::shared_ptr<segment_v2::IndexFileReader> _file_reader;
    std::vector<std::unique_ptr<segment_v2::IndexIterator>> _iterators;
    RuntimeState _runtime_state;
    OlapReaderStatistics _stats;
    io::IOContext _io_context;
    segment_v2::IndexQueryContextPtr _query_context;
};

TEST_F(LikeGramBindingTest, LikePreservesOperandRolesAndPrunesIndexedValues) {
    PatternEvaluation reverse;
    auto status = evaluate_pattern("like", /*column_on_left=*/false, "abcdef", &reverse);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(reverse.row_result->get_bool(0)); // 'abcdef' LIKE '%'
    ASSERT_TRUE(reverse.row_result->get_bool(1));
    ASSERT_FALSE(reverse.row_result->get_bool(2));
    EXPECT_FALSE(reverse.has_exact_result);
    EXPECT_TRUE(reverse.candidates.is_empty() || reverse.candidates.get_data_bitmap()->contains(0))
            << "The index on the pattern operand must not discard a TRUE LIKE row";

    PatternEvaluation forward;
    status = evaluate_pattern("like", /*column_on_left=*/true, "%abcdef%", &forward);
    ASSERT_TRUE(status.ok()) << status;
    check_selective_result(forward);
}

TEST_F(LikeGramBindingTest, LikeUsesTheWrittenSchemeAfterSameNamePolicyReplacement) {
    // SetUp has written and opened a dense3 index. A table can retain that index in the
    // recycle bin while its policy names are dropped and recreated with different settings.
    TIndexPolicy tokenizer;
    tokenizer.id = 6753812;
    tokenizer.name = "like_binding_dense_tokenizer";
    tokenizer.type = TIndexPolicyType::TOKENIZER;
    tokenizer.properties = {{"type", "ngram"}, {"mode", "dense"}, {"min_gram", "4"}};
    TIndexPolicy analyzer;
    analyzer.id = 6753813;
    analyzer.name = "like_binding_dense_analyzer";
    analyzer.type = TIndexPolicyType::ANALYZER;
    analyzer.properties = {{"tokenizer", tokenizer.name}};
    _policy_mgr.apply_policy_changes({tokenizer, analyzer}, {6753803, 6753802});

    PatternEvaluation result;
    const auto status = evaluate_pattern("like", /*column_on_left=*/true, "%abcdef%", &result);
    ASSERT_TRUE(status.ok()) << status;
    check_selective_result(result);
}

TEST_F(LikeGramBindingTest, RegexpPreservesOperandRolesAndPrunesIndexedValues) {
    PatternEvaluation reverse;
    auto status = evaluate_pattern("regexp", /*column_on_left=*/false, "abcdef%", &reverse);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_TRUE(reverse.row_result->get_bool(0)); // 'abcdef%' REGEXP '%'
    ASSERT_TRUE(reverse.row_result->get_bool(1));
    ASSERT_FALSE(reverse.row_result->get_bool(2));
    EXPECT_FALSE(reverse.has_exact_result);
    EXPECT_TRUE(reverse.candidates.is_empty() || reverse.candidates.get_data_bitmap()->contains(0))
            << "The index on the pattern operand must not discard a TRUE REGEXP row";
    EXPECT_TRUE(reverse.candidates.is_empty() || reverse.candidates.get_data_bitmap()->contains(1));

    PatternEvaluation forward;
    status = evaluate_pattern("regexp", /*column_on_left=*/true, "abcdef", &forward);
    ASSERT_TRUE(status.ok()) << status;
    check_selective_result(forward);
}

TEST_F(LikeGramBindingTest, PatternsUseTheSelectedReaderSchemeInASharedContainer) {
    TIndexPolicy tokenizer;
    tokenizer.id = 6753822;
    tokenizer.name = "like_binding_dense4_tokenizer";
    tokenizer.type = TIndexPolicyType::TOKENIZER;
    tokenizer.properties = {{"type", "ngram"}, {"mode", "dense"}, {"min_gram", "4"}};
    TIndexPolicy analyzer;
    analyzer.id = 6753823;
    analyzer.name = "like_binding_dense4_analyzer";
    analyzer.type = TIndexPolicyType::ANALYZER;
    analyzer.properties = {{"tokenizer", tokenizer.name}};
    _policy_mgr.apply_policy_changes({tokenizer, analyzer}, {});

    TabletIndexPB index_pb;
    _index_meta.to_schema_pb(&index_pb);
    index_pb.set_index_id(100000);
    index_pb.set_index_name("like_binding_dense3_index");
    TabletIndex dense3_index;
    dense3_index.init_from_pb(index_pb);
    index_pb.set_index_id(2000);
    index_pb.set_index_name("like_binding_dense4_index");
    (*index_pb.mutable_properties())["analyzer"] = analyzer.name;
    TabletIndex dense4_index;
    dense4_index.init_from_pb(index_pb);

    const std::string path_prefix = std::string(kTestDir) + "/multiple_indexes";
    auto status = write_indexes(path_prefix, {&dense3_index, &dense4_index});
    ASSERT_TRUE(status.ok()) << status;
    auto file_reader = std::make_shared<segment_v2::IndexFileReader>(
            io::global_local_filesystem(), path_prefix, InvertedIndexStorageFormatPB::SNII);
    status = file_reader->init();
    ASSERT_TRUE(status.ok()) << status;
    auto dense3_reader = segment_v2::SniiIndexReader::create_shared(
            &dense3_index, file_reader, segment_v2::InvertedIndexReaderType::FULLTEXT,
            _values.size(), /*column_is_array=*/false);
    auto dense4_reader = segment_v2::SniiIndexReader::create_shared(
            &dense4_index, file_reader, segment_v2::InvertedIndexReaderType::FULLTEXT,
            _values.size(), /*column_is_array=*/false);
    _iterators[0].reset();
    // Registration order differs from selection order: the smaller index id wins.
    status = dense3_reader->new_iterator(_iterators.data());
    ASSERT_TRUE(status.ok()) << status;
    status = dense4_reader->new_iterator(_iterators.data());
    ASSERT_TRUE(status.ok()) << status;
    _iterators[0]->set_context(_query_context);

    for (const auto& [name, pattern] : std::vector<std::pair<std::string, std::string>> {
                 {"like", "%abcdef%"}, {"regexp", "abcdef"}}) {
        SCOPED_TRACE(name);
        PatternEvaluation result;
        status = evaluate_pattern(name, /*column_on_left=*/true, pattern, &result);
        ASSERT_TRUE(status.ok()) << status;
        check_selective_result(result);
    }
}

// A superset of candidate rows is usable in one place only: narrowing the candidates of a
// conjunct that stays pushed down for row-level re-verification. Wherever it is not read back --
// under NOT / OR, or as a virtual column projection -- the push-down must not run at all. Doing
// the term dictionary lookups and posting reads and then discarding the bitmap is a straight
// performance regression against not having the index in the first place.
TEST_F(LikeGramBindingTest, GramPushDownDoesNoIndexIoWhenItsResultWouldBeDiscarded) {
    // Baseline: the shape SegmentIterator actually pushes down does read the index and does
    // produce candidates, so the assertions below are about a skipped read and not about an
    // index that never works.
    PushDownProbe consumed;
    ASSERT_TRUE(probe_push_down(PushDownShape::kConsumedRoot, &consumed).ok());
    EXPECT_GT(consumed.index_reader_opens, 0);
    EXPECT_TRUE(consumed.has_candidates);

    for (const auto& [name, shape] : std::vector<std::pair<const char*, PushDownShape>> {
                 {"virtual column projection", PushDownShape::kUnconsumedRoot},
                 {"NOT(LIKE)", PushDownShape::kUnderNot},
                 {"LIKE OR LIKE", PushDownShape::kUnderOr}}) {
        SCOPED_TRACE(name);
        PushDownProbe probe;
        ASSERT_TRUE(probe_push_down(shape, &probe).ok());
        EXPECT_EQ(probe.index_reader_opens, 0) << "the index was read for a result nobody can use";
        EXPECT_FALSE(probe.has_candidates);
    }
}

// An analyzed query (MATCH_*) is only exact on a gram segment when the current analyzer cuts
// it with the scheme that cut the segment. Here the physical index was written dense-3, while
// the index metadata now names a dense-4 analyzer (a recovered table whose policy was
// recreated; a calibrated density differs from the configured one the same way): the query
// must be skipped, not answered from grams that mean something else. Through the scheme that
// cut the segment the same query is answered by the index.
TEST_F(LikeGramBindingTest, MatchIsSkippedWhenTheCurrentSchemeDiffersFromTheSegments) {
    TIndexPolicy tokenizer;
    tokenizer.id = 6753832;
    tokenizer.name = "like_binding_match_dense4_tokenizer";
    tokenizer.type = TIndexPolicyType::TOKENIZER;
    tokenizer.properties = {{"type", "ngram"}, {"mode", "dense"}, {"min_gram", "4"}};
    TIndexPolicy analyzer;
    analyzer.id = 6753833;
    analyzer.name = "like_binding_match_dense4_analyzer";
    analyzer.type = TIndexPolicyType::ANALYZER;
    analyzer.properties = {{"tokenizer", tokenizer.name}};
    _policy_mgr.apply_policy_changes({tokenizer, analyzer}, {});

    TabletIndexPB index_pb;
    _index_meta.to_schema_pb(&index_pb);
    (*index_pb.mutable_properties())["analyzer"] = analyzer.name;
    TabletIndex recovered;
    recovered.init_from_pb(index_pb);

    const auto value = Field::create_field<TYPE_STRING>(std::string("abcdef"));
    std::shared_ptr<roaring::Roaring> bitmap;
    auto mismatched = segment_v2::SniiIndexReader::create_shared(
            &recovered, _file_reader, segment_v2::InvertedIndexReaderType::FULLTEXT, _values.size(),
            /*column_is_array=*/false);
    for (const auto type : {segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY,
                            segment_v2::InvertedIndexQueryType::MATCH_ALL_QUERY}) {
        Status status = mismatched->query(_query_context, "p", value, type, bitmap);
        EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_EVALUATE_SKIPPED>()) << status;
    }

    auto same = segment_v2::SniiIndexReader::create_shared(
            &_index_meta, _file_reader, segment_v2::InvertedIndexReaderType::FULLTEXT,
            _values.size(), /*column_is_array=*/false);
    Status status = same->query(_query_context, "p", value,
                                segment_v2::InvertedIndexQueryType::MATCH_ANY_QUERY, bitmap);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(bitmap, nullptr);
    EXPECT_FALSE(bitmap->isEmpty()) << "the segment's own scheme answers the query";
}

// Which LIKE forms the index may answer. A backslash escape is the default escaping spelled out,
// so that three-child form is compiled and accelerated like the two-child one. A LIKE carrying its
// own escape character is not: the gram compiler implements the default escaping only, so
// `%100!%%` ESCAPE `!` -- which means "contains 100%" -- would be compiled as if `!` were an
// ordinary byte and would look for rows holding `100!`, dropping the row that matches. It is
// refused on its arguments, before any index is opened, and the predicate is evaluated on the rows.
TEST_F(LikeGramBindingTest, OnlyTheDefaultEscapingIsEligibleForTheIndex) {
    const DataTypePtr string_type = std::make_shared<DataTypeString>();
    const auto literal = [&](const std::string& value) {
        return VLiteral::create_shared(string_type, Field::create_field<TYPE_STRING>(value));
    };
    auto column = VSlotRef::create_shared(0, 0, 0, string_type, "p");
    auto pattern = literal("%100!%%");
    auto like = FunctionLike::create();

    EXPECT_TRUE(like->can_evaluate_inverted_index({column, pattern}));
    EXPECT_TRUE(like->can_evaluate_inverted_index({column, pattern, literal("\\")}));
    EXPECT_FALSE(like->can_evaluate_inverted_index({column, pattern, literal("!")}));
    // An escape that is not a literal cannot be read at all, so it is refused as well.
    EXPECT_FALSE(like->can_evaluate_inverted_index({column, pattern, column}));
}

} // namespace
} // namespace doris
