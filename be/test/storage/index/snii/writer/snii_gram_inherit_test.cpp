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

#include <cstdint>
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
#include "storage/index/inverted/gram/gram_scheme.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_iterator.h"
#include "storage/index/snii/reader/logical_index_reader.h"
#include "storage/index/snii/reader/snii_segment_reader.h"
#include "storage/index/snii/snii_index_reader.h"
#include "storage/index/snii/snii_index_writer.h"
#include "storage/olap_common.h"
#include "storage/segment/column_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris {
namespace {

using segment_v2::gram::GramMode;
using segment_v2::gram::GramScheme;

constexpr const char* kTestDir = "./ut_dir/snii_gram_inherit_test";
constexpr const char* kSourcePrefix = "./ut_dir/snii_gram_inherit_test/source";
constexpr const char* kInheritedPrefix = "./ut_dir/snii_gram_inherit_test/inherited";

struct InheritedPatternResult {
    ColumnPtr row_result;
    segment_v2::InvertedIndexResultBitmap candidates;
    bool has_exact_result = false;
};

class SniiGramInheritTest : public testing::Test {
protected:
    void SetUp() override {
        auto* exec_env = ExecEnv::GetInstance();
        _previous_policy_mgr = exec_env->index_policy_mgr();
        exec_env->_index_policy_mgr = &_policy_mgr;
        _previous_query_cache = exec_env->get_inverted_index_query_cache();
        _query_cache.reset(
                segment_v2::InvertedIndexQueryCache::create_global_cache(1024 * 1024, 1));
        exec_env->set_inverted_index_query_cache(_query_cache.get());

        register_index(6753900, "dense", _dense_scheme, &_dense_index);
        register_index(6753910, "sparse", _sparse_scheme, &_sparse_index);
        register_index(6753920, "fresh", _fresh_scheme, &_fresh_index);
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kTestDir).ok());

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
    }

    void TearDown() override {
        auto* exec_env = ExecEnv::GetInstance();
        exec_env->set_inverted_index_query_cache(_previous_query_cache);
        _query_cache.reset();
        exec_env->_index_policy_mgr = _previous_policy_mgr;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    void register_index(int64_t index_id, const std::string& label, const GramScheme& scheme,
                        TabletIndex* index) {
        TIndexPolicy tokenizer;
        tokenizer.id = index_id + 1;
        tokenizer.name = "gram_inherit_" + label + "_tokenizer";
        tokenizer.type = TIndexPolicyType::TOKENIZER;
        tokenizer.properties = scheme.to_properties();
        tokenizer.properties["type"] = "ngram";
        TIndexPolicy analyzer;
        analyzer.id = index_id + 2;
        analyzer.name = "gram_inherit_" + label + "_analyzer";
        analyzer.type = TIndexPolicyType::ANALYZER;
        analyzer.properties = {{"tokenizer", tokenizer.name}};
        _policy_mgr.apply_policy_changes({tokenizer, analyzer}, {});

        TabletIndexPB index_pb;
        index_pb.set_index_type(IndexType::INVERTED);
        index_pb.set_index_id(index_id);
        index_pb.set_index_name("gram_inherit_" + label + "_index");
        index_pb.add_col_unique_id(0);
        index_pb.mutable_properties()->insert({"analyzer", analyzer.name});
        index_pb.mutable_properties()->insert({"support_phrase", "false"});
        index->init_from_pb(index_pb);
    }

    Status append_index(segment_v2::IndexFileWriter* file_writer, const TabletIndex& index) {
        segment_v2::SniiIndexColumnWriter writer(file_writer, &index,
                                                 FieldType::OLAP_FIELD_TYPE_VARCHAR);
        RETURN_IF_ERROR(writer.init());
        std::vector<Slice> slices;
        for (const auto& value : _values) {
            slices.emplace_back(value);
        }
        RETURN_IF_ERROR(writer.add_values("p", slices.data(), slices.size()));
        return writer.finish();
    }

    Status write_source() {
        io::FileWriterPtr file_writer;
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(
                std::string(kSourcePrefix) + ".idx", &file_writer));
        segment_v2::IndexFileWriter container(
                io::global_local_filesystem(), kSourcePrefix, "gram_inherit_source", 0,
                InvertedIndexStorageFormatPB::SNII, std::move(file_writer), true, 6753900);
        RETURN_IF_ERROR(append_index(&container, _dense_index));
        RETURN_IF_ERROR(append_index(&container, _sparse_index));
        RETURN_IF_ERROR(container.begin_close());
        return container.finish_close();
    }

    Status inherit_and_append() {
        // These are the same public entry points BUILD INDEX uses. The source reader and
        // snapshot are local: no source file handle can survive this method's return.
        auto source = std::make_shared<segment_v2::IndexFileReader>(
                io::global_local_filesystem(), kSourcePrefix, InvertedIndexStorageFormatPB::SNII);
        RETURN_IF_ERROR(source->init());
        std::vector<snii::reader::LogicalIndexKey> keep;
        for (const auto* index : {&_dense_index, &_sparse_index}) {
            keep.push_back({.index_id = static_cast<uint64_t>(index->index_id()),
                            .index_suffix = index->get_index_suffix()});
        }
        snii::reader::SniiRewriteSnapshot snapshot;
        RETURN_IF_ERROR(source->prepare_snii_rewrite_snapshot(keep, _values.size(), &snapshot));
        io::FileWriterPtr file_writer;
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(
                std::string(kInheritedPrefix) + ".idx", &file_writer));
        segment_v2::IndexFileWriter container(
                io::global_local_filesystem(), kInheritedPrefix, "gram_inherit_output", 0,
                InvertedIndexStorageFormatPB::SNII, std::move(file_writer), true, 6753900);
        RETURN_IF_ERROR(container.inherit_snii(snapshot, source->snii_io_reader()));
        RETURN_IF_ERROR(append_index(&container, _fresh_index));
        RETURN_IF_ERROR(container.begin_close());
        return container.finish_close();
    }

    Status evaluate_pattern(const std::shared_ptr<segment_v2::IndexFileReader>& file_reader,
                            const TabletIndex& index, const std::string& function_name,
                            const std::string& pattern, InheritedPatternResult* result) {
        auto reader = segment_v2::SniiIndexReader::create_shared(
                &index, file_reader, segment_v2::InvertedIndexReaderType::FULLTEXT, _values.size(),
                /*column_is_array=*/false);
        std::vector<std::unique_ptr<segment_v2::IndexIterator>> iterators(1);
        RETURN_IF_ERROR(reader->new_iterator(iterators.data()));
        iterators[0]->set_context(_query_context);

        const DataTypePtr string_type = std::make_shared<DataTypeString>();
        TFunction function;
        TFunctionName name;
        name.__set_function_name(function_name);
        function.__set_name(name);
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
        expression->add_child(VSlotRef::create_shared(0, 0, 0, string_type, "p"));
        expression->add_child(
                VLiteral::create_shared(string_type, Field::create_field<TYPE_STRING>(pattern)));

        std::vector<IndexFieldNameAndTypePair> storage_types {{"p", string_type}};
        std::unordered_map<ColumnId, std::unordered_map<const VExpr*, bool>> index_status;
        segment_v2::ColumnIteratorOptions column_options;
        auto index_context = std::make_shared<IndexExecContext>(
                iterators, storage_types, index_status, nullptr, nullptr, column_options);
        VExprContext context(expression);
        context.set_index_context(index_context);
        RETURN_IF_ERROR(context.prepare(&_runtime_state, RowDescriptor {}));
        RETURN_IF_ERROR(context.open(&_runtime_state));
        auto column = ColumnString::create();
        for (const auto& value : _values) {
            column->insert_data(value.data(), value.size());
        }
        Block block;
        block.insert({std::move(column), string_type, "p"});
        RETURN_IF_ERROR(context.execute(&block, result->row_result));
        RETURN_IF_ERROR(context.evaluate_inverted_index(_values.size()));
        result->has_exact_result = index_context->has_index_result_for_expr(expression.get());
        if (const auto* candidates =
                    index_context->get_approx_index_result_for_expr(expression.get())) {
            result->candidates = *candidates;
        }
        return Status::OK();
    }

    void check_pattern_result(const InheritedPatternResult& result,
                              const roaring::Roaring& expected) {
        ASSERT_TRUE(result.row_result->get_bool(0));
        ASSERT_FALSE(result.row_result->get_bool(1));
        ASSERT_FALSE(result.row_result->get_bool(2));
        EXPECT_FALSE(result.has_exact_result);
        // Missing metadata would fall back to a scalar scan. That is safe for queries,
        // but is NOT a passing inheritance test: both schemes must still prune.
        ASSERT_FALSE(result.candidates.is_empty());
        EXPECT_TRUE(result.candidates.approximate());
        EXPECT_TRUE(*result.candidates.get_data_bitmap() == expected)
                << result.candidates.get_data_bitmap()->toString();
        EXPECT_TRUE(result.candidates.get_data_bitmap()->contains(0));
        EXPECT_FALSE(result.candidates.get_data_bitmap()->contains(2));
    }

    void check_reopened_index(const char* prefix, const TabletIndex& index,
                              const GramScheme& expected_scheme,
                              const std::vector<uint32_t>& expected_candidates) {
        SCOPED_TRACE(prefix);
        SCOPED_TRACE(index.index_id());
        auto file_reader = std::make_shared<segment_v2::IndexFileReader>(
                io::global_local_filesystem(), prefix, InvertedIndexStorageFormatPB::SNII);
        auto status = file_reader->init();
        ASSERT_TRUE(status.ok()) << status;
        auto logical_reader = file_reader->open_snii_index(&index);
        ASSERT_TRUE(logical_reader.has_value()) << logical_reader.error();
        const auto& actual_scheme = (*logical_reader)->gram_scheme();
        ASSERT_TRUE(actual_scheme.has_value());
        // Equality includes mode, min/max length, density, stop DF, case and hash version.
        EXPECT_EQ(expected_scheme.to_properties(), actual_scheme->to_properties());

        roaring::Roaring expected;
        for (uint32_t row : expected_candidates) {
            expected.add(row);
        }
        for (const auto& [name, pattern] : std::vector<std::pair<std::string, std::string>> {
                     {"like", "%abcdefgh%"}, {"regexp", "abcdefgh"}}) {
            SCOPED_TRACE(name);
            InheritedPatternResult result;
            status = evaluate_pattern(file_reader, index, name, pattern, &result);
            ASSERT_TRUE(status.ok()) << status;
            ASSERT_NO_FATAL_FAILURE(check_pattern_result(result, expected));
        }
    }

    // Row 1 contains every dense3 gram of "abcdefgh", but no four-byte gram of it.
    // It must therefore remain a dense false positive and be pruned by the sparse4 index.
    const std::vector<std::string> _values {"abcdefgh", "abc!bcd!cde!def!efg!fgh", "unrelated"};
    const GramScheme _dense_scheme {.mode = GramMode::DENSE,
                                    .min_len = 3,
                                    .max_len = 7,
                                    .density_permille = 625,
                                    .stop_df_permille = 0,
                                    .lower_case = false,
                                    .hash_version = 1};
    const GramScheme _sparse_scheme {.mode = GramMode::SPARSE,
                                     .min_len = 4,
                                     .max_len = 8,
                                     .density_permille = 1000,
                                     .stop_df_permille = 0,
                                     .lower_case = false,
                                     .hash_version = 1};
    const GramScheme _fresh_scheme {.mode = GramMode::DENSE,
                                    .min_len = 5,
                                    .max_len = 9,
                                    .density_permille = 500,
                                    .stop_df_permille = 0,
                                    .lower_case = false,
                                    .hash_version = 1};
    IndexPolicyMgr _policy_mgr;
    IndexPolicyMgr* _previous_policy_mgr = nullptr;
    segment_v2::InvertedIndexQueryCache* _previous_query_cache = nullptr;
    std::unique_ptr<segment_v2::InvertedIndexQueryCache> _query_cache;
    TabletIndex _dense_index;
    TabletIndex _sparse_index;
    TabletIndex _fresh_index;
    RuntimeState _runtime_state;
    OlapReaderStatistics _stats;
    io::IOContext _io_context;
    segment_v2::IndexQueryContextPtr _query_context;
};

TEST_F(SniiGramInheritTest, ReopenedContainerKeepsDistinctSchemesAndPatternCandidates) {
    auto status = write_source();
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NO_FATAL_FAILURE(
            check_reopened_index(kSourcePrefix, _dense_index, _dense_scheme, {0, 1}));
    ASSERT_NO_FATAL_FAILURE(
            check_reopened_index(kSourcePrefix, _sparse_index, _sparse_scheme, {0}));

    status = inherit_and_append();
    ASSERT_TRUE(status.ok()) << status;
    // All source readers have been destroyed. Removing the source additionally ensures that
    // reopening the inherited container cannot accidentally consult the original file.
    status = io::global_local_filesystem()->delete_file(std::string(kSourcePrefix) + ".idx");
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NO_FATAL_FAILURE(
            check_reopened_index(kInheritedPrefix, _dense_index, _dense_scheme, {0, 1}));
    ASSERT_NO_FATAL_FAILURE(
            check_reopened_index(kInheritedPrefix, _sparse_index, _sparse_scheme, {0}));
    ASSERT_NO_FATAL_FAILURE(
            check_reopened_index(kInheritedPrefix, _fresh_index, _fresh_scheme, {0}));
}

} // namespace
} // namespace doris
