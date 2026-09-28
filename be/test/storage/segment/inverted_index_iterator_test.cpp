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

#include "storage/index/inverted/inverted_index_iterator.h"

#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/primitive_type.h"
#include "exprs/vmatch_predicate.h"
#include "runtime/exec_env.h"
#include "runtime/index_policy/index_policy_mgr.h"
#include "storage/index/index_reader_helper.h"
#include "storage/index/inverted/analyzer/analyzer.h"
#include "storage/index/inverted/analyzer/ik/dic/Dictionary.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/tablet/tablet_schema.h"
#include "util/defer_op.h"

namespace doris::segment_v2 {

// Mock InvertedIndexReader for testing
class MockInvertedIndexReader : public InvertedIndexReader {
public:
    // Factory method to create instances
    static std::shared_ptr<MockInvertedIndexReader> create(
            const std::map<std::string, std::string>& properties, int64_t index_id = 1) {
        auto index = std::make_shared<TabletIndex>();
        // Initialize TabletIndex with protobuf to ensure all fields are properly set
        TabletIndexPB pb;
        pb.set_index_id(index_id);
        pb.set_index_name("test_index_" + std::to_string(index_id));
        pb.set_index_type(IndexType::INVERTED);
        index->init_from_pb(pb);
        return std::shared_ptr<MockInvertedIndexReader>(
                new MockInvertedIndexReader(index, properties));
    }

    InvertedIndexReaderType type() override { return _type; }
    void set_type(InvertedIndexReaderType type) { _type = type; }
    bool queried = false;
    const std::map<std::string, std::string>& get_index_properties() const override {
        return _properties;
    }

    Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                 const Field& query_value, InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& roaring,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override {
        queried = true;
        return Status::OK();
    }

    Status try_query(const IndexQueryContextPtr& context, const std::string& column_name,
                     const Field& query_value, InvertedIndexQueryType query_type,
                     size_t* count) override {
        *count = 0;
        return Status::OK();
    }

    Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override { return Status::OK(); }

private:
    MockInvertedIndexReader(std::shared_ptr<TabletIndex> index,
                            const std::map<std::string, std::string>& properties)
            : InvertedIndexReader(index.get(), nullptr),
              _mock_index(index), // Copy shared_ptr to keep index alive
              _properties(properties) {}

    std::shared_ptr<TabletIndex> _mock_index; // Keep index alive
    std::map<std::string, std::string> _properties;
    InvertedIndexReaderType _type = InvertedIndexReaderType::FULLTEXT;
};

class InvertedIndexIteratorTest : public testing::Test {
protected:
    std::shared_ptr<MockInvertedIndexReader> create_mock_reader(
            const std::string& analyzer_key,
            InvertedIndexReaderType type = InvertedIndexReaderType::FULLTEXT,
            int64_t index_id = 1) {
        std::map<std::string, std::string> properties;
        // New design: empty string means "user did not specify", non-empty means explicit.
        // We only set properties when analyzer_key is non-empty.
        if (!analyzer_key.empty()) {
            if (AnalyzerConfigParser::is_builtin_analyzer(analyzer_key)) {
                properties[INVERTED_INDEX_PARSER_KEY] = analyzer_key;
            } else {
                properties[INVERTED_INDEX_ANALYZER_NAME_KEY] = analyzer_key;
            }
        }
        auto reader = MockInvertedIndexReader::create(properties, index_id);
        reader->set_type(type);
        return reader;
    }

    std::shared_ptr<VMatchPredicate> create_match_predicate(const std::string& analyzer_name) {
        TMatchPredicate match;
        match.__set_analyzer_name(analyzer_name);
        match.__set_parser_type("english");
        match.__set_parser_mode("coarse_grained");
        match.__set_parser_lowercase(true);
        match.__set_parser_stopwords("none");
        TExprNode node;
        node.__set_node_type(TExprNodeType::MATCH_PRED);
        node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        node.__set_num_children(2);
        node.__set_match_predicate(match);
        return VMatchPredicate::create_shared(node);
    }

    void expect_matching_query_terms(const InvertedIndexReaderPtr& reader,
                                     const VMatchPredicate& predicate, size_t expected_count) {
        const std::string text = "one two";
        const auto indexed_terms = inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                text, reader->get_index_properties());
        auto query_reader = inverted_index::InvertedIndexAnalyzer::create_reader({});
        query_reader->init(text.data(), static_cast<int32_t>(text.size()), false);
        const auto query_terms = inverted_index::InvertedIndexAnalyzer::get_analyse_result(
                query_reader, predicate.query_analyzer_ctx()->get_analyzer().get());
        EXPECT_EQ(indexed_terms.size(), expected_count);
        EXPECT_EQ(query_terms.size(), indexed_terms.size());
    }
};

// ensure_normalized_key tests
TEST_F(InvertedIndexIteratorTest, EnsureNormalizedKey_EmptyInput) {
    // New design: empty string stays empty (means "user did not specify")
    EXPECT_EQ(InvertedIndexIterator::ensure_normalized_key(""), "");
}

TEST_F(InvertedIndexIteratorTest, EnsureNormalizedKey_Uppercase) {
    EXPECT_EQ(InvertedIndexIterator::ensure_normalized_key("CHINESE"), "CHINESE");
}

TEST_F(InvertedIndexIteratorTest, EnsureNormalizedKey_MixedCase) {
    EXPECT_EQ(InvertedIndexIterator::ensure_normalized_key("ChInEsE"), "ChInEsE");
}

TEST_F(InvertedIndexIteratorTest, EnsureNormalizedKey_NonEmptyString) {
    // Non-empty keys retain the resolved policy's spelling.
    EXPECT_EQ(InvertedIndexIterator::ensure_normalized_key("__default__"), "__default__");
    EXPECT_EQ(InvertedIndexIterator::ensure_normalized_key("NONE"), "NONE");
}

// add_reader tests
TEST_F(InvertedIndexIteratorTest, AddReader_SingleReader) {
    InvertedIndexIterator iterator;
    try {
        auto reader = create_mock_reader("chinese");
        iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);

        auto result = iterator.select_best_reader("chinese");
        EXPECT_TRUE(result.has_value());
        EXPECT_EQ(result.value(), reader);
    } catch (const std::exception& e) {
        EXPECT_TRUE(false) << "Exception thrown: " << e.what();
    }
}

TEST_F(InvertedIndexIteratorTest, AddReader_MultipleReadersWithDifferentKeys) {
    InvertedIndexIterator iterator;
    auto reader1 = create_mock_reader("chinese", InvertedIndexReaderType::FULLTEXT, 1);
    auto reader2 = create_mock_reader("english", InvertedIndexReaderType::FULLTEXT, 2);
    auto reader3 = create_mock_reader("", InvertedIndexReaderType::FULLTEXT,
                                      3); // empty key stays empty (no properties set)

    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader1);
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader2);
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader3);

    // Explicit analyzer keys (chinese, english) do exact match
    auto result1 = iterator.select_best_reader("chinese");
    EXPECT_TRUE(result1.has_value());
    EXPECT_EQ(result1.value(), reader1);

    auto result2 = iterator.select_best_reader("english");
    EXPECT_TRUE(result2.has_value());
    EXPECT_EQ(result2.value(), reader2);

    // Empty string = fallback mode (user did not specify).
    // The simple select_best_reader(key) overload returns the first candidate.
    // This is by design: empty means "no preference, let system choose".
    auto result3 = iterator.select_best_reader("");
    EXPECT_TRUE(result3.has_value());
    // Don't assert specific reader - fallback mode returns first available
}

TEST_F(InvertedIndexIteratorTest, SelectBestReaderPreservesCaseDistinctLegacyAnalyzerKeys) {
    for (const std::string legacy_name : {"IK", "Legacy"}) {
        SCOPED_TRACE(legacy_name);
        const std::string lowercase_name = legacy_name == "IK" ? "ik" : "legacy";
        auto legacy_reader = MockInvertedIndexReader::create({{"analyzer", legacy_name}}, 2);
        auto lowercase_reader = MockInvertedIndexReader::create({{"analyzer", lowercase_name}}, 1);
        auto column_type = std::make_shared<DataTypeString>();

        for (const bool legacy_first : {true, false}) {
            SCOPED_TRACE(legacy_first);
            InvertedIndexIterator iterator;
            iterator.add_reader(InvertedIndexReaderType::FULLTEXT,
                                legacy_first ? legacy_reader : lowercase_reader);
            iterator.add_reader(InvertedIndexReaderType::FULLTEXT,
                                legacy_first ? lowercase_reader : legacy_reader);

            const auto legacy = iterator.select_best_reader(
                    column_type, InvertedIndexQueryType::MATCH_ANY_QUERY, legacy_name);
            ASSERT_TRUE(legacy.has_value()) << legacy.error();
            EXPECT_EQ(*legacy, legacy_reader);

            const auto lowercase = iterator.select_best_reader(
                    column_type, InvertedIndexQueryType::MATCH_ANY_QUERY,
                    AnalyzerConfigParser::parse(lowercase_name, "").analyzer_key);
            ASSERT_TRUE(lowercase.has_value()) << lowercase.error();
            EXPECT_EQ(*lowercase, lowercase_reader);
        }
    }
}

TEST_F(InvertedIndexIteratorTest, MatchBindsOldFeNormalizedPolicyName) {
    IndexPolicyMgr policy_mgr;
    auto* exec_env = ExecEnv::GetInstance();
    auto* original_policy_mgr = exec_env->index_policy_mgr();
    exec_env->_index_policy_mgr = &policy_mgr;
    Defer restore_policy_mgr([&] { exec_env->_index_policy_mgr = original_policy_mgr; });

    TIndexPolicy policy;
    policy.id = 100;
    policy.name = "Foo";
    policy.type = TIndexPolicyType::ANALYZER;
    policy.properties["tokenizer"] = "keyword";
    policy_mgr.apply_policy_changes({policy}, {});

    auto reader = MockInvertedIndexReader::create({{"analyzer", "Foo"}});
    InvertedIndexIterator iterator;
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);

    // Older FE versions lowercase the saved analyzer name in MATCH requests.
    const auto predicate = create_match_predicate("foo");

    const auto selected = iterator.select_best_reader(
            std::make_shared<DataTypeString>(), InvertedIndexQueryType::MATCH_ANY_QUERY,
            predicate->get_analyzer_key(), predicate->query_analyzer_ctx()->legacy_analyzer_key);
    ASSERT_TRUE(selected.has_value()) << selected.error();
    EXPECT_EQ(*selected, reader);
}

TEST_F(InvertedIndexIteratorTest, MatchBindsOldFeCanonicalizedPolicyName) {
    IndexPolicyMgr policy_mgr;
    auto* exec_env = ExecEnv::GetInstance();
    auto* original_policy_mgr = exec_env->index_policy_mgr();
    exec_env->_index_policy_mgr = &policy_mgr;
    Defer restore_policy_mgr([&] { exec_env->_index_policy_mgr = original_policy_mgr; });

    TIndexPolicy policy;
    policy.id = 100;
    policy.name = "Foo";
    policy.type = TIndexPolicyType::ANALYZER;
    policy.properties["tokenizer"] = "keyword";
    policy_mgr.apply_policy_changes({policy}, {});

    auto reader = MockInvertedIndexReader::create({{"analyzer", "foo"}});
    InvertedIndexIterator iterator;
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);

    iterator.set_context(std::make_shared<IndexQueryContext>());
    InvertedIndexParam param {};
    param.column_type = std::make_shared<DataTypeString>();
    param.query_type = InvertedIndexQueryType::MATCH_ANY_QUERY;
    // Older FE versions lowercase index properties while preserving replayed policy names.
    for (const std::string name : {"foo", "Foo"}) {
        SCOPED_TRACE(name);
        const auto predicate = create_match_predicate(name);
        EXPECT_EQ(predicate->get_analyzer_key(), "Foo");
        EXPECT_EQ(predicate->query_analyzer_ctx()->legacy_analyzer_key, "foo");
        param.analyzer_ctx = predicate->query_analyzer_ctx();
        reader->queried = false;
        const auto status = iterator.read_from_index(&param);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_TRUE(reader->queried);
    }

    auto exact_reader = MockInvertedIndexReader::create({{"analyzer", "Foo"}}, 2);
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, exact_reader);
    const auto predicate = create_match_predicate("Foo");
    param.analyzer_ctx = predicate->query_analyzer_ctx();
    reader->queried = false;
    const auto status = iterator.read_from_index(&param);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(exact_reader->queried);
    EXPECT_FALSE(reader->queried);
}

TEST_F(InvertedIndexIteratorTest, MatchRejectsLegacyMetadataBoundToAnotherPolicy) {
    IndexPolicyMgr policy_mgr;
    auto* exec_env = ExecEnv::GetInstance();
    auto* original_policy_mgr = exec_env->index_policy_mgr();
    exec_env->_index_policy_mgr = &policy_mgr;
    Defer restore_policy_mgr([&] { exec_env->_index_policy_mgr = original_policy_mgr; });

    TIndexPolicy original;
    original.id = 100;
    original.name = "Foo";
    original.type = TIndexPolicyType::ANALYZER;
    original.properties["tokenizer"] = "keyword";
    TIndexPolicy authoritative = original;
    authoritative.id = 101;
    authoritative.name = "FOO";
    authoritative.properties["tokenizer"] = "standard";
    policy_mgr.apply_policy_changes({authoritative, original}, {});

    auto reader = MockInvertedIndexReader::create({{"analyzer", "foo"}});
    InvertedIndexIterator iterator;
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);
    iterator.set_context(std::make_shared<IndexQueryContext>());
    InvertedIndexParam param {};
    param.column_type = std::make_shared<DataTypeString>();
    param.query_type = InvertedIndexQueryType::MATCH_ANY_QUERY;

    const auto original_predicate = create_match_predicate("Foo");
    EXPECT_TRUE(original_predicate->query_analyzer_ctx()->legacy_analyzer_key.empty());
    param.analyzer_ctx = original_predicate->query_analyzer_ctx();
    const auto missing = iterator.read_from_index(&param);
    EXPECT_TRUE(missing.is<ErrorCode::INVERTED_INDEX_BYPASS>()) << missing;
    EXPECT_FALSE(reader->queried);

    const auto authoritative_predicate = create_match_predicate("FOO");
    EXPECT_EQ(authoritative_predicate->query_analyzer_ctx()->legacy_analyzer_key, "foo");
    param.analyzer_ctx = authoritative_predicate->query_analyzer_ctx();
    const auto status = iterator.read_from_index(&param);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(reader->queried);
}

TEST_F(InvertedIndexIteratorTest, MatchRejectsLegacyAliasReservedForBuiltinAnalyzer) {
    IndexPolicyMgr policy_mgr;
    auto* exec_env = ExecEnv::GetInstance();
    auto* original_policy_mgr = exec_env->index_policy_mgr();
    exec_env->_index_policy_mgr = &policy_mgr;
    Defer restore_policy_mgr([&] { exec_env->_index_policy_mgr = original_policy_mgr; });

    TIndexPolicy policy;
    policy.id = 100;
    policy.name = "STANDARD";
    policy.type = TIndexPolicyType::ANALYZER;
    policy.properties["tokenizer"] = "keyword";
    policy_mgr.apply_policy_changes({policy}, {});
    auto reader = MockInvertedIndexReader::create({{"analyzer", "standard"}});
    InvertedIndexIterator iterator;
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);
    iterator.set_context(std::make_shared<IndexQueryContext>());
    InvertedIndexParam param {};
    param.column_type = std::make_shared<DataTypeString>();
    param.query_type = InvertedIndexQueryType::MATCH_ANY_QUERY;

    const auto custom = create_match_predicate("STANDARD");
    param.analyzer_ctx = custom->query_analyzer_ctx();
    const auto missing = iterator.read_from_index(&param);
    EXPECT_TRUE(missing.is<ErrorCode::INVERTED_INDEX_BYPASS>()) << missing;
    EXPECT_FALSE(reader->queried);

    const auto builtin = create_match_predicate("standard");
    param.analyzer_ctx = builtin->query_analyzer_ctx();
    reader->queried = false;
    const auto status = iterator.read_from_index(&param);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(reader->queried);
    expect_matching_query_terms(reader, *builtin, 2);
}

TEST_F(InvertedIndexIteratorTest, MatchReaderSelectionFollowsResolvedPolicyWhenNamesCollide) {
    IndexPolicyMgr policy_mgr;
    auto* exec_env = ExecEnv::GetInstance();
    auto* original_policy_mgr = exec_env->index_policy_mgr();
    exec_env->_index_policy_mgr = &policy_mgr;
    Defer restore_policy_mgr([&] { exec_env->_index_policy_mgr = original_policy_mgr; });

    TIndexPolicy original;
    original.id = 100;
    original.name = "Foo";
    original.type = TIndexPolicyType::ANALYZER;
    original.properties["tokenizer"] = "keyword";
    TIndexPolicy authoritative = original;
    authoritative.id = 101;
    authoritative.name = "FOO";
    authoritative.properties["tokenizer"] = "standard";
    policy_mgr.apply_policy_changes({authoritative, original}, {});

    auto original_reader = MockInvertedIndexReader::create({{"analyzer", "Foo"}}, 1);
    auto authoritative_reader = MockInvertedIndexReader::create({{"analyzer", "FOO"}}, 2);
    InvertedIndexIterator iterator;
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, original_reader);
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, authoritative_reader);

    for (const std::string name : {"Foo", "FOO", "foo", "fOo"}) {
        SCOPED_TRACE(name);
        const auto predicate = create_match_predicate(name);
        const auto selected = iterator.select_best_reader(
                std::make_shared<DataTypeString>(), InvertedIndexQueryType::MATCH_ANY_QUERY,
                predicate->get_analyzer_key(),
                predicate->query_analyzer_ctx()->legacy_analyzer_key);
        ASSERT_TRUE(selected.has_value()) << selected.error();
        EXPECT_EQ(*selected, name == "Foo" ? original_reader : authoritative_reader);
        EXPECT_EQ(predicate->query_analyzer_ctx()->analyzer_name, name == "Foo" ? "Foo" : "FOO");
        expect_matching_query_terms(*selected, *predicate, name == "Foo" ? 1 : 2);
    }

    InvertedIndexIterator original_only;
    original_only.add_reader(InvertedIndexReaderType::FULLTEXT, original_reader);
    const auto normalized = create_match_predicate("foo");
    const auto missing = original_only.select_best_reader(
            std::make_shared<DataTypeString>(), InvertedIndexQueryType::MATCH_ANY_QUERY,
            normalized->get_analyzer_key(), normalized->query_analyzer_ctx()->legacy_analyzer_key);
    ASSERT_FALSE(missing.has_value());
    EXPECT_TRUE(missing.error().is<ErrorCode::INVERTED_INDEX_BYPASS>());

    TIndexPolicy lowercase = original;
    lowercase.id = 99;
    lowercase.name = "foo";
    policy_mgr.apply_policy_changes({lowercase}, {});
    auto lowercase_reader = MockInvertedIndexReader::create({{"analyzer", "foo"}}, 3);
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, lowercase_reader);
    const auto exact = create_match_predicate("foo");
    const auto selected = iterator.select_best_reader(
            std::make_shared<DataTypeString>(), InvertedIndexQueryType::MATCH_ANY_QUERY,
            exact->get_analyzer_key(), exact->query_analyzer_ctx()->legacy_analyzer_key);
    ASSERT_TRUE(selected.has_value()) << selected.error();
    EXPECT_EQ(*selected, lowercase_reader);
    EXPECT_EQ(exact->query_analyzer_ctx()->analyzer_name, "foo");
}

TEST_F(InvertedIndexIteratorTest, MatchBindsOldFeNormalizedNormalizerName) {
    IndexPolicyMgr policy_mgr;
    auto* exec_env = ExecEnv::GetInstance();
    auto* original_policy_mgr = exec_env->index_policy_mgr();
    exec_env->_index_policy_mgr = &policy_mgr;
    Defer restore_policy_mgr([&] { exec_env->_index_policy_mgr = original_policy_mgr; });

    TIndexPolicy policy;
    policy.id = 100;
    policy.name = "FooNormalizer";
    policy.type = TIndexPolicyType::NORMALIZER;
    policy.properties["token_filter"] = "lowercase";
    policy_mgr.apply_policy_changes({policy}, {});

    auto reader = MockInvertedIndexReader::create({{"normalizer", "FooNormalizer"}});
    InvertedIndexIterator iterator;
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);
    const auto predicate = create_match_predicate("foonormalizer");
    const auto selected = iterator.select_best_reader(
            std::make_shared<DataTypeString>(), InvertedIndexQueryType::MATCH_ANY_QUERY,
            predicate->get_analyzer_key(), predicate->query_analyzer_ctx()->legacy_analyzer_key);
    ASSERT_TRUE(selected.has_value()) << selected.error();
    EXPECT_EQ(*selected, reader);
    EXPECT_EQ(predicate->query_analyzer_ctx()->analyzer_name, policy.name);

    auto lowercase_reader = MockInvertedIndexReader::create({{"normalizer", "foonormalizer"}});
    InvertedIndexIterator lowercase_only;
    lowercase_only.add_reader(InvertedIndexReaderType::FULLTEXT, lowercase_reader);
    lowercase_only.set_context(std::make_shared<IndexQueryContext>());
    InvertedIndexParam param {};
    param.column_type = std::make_shared<DataTypeString>();
    param.query_type = InvertedIndexQueryType::MATCH_ANY_QUERY;
    param.analyzer_ctx = predicate->query_analyzer_ctx();
    const auto status = lowercase_only.read_from_index(&param);
    ASSERT_TRUE(status.ok()) << status;
    EXPECT_TRUE(lowercase_reader->queried);
}

TEST_F(InvertedIndexIteratorTest, MatchBindsDistinctIkModesAndLowercaseStates) {
    const auto original_dict_path = config::inverted_index_dict_path;
    Defer restore_dict_path([&] { config::inverted_index_dict_path = original_dict_path; });
    const char* doris_home = std::getenv("DORIS_HOME");
    ASSERT_NE(doris_home, nullptr);
    config::inverted_index_dict_path = std::string(doris_home) + "../../dict";
    Configuration dictionary_config;
    dictionary_config.setDictPath(config::inverted_index_dict_path + "/ik");
    try {
        Dictionary::initial(dictionary_config);
    } catch (const CLuceneError&) {
        // Another test may have initialized the shared dictionary with an invalid path.
        Dictionary::getSingleton()->getConfiguration()->setDictPath(
                dictionary_config.getDictPath());
        Dictionary::reload();
    }
    const std::vector<std::map<std::string, std::string>> properties {
            {{"parser", "ik"}, {"lower_case", "false"}},
            {{"analyzer", "ik"}, {"lower_case", "false"}},
            {{"analyzer", "ik"}},
            {{"parser", "ik"}}};
    InvertedIndexIterator iterator;
    std::vector<std::shared_ptr<MockInvertedIndexReader>> readers;
    std::set<std::string> keys;
    for (size_t i = 0; i < properties.size(); ++i) {
        auto reader = MockInvertedIndexReader::create(properties[i], i + 1);
        iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);
        readers.push_back(reader);
        keys.insert(build_analyzer_key_from_properties(properties[i]));
    }
    EXPECT_EQ(keys.size(), properties.size());

    for (size_t i = 0; i < properties.size(); ++i) {
        SCOPED_TRACE(i);
        TMatchPredicate match;
        match.__set_analyzer_name("ik");
        match.__set_parser_type("ik");
        match.__set_parser_mode(get_parser_mode_string_from_properties(properties[i]));
        match.__set_parser_lowercase(get_parser_lowercase_from_properties<true>(properties[i]) ==
                                     "true");
        TExprNode node;
        node.__set_node_type(TExprNodeType::MATCH_PRED);
        node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        node.__set_num_children(2);
        node.__set_match_predicate(match);
        const auto predicate = VMatchPredicate::create_shared(node);
        const auto selected = iterator.select_best_reader(std::make_shared<DataTypeString>(),
                                                          InvertedIndexQueryType::MATCH_ANY_QUERY,
                                                          predicate->get_analyzer_key());
        ASSERT_TRUE(selected.has_value()) << selected.error();
        EXPECT_EQ(*selected, readers[i]);
    }
}

TEST_F(InvertedIndexIteratorTest, MatchBindsEffectiveOuterCharacterFilters) {
    const std::vector<std::map<std::string, std::string>> properties {
            {{"analyzer", "standard"}},
            {{"analyzer", "standard"},
             {"char_filter_type", "char_replace"},
             {"char_filter_pattern", "_-"},
             {"char_filter_replacement", " "}},
            {{"analyzer", "standard"},
             {"char_filter_type", "char_replace"},
             {"char_filter_pattern", "_-"},
             {"char_filter_replacement", "a"}}};
    InvertedIndexIterator iterator;
    std::vector<std::shared_ptr<MockInvertedIndexReader>> readers;
    for (size_t i = 0; i < properties.size(); ++i) {
        auto reader = MockInvertedIndexReader::create(properties[i], i + 1);
        iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);
        readers.push_back(reader);
    }

    for (size_t i = 0; i < properties.size(); ++i) {
        SCOPED_TRACE(i);
        auto query_properties = properties[i];
        query_properties["char_filter_type"] = "char_replace";
        query_properties["char_filter_pattern"] = i == 0 ? "a" : "-__-";
        query_properties["char_filter_replacement"] = i == 1 ? " " : "a";
        TMatchPredicate match;
        match.__set_analyzer_name("standard");
        match.__set_parser_type("standard");
        match.__set_parser_lowercase(true);
        match.__set_char_filter_map(get_parser_char_filter_map_from_properties(query_properties));
        TExprNode node;
        node.__set_node_type(TExprNodeType::MATCH_PRED);
        node.__set_type(create_type_desc(PrimitiveType::TYPE_BOOLEAN));
        node.__set_num_children(2);
        node.__set_match_predicate(match);
        const auto predicate = VMatchPredicate::create_shared(node);
        const auto selected = iterator.select_best_reader(std::make_shared<DataTypeString>(),
                                                          InvertedIndexQueryType::MATCH_ANY_QUERY,
                                                          predicate->get_analyzer_key());
        ASSERT_TRUE(selected.has_value()) << selected.error();
        EXPECT_EQ(*selected, readers[i]);
    }
}

TEST_F(InvertedIndexIteratorTest, EncodedSelectionKeysDoNotCollideWithPolicyNames) {
    for (const std::map<std::string, std::string>& properties :
         {std::map<std::string, std::string> {{"analyzer", "ik"}},
          {{"analyzer", "standard"},
           {"char_filter_type", "char_replace"},
           {"char_filter_pattern", "_"}}}) {
        const auto policy_name = build_analyzer_key_from_properties(properties);
        SCOPED_TRACE(policy_name);
        auto policy_reader = MockInvertedIndexReader::create({{"analyzer", policy_name}}, 1);
        auto configured_reader = MockInvertedIndexReader::create(properties, 2);
        InvertedIndexIterator iterator;
        iterator.add_reader(InvertedIndexReaderType::FULLTEXT, policy_reader);
        iterator.add_reader(InvertedIndexReaderType::FULLTEXT, configured_reader);

        const auto configured = AnalyzerConfigParser::parse(
                properties.at("analyzer"), "", get_parser_mode_string_from_properties(properties),
                true, get_parser_char_filter_map_from_properties(properties));
        const auto selected = iterator.select_best_reader(configured.analyzer_key);
        ASSERT_TRUE(selected.has_value());
        EXPECT_EQ(*selected, configured_reader);

        const auto named = AnalyzerConfigParser::parse(policy_name, "");
        EXPECT_EQ(named.provider_name, policy_name);
        EXPECT_NE(named.analyzer_key, configured.analyzer_key);
        const auto selected_policy = iterator.select_best_reader(named.analyzer_key);
        ASSERT_TRUE(selected_policy.has_value());
        EXPECT_EQ(*selected_policy, policy_reader);
    }
}

TEST_F(InvertedIndexIteratorTest, AddReader_DuplicateIndexIdFails) {
    auto first_reader = create_mock_reader("chinese", InvertedIndexReaderType::FULLTEXT, 7);
    auto duplicate_reader = create_mock_reader("english", InvertedIndexReaderType::FULLTEXT, 7);

#ifndef NDEBUG
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
            {
                InvertedIndexIterator iterator;
                iterator.add_reader(InvertedIndexReaderType::FULLTEXT, first_reader);
                iterator.add_reader(InvertedIndexReaderType::FULLTEXT, duplicate_reader);
            },
            "Duplicate inverted index id 7 in one field");
#else
    InvertedIndexIterator iterator;
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, first_reader);
    try {
        iterator.add_reader(InvertedIndexReaderType::FULLTEXT, duplicate_reader);
        FAIL() << "Expected doris::Exception";
    } catch (const Exception& e) {
        const auto message = e.to_string();
        EXPECT_NE(message.find("Duplicate inverted index id 7 in one field"), std::string::npos)
                << message;
    }
#endif
}

TEST_F(InvertedIndexIteratorTest, DefaultRawReaderMatchesExplicitNone) {
    InvertedIndexIterator iterator;
    auto raw_reader = create_mock_reader("");
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, raw_reader);

    auto result_none = iterator.select_best_reader(INVERTED_INDEX_PARSER_NONE);
    EXPECT_TRUE(result_none.has_value());
    EXPECT_EQ(result_none.value(), raw_reader);
}

// find_reader_candidates tests (via select_best_reader)
TEST_F(InvertedIndexIteratorTest, FindReaderCandidates_ExactMatch) {
    InvertedIndexIterator iterator;
    auto reader = create_mock_reader("chinese");
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);

    auto result = iterator.select_best_reader("chinese");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), reader);
}

TEST_F(InvertedIndexIteratorTest, FindReaderCandidates_FallbackWithEmptyKey) {
    InvertedIndexIterator iterator;
    auto empty_reader = create_mock_reader("");
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, empty_reader);

    // Empty string = fallback mode, returns available reader
    auto result = iterator.select_best_reader("");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), empty_reader);
}

TEST_F(InvertedIndexIteratorTest, FindReaderCandidates_FallbackToAny) {
    InvertedIndexIterator iterator;
    auto chinese_reader = create_mock_reader("chinese");
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, chinese_reader);

    // Empty string = fallback mode, returns any available reader
    auto result = iterator.select_best_reader("");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), chinese_reader);
}

TEST_F(InvertedIndexIteratorTest, FindReaderCandidates_EmptyReaders) {
    InvertedIndexIterator iterator;
    auto result = iterator.select_best_reader("chinese");
    EXPECT_FALSE(result.has_value());
}

// Test: explicit analyzer that doesn't exist returns error
TEST_F(InvertedIndexIteratorTest, ExplicitAnalyzer_NotFound_ReturnsBypass) {
    InvertedIndexIterator iterator;
    auto chinese_reader = create_mock_reader("chinese");
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, chinese_reader);

    // Query for "english" (explicit) but only "chinese" exists
    auto result = iterator.select_best_reader("english");
    EXPECT_FALSE(result.has_value());
    // Error should be INVERTED_INDEX_BYPASS
}

// Test: empty string (user did not specify) returns any available reader
TEST_F(InvertedIndexIteratorTest, EmptyString_NoSpecifiedAnalyzer_ReturnsAny) {
    InvertedIndexIterator iterator;
    auto chinese_reader = create_mock_reader("chinese");
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, chinese_reader);

    // Empty = fallback mode, should return the chinese_reader
    auto result = iterator.select_best_reader("");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), chinese_reader);
}

// select_best_reader with column_type tests
TEST_F(InvertedIndexIteratorTest, SelectBestReader_MatchQuerySelectsFulltext) {
    InvertedIndexIterator iterator;
    auto fulltext_reader = create_mock_reader("chinese", InvertedIndexReaderType::FULLTEXT, 1);
    auto string_reader = create_mock_reader("chinese", InvertedIndexReaderType::STRING_TYPE, 2);

    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, fulltext_reader);
    iterator.add_reader(InvertedIndexReaderType::STRING_TYPE, string_reader);

    auto string_type = std::make_shared<DataTypeString>();
    auto result = iterator.select_best_reader(string_type, InvertedIndexQueryType::MATCH_ANY_QUERY,
                                              "chinese");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), fulltext_reader);
}

TEST_F(InvertedIndexIteratorTest, SelectBestReader_EqualQuerySelectsStringType) {
    InvertedIndexIterator iterator;
    auto fulltext_reader = create_mock_reader("chinese", InvertedIndexReaderType::FULLTEXT, 1);
    auto string_reader = create_mock_reader("chinese", InvertedIndexReaderType::STRING_TYPE, 2);

    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, fulltext_reader);
    iterator.add_reader(InvertedIndexReaderType::STRING_TYPE, string_reader);

    auto string_type = std::make_shared<DataTypeString>();
    auto result = iterator.select_best_reader(string_type, InvertedIndexQueryType::EQUAL_QUERY,
                                              "chinese");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), string_reader);
}

TEST_F(InvertedIndexIteratorTest, SelectBestReader_ArrayUsesLeafStringType) {
    InvertedIndexIterator iterator;
    auto string_reader = create_mock_reader("chinese", InvertedIndexReaderType::STRING_TYPE, 10);
    auto fulltext_reader = create_mock_reader("chinese", InvertedIndexReaderType::FULLTEXT, 20);

    iterator.add_reader(InvertedIndexReaderType::STRING_TYPE, string_reader);
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, fulltext_reader);

    DataTypePtr column_type = std::make_shared<DataTypeArray>(make_nullable(
            std::make_shared<DataTypeArray>(make_nullable(std::make_shared<DataTypeString>()))));
    auto result = iterator.select_best_reader(column_type, InvertedIndexQueryType::MATCH_ANY_QUERY,
                                              "chinese");
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result.value(), fulltext_reader);
}

TEST_F(InvertedIndexIteratorTest, SelectAnyReaderIsDeterministicByIndexId) {
    InvertedIndexIterator iterator;
    auto reader_id_100 = create_mock_reader("chinese", InvertedIndexReaderType::FULLTEXT, 100);
    auto reader_id_50 = create_mock_reader("english", InvertedIndexReaderType::FULLTEXT, 50);

    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader_id_100);
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader_id_50);

    auto result = iterator.select_any_reader();
    ASSERT_TRUE(result.has_value()) << result.error();
    EXPECT_EQ(result.value(), reader_id_50);
}

// Index lookup performance test
TEST_F(InvertedIndexIteratorTest, IndexLookup_ManyReadersStillFast) {
    InvertedIndexIterator iterator;

    std::vector<std::shared_ptr<MockInvertedIndexReader>> readers;
    for (int i = 0; i < 100; i++) {
        auto reader = create_mock_reader("analyzer_" + std::to_string(i),
                                         InvertedIndexReaderType::FULLTEXT, i + 1);
        readers.push_back(reader);
        iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);
    }

    auto result = iterator.select_best_reader("analyzer_50");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), readers[50]);

    auto result_first = iterator.select_best_reader("analyzer_0");
    EXPECT_TRUE(result_first.has_value());
    EXPECT_EQ(result_first.value(), readers[0]);

    auto result_last = iterator.select_best_reader("analyzer_99");
    EXPECT_TRUE(result_last.has_value());
    EXPECT_EQ(result_last.value(), readers[99]);
}

// Edge cases
TEST_F(InvertedIndexIteratorTest, EdgeCase_EmptyAnalyzerKeyQuery) {
    InvertedIndexIterator iterator;
    auto reader = create_mock_reader("");
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);

    auto result = iterator.select_best_reader("");
    EXPECT_TRUE(result.has_value());
    EXPECT_EQ(result.value(), reader);
}

TEST_F(InvertedIndexIteratorTest, EdgeCase_CaseDistinctKeyDoesNotFallBackToBuiltin) {
    InvertedIndexIterator iterator;
    auto reader = create_mock_reader("chinese");
    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, reader);

    auto result = iterator.select_best_reader("CHINESE");
    EXPECT_FALSE(result.has_value());

    auto builtin = iterator.select_best_reader("chinese");
    ASSERT_TRUE(builtin.has_value());
    EXPECT_EQ(*builtin, reader);
}

TEST_F(InvertedIndexIteratorTest, EdgeCase_GetReaderByType) {
    InvertedIndexIterator iterator;
    auto fulltext = create_mock_reader("chinese", InvertedIndexReaderType::FULLTEXT, 1);
    auto string_type = create_mock_reader("english", InvertedIndexReaderType::STRING_TYPE, 2);

    iterator.add_reader(InvertedIndexReaderType::FULLTEXT, fulltext);
    iterator.add_reader(InvertedIndexReaderType::STRING_TYPE, string_type);

    auto reader = iterator.get_reader(InvertedIndexReaderType::FULLTEXT);
    EXPECT_NE(reader, nullptr);

    auto reader2 = iterator.get_reader(InvertedIndexReaderType::STRING_TYPE);
    EXPECT_NE(reader2, nullptr);

    auto reader3 = iterator.get_reader(InvertedIndexReaderType::BKD);
    EXPECT_EQ(reader3, nullptr);
}

// Test: select_best_reader picks the smallest index_id when multiple
// candidates match, regardless of insertion order. This ensures consistent
// index selection across segments with different schema orderings.
TEST_F(InvertedIndexIteratorTest, SelectBestReader_DeterministicByIndexId) {
    // Simulate two segments with the same indexes in different order.
    // Both should select the reader with the smallest index_id.
    auto reader_id_100 = create_mock_reader("my_analyzer1", InvertedIndexReaderType::FULLTEXT, 100);
    auto reader_id_50 = create_mock_reader("my_analyzer2", InvertedIndexReaderType::FULLTEXT, 50);

    // Segment 1: add id=100 first, then id=50
    {
        InvertedIndexIterator iter;
        iter.add_reader(InvertedIndexReaderType::FULLTEXT, reader_id_100);
        iter.add_reader(InvertedIndexReaderType::FULLTEXT, reader_id_50);

        auto col_type = std::make_shared<DataTypeString>();
        auto result =
                iter.select_best_reader(col_type, InvertedIndexQueryType::MATCH_REGEXP_QUERY, "");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value()->get_index_id(), 50);
    }

    // Segment 2: add id=50 first, then id=100 (opposite order)
    {
        InvertedIndexIterator iter;
        iter.add_reader(InvertedIndexReaderType::FULLTEXT, reader_id_50);
        iter.add_reader(InvertedIndexReaderType::FULLTEXT, reader_id_100);

        auto col_type = std::make_shared<DataTypeString>();
        auto result =
                iter.select_best_reader(col_type, InvertedIndexQueryType::MATCH_REGEXP_QUERY, "");
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value()->get_index_id(), 50);
    }
}

TEST_F(InvertedIndexIteratorTest, PhraseSupportIsCheckedOnTheSelectedReader) {
    // Two full-text indexes on one column, told apart by their analyzer and disagreeing about
    // support_phrase. Index order must not decide whether a phrase query is allowed.
    auto with_positions = MockInvertedIndexReader::create(
            {{"analyzer", "phrase_analyzer"}, {"support_phrase", "true"}}, 1);
    auto without_positions = MockInvertedIndexReader::create(
            {{"analyzer", "plain_analyzer"}, {"support_phrase", "false"}}, 2);

    for (const bool positions_first : {true, false}) {
        SCOPED_TRACE(positions_first);
        InvertedIndexIterator iterator;
        iterator.add_reader(InvertedIndexReaderType::FULLTEXT,
                            positions_first ? with_positions : without_positions);
        iterator.add_reader(InvertedIndexReaderType::FULLTEXT,
                            positions_first ? without_positions : with_positions);
        iterator.set_context(std::make_shared<IndexQueryContext>());

        InvertedIndexAnalyzerCtx analyzer_ctx;
        InvertedIndexParam param {};
        param.column_type = std::make_shared<DataTypeString>();
        param.query_type = InvertedIndexQueryType::MATCH_PHRASE_QUERY;
        param.analyzer_ctx = &analyzer_ctx;

        analyzer_ctx.analyzer_key = "phrase_analyzer";
        with_positions->queried = false;
        auto status = iterator.read_from_index(&param);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_TRUE(with_positions->queried);

        // The index that stored no positions is refused even when it is not the first full-text
        // candidate, which is all the old preflight looked at.
        analyzer_ctx.analyzer_key = "plain_analyzer";
        without_positions->queried = false;
        status = iterator.read_from_index(&param);
        EXPECT_EQ(status.code(), ErrorCode::INDEX_INVALID_PARAMETERS) << status;
        EXPECT_FALSE(without_positions->queried);

        // A non-positional query keeps using that same index.
        param.query_type = InvertedIndexQueryType::MATCH_ANY_QUERY;
        status = iterator.read_from_index(&param);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_TRUE(without_positions->queried);

        // The first candidate of the type is what the old preflight looked at, and it disagrees
        // with the selected reader in one of the two orderings.
        EXPECT_EQ(IndexReaderHelper::is_support_phrase(
                          iterator.get_reader(InvertedIndexReaderType::FULLTEXT)),
                  positions_first);
    }
}

TEST_F(InvertedIndexIteratorTest, PhraseQueriesStillRunOnAnUntokenizedIndex) {
    // An untokenized index never declares support_phrase, yet it answers phrase queries by
    // matching the whole value as one term. The phrase check must only apply to tokenized
    // indexes, otherwise MATCH_PHRASE on a plain string index starts failing.
    auto untokenized = MockInvertedIndexReader::create({}, 3);
    untokenized->set_type(InvertedIndexReaderType::STRING_TYPE);
    ASSERT_FALSE(IndexReaderHelper::is_support_phrase(untokenized));

    InvertedIndexIterator iterator;
    iterator.add_reader(InvertedIndexReaderType::STRING_TYPE, untokenized);
    iterator.set_context(std::make_shared<IndexQueryContext>());

    InvertedIndexParam param {};
    param.column_type = std::make_shared<DataTypeString>();
    for (const auto query_type : {InvertedIndexQueryType::MATCH_PHRASE_QUERY,
                                  InvertedIndexQueryType::MATCH_PHRASE_PREFIX_QUERY,
                                  InvertedIndexQueryType::MATCH_PHRASE_EDGE_QUERY}) {
        SCOPED_TRACE(static_cast<int>(query_type));
        param.query_type = query_type;
        untokenized->queried = false;
        const auto status = iterator.read_from_index(&param);
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_TRUE(untokenized->queried);
    }
}

} // namespace doris::segment_v2
