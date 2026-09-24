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

#include "storage/index/query/logical/search_lowering.h"

#include <gen_cpp/Exprs_types.h>
#include <gtest/gtest.h>

#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "common/status.h"
#include "storage/index/query/logical/node.h"
#include "util/string_util.h"

// The SEARCH lowering contract: every clause type maps to one IR shape, values
// are analyzed exactly once with the bound index's analyzer, and the operator,
// threshold and normalization rules the two format branches used to apply
// separately are applied here.
namespace doris::index_query::logical {
namespace {

using segment_v2::InvertedIndexQueryType;

constexpr const char* kText = "body";       // analyzed, lowercasing
constexpr const char* kKeyword = "tag";     // untokenized string index
constexpr const char* kNumber = "price";    // scalar (BKD) index
constexpr const char* kUnbound = "missing"; // no index in this segment
constexpr const char* kCased = "cased";     // analyzed, case-preserving

// Splits on spaces and lowercases every analyzed field but kCased; "a|b" becomes two terms
// at one position, so a test can produce the multi-term slots a synonym filter would.
class FakeCatalog final : public FieldCatalog {
public:
    FakeCatalog() {
        _fields[kText] = {
                .bound = true, .direct_index = false, .analyzed = true, .binding = "body#any"};
        _fields[kCased] = {
                .bound = true, .direct_index = false, .analyzed = true, .binding = "cased#any"};
        _fields[kKeyword] = {
                .bound = true, .direct_index = false, .analyzed = false, .binding = "tag#eq"};
        _fields[kNumber] = {
                .bound = true, .direct_index = true, .analyzed = false, .binding = "price#eq"};
        _fields[kUnbound] = {
                .bound = false, .direct_index = false, .analyzed = false, .binding = ""};
    }

    Status resolve(const std::string& field, InvertedIndexQueryType query_type,
                   FieldProps* out) override {
        resolved_query_types[field] = query_type;
        auto it = _fields.find(field);
        if (it == _fields.end()) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>("no field {}", field);
        }
        *out = it->second;
        return Status::OK();
    }

    Status analyze(const FieldProps& props, const std::string& value,
                   std::vector<Token>* out) override {
        analyzed_values.push_back(value);
        out->clear();
        std::istringstream words(value);
        std::string word;
        int32_t position = 0;
        while (words >> word) {
            ++position;
            std::istringstream alternatives(word);
            std::string alternative;
            while (std::getline(alternatives, alternative, '|')) {
                Token token;
                token.term = lowercases(props) ? to_lower(alternative) : alternative;
                token.position = position;
                out->push_back(std::move(token));
            }
        }
        return Status::OK();
    }

    Status normalize(const FieldProps& props, const std::string& value, std::string* out) override {
        normalized_values.push_back(value);
        *out = lowercases(props) ? to_lower(value) : value;
        return Status::OK();
    }

    std::map<std::string, InvertedIndexQueryType> resolved_query_types;
    std::vector<std::string> analyzed_values;
    std::vector<std::string> normalized_values;

private:
    bool lowercases(const FieldProps& props) const {
        return props.binding != _fields.at(kCased).binding;
    }

    std::map<std::string, FieldProps> _fields;
};

TSearchClause leaf(const std::string& clause_type, const std::string& field,
                   const std::string& value) {
    TSearchClause clause;
    clause.__set_clause_type(clause_type);
    clause.__set_field_name(field);
    clause.__set_value(value);
    return clause;
}

TSearchClause compound(const std::string& clause_type, const std::vector<TSearchClause>& children) {
    TSearchClause clause;
    clause.__set_clause_type(clause_type);
    clause.__set_children(children);
    return clause;
}

TSearchClause with_occur(TSearchClause clause, TSearchOccur::type occur) {
    clause.__set_occur(occur);
    return clause;
}

// Reports a failed lowering and returns a placeholder so the test can go on.
NodePtr lower(const TSearchClause& clause, FakeCatalog& catalog,
              const LoweringOptions& options = {}) {
    NodePtr node;
    Status status = lower_search_clause(clause, options, catalog, &node);
    EXPECT_TRUE(status.ok()) << status;
    EXPECT_NE(node, nullptr);
    return node != nullptr ? node : make_node(Unknown {});
}

std::vector<std::string> terms_of(const std::vector<Token>& tokens) {
    std::vector<std::string> out;
    for (const auto& token : tokens) {
        out.push_back(token.get_single_term());
    }
    return out;
}

TEST(SearchLoweringTest, QueryTypeHintPrefersTokenizedIndexForPatternClauses) {
    for (const char* clause_type : {"TERM", "WILDCARD", "PREFIX", "REGEXP", "ANY", "MATCH"}) {
        EXPECT_EQ(search_clause_query_type(clause_type), InvertedIndexQueryType::MATCH_ANY_QUERY)
                << clause_type;
    }
    EXPECT_EQ(search_clause_query_type("EXACT"), InvertedIndexQueryType::EQUAL_QUERY);
    EXPECT_EQ(search_clause_query_type("PHRASE"), InvertedIndexQueryType::MATCH_PHRASE_QUERY);
    EXPECT_EQ(search_clause_query_type("ALL"), InvertedIndexQueryType::MATCH_ALL_QUERY);
    EXPECT_EQ(search_clause_query_type("RANGE"), InvertedIndexQueryType::RANGE_QUERY);
    EXPECT_EQ(search_clause_query_type("LIST"), InvertedIndexQueryType::LIST_QUERY);
    EXPECT_EQ(search_clause_query_type("nonsense"), InvertedIndexQueryType::EQUAL_QUERY);
}

TEST(SearchLoweringTest, LeafPassesItsQueryTypeHintToTheCatalog) {
    FakeCatalog catalog;
    lower(leaf("PHRASE", kText, "a b"), catalog);
    lower(leaf("EXACT", kKeyword, "x"), catalog);
    EXPECT_EQ(catalog.resolved_query_types[kText], InvertedIndexQueryType::MATCH_PHRASE_QUERY);
    EXPECT_EQ(catalog.resolved_query_types[kKeyword], InvertedIndexQueryType::EQUAL_QUERY);
}

TEST(SearchLoweringTest, UnboundFieldLowersToUnknown) {
    FakeCatalog catalog;
    auto node = lower(leaf("TERM", kUnbound, "x"), catalog);
    EXPECT_NE(node->as<Unknown>(), nullptr);
    ASSERT_NE(node->field(), nullptr);
    EXPECT_EQ(node->field()->name, kUnbound);
    EXPECT_TRUE(node->field()->binding.empty());
    EXPECT_TRUE(catalog.analyzed_values.empty());
}

TEST(SearchLoweringTest, ResolveErrorPropagates) {
    FakeCatalog catalog;
    NodePtr node;
    Status status = lower_search_clause(leaf("TERM", "nowhere", "x"), {}, catalog, &node);
    EXPECT_TRUE(status.is<ErrorCode::INVERTED_INDEX_FILE_NOT_FOUND>()) << status;
}

TEST(SearchLoweringTest, LeafWithoutFieldOrValueIsInvalid) {
    FakeCatalog catalog;
    TSearchClause no_value;
    no_value.__set_clause_type("TERM");
    no_value.__set_field_name(kText);
    NodePtr node;
    EXPECT_TRUE(
            lower_search_clause(no_value, {}, catalog, &node).is<ErrorCode::INVALID_ARGUMENT>());
    TSearchClause no_field;
    no_field.__set_clause_type("TERM");
    no_field.__set_value("x");
    EXPECT_TRUE(
            lower_search_clause(no_field, {}, catalog, &node).is<ErrorCode::INVALID_ARGUMENT>());
}

TEST(SearchLoweringTest, TermOnKeywordIndexIsTheRawValue) {
    FakeCatalog catalog;
    auto node = lower(leaf("TERM", kKeyword, "Hello World"), catalog);
    const auto* term = node->as<Term>();
    ASSERT_NE(term, nullptr);
    EXPECT_EQ(term->term, "Hello World");
    EXPECT_EQ(term->field.name, kKeyword);
    EXPECT_EQ(term->field.binding, "tag#eq");
    EXPECT_TRUE(catalog.analyzed_values.empty());
}

// An analyzed value is a TermSet even with one token: Term is reserved for
// values that were never analyzed.
TEST(SearchLoweringTest, TermOnAnalyzedIndexWithOneTokenIsAOneTermSet) {
    FakeCatalog catalog;
    auto node = lower(leaf("TERM", kText, "Hello"), catalog);
    const auto* set = node->as<TermSet>();
    ASSERT_NE(set, nullptr);
    EXPECT_EQ(set->terms, std::vector<std::string> {"hello"});
    EXPECT_FALSE(set->require_all);
    EXPECT_EQ(set->min_should_match, 0U);
    EXPECT_EQ(catalog.analyzed_values, std::vector<std::string> {"Hello"});
}

TEST(SearchLoweringTest, TermOnAnalyzedIndexFollowsTheDefaultOperator) {
    FakeCatalog catalog;
    auto any = lower(leaf("TERM", kText, "quick Brown fox"), catalog);
    const auto* any_set = any->as<TermSet>();
    ASSERT_NE(any_set, nullptr);
    EXPECT_EQ(any_set->terms, (std::vector<std::string> {"quick", "brown", "fox"}));
    EXPECT_FALSE(any_set->require_all);
    EXPECT_EQ(any_set->min_should_match, 0U);

    auto all = lower(leaf("TERM", kText, "quick brown"), catalog, {.default_operator = "and"});
    const auto* all_set = all->as<TermSet>();
    ASSERT_NE(all_set, nullptr);
    EXPECT_TRUE(all_set->require_all);
}

TEST(SearchLoweringTest, TermKeepsMinimumShouldMatchOnlyForSeveralTokens) {
    FakeCatalog catalog;
    LoweringOptions options {.minimum_should_match = 2};
    auto several = lower(leaf("TERM", kText, "a b c"), catalog, options);
    const auto* set = several->as<TermSet>();
    ASSERT_NE(set, nullptr);
    EXPECT_EQ(set->min_should_match, 2U);
    EXPECT_FALSE(set->require_all);

    auto with_and = lower(leaf("TERM", kText, "a b c"), catalog,
                          {.default_operator = "and", .minimum_should_match = 2});
    ASSERT_NE(with_and->as<TermSet>(), nullptr);
    EXPECT_TRUE(with_and->as<TermSet>()->require_all);
    EXPECT_EQ(with_and->as<TermSet>()->min_should_match, 2U);

    auto single = lower(leaf("TERM", kText, "a"), catalog, options);
    ASSERT_NE(single->as<TermSet>(), nullptr);
    EXPECT_EQ(single->as<TermSet>()->min_should_match, 0U);
}

TEST(SearchLoweringTest, TermWithoutTokensIsEmptyAndKeepsTheField) {
    FakeCatalog catalog;
    auto node = lower(leaf("TERM", kText, "   "), catalog);
    const auto* empty = node->as<Empty>();
    ASSERT_NE(empty, nullptr);
    EXPECT_EQ(empty->field.binding, "body#any");
}

TEST(SearchLoweringTest, ExactNeverAnalyzes) {
    FakeCatalog catalog;
    auto node = lower(leaf("EXACT", kText, "Hello World"), catalog);
    const auto* term = node->as<Term>();
    ASSERT_NE(term, nullptr);
    EXPECT_EQ(term->term, "Hello World");
    EXPECT_TRUE(catalog.analyzed_values.empty());
}

TEST(SearchLoweringTest, PhraseGroupsTokensByPosition) {
    FakeCatalog catalog;
    auto node = lower(leaf("PHRASE", kText, "Quick brown|red fox"), catalog);
    const auto* phrase = node->as<Phrase>();
    ASSERT_NE(phrase, nullptr);
    ASSERT_EQ(phrase->slots.size(), 3U);
    EXPECT_EQ(phrase->slots[0].get_single_term(), "quick");
    EXPECT_EQ(phrase->slots[0].position, 1);
    ASSERT_TRUE(phrase->slots[1].is_multi_terms());
    EXPECT_EQ(phrase->slots[1].get_multi_terms(), (std::vector<std::string> {"brown", "red"}));
    EXPECT_EQ(phrase->slots[1].position, 2);
    EXPECT_EQ(phrase->slots[2].get_single_term(), "fox");
}

TEST(SearchLoweringTest, PhraseWithOneSlotCollapses) {
    FakeCatalog catalog;
    auto single = lower(leaf("PHRASE", kText, "fox"), catalog);
    ASSERT_NE(single->as<TermSet>(), nullptr);
    EXPECT_EQ(single->as<TermSet>()->terms, std::vector<std::string> {"fox"});

    auto alternatives = lower(leaf("PHRASE", kText, "brown|red"), catalog);
    const auto* set = alternatives->as<TermSet>();
    ASSERT_NE(set, nullptr);
    EXPECT_EQ(set->terms, (std::vector<std::string> {"brown", "red"}));
    EXPECT_FALSE(set->require_all);
}

// The DSL has no slop syntax, so a trailing "~2" is ordinary text for every
// format. Before lowering existed, an SNII field parsed it as slop.
TEST(SearchLoweringTest, PhraseDoesNotParseSlop) {
    FakeCatalog catalog;
    auto node = lower(leaf("PHRASE", kText, "quick fox ~2"), catalog);
    const auto* phrase = node->as<Phrase>();
    ASSERT_NE(phrase, nullptr);
    EXPECT_EQ(terms_of(phrase->slots), (std::vector<std::string> {"quick", "fox", "~2"}));
}

TEST(SearchLoweringTest, PhraseOnKeywordIndexIsTheRawValue) {
    FakeCatalog catalog;
    auto node = lower(leaf("PHRASE", kKeyword, "Quick Fox"), catalog);
    ASSERT_NE(node->as<Term>(), nullptr);
    EXPECT_EQ(node->as<Term>()->term, "Quick Fox");
}

TEST(SearchLoweringTest, PhraseWithoutTokensIsEmpty) {
    FakeCatalog catalog;
    EXPECT_NE(lower(leaf("PHRASE", kText, " "), catalog)->as<Empty>(), nullptr);
}

TEST(SearchLoweringTest, MatchLowersToAnyOfTheAnalyzedTokens) {
    FakeCatalog catalog;
    auto node = lower(leaf("MATCH", kText, "quick fox"), catalog);
    const auto* set = node->as<TermSet>();
    ASSERT_NE(set, nullptr);
    EXPECT_EQ(set->terms, (std::vector<std::string> {"quick", "fox"}));
    EXPECT_FALSE(set->require_all);
    EXPECT_NE(lower(leaf("MATCH", kKeyword, "quick fox"), catalog)->as<Term>(), nullptr);
}

TEST(SearchLoweringTest, AnyAndAllLowerToTermSets) {
    FakeCatalog catalog;
    auto any = lower(leaf("ANY", kText, "a b"), catalog);
    ASSERT_NE(any->as<TermSet>(), nullptr);
    EXPECT_FALSE(any->as<TermSet>()->require_all);
    auto all = lower(leaf("ALL", kText, "a b"), catalog);
    ASSERT_NE(all->as<TermSet>(), nullptr);
    EXPECT_TRUE(all->as<TermSet>()->require_all);
    auto one = lower(leaf("ALL", kText, "a"), catalog);
    ASSERT_NE(one->as<TermSet>(), nullptr);
    EXPECT_TRUE(one->as<TermSet>()->require_all);
    EXPECT_NE(lower(leaf("ANY", kKeyword, "a b"), catalog)->as<Term>(), nullptr);
    EXPECT_NE(lower(leaf("ALL", kText, ""), catalog)->as<Empty>(), nullptr);
}

TEST(SearchLoweringTest, PrefixOnAnalyzedIndexIsItsNormalizedStem) {
    FakeCatalog catalog;
    auto node = lower(leaf("PREFIX", kText, "Quick Fo*"), catalog);
    const auto* expand = node->as<Expand>();
    ASSERT_NE(expand, nullptr);
    EXPECT_EQ(expand->kind, ExpandKind::kPrefix);
    // As in Elasticsearch's query_string, the stem is normalized whole and never analyzed.
    EXPECT_EQ(expand->pattern, "quick fo");
    EXPECT_EQ(catalog.normalized_values, std::vector<std::string> {"Quick Fo"});
    EXPECT_TRUE(catalog.analyzed_values.empty());

    EXPECT_EQ(lower(leaf("PREFIX", kCased, "Quick Fo*"), catalog)->as<Expand>()->pattern,
              "Quick Fo");
}

TEST(SearchLoweringTest, PrefixOnKeywordIndexIsItsLiteralStem) {
    FakeCatalog catalog;
    auto node = lower(leaf("PREFIX", kKeyword, "Quick Fo*"), catalog);
    const auto* expand = node->as<Expand>();
    ASSERT_NE(expand, nullptr);
    EXPECT_EQ(expand->kind, ExpandKind::kPrefix);
    EXPECT_EQ(expand->pattern, "Quick Fo");
    // Only the trailing '*' belongs to the DSL; an escaped one stays in the stem.
    EXPECT_EQ(lower(leaf("PREFIX", kKeyword, "a*b*"), catalog)->as<Expand>()->pattern, "a*b");
    EXPECT_TRUE(catalog.normalized_values.empty());
}

TEST(SearchLoweringTest, WildcardStarIsExists) {
    FakeCatalog catalog;
    auto node = lower(leaf("WILDCARD", kText, "*"), catalog);
    ASSERT_NE(node->as<Exists>(), nullptr);
    EXPECT_EQ(node->as<Exists>()->field.name, kText);
}

TEST(SearchLoweringTest, WildcardIsNormalizedOnlyOnAnAnalyzedIndex) {
    FakeCatalog catalog;
    auto lowered = lower(leaf("WILDCARD", kText, "Qu?ck*"), catalog);
    ASSERT_NE(lowered->as<Expand>(), nullptr);
    EXPECT_EQ(lowered->as<Expand>()->pattern, "qu?ck*");
    EXPECT_EQ(lowered->as<Expand>()->kind, ExpandKind::kWildcard);
    EXPECT_EQ(lower(leaf("WILDCARD", kCased, "Qu?ck*"), catalog)->as<Expand>()->pattern, "Qu?ck*");
    EXPECT_EQ(lower(leaf("WILDCARD", kKeyword, "Qu?ck*"), catalog)->as<Expand>()->pattern,
              "Qu?ck*");
    EXPECT_EQ(catalog.normalized_values, (std::vector<std::string> {"Qu?ck*", "Qu?ck*"}));
    EXPECT_TRUE(catalog.analyzed_values.empty());
}

TEST(SearchLoweringTest, RegexpIsAnchoredButNeverNormalized) {
    FakeCatalog catalog;
    auto node = lower(leaf("REGEXP", kText, "^Qu.*"), catalog);
    ASSERT_NE(node->as<Expand>(), nullptr);
    EXPECT_EQ(node->as<Expand>()->kind, ExpandKind::kRegexp);
    // SEARCH matches a regular expression against whole terms on every format.
    EXPECT_EQ(node->as<Expand>()->pattern, "^(^Qu.*)$");
    EXPECT_EQ(lower(leaf("REGEXP", kText, "a|b"), catalog)->as<Expand>()->pattern, "^(a|b)$");
    EXPECT_TRUE(catalog.analyzed_values.empty());
    EXPECT_TRUE(catalog.normalized_values.empty());
}

TEST(SearchLoweringTest, RangeListAndUnknownClauseTypesFallBackToTheRawTerm) {
    FakeCatalog catalog;
    for (const char* clause_type : {"RANGE", "LIST", "SOMETHING_NEW"}) {
        auto node = lower(leaf(clause_type, kText, "Raw Value"), catalog);
        ASSERT_NE(node->as<Term>(), nullptr) << clause_type;
        EXPECT_EQ(node->as<Term>()->term, "Raw Value") << clause_type;
    }
    EXPECT_TRUE(catalog.analyzed_values.empty());
}

TEST(SearchLoweringTest, ScalarIndexLowersTermAndExactToCompare) {
    FakeCatalog catalog;
    for (const char* clause_type : {"TERM", "EXACT"}) {
        auto node = lower(leaf(clause_type, kNumber, "42"), catalog);
        const auto* compare = node->as<Compare>();
        ASSERT_NE(compare, nullptr) << clause_type;
        EXPECT_EQ(compare->op, CompareOp::kEqual);
        EXPECT_EQ(compare->value, "42");
        EXPECT_EQ(compare->field.binding, "price#eq");
    }
    for (const char* clause_type : {"PHRASE", "PREFIX", "WILDCARD", "REGEXP", "RANGE", "LIST"}) {
        EXPECT_NE(lower(leaf(clause_type, kNumber, "42"), catalog)->as<Unknown>(), nullptr)
                << clause_type;
    }
    EXPECT_TRUE(catalog.analyzed_values.empty());
}

TEST(SearchLoweringTest, OperatorCompoundsLowerTheirChildrenInOrder) {
    FakeCatalog catalog;
    auto node = lower(compound("AND", {leaf("TERM", kKeyword, "a"),
                                       compound("NOT", {leaf("TERM", kKeyword, "b")}),
                                       compound("OR", {leaf("TERM", kKeyword, "c")})}),
                      catalog);
    const auto* root = node->as<Bool>();
    ASSERT_NE(root, nullptr);
    EXPECT_EQ(root->op, BoolOp::kAnd);
    ASSERT_EQ(root->clauses.size(), 3U);
    EXPECT_EQ(root->clauses[0].first, Occur::kMust);
    EXPECT_EQ(root->clauses[0].second->as<Term>()->term, "a");
    const auto* negation = root->clauses[1].second->as<Bool>();
    ASSERT_NE(negation, nullptr);
    EXPECT_EQ(negation->op, BoolOp::kNot);
    EXPECT_EQ(negation->clauses[0].second->as<Term>()->term, "b");
    EXPECT_EQ(root->clauses[2].second->as<Bool>()->op, BoolOp::kOr);
    EXPECT_EQ(root->min_should_match, 0U);
}

TEST(SearchLoweringTest, CompoundWithoutChildrenIsAnEmptyBool) {
    FakeCatalog catalog;
    auto node = lower(compound("OR", {}), catalog);
    ASSERT_NE(node->as<Bool>(), nullptr);
    EXPECT_TRUE(node->as<Bool>()->clauses.empty());
}

TEST(SearchLoweringTest, OccurBooleanKeepsOccurAndThreshold) {
    FakeCatalog catalog;
    auto clause = compound("OCCUR_BOOLEAN",
                           {with_occur(leaf("TERM", kKeyword, "a"), TSearchOccur::SHOULD),
                            with_occur(leaf("TERM", kKeyword, "b"), TSearchOccur::MUST_NOT),
                            leaf("TERM", kKeyword, "c")});
    clause.__set_minimum_should_match(1);
    auto node = lower(clause, catalog);
    const auto* root = node->as<Bool>();
    ASSERT_NE(root, nullptr);
    EXPECT_EQ(root->op, BoolOp::kOccur);
    EXPECT_EQ(root->min_should_match, 1U);
    ASSERT_EQ(root->clauses.size(), 3U);
    EXPECT_EQ(root->clauses[0].first, Occur::kShould);
    EXPECT_EQ(root->clauses[1].first, Occur::kMustNot);
    EXPECT_EQ(root->clauses[2].first, Occur::kMust);

    auto without_threshold =
            lower(compound("OCCUR_BOOLEAN", {leaf("TERM", kKeyword, "a")}), catalog);
    EXPECT_EQ(without_threshold->as<Bool>()->min_should_match, 0U);
}

TEST(SearchLoweringTest, MatchAllDocsIsAll) {
    FakeCatalog catalog;
    TSearchClause clause;
    clause.__set_clause_type("MATCH_ALL_DOCS");
    EXPECT_NE(lower(clause, catalog)->as<All>(), nullptr);
}

TEST(SearchLoweringTest, NestedIsRejected) {
    FakeCatalog catalog;
    TSearchClause clause;
    clause.__set_clause_type("NESTED");
    NodePtr node;
    EXPECT_TRUE(lower_search_clause(clause, {}, catalog, &node).is<ErrorCode::INVALID_ARGUMENT>());
}

} // namespace
} // namespace doris::index_query::logical
