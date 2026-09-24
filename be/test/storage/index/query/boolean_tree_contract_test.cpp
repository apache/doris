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

#include <array>
#include <cstdint>
#include <memory>
#include <random>
#include <roaring/roaring.hh>
#include <span>
#include <utility>
#include <vector>

#include "storage/index/inverted/query_v2/all_query/all_query.h"
#include "storage/index/inverted/query_v2/bit_set_query/bit_set_query.h"
#include "storage/index/inverted/query_v2/boolean_query/boolean_query_builder.h"

namespace doris::segment_v2::inverted_index::query_v2 {
namespace {

enum class Truth { False, True, Unknown };
enum class TreeKind { Leaf, And, Or, Not };

struct TruthTree {
    TreeKind kind;
    size_t column = 0;
    std::vector<TruthTree> children;
};

using TruthColumns = std::array<std::vector<Truth>, 3>;

Truth conjunction(Truth left, Truth right) {
    if (left == Truth::False || right == Truth::False) {
        return Truth::False;
    }
    if (left == Truth::Unknown || right == Truth::Unknown) {
        return Truth::Unknown;
    }
    return Truth::True;
}

Truth disjunction(Truth left, Truth right) {
    if (left == Truth::True || right == Truth::True) {
        return Truth::True;
    }
    if (left == Truth::Unknown || right == Truth::Unknown) {
        return Truth::Unknown;
    }
    return Truth::False;
}

Truth negation(Truth value) {
    if (value == Truth::Unknown) {
        return Truth::Unknown;
    }
    return value == Truth::True ? Truth::False : Truth::True;
}

struct OracleRow {
    Truth value;
    float score = 0.0F;
};

OracleRow interpret_tree(const TruthTree& tree, const TruthColumns& columns, uint32_t row) {
    if (tree.kind == TreeKind::Leaf) {
        return {.value = columns[tree.column][row], .score = 1.0F};
    }
    const bool is_and = tree.kind == TreeKind::And;
    OracleRow result {.value = is_and ? Truth::True : Truth::False};
    bool has_positive = false;
    for (const auto& child : tree.children) {
        const auto input = interpret_tree(child, columns, row);
        result.value = is_and ? conjunction(result.value, input.value)
                              : disjunction(result.value, input.value);
        if (!is_and || child.kind != TreeKind::Not) {
            has_positive = true;
            if (input.value == Truth::True) {
                result.score += input.score;
            }
        }
    }
    if (tree.kind == TreeKind::Not) {
        result.value = negation(result.value);
        result.score = 1.0F;
    } else if (is_and && !has_positive) {
        result.score = 1.0F;
    }
    return result;
}

QueryPtr compile_tree(const TruthTree& tree, const TruthColumns& columns) {
    if (tree.kind == TreeKind::Leaf) {
        auto truths = std::make_shared<roaring::Roaring>();
        auto nulls = std::make_shared<roaring::Roaring>();
        for (uint32_t row = 0; row < columns[tree.column].size(); ++row) {
            if (columns[tree.column][row] == Truth::True) {
                truths->add(row);
            } else if (columns[tree.column][row] == Truth::Unknown) {
                nulls->add(row);
            }
        }
        return std::make_shared<BitSetQuery>(std::move(truths), std::move(nulls));
    }
    OperatorType op = OperatorType::OP_OR;
    if (tree.kind == TreeKind::And) {
        op = OperatorType::OP_AND;
    } else if (tree.kind == TreeKind::Not) {
        op = OperatorType::OP_NOT;
    }
    OperatorBooleanQueryBuilder builder(op);
    for (const auto& child : tree.children) {
        builder.add(compile_tree(child, columns));
    }
    return builder.build();
}

roaring::Roaring observed_nulls(const ScorerPtr& scorer) {
    const auto* nulls = scorer->get_null_bitmap();
    return nulls == nullptr ? roaring::Roaring() : *nulls;
}

void verify_tree_case(const TruthTree& tree, const TruthColumns& columns, bool scoring,
                      uint32_t seek_target) {
    const auto row_count = static_cast<uint32_t>(columns[0].size());
    std::vector<OracleRow> expected;
    roaring::Roaring true_rows;
    roaring::Roaring null_rows;
    for (uint32_t row = 0; row < row_count; ++row) {
        expected.push_back(interpret_tree(tree, columns, row));
        if (expected.back().value == Truth::True && row >= seek_target) {
            true_rows.add(row);
        } else if (expected.back().value == Truth::Unknown) {
            null_rows.add(row);
        }
    }
    QueryExecutionContext context;
    context.segment_num_rows = row_count;
    auto scorer = compile_tree(tree, columns)->weight(scoring)->scorer(context);
    ASSERT_NE(scorer, nullptr);
    EXPECT_EQ(observed_nulls(scorer), null_rows);
    uint32_t doc = scorer->seek(seek_target);
    EXPECT_EQ(observed_nulls(scorer), null_rows);
    roaring::Roaring actual;
    while (doc != TERMINATED) {
        ASSERT_LT(doc, row_count);
        actual.add(doc);
        if (scoring) {
            EXPECT_FLOAT_EQ(scorer->score(), expected[doc].score) << "row=" << doc;
        }
        doc = scorer->advance();
    }
    EXPECT_EQ(actual, true_rows);
    EXPECT_EQ(observed_nulls(scorer), null_rows);
}

void verify_tree(const TruthTree& tree, const TruthColumns& columns) {
    for (bool scoring : {false, true}) {
        for (uint32_t target : {0U, 7U, 23U}) {
            SCOPED_TRACE(testing::Message() << "scoring=" << scoring << " seek=" << target);
            verify_tree_case(tree, columns, scoring, target);
        }
    }
}

TruthColumns exhaustive_columns() {
    TruthColumns result;
    for (uint32_t row = 0; row < 27; ++row) {
        uint32_t code = row;
        for (auto& column : result) {
            column.push_back(static_cast<Truth>(code % 3));
            code /= 3;
        }
    }
    return result;
}

TruthTree maybe_negate(TruthTree tree, bool negate) {
    if (negate) {
        return {.kind = TreeKind::Not, .children = {std::move(tree)}};
    }
    return tree;
}

TruthTree binary_tree(TreeKind kind, TruthTree left, TruthTree right) {
    return {.kind = kind, .children = {std::move(left), std::move(right)}};
}

void verify_three_leaf_shapes(TreeKind outer, TreeKind inner, uint32_t negations,
                              const TruthColumns& columns) {
    auto a = maybe_negate({.kind = TreeKind::Leaf, .column = 0, .children = {}},
                          (negations & 1) != 0);
    auto b = maybe_negate({.kind = TreeKind::Leaf, .column = 1, .children = {}},
                          (negations & 2) != 0);
    auto c = maybe_negate({.kind = TreeKind::Leaf, .column = 2, .children = {}},
                          (negations & 4) != 0);
    for (bool negate_root : {false, true}) {
        verify_tree(maybe_negate(binary_tree(outer, binary_tree(inner, a, b), c), negate_root),
                    columns);
        verify_tree(maybe_negate(binary_tree(outer, a, binary_tree(inner, b, c)), negate_root),
                    columns);
    }
}

TEST(BooleanTreeContractTest, ExhaustiveThreeLeafTreesMatchIndependentScalarOracle) {
    const auto columns = exhaustive_columns();
    for (TreeKind outer : {TreeKind::And, TreeKind::Or}) {
        for (TreeKind inner : {TreeKind::And, TreeKind::Or}) {
            for (uint32_t negations = 0; negations < 8; ++negations) {
                SCOPED_TRACE(testing::Message()
                             << "outer=" << static_cast<int>(outer)
                             << " inner=" << static_cast<int>(inner) << " negations=" << negations);
                verify_three_leaf_shapes(outer, inner, negations, columns);
            }
        }
    }
}

TruthTree random_tree(std::mt19937& random, uint32_t depth) {
    if (depth == 0 || random() % 4 == 0) {
        return {.kind = TreeKind::Leaf, .column = random() % 3, .children = {}};
    }
    TruthTree tree {.kind = static_cast<TreeKind>(1 + random() % 3), .children = {}};
    const uint32_t children = tree.kind == TreeKind::Not ? 1 : 2 + random() % 3;
    for (uint32_t index = 0; index < children; ++index) {
        tree.children.push_back(random_tree(random, depth - 1));
    }
    return tree;
}

TEST(BooleanTreeContractTest, SeededNestedTreesMatchIndependentScalarOracle) {
    std::mt19937 random(20260919);
    const auto columns = exhaustive_columns();
    for (uint32_t trial = 0; trial < 128; ++trial) {
        SCOPED_TRACE(testing::Message() << "seed=20260919 trial=" << trial);
        verify_tree(random_tree(random, 4), columns);
    }
}

TEST(BooleanTreeContractTest, EmptyLogicalOperatorsPreserveTheirIdentities) {
    const auto columns = exhaustive_columns();
    for (TreeKind kind : {TreeKind::And, TreeKind::Or, TreeKind::Not}) {
        verify_tree({.kind = kind, .children = {}}, columns);
    }
}

struct OccurrenceCase {
    std::vector<Occur> roles;
    size_t minimum_should_match;
};

// An occur Boolean is two-valued, as in Elasticsearch: a clause that is not TRUE does not match,
// and the Boolean is never UNKNOWN.
OracleRow interpret_occurrence_inputs(const OccurrenceCase& query,
                                      std::span<const OracleRow> inputs) {
    bool required = true;
    bool excluded = false;
    size_t required_count = 0;
    size_t optional_count = 0;
    size_t optional_true = 0;
    float score = 0.0F;
    for (size_t index = 0; index < query.roles.size(); ++index) {
        const bool matches = inputs[index].value == Truth::True;
        switch (query.roles[index]) {
        case Occur::MUST:
            required = required && matches;
            ++required_count;
            score += matches ? inputs[index].score : 0.0F;
            break;
        case Occur::SHOULD:
            ++optional_count;
            optional_true += matches;
            score += matches ? inputs[index].score : 0.0F;
            break;
        case Occur::MUST_NOT:
            excluded = excluded || matches;
            break;
        }
    }
    if (required_count + optional_count == 0) {
        return {.value = Truth::False};
    }
    size_t minimum = query.minimum_should_match;
    if (required_count == 0 && minimum == 0) {
        minimum = 1;
    }
    const bool matches = required && optional_true >= minimum && !excluded;
    return {.value = matches ? Truth::True : Truth::False, .score = score};
}

OracleRow interpret_occurrences(const OccurrenceCase& query, const TruthColumns& columns,
                                uint32_t row) {
    std::vector<OracleRow> inputs;
    for (const auto& column : columns) {
        inputs.push_back({.value = column[row], .score = 1.0F});
    }
    return interpret_occurrence_inputs(query, inputs);
}

QueryPtr compile_occurrences(const OccurrenceCase& query, const TruthColumns& columns) {
    OccurBooleanQueryBuilder builder;
    builder.set_minimum_number_should_match(query.minimum_should_match);
    for (size_t index = 0; index < query.roles.size(); ++index) {
        builder.add(
                compile_tree({.kind = TreeKind::Leaf, .column = index, .children = {}}, columns),
                query.roles[index]);
    }
    return builder.build();
}

void verify_occurrence_case(const OccurrenceCase& query, const TruthColumns& columns, bool scoring,
                            uint32_t target) {
    QueryExecutionContext context;
    context.segment_num_rows = static_cast<uint32_t>(columns[0].size());
    roaring::Roaring expected_true;
    roaring::Roaring expected_null;
    std::vector<OracleRow> expected;
    for (uint32_t row = 0; row < context.segment_num_rows; ++row) {
        expected.push_back(interpret_occurrences(query, columns, row));
        if (expected.back().value == Truth::True && row >= target) {
            expected_true.add(row);
        } else if (expected.back().value == Truth::Unknown) {
            expected_null.add(row);
        }
    }
    auto scorer = compile_occurrences(query, columns)->weight(scoring)->scorer(context);
    ASSERT_NE(scorer, nullptr);
    EXPECT_EQ(observed_nulls(scorer), expected_null);
    uint32_t doc = scorer->seek(target);
    EXPECT_EQ(observed_nulls(scorer), expected_null);
    roaring::Roaring actual;
    while (doc != TERMINATED) {
        ASSERT_LT(doc, expected.size());
        actual.add(doc);
        if (scoring) {
            EXPECT_FLOAT_EQ(scorer->score(), expected[doc].score) << "row=" << doc;
        }
        doc = scorer->advance();
    }
    EXPECT_EQ(actual, expected_true);
    EXPECT_EQ(observed_nulls(scorer), expected_null);
}

TEST(BooleanTreeContractTest, OccurrencesAndMinimumMatchesAreTwoValued) {
    const std::vector<OccurrenceCase> cases {
            {.roles = {}, .minimum_should_match = 0},
            {.roles = {Occur::MUST_NOT, Occur::MUST_NOT}, .minimum_should_match = 0},
            {.roles = {Occur::MUST}, .minimum_should_match = 0},
            {.roles = {Occur::MUST}, .minimum_should_match = 1},
            {.roles = {Occur::SHOULD}, .minimum_should_match = 0},
            {.roles = {Occur::SHOULD}, .minimum_should_match = 1},
            {.roles = {Occur::SHOULD}, .minimum_should_match = 2},
            {.roles = {Occur::MUST, Occur::MUST, Occur::MUST_NOT}, .minimum_should_match = 0}};
    auto expanded = cases;
    for (size_t minimum = 0; minimum <= 4; ++minimum) {
        expanded.push_back({.roles = {Occur::SHOULD, Occur::SHOULD, Occur::SHOULD},
                            .minimum_should_match = minimum});
    }
    for (size_t minimum = 0; minimum <= 3; ++minimum) {
        expanded.push_back({.roles = {Occur::MUST, Occur::SHOULD, Occur::SHOULD},
                            .minimum_should_match = minimum});
    }
    for (size_t minimum = 0; minimum <= 2; ++minimum) {
        expanded.push_back({.roles = {Occur::MUST, Occur::MUST_NOT, Occur::SHOULD},
                            .minimum_should_match = minimum});
        expanded.push_back(
                {.roles = {Occur::SHOULD, Occur::MUST_NOT}, .minimum_should_match = minimum});
    }
    const auto columns = exhaustive_columns();
    for (size_t index = 0; index < expanded.size(); ++index) {
        for (bool scoring : {false, true}) {
            for (uint32_t target : {0U, 7U, 23U}) {
                SCOPED_TRACE(testing::Message()
                             << "case=" << index << " scoring=" << scoring << " seek=" << target);
                verify_occurrence_case(expanded[index], columns, scoring, target);
            }
        }
    }
}

struct CompiledTruthTree {
    QueryPtr query;
    std::vector<OracleRow> rows;
    bool negated_root = false;
};

CompiledTruthTree build_mixed_tree(std::mt19937& random, uint32_t depth,
                                   const TruthColumns& columns) {
    CompiledTruthTree result;
    const auto row_count = columns.front().size();
    if (depth == 0 || random() % 5 == 0) {
        const size_t choice = random() % 5;
        if (choice == 3) {
            result.query = std::make_shared<AllQuery>();
            result.rows.resize(row_count, {.value = Truth::True, .score = 1.0F});
        } else if (choice == 4) {
            result.query = std::make_shared<BitSetQuery>(std::make_shared<roaring::Roaring>());
            result.rows.resize(row_count, {.value = Truth::False});
        } else {
            const TruthTree leaf {.kind = TreeKind::Leaf, .column = choice, .children = {}};
            result.query = compile_tree(leaf, columns);
            for (uint32_t row = 0; row < row_count; ++row) {
                result.rows.push_back(interpret_tree(leaf, columns, row));
            }
        }
        return result;
    }
    const auto kind = random() % 4;
    const size_t child_count = kind == 2 ? 1 : 2 + random() % 3;
    std::vector<CompiledTruthTree> children;
    for (size_t index = 0; index < child_count; ++index) {
        children.push_back(build_mixed_tree(random, depth - 1, columns));
    }
    if (kind == 3) {
        OccurrenceCase occurrence {.roles = {},
                                   .minimum_should_match = random() % (child_count + 2)};
        OccurBooleanQueryBuilder builder;
        builder.set_minimum_number_should_match(occurrence.minimum_should_match);
        constexpr std::array roles {Occur::MUST, Occur::SHOULD, Occur::MUST_NOT};
        for (const auto& child : children) {
            const auto role = roles[random() % roles.size()];
            occurrence.roles.push_back(role);
            builder.add(child.query, role);
        }
        result.query = builder.build();
        for (uint32_t row = 0; row < row_count; ++row) {
            std::vector<OracleRow> inputs;
            for (const auto& child : children) {
                inputs.push_back(child.rows[row]);
            }
            result.rows.push_back(interpret_occurrence_inputs(occurrence, inputs));
        }
        return result;
    }
    constexpr std::array ops {OperatorType::OP_AND, OperatorType::OP_OR, OperatorType::OP_NOT};
    OperatorBooleanQueryBuilder builder(ops[kind]);
    for (const auto& child : children) {
        builder.add(child.query);
    }
    result.query = builder.build();
    result.negated_root = kind == 2;
    for (uint32_t row = 0; row < row_count; ++row) {
        OracleRow value {.value = kind == 0 ? Truth::True : Truth::False};
        bool has_positive = false;
        for (const auto& child : children) {
            const auto input = child.rows[row];
            value.value = kind == 0 ? conjunction(value.value, input.value)
                                    : disjunction(value.value, input.value);
            if (kind != 0 || !child.negated_root) {
                has_positive = true;
                value.score += input.value == Truth::True ? input.score : 0.0F;
            }
        }
        if (kind == 2) {
            value.value = negation(value.value);
            value.score = 1.0F;
        } else if (kind == 0 && !has_positive) {
            value.score = 1.0F;
        }
        result.rows.push_back(value);
    }
    return result;
}

void verify_compiled_tree(const CompiledTruthTree& tree, const QueryExecutionContext& context,
                          bool scoring, uint32_t target) {
    roaring::Roaring expected_true;
    roaring::Roaring expected_nulls;
    for (uint32_t row = 0; row < context.segment_num_rows; ++row) {
        if (tree.rows[row].value == Truth::Unknown) {
            expected_nulls.add(row);
        } else if (row >= target && tree.rows[row].value == Truth::True) {
            expected_true.add(row);
        }
    }
    auto scorer = tree.query->weight(scoring)->scorer(context);
    EXPECT_EQ(observed_nulls(scorer), expected_nulls);
    roaring::Roaring actual;
    uint32_t doc = scorer->seek(target);
    EXPECT_EQ(observed_nulls(scorer), expected_nulls);
    while (doc != TERMINATED) {
        ASSERT_LT(doc, context.segment_num_rows);
        actual.add(doc);
        if (scoring) {
            EXPECT_FLOAT_EQ(scorer->score(), tree.rows[doc].score) << "row=" << doc;
        }
        doc = scorer->advance();
    }
    EXPECT_EQ(actual, expected_true);
    EXPECT_EQ(observed_nulls(scorer), expected_nulls);
}

TEST(BooleanTreeContractTest, SeededMixedOperatorsAndOccurrencesPreserveTruthAndScores) {
    std::mt19937 random(20260920);
    const auto columns = exhaustive_columns();
    QueryExecutionContext context;
    context.segment_num_rows = static_cast<uint32_t>(columns.front().size());
    for (uint32_t trial = 0; trial < 128; ++trial) {
        const auto tree = build_mixed_tree(random, 4, columns);
        for (bool scoring : {false, true}) {
            for (uint32_t target : {0U, 7U, 23U}) {
                SCOPED_TRACE(testing::Message() << "seed=20260920 trial=" << trial
                                                << " scoring=" << scoring << " seek=" << target);
                verify_compiled_tree(tree, context, scoring, target);
            }
        }
    }
}

} // namespace
} // namespace doris::segment_v2::inverted_index::query_v2
