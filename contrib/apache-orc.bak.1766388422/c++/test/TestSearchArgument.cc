/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "sargs/SearchArgument.hh"
#include "wrap/gtest-wrapper.h"

#include <unordered_set>

namespace orc {

  TEST(TestSearchArgument, literalTest) {
    Literal literal0(static_cast<int64_t>(1234));
    EXPECT_EQ(PredicateDataType::LONG, literal0.getType());
    EXPECT_TRUE("1234" == literal0.toString());

    Literal literal1(0.1234);
    EXPECT_EQ(PredicateDataType::FLOAT, literal1.getType());
    EXPECT_TRUE("0.1234" == literal1.toString());

    Literal literal2(false);
    EXPECT_EQ(PredicateDataType::BOOLEAN, literal2.getType());
    EXPECT_TRUE("false" == literal2.toString());

    Literal literal3(static_cast<int64_t>(123456), 123456789);
    EXPECT_EQ(PredicateDataType::TIMESTAMP, literal3.getType());
    EXPECT_TRUE("123456.123456789" == literal3.toString());
    EXPECT_EQ(123456123, literal3.getTimestamp().getMillis());

    Literal literal4(Int128(54321), 6, 2);
    EXPECT_EQ(PredicateDataType::DECIMAL, literal4.getType());
    EXPECT_TRUE("543.21" == literal4.toString());

    Literal literal5("test", 4);
    EXPECT_EQ(PredicateDataType::STRING, literal5.getType());
    EXPECT_TRUE("test" == literal5.toString());

    Literal left(static_cast<int64_t>(123)), right(static_cast<int64_t>(123));
    EXPECT_TRUE(left == right);
    EXPECT_FALSE(left != right);
    Literal left2(static_cast<int64_t>(321));
    EXPECT_TRUE(left2 != right);
    EXPECT_FALSE(left2 == right);
  }

  TEST(TestSearchArgument, predicateLeafTest) {
    PredicateLeaf leaf1(PredicateLeaf::Operator::EQUALS, PredicateDataType::LONG, "id",
                        Literal(static_cast<int64_t>(1234)));
    EXPECT_EQ(PredicateLeaf::Operator::EQUALS, leaf1.getOperator());
    EXPECT_EQ(PredicateDataType::LONG, leaf1.getType());
    EXPECT_EQ("id", leaf1.getColumnName());
    EXPECT_EQ("(id = 1234)", leaf1.toString());

    const char *str1 = "aaa", *str2 = "zzz";
    PredicateLeaf leaf2(PredicateLeaf::Operator::BETWEEN, PredicateDataType::STRING, "name",
                        {Literal(str1, 3), Literal(str2, 3)});
    EXPECT_EQ(PredicateLeaf::Operator::BETWEEN, leaf2.getOperator());
    EXPECT_EQ(PredicateDataType::STRING, leaf2.getType());
    EXPECT_EQ("name", leaf2.getColumnName());
    EXPECT_EQ("(name between [aaa, zzz])", leaf2.toString());

    PredicateLeaf leaf3(leaf2);
    EXPECT_TRUE(leaf2 == leaf3);
    EXPECT_FALSE(leaf1 == leaf3);

    PredicateLeaf leaf4(PredicateLeaf::Operator::LESS_THAN_EQUALS, PredicateDataType::DECIMAL,
                        "sales", {Literal(111222, 6, 3)});
    EXPECT_EQ(PredicateLeaf::Operator::LESS_THAN_EQUALS, leaf4.getOperator());
    EXPECT_EQ(PredicateDataType::DECIMAL, leaf4.getType());
    EXPECT_EQ("(sales <= 111.222)", leaf4.toString());
  }

  TEST(TestSearchArgument, truthValueTest) {
    EXPECT_EQ(TruthValue::YES, TruthValue::YES && TruthValue::YES);
    EXPECT_EQ(TruthValue::NO, TruthValue::YES && TruthValue::NO);
    EXPECT_EQ(TruthValue::IS_NULL, TruthValue::YES && TruthValue::IS_NULL);
    EXPECT_EQ(TruthValue::YES_NULL, TruthValue::YES && TruthValue::YES_NULL);

    EXPECT_EQ(TruthValue::NO, TruthValue::NO || TruthValue::NO);
    EXPECT_EQ(TruthValue::YES, TruthValue::YES || TruthValue::NO);

    EXPECT_EQ(TruthValue::YES, !TruthValue::NO);
  }

  static TreeNode _not(TreeNode arg) {
    return std::make_shared<ExpressionTree>(ExpressionTree::Operator::NOT, NodeList{arg});
  }

  static TreeNode _and(NodeList args) {
    return std::make_shared<ExpressionTree>(ExpressionTree::Operator::AND, args);
  }

  static TreeNode _and(TreeNode l, TreeNode r) {
    return std::make_shared<ExpressionTree>(ExpressionTree::Operator::AND, NodeList{l, r});
  }

  static TreeNode _or(NodeList args) {
    return std::make_shared<ExpressionTree>(ExpressionTree::Operator::OR, args);
  }

  static TreeNode _or(TreeNode l, TreeNode r) {
    return std::make_shared<ExpressionTree>(ExpressionTree::Operator::OR, NodeList{l, r});
  }

  static TreeNode _leaf(int leaf) {
    return std::make_shared<ExpressionTree>(leaf);
  }

  static TreeNode _constant(TruthValue val) {
    return std::make_shared<ExpressionTree>(val);
  }

  TEST(TestSearchArgument, testNotPushdown) {
    EXPECT_EQ("leaf-1", SearchArgumentBuilderImpl::pushDownNot(_leaf(1))->toString());
    EXPECT_EQ("(not leaf-1)", SearchArgumentBuilderImpl::pushDownNot(_not(_leaf(1)))->toString());
    EXPECT_EQ("leaf-1", SearchArgumentBuilderImpl::pushDownNot(_not(_not(_leaf(1))))->toString());
    EXPECT_EQ("(not leaf-1)",
              SearchArgumentBuilderImpl::pushDownNot(_not(_not(_not(_leaf(1)))))->toString());
    EXPECT_EQ(
        "(or leaf-1 (not leaf-2))",
        SearchArgumentBuilderImpl::pushDownNot(_not(_and(_not(_leaf(1)), _leaf(2))))->toString());
    EXPECT_EQ(
        "(and (not leaf-1) leaf-2)",
        SearchArgumentBuilderImpl::pushDownNot(_not(_or(_leaf(1), _not(_leaf(2)))))->toString());
    EXPECT_EQ("(or (or (not leaf-1) leaf-2) leaf-3)",
              SearchArgumentBuilderImpl::pushDownNot(
                  _or(_not(_and(_leaf(1), _not(_leaf(2)))), _not(_not(_leaf(3)))))
                  ->toString());
    EXPECT_EQ("NO",
              SearchArgumentBuilderImpl::pushDownNot(_not(_constant(TruthValue::YES)))->toString());
    EXPECT_EQ("YES",
              SearchArgumentBuilderImpl::pushDownNot(_not(_constant(TruthValue::NO)))->toString());
    EXPECT_EQ(
        "IS_NULL",
        SearchArgumentBuilderImpl::pushDownNot(_not(_constant(TruthValue::IS_NULL)))->toString());
    EXPECT_EQ(
        "YES_NO",
        SearchArgumentBuilderImpl::pushDownNot(_not(_constant(TruthValue::YES_NO)))->toString());
    EXPECT_EQ(
        "YES_NULL",
        SearchArgumentBuilderImpl::pushDownNot(_not(_constant(TruthValue::NO_NULL)))->toString());
    EXPECT_EQ(
        "NO_NULL",
        SearchArgumentBuilderImpl::pushDownNot(_not(_constant(TruthValue::YES_NULL)))->toString());
    EXPECT_EQ("YES_NO_NULL",
              SearchArgumentBuilderImpl::pushDownNot(_not(_constant(TruthValue::YES_NO_NULL)))
                  ->toString());
  }

  TEST(TestSearchArgument, testFlatten) {
    EXPECT_EQ("leaf-1", SearchArgumentBuilderImpl::flatten(_leaf(1))->toString());
    EXPECT_EQ("NO", SearchArgumentBuilderImpl::flatten(_constant(TruthValue::NO))->toString());
    EXPECT_EQ("(not (not leaf-1))",
              SearchArgumentBuilderImpl::flatten(_not(_not(_leaf(1))))->toString());
    EXPECT_EQ("(and leaf-1 leaf-2)",
              SearchArgumentBuilderImpl::flatten(_and(_leaf(1), _leaf(2)))->toString());
    EXPECT_EQ(
        "(and (or leaf-1 leaf-2) leaf-3)",
        SearchArgumentBuilderImpl::flatten(_and(_or(_leaf(1), _leaf(2)), _leaf(3)))->toString());
    EXPECT_EQ(
        "(and leaf-1 leaf-2 leaf-3 leaf-4)",
        SearchArgumentBuilderImpl::flatten(_and(_and(_leaf(1), _leaf(2)), _and(_leaf(3), _leaf(4))))
            ->toString());
    EXPECT_EQ(
        "(or leaf-1 leaf-2 leaf-3 leaf-4)",
        SearchArgumentBuilderImpl::flatten(_or(_leaf(1), _or(_leaf(2), _or(_leaf(3), _leaf(4)))))
            ->toString());
    EXPECT_EQ(
        "(or leaf-1 leaf-2 leaf-3 leaf-4)",
        SearchArgumentBuilderImpl::flatten(_or(_or(_or(_leaf(1), _leaf(2)), _leaf(3)), _leaf(4)))
            ->toString());
    EXPECT_EQ("(or leaf-1 leaf-2 leaf-3 leaf-4 leaf-5 leaf-6)",
              SearchArgumentBuilderImpl::flatten(_or(_or(_leaf(1), _or(_leaf(2), _leaf(3))),
                                                     _or(_or(_leaf(4), _leaf(5)), _leaf(6))))
                  ->toString());
    EXPECT_EQ("(and (not leaf-1) leaf-2 (not leaf-3) leaf-4 (not leaf-5) leaf-6)",
              SearchArgumentBuilderImpl::flatten(
                  _and(_and(_not(_leaf(1)), _and(_leaf(2), _not(_leaf(3)))),
                       _and(_and(_leaf(4), _not(_leaf(5))), _leaf(6))))
                  ->toString());
    EXPECT_EQ("(not (and leaf-1 leaf-2 leaf-3))",
              SearchArgumentBuilderImpl::flatten(_not(_and(_leaf(1), _and(_leaf(2), _leaf(3)))))
                  ->toString());
  }

  TEST(TestSearchArgument, testFoldMaybe) {
    EXPECT_EQ("(and leaf-1)", SearchArgumentBuilderImpl::foldMaybe(
                                  _and(_leaf(1), _constant(TruthValue::YES_NO_NULL)))
                                  ->toString());
    EXPECT_EQ("(and leaf-1 leaf-2)",
              SearchArgumentBuilderImpl::foldMaybe(
                  _and(NodeList{_leaf(1), _constant(TruthValue::YES_NO_NULL), _leaf(2)}))
                  ->toString());
    EXPECT_EQ("(and leaf-1 leaf-2)",
              SearchArgumentBuilderImpl::foldMaybe(
                  _and(NodeList{_constant(TruthValue::YES_NO_NULL), _leaf(1), _leaf(2),
                                _constant(TruthValue::YES_NO_NULL)}))
                  ->toString());
    EXPECT_EQ("YES_NO_NULL",
              SearchArgumentBuilderImpl::foldMaybe(
                  _and(_constant(TruthValue::YES_NO_NULL), _constant(TruthValue::YES_NO_NULL)))
                  ->toString());
    EXPECT_EQ("YES_NO_NULL", SearchArgumentBuilderImpl::foldMaybe(
                                 _or(_leaf(1), _constant(TruthValue::YES_NO_NULL)))
                                 ->toString());
    EXPECT_EQ("(or leaf-1 (and leaf-2))",
              SearchArgumentBuilderImpl::foldMaybe(
                  _or(_leaf(1), _and(_leaf(2), _constant(TruthValue::YES_NO_NULL))))
                  ->toString());
    EXPECT_EQ("(and leaf-1)", SearchArgumentBuilderImpl::foldMaybe(
                                  _and(_or(_leaf(2), _constant(TruthValue::YES_NO_NULL)), _leaf(1)))
                                  ->toString());
    EXPECT_EQ(
        "(and leaf-100)",
        SearchArgumentBuilderImpl::foldMaybe(
            SearchArgumentBuilderImpl::convertToCNF(_and(
                _leaf(100), _or(NodeList{_and(_leaf(0), _leaf(1)), _and(_leaf(2), _leaf(3)),
                                         _and(_leaf(4), _leaf(5)), _and(_leaf(6), _leaf(7)),
                                         _and(_leaf(8), _leaf(9)), _and(_leaf(10), _leaf(11)),
                                         _and(_leaf(12), _leaf(13)), _and(_leaf(14), _leaf(15)),
                                         _and(_leaf(16), _leaf(17))}))))
            ->toString());
  }

  static void assertNoSharedNodes(TreeNode tree, std::unordered_set<TreeNode>& seen) {
    if (seen.find(tree) != seen.end() && tree->getOperator() != ExpressionTree::Operator::LEAF) {
      throw std::runtime_error("repeated node in expression " + tree->toString());
    }
    seen.insert(tree);
    for (auto& child : tree->getChildren()) {
      assertNoSharedNodes(child, seen);
    }
  }

  TEST(TestSearchArgument, testCNF) {
    EXPECT_EQ("leaf-1", SearchArgumentBuilderImpl::convertToCNF(_leaf(1))->toString());
    EXPECT_EQ("NO", SearchArgumentBuilderImpl::convertToCNF(_constant(TruthValue::NO))->toString());
    EXPECT_EQ("(not leaf-1)", SearchArgumentBuilderImpl::convertToCNF(_not(_leaf(1)))->toString());
    EXPECT_EQ("(and leaf-1 leaf-2)",
              SearchArgumentBuilderImpl::convertToCNF(_and(_leaf(1), _leaf(2)))->toString());
    EXPECT_EQ("(or (not leaf-1) leaf-2)",
              SearchArgumentBuilderImpl::convertToCNF(_or(_not(_leaf(1)), _leaf(2)))->toString());
    EXPECT_EQ("(and (or leaf-1 leaf-2) (not leaf-3))",
              SearchArgumentBuilderImpl::convertToCNF(_and(_or(_leaf(1), _leaf(2)), _not(_leaf(3))))
                  ->toString());
    EXPECT_EQ(
        "(and (or leaf-1 leaf-3) (or leaf-2 leaf-3)"
        " (or leaf-1 leaf-4) (or leaf-2 leaf-4))",
        SearchArgumentBuilderImpl::convertToCNF(
            _or(_and(_leaf(1), _leaf(2)), _and(_leaf(3), _leaf(4))))
            ->toString());
    EXPECT_EQ(
        "(and"
        " (or leaf-1 leaf-5) (or leaf-2 leaf-5)"
        " (or leaf-3 leaf-5) (or leaf-4 leaf-5)"
        " (or leaf-1 leaf-6) (or leaf-2 leaf-6)"
        " (or leaf-3 leaf-6) (or leaf-4 leaf-6))",
        SearchArgumentBuilderImpl::convertToCNF(
            _or(_and(NodeList{_leaf(1), _leaf(2), _leaf(3), _leaf(4)}), _and(_leaf(5), _leaf(6))))
            ->toString());
    EXPECT_EQ(
        "(and"
        " (or leaf-5 leaf-6 (not leaf-7) leaf-1 leaf-3)"
        " (or leaf-5 leaf-6 (not leaf-7) leaf-2 leaf-3)"
        " (or leaf-5 leaf-6 (not leaf-7) leaf-1 leaf-4)"
        " (or leaf-5 leaf-6 (not leaf-7) leaf-2 leaf-4))",
        SearchArgumentBuilderImpl::convertToCNF(
            _or(NodeList{_and(_leaf(1), _leaf(2)), _and(_leaf(3), _leaf(4)),
                         _or(_leaf(5), _leaf(6)), _not(_leaf(7))}))
            ->toString());
    EXPECT_EQ(
        "(and"
        " (or leaf-8 leaf-0 leaf-3 leaf-6)"
        " (or leaf-8 leaf-1 leaf-3 leaf-6)"
        " (or leaf-8 leaf-2 leaf-3 leaf-6)"
        " (or leaf-8 leaf-0 leaf-4 leaf-6)"
        " (or leaf-8 leaf-1 leaf-4 leaf-6)"
        " (or leaf-8 leaf-2 leaf-4 leaf-6)"
        " (or leaf-8 leaf-0 leaf-5 leaf-6)"
        " (or leaf-8 leaf-1 leaf-5 leaf-6)"
        " (or leaf-8 leaf-2 leaf-5 leaf-6)"
        " (or leaf-8 leaf-0 leaf-3 leaf-7)"
        " (or leaf-8 leaf-1 leaf-3 leaf-7)"
        " (or leaf-8 leaf-2 leaf-3 leaf-7)"
        " (or leaf-8 leaf-0 leaf-4 leaf-7)"
        " (or leaf-8 leaf-1 leaf-4 leaf-7)"
        " (or leaf-8 leaf-2 leaf-4 leaf-7)"
        " (or leaf-8 leaf-0 leaf-5 leaf-7)"
        " (or leaf-8 leaf-1 leaf-5 leaf-7)"
        " (or leaf-8 leaf-2 leaf-5 leaf-7))",
        SearchArgumentBuilderImpl::convertToCNF(
            _or(NodeList{_and(NodeList{_leaf(0), _leaf(1), _leaf(2)}),
                         _and(NodeList{_leaf(3), _leaf(4), _leaf(5)}), _and(_leaf(6), _leaf(7)),
                         _leaf(8)}))
            ->toString());
    EXPECT_EQ("YES_NO_NULL",
              SearchArgumentBuilderImpl::convertToCNF(
                  _or(NodeList{_and(_leaf(0), _leaf(1)), _and(_leaf(2), _leaf(3)),
                               _and(_leaf(4), _leaf(5)), _and(_leaf(6), _leaf(7)),
                               _and(_leaf(8), _leaf(9)), _and(_leaf(10), _leaf(11)),
                               _and(_leaf(12), _leaf(13)), _and(_leaf(14), _leaf(15)),
                               _and(_leaf(16), _leaf(17))}))
                  ->toString());
    EXPECT_EQ(
        "(and leaf-100 YES_NO_NULL)",
        SearchArgumentBuilderImpl::convertToCNF(
            _and(_leaf(100), _or(NodeList{_and(_leaf(0), _leaf(1)), _and(_leaf(2), _leaf(3)),
                                          _and(_leaf(4), _leaf(5)), _and(_leaf(6), _leaf(7)),
                                          _and(_leaf(8), _leaf(9)), _and(_leaf(10), _leaf(11)),
                                          _and(_leaf(12), _leaf(13)), _and(_leaf(14), _leaf(15)),
                                          _and(_leaf(16), _leaf(17))})))
            ->toString());

    std::unordered_set<TreeNode> seen;
    EXPECT_NO_THROW(assertNoSharedNodes(
        SearchArgumentBuilderImpl::convertToCNF(_or(NodeList{
            _and(NodeList{_leaf(0), _leaf(1), _leaf(2)}),
            _and(NodeList{_leaf(3), _leaf(4), _leaf(5)}), _and(_leaf(6), _leaf(7)), _leaf(8)})),
        seen));
  }

  TEST(TestSearchArgument, testBuilder) {
    auto sarg = SearchArgumentFactory::newBuilder()
                    ->startAnd()
                    .lessThan("x", PredicateDataType::LONG, Literal(static_cast<int64_t>(10)))
                    .lessThanEquals("y", PredicateDataType::STRING, Literal("hi", 2))
                    .equals("z", PredicateDataType::FLOAT, Literal(1.1))
                    .end()
                    .build();
    EXPECT_EQ(
        "leaf-0 = (x < 10), "
        "leaf-1 = (y <= hi), "
        "leaf-2 = (z = 1.1), "
        "expr = (and leaf-0 leaf-1 leaf-2)",
        sarg->toString());
    sarg = SearchArgumentFactory::newBuilder()
               ->startNot()
               .startOr()
               .isNull("x", PredicateDataType::LONG)
               .between("y", PredicateDataType::LONG, Literal(static_cast<int64_t>(10)),
                        Literal(static_cast<int64_t>(20)))
               .in("z", PredicateDataType::LONG,
                   {Literal(static_cast<int64_t>(1)), Literal(static_cast<int64_t>(2)),
                    Literal(static_cast<int64_t>(3))})
               .nullSafeEquals("a", PredicateDataType::STRING, Literal("stinger", 7))
               .end()
               .end()
               .build();
    EXPECT_EQ(
        "leaf-0 = (x is null), "
        "leaf-1 = (y between [10, 20]), "
        "leaf-2 = (z in [1, 2, 3]), "
        "leaf-3 = (a null_safe_= stinger), "
        "expr = (and (not leaf-0) (not leaf-1) (not leaf-2) (not leaf-3))",
        sarg->toString());
  }

  TEST(TestSearchArgument, testBuilderComplexTypes) {
    auto sarg = SearchArgumentFactory::newBuilder()
                    ->startAnd()
                    .lessThan("x", PredicateDataType::DATE,  // 1970-01-11
                              Literal(PredicateDataType::DATE, 123456L))
                    .lessThanEquals("y", PredicateDataType::STRING, Literal("hi        ", 10))
                    .equals("z", PredicateDataType::DECIMAL, Literal(10, 2, 1))
                    .end()
                    .build();
    EXPECT_EQ(
        "leaf-0 = (x < 123456), "
        "leaf-1 = (y <= hi        ), "
        "leaf-2 = (z = 1.0), "
        "expr = (and leaf-0 leaf-1 leaf-2)",
        sarg->toString());

    sarg = SearchArgumentFactory::newBuilder()
               ->startNot()
               .startOr()
               .isNull("x", PredicateDataType::LONG)
               .between("y", PredicateDataType::DECIMAL, Literal(10, 3, 0), Literal(200, 3, 1))
               .in("z", PredicateDataType::LONG,
                   {Literal(static_cast<int64_t>(1)), Literal(static_cast<int64_t>(2)),
                    Literal(static_cast<int64_t>(3))})
               .nullSafeEquals("a", PredicateDataType::STRING, Literal("stinger", 7))
               .end()
               .end()
               .build();
    EXPECT_EQ(
        "leaf-0 = (x is null), "
        "leaf-1 = (y between [10, 20.0]), "
        "leaf-2 = (z in [1, 2, 3]), "
        "leaf-3 = (a null_safe_= stinger), "
        "expr = (and (not leaf-0) (not leaf-1) (not leaf-2) (not leaf-3))",
        sarg->toString());
  }

  TEST(TestSearchArgument, testBuilderComplexTypes2) {
    auto sarg = SearchArgumentFactory::newBuilder()
                    ->startAnd()
                    .lessThan("x", PredicateDataType::DATE,
                              Literal(PredicateDataType::DATE, 11111L))  // "2005-3-12"
                    .lessThanEquals("y", PredicateDataType::STRING, Literal("hi        ", 10))
                    .equals("z", PredicateDataType::DECIMAL, Literal(10, 2, 1))
                    .end()
                    .build();
    EXPECT_EQ(
        "leaf-0 = (x < 11111), "
        "leaf-1 = (y <= hi        ), "
        "leaf-2 = (z = 1.0), "
        "expr = (and leaf-0 leaf-1 leaf-2)",
        sarg->toString());

    sarg = SearchArgumentFactory::newBuilder()
               ->startNot()
               .startOr()
               .isNull("x", PredicateDataType::LONG)
               .between("y", PredicateDataType::DECIMAL, Literal(10, 2, 0), Literal(200, 3, 1))
               .in("z", PredicateDataType::LONG,
                   {Literal(static_cast<int64_t>(1)), Literal(static_cast<int64_t>(2)),
                    Literal(static_cast<int64_t>(3))})
               .nullSafeEquals("a", PredicateDataType::STRING, Literal("stinger", 7))
               .end()
               .end()
               .build();
    EXPECT_EQ(
        "leaf-0 = (x is null), "
        "leaf-1 = (y between [10, 20.0]), "
        "leaf-2 = (z in [1, 2, 3]), "
        "leaf-3 = (a null_safe_= stinger), "
        "expr = (and (not leaf-0) (not leaf-1) (not leaf-2) (not leaf-3))",
        sarg->toString());
  }

  TEST(TestSearchArgument, testBuilderFloat) {
    auto sarg = SearchArgumentFactory::newBuilder()
                    ->startAnd()
                    .lessThan("x", PredicateDataType::LONG, Literal(static_cast<int64_t>(22)))
                    .lessThan("x1", PredicateDataType::LONG, Literal(static_cast<int64_t>(22)))
                    .lessThanEquals("y", PredicateDataType::STRING, Literal("hi        ", 10))
                    .equals("z", PredicateDataType::FLOAT, Literal(0.22))
                    .equals("z1", PredicateDataType::FLOAT, Literal(0.22))
                    .end()
                    .build();
    EXPECT_EQ(
        "leaf-0 = (x < 22), "
        "leaf-1 = (x1 < 22), "
        "leaf-2 = (y <= hi        ), "
        "leaf-3 = (z = 0.22), "
        "leaf-4 = (z1 = 0.22), "
        "expr = (and leaf-0 leaf-1 leaf-2 leaf-3 leaf-4)",
        sarg->toString());
  }

  TEST(TestSearchArgument, testBadLiteral) {
    EXPECT_THROW(SearchArgumentFactory::newBuilder()
                     ->startAnd()
                     .lessThan("x", PredicateDataType::LONG, Literal("hi", 2))
                     .end()
                     .build(),
                 std::invalid_argument);
  }

  TEST(TestSearchArgument, testBadLiteralList) {
    EXPECT_THROW(SearchArgumentFactory::newBuilder()
                     ->startAnd()
                     .in("x", PredicateDataType::STRING, {Literal("hi                     ", 23)})
                     .end()
                     .build(),
                 std::invalid_argument);
  }

}  // namespace orc
