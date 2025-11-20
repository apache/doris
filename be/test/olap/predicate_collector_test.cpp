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

#include "olap/predicate_collector.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "olap/tablet_schema.h"
#include "vec/exprs/vsearch.h"

namespace doris {

using vectorized::VExprSPtr;
using vectorized::VSearchExpr;

class SearchPredicateCollectorTest : public ::testing::Test {
protected:
    TabletSchemaSPtr create_tablet_schema_with_inverted_index_no_analyzer() {
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
        index._properties = properties;

        tablet_schema->append_index(std::move(index));
        return tablet_schema;
    }

    TExprNode make_search_expr_node(const TSearchParam& search_param) {
        TExprNode node;
        node.node_type = TExprNodeType::SEARCH_EXPR;

        TTypeDesc type_desc;
        TTypeNode type_node;
        type_node.type = TTypeNodeType::SCALAR;
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::BOOLEAN);
        type_node.__set_scalar_type(scalar_type);
        type_desc.types.push_back(type_node);
        node.__set_type(type_desc);

        node.num_children = 0;
        node.search_param = search_param;
        node.__isset.search_param = true;
        return node;
    }
};

TEST_F(SearchPredicateCollectorTest, CollectSimpleTermClause) {
    auto tablet_schema = create_tablet_schema_with_inverted_index_no_analyzer();

    TSearchClause clause;
    clause.clause_type = "TERM";
    clause.field_name = "content";
    clause.value = "hello";
    clause.__isset.field_name = true;
    clause.__isset.value = true;

    TSearchParam search_param;
    search_param.original_dsl = "content:hello";
    search_param.root = clause;

    TExprNode node = make_search_expr_node(search_param);
    VExprSPtr expr = VSearchExpr::create_shared(node);

    SearchPredicateCollector collector;
    CollectInfoMap collect_infos;
    auto st = collector.collect(/*state=*/nullptr, tablet_schema, expr, &collect_infos);
    EXPECT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(1u, collect_infos.size());
    const auto& entry = *collect_infos.begin();
    const CollectInfo& info = entry.second;
    ASSERT_NE(info.index_meta, nullptr);
    ASSERT_EQ(1u, info.term_infos.size());

    const auto& term_info = *info.term_infos.begin();
    EXPECT_TRUE(term_info.is_single_term());
    EXPECT_EQ("hello", term_info.get_single_term());
    EXPECT_EQ(0, term_info.position);
}

TEST_F(SearchPredicateCollectorTest, CollectCompoundAndClause) {
    auto tablet_schema = create_tablet_schema_with_inverted_index_no_analyzer();

    TSearchClause term1;
    term1.clause_type = "TERM";
    term1.field_name = "content";
    term1.value = "hello";
    term1.__isset.field_name = true;
    term1.__isset.value = true;

    TSearchClause term2;
    term2.clause_type = "TERM";
    term2.field_name = "content";
    term2.value = "world";
    term2.__isset.field_name = true;
    term2.__isset.value = true;

    TSearchClause root;
    root.clause_type = "AND";
    root.children = {term1, term2};
    root.__isset.children = true;

    TSearchParam search_param;
    search_param.original_dsl = "content:hello AND content:world";
    search_param.root = root;

    TExprNode node = make_search_expr_node(search_param);
    VExprSPtr expr = VSearchExpr::create_shared(node);

    SearchPredicateCollector collector;
    CollectInfoMap collect_infos;
    auto st = collector.collect(/*state=*/nullptr, tablet_schema, expr, &collect_infos);
    EXPECT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(1u, collect_infos.size());
    const auto& entry = *collect_infos.begin();
    const CollectInfo& info = entry.second;

    ASSERT_EQ(2u, info.term_infos.size());
    std::vector<std::string> terms;
    for (const auto& ti : info.term_infos) {
        ASSERT_TRUE(ti.is_single_term());
        terms.push_back(ti.get_single_term());
    }
    ASSERT_EQ(2u, terms.size());
    EXPECT_EQ("hello", terms[0] == "hello" ? terms[0] : terms[1]);
    EXPECT_EQ("world", terms[0] == "hello" ? terms[1] : terms[0]);
}

TEST_F(SearchPredicateCollectorTest, NonScoreClauseIsIgnored) {
    auto tablet_schema = create_tablet_schema_with_inverted_index_no_analyzer();

    TSearchClause clause;
    clause.clause_type = "RANGE";
    clause.field_name = "content";
    clause.value = "[1 TO 10]";
    clause.__isset.field_name = true;
    clause.__isset.value = true;

    TSearchParam search_param;
    search_param.original_dsl = "content:[1 TO 10]";
    search_param.root = clause;

    TExprNode node = make_search_expr_node(search_param);
    VExprSPtr expr = VSearchExpr::create_shared(node);

    SearchPredicateCollector collector;
    CollectInfoMap collect_infos;
    auto st = collector.collect(/*state=*/nullptr, tablet_schema, expr, &collect_infos);
    EXPECT_TRUE(st.ok()) << st.to_string();

    EXPECT_TRUE(collect_infos.empty());
}

TEST_F(SearchPredicateCollectorTest, TokenizedMatchClauseWithoutAnalyzer) {
    auto tablet_schema = create_tablet_schema_with_inverted_index_no_analyzer();

    TSearchClause clause;
    clause.clause_type = "MATCH";
    clause.field_name = "content";
    clause.value = "machine learning";
    clause.__isset.field_name = true;
    clause.__isset.value = true;

    TSearchParam search_param;
    search_param.original_dsl = "content:MATCH(machine learning)";
    search_param.root = clause;

    TExprNode node = make_search_expr_node(search_param);
    VExprSPtr expr = VSearchExpr::create_shared(node);

    SearchPredicateCollector collector;
    CollectInfoMap collect_infos;
    auto st = collector.collect(/*state=*/nullptr, tablet_schema, expr, &collect_infos);
    EXPECT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(1u, collect_infos.size());
    const auto& entry = *collect_infos.begin();
    const CollectInfo& info = entry.second;
    ASSERT_EQ(1u, info.term_infos.size());
    const auto& ti = *info.term_infos.begin();
    EXPECT_TRUE(ti.is_single_term());
    EXPECT_EQ("machine learning", ti.get_single_term());
}

} // namespace doris