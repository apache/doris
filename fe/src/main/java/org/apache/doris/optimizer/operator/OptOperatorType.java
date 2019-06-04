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

package org.apache.doris.optimizer.operator;

public enum OptOperatorType {
    OP_LOGICAL_AGGREGATE("LogicalAggregate"),
    OP_LOGICAL_JOIN("LogicalEqJoin"),
    OP_LOGICAL_INNER_JOIN("LogicalInnerJoin"),
    OP_LOGICAL_NARY_JOIN("LogicalNAryJoin"),
    OP_LOGICAL_LEFT_OUTER_JOIN("LogicalLeftOuterJoin"),
    OP_LOGICAL_LEFT_SEMI_JOIN("LogicalLeftSemiJoin"),
    OP_LOGICAL_LEFT_ANTI_JOIN("LogicalLeftAntiJoin"),
    OP_LOGICAL_FULL_OUTER_JOIN("LogicalFullOuterJoin"),
    OP_LOGICAL_PROJECT("LogicalProject"),
    OP_LOGICAL_SCAN("LogicalScan"),
    OP_LOGICAL_MYSQL_SCAN("LogicalMysqlScan"),
    OP_LOGICAL_SELECT("LogicalSelect"),
    OP_LOGICAL_UNION("LogicalUnion"),
    OP_LOGICAL_LIMIT("LogicalLimit"),

    OP_PHYSICAL_OLAP_SCAN("PhysicalOlapScan"),
    OP_PHYSICAL_HASH_JOIN("PhysicalHashJoin"),
    OP_PHYSICAL_HASH_AGG("PhysicalHashAgg"),
    OP_PHYSICAL_SORT("PhysicalSort"),
    OP_PHYSICAL_UNION("PhysicalUnion"),
    OP_PHYSICAL_FILTER("PhysicalFilter"),
    OP_PHYSICAL_PROJECT("PhysicalProject"),
    OP_PHYSICAL_LIMIT("PhysicalLimit"),
    OP_PHYSICAL_DISTRIBUTION("PhysicalDistribution"),
    OP_PHYSICAL_MYSQL_SCAN("PhysicalMysqlScan"),

    OP_ITEM_AGG_FUNC("ItemAggregateFunction"),
    OP_ITEM_ARITHMETIC("ItemArithmetic"),
    OP_ITEM_BINARY_PREDICATE("ItemBinaryPredicate"),
    OP_ITEM_CASE("ItemCase"),
    OP_ITEM_CAST("ItemCast"),
    OP_ITEM_COLUMN_REF("ItemColumnReference"),
    OP_ITEM_COMPOUND_PREDICATE("ItemCompoundPredicate"),
    OP_ITEM_CONST("ItemConst"),
    OP_ITEM_FUNCTION_CALL("ItemFunctionCall"),
    OP_ITEM_IN_PREDICATE("ItemInPredicate"),
    OP_ITEM_IS_NULL_PREDICATE("ItemIsNullPredicate"),
    OP_ITEM_LIKE_PREDICATE("ItemLikePredicate"),
    OP_ITEM_PROJECT_ELEMENT("ItemProjectElement"),
    OP_ITEM_PROJECT_LIST("ItemProjectList"),
    OP_ITEM_ARRAY("ItemArray"),
    OP_ITEM_SUBQUERY_ALL("ItemSubqueryAll"),
    OP_ITEM_SUBQUERY_ANY("ItemSubqueryAny"),
    OP_ITEM_SUBQUERY_EXISTS("ItemSubqueryExists"),
    OP_ITEM_SUBQUERY_NOT_EXISTS("ItemSubqueryNotExists"),

    OP_PATTERN_LEAF("PatternLeaf"),
    OP_PATTERN_TREE("PatternTree"),
    OP_PATTERN_MULTI_LEAF("PatternMultiLeaf"),
    OP_PATTERN_MULTI_TREE("PatternMultiTree"),

    // The following is only used in unit test
    OP_LOGICAL_UNIT_TEST_INTERNAL("LogicalUnitTestInternalNode"),
    OP_LOGICAL_UNIT_TEST_LEAF("LogicalUnitTestLeafNode"),
    OP_PHYSICAL_UNIT_TEST_INTERNAL("PhysicalUnitTestInternalNode"),
    OP_PHYSICAL_UNIT_TEST_LEAF("PhysicalUnitTestLeafNode");

    private String name;

    OptOperatorType(String name) {
        this.name = name;
    }

    public String getName() { return name; }

    @Override
    public String toString() { return name; }
}
