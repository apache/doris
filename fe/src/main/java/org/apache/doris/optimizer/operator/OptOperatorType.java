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
    OP_LOGICAL_SCAN("LogicalScan"),
    OP_LOGICAL_JOIN("LogicalEqJoin"),
    OP_LOGICAL_AGGREGATE("LogicalAggregate"),
    OP_LOGICAL_UNION("LogicalUnion"),

    OP_PHYSICAL_OLAP_SCAN("PhysicalOlapScan"),
    OP_PHYSICAL_HASH_JOIN("PhysicalHashJoin"),
    OP_PHYSICAL_HASH_AGG("PhysicalHashAgg"),

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

    OP_PATTERN_LEAF("PatternLeaf"),
    // following is only used in unit test
    OP_UNIT_TEST_INTERNAL("UnitTestInternalNode"),
    OP_UNIT_TEST_LEAF("UnitTestLeafNode");

    private String name;

    OptOperatorType(String name) {
        this.name = name;
    }

    public String getName() { return name; }

    @Override
    public String toString() { return name; }
}
