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

package org.apache.doris.nereids.trees.expressions;

/**
 * Types for all Expression in Nereids.
 */
public enum ExpressionType {
    UNBOUND_FUNCTION,
    UNBOUND_ALIAS,
    UNBOUND_SLOT,
    UNBOUND_STAR,
    BOUND_STAR,
    BOUND_FUNCTION,
    LITERAL,
    SLOT_REFERENCE,
    COMPARISON_PREDICATE,
    EQUAL_TO,
    LESS_THAN,
    GREATER_THAN,
    LESS_THAN_EQUAL,
    GREATER_THAN_EQUAL,
    NULL_SAFE_EQUAL,
    NOT,
    ALIAS,
    COMPOUND,
    AND,
    OR,
    BETWEEN,
    LIKE,
    REGEXP,
    MULTIPLY,
    DIVIDE,
    MOD,
    INT_DIVIDE,
    ADD,
    SUBTRACT,
    BITAND,
    BITOR,
    BITXOR,
    BITNOT,
    FACTORIAL,
    FUNCTION_CALL
}
