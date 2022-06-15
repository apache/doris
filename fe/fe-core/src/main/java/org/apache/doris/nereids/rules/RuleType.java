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

package org.apache.doris.nereids.rules;

/**
 * Type of rules, each rule has its unique type.
 */
public enum RuleType {
    // binding rules
    BINDING_UNBOUND_RELATION_RULE(RuleTypeClass.REWRITE),
    BINDING_SENTINEL(RuleTypeClass.REWRITE),

    // rewrite rules
    COLUMN_PRUNE_PROJECTION(RuleTypeClass.REWRITE),
    REWRITE_SENTINEL(RuleTypeClass.REWRITE),

    // exploration rules
    LOGICAL_JOIN_COMMUTATIVE(RuleTypeClass.EXPLORATION),
    LOGICAL_LEFT_JOIN_ASSOCIATIVE(RuleTypeClass.EXPLORATION),
    LOGICAL_JOIN_L_ASSCOM(RuleTypeClass.EXPLORATION),
    LOGICAL_JOIN_EXCHANGE(RuleTypeClass.EXPLORATION),

    // implementation rules
    LOGICAL_JOIN_TO_HASH_JOIN_RULE(RuleTypeClass.IMPLEMENTATION),
    Projection(RuleTypeClass.IMPLEMENTATION),
    Filter(RuleTypeClass.IMPLEMENTATION),
    Limit(RuleTypeClass.IMPLEMENTATION),
    HashAgg(RuleTypeClass.IMPLEMENTATION),
    OLAP_SCAN(RuleTypeClass.IMPLEMENTATION),
    IMPLEMENTATION_SENTINEL(RuleTypeClass.IMPLEMENTATION),

    // sentinel, use to count rules
    SENTINEL(RuleTypeClass.SENTINEL),
    ;

    private final RuleTypeClass ruleTypeClass;

    RuleType(RuleTypeClass ruleTypeClass) {
        this.ruleTypeClass = ruleTypeClass;
    }

    public int type() {
        return ordinal();
    }

    public RuleTypeClass getRuleTypeClass() {
        return ruleTypeClass;
    }

    enum RuleTypeClass {
        REWRITE,
        EXPLORATION,
        IMPLEMENTATION,
        SENTINEL,
        ;
    }
}
