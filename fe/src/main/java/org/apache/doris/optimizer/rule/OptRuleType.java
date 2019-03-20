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

package org.apache.doris.optimizer.rule;

public enum OptRuleType {
    // Used for initial expressions, which do't come from any rules.
    RULE_NONE(0, "none"),
    RULE_IMP_OLAP_LSCAN_TO_PSCAN(1, "OlapLogicalScanToPhysicalScan"),
    RULE_IMP_EQ_JOIN_TO_HASH_JOIN(2, "EqualJoinToHashJoin"),
    RULE_EXP_JOIN_COMMUTATIVITY(3, "JoinCommutativity"),
    RULE_EXP_JOIN_ASSOCIATIVITY(4, "JoinAssociativity"),
    RULE_EXP_UT_COMMUTATIVITY(5, "UtCommutatitivity"),
    RULE_EXP_UT_ASSOCIATIVITY(6, "UtAssocivity"),
    RULE_IMP_UT_INTERNAL(7, "UtInternal"),
    RULE_IMP_UT_LEAF(8, "UtLeaf");

    private int code;
    private String name;

    OptRuleType(int code, String name) {
        this.code = code;
        this.name = name;
    }

    public int getCode() { return code; }
    public String getName() { return name; }

    @Override
    public String toString() { return name; }
}
