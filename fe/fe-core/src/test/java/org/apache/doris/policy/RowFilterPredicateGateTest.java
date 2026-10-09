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

package org.apache.doris.policy;

import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.logical.LogicalCheckPolicy;
import org.apache.doris.nereids.types.DataType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A row filter has to be a predicate, and the one payload that must never be accepted is one that restricts
 * nothing: the ordinary filter path coerces whatever it is handed, so {@code 1} becomes
 * {@code cast(1 as boolean)} - a filter admitting every row of a table somebody wrote a policy for.
 *
 * <p>The same question is asked in two places - by {@code CREATE ROW POLICY} of the predicate it is about to
 * store, and by the planner of whatever an authorization source hands back - so what is pinned here is the
 * answer itself. Both directions matter: a gate that also refused ordinary predicates would take working
 * policies away.
 */
public class RowFilterPredicateGateTest {

    private static final NereidsParser PARSER = new NereidsParser();

    @ParameterizedTest
    @ValueSource(strings = {
            "1",
            "0",
            "'yes'",
            // Every branch of a CASE is asked, so one non-boolean branch is enough - CaseWhen#getDataType
            // reports the first branch alone and would pass this.
            "case when k1 = 1 then null else 2 end",
            "case when k1 = 1 then true else 2 end"})
    public void testAPayloadThatIsNotAPredicateIsRecognised(String payload) {
        DataType refused = LogicalCheckPolicy.nonPredicateTypeOf(PARSER.parseExpression(payload));
        Assertions.assertNotNull(refused,
                "[" + payload + "] would be applied as a row filter, which coerces it to boolean rather than"
                        + " refusing it - and for a payload that is true of every row that is a policy"
                        + " restricting nothing");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "k1 = 1",
            "k1 >= 1 and k2 < 10",
            "k1 = 1 or k2 = 2",
            "not (k1 = 1)",
            "k1 in (1, 2, 3)",
            "k1 is null",
            "region like 'a%'",
            "substring(region, 1, 2) = 'cn'",
            // Not knowable before binding: an unbound function reports no type at all here, and guessing one
            // would turn away filters that work.
            "some_udf(region)",
            // The two orderings of one CASE have to be answered the same way. Reading the first branch alone
            // refuses this one and accepts the next.
            "case when k1 = 1 then null else k2 = 1 end",
            "case when k1 = 1 then k2 = 1 else null end"})
    public void testAnOrdinaryPredicateIsLeftAlone(String predicate) {
        DataType refused = LogicalCheckPolicy.nonPredicateTypeOf(PARSER.parseExpression(predicate));
        Assertions.assertNull(refused,
                "[" + predicate + "] is a row filter that works, and it was refused as being of type "
                        + refused);
    }
}
