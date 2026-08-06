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

package org.apache.doris.connector.spi.pushdown;

import org.apache.doris.connector.spi.ConnectorType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Pins the construction contract of {@link ConnectorOr}.
 *
 * <p><b>WHY this matters:</b> connectors translate this node arm by arm into their own predicate
 * dialect. The class documents itself as "two or more disjuncts", but nothing used to enforce it,
 * and a degenerate or mutated node does not fail loudly on the consumer side - it silently produces
 * a NARROWER pushed-down predicate, which makes the source skip data the query should have returned.
 * Missing rows have no error, no warning and no EXPLAIN signal, so the invariant has to be enforced
 * where it is cheap to check: at construction.
 */
public class ConnectorOrTest {

    private static ConnectorExpression arm(String column) {
        return new ConnectorColumnRef(column, ConnectorType.of("INT"));
    }

    @Test
    public void testEmptyDisjunctListRejected() {
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ConnectorOr(Collections.emptyList()));
        Assertions.assertTrue(e.getMessage().contains("at least two disjuncts"), e.getMessage());
    }

    @Test
    public void testSingleDisjunctRejected() {
        // A one-armed OR is the caller's logic error (the arm should have been used directly).
        // Absorbing it silently is what lets a lost arm reach a connector unnoticed.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ConnectorOr(Collections.singletonList(arm("a"))));
    }

    @Test
    public void testNullDisjunctListRejected() {
        Assertions.assertThrows(NullPointerException.class, () -> new ConnectorOr(null));
    }

    @Test
    public void testTwoOrMoreDisjunctsAccepted() {
        Assertions.assertEquals(2, new ConnectorOr(Arrays.asList(arm("a"), arm("b")))
                .getDisjuncts().size());
        Assertions.assertEquals(3, new ConnectorOr(Arrays.asList(arm("a"), arm("b"), arm("c")))
                .getDisjuncts().size());
    }

    @Test
    public void testCallerListMutationDoesNotChangeNode() {
        // The node used to wrap the caller's list as an unmodifiable VIEW, so a caller that kept
        // building its own list afterwards would mutate an already-constructed predicate node.
        // ConnectorIn in this same package copies defensively; this pins the two to one behavior.
        List<ConnectorExpression> arms = new ArrayList<>(Arrays.asList(arm("a"), arm("b")));
        ConnectorOr or = new ConnectorOr(arms);
        arms.add(arm("c"));
        Assertions.assertEquals(2, or.getDisjuncts().size());
    }

    @Test
    public void testDisjunctsAreUnmodifiable() {
        ConnectorOr or = new ConnectorOr(Arrays.asList(arm("a"), arm("b")));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> or.getDisjuncts().add(arm("c")));
    }
}
