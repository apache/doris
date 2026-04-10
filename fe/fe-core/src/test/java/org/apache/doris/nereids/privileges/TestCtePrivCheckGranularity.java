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

package org.apache.doris.nereids.privileges;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.properties.PhysicalProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/**
 * Tests that privChecked flag is per-CascadesContext, not per-StatementContext.
 * This ensures CTE producer and consumer subtrees check privileges independently.
 */
public class TestCtePrivCheckGranularity {

    @Test
    public void testPrivCheckedDefaultFalse() {
        CascadesContext ctx = CascadesContext.initTempContext();
        Assertions.assertFalse(ctx.isPrivChecked(), "privChecked should default to false");
    }

    @Test
    public void testPrivCheckedSetAndGet() {
        CascadesContext ctx = CascadesContext.initTempContext();

        ctx.setPrivChecked(true);
        Assertions.assertTrue(ctx.isPrivChecked());

        ctx.setPrivChecked(false);
        Assertions.assertFalse(ctx.isPrivChecked());
    }

    @Test
    public void testSubtreeContextHasIndependentPrivChecked() {
        CascadesContext parentCtx = CascadesContext.initTempContext();

        // Simulate CTE: create subtree context (shares StatementContext but different CascadesContext)
        CascadesContext subtreeCtx = CascadesContext.newSubtreeContext(
                Optional.empty(), parentCtx, parentCtx.getRewritePlan(), PhysicalProperties.ANY);

        // Both should share the same StatementContext
        Assertions.assertSame(parentCtx.getStatementContext(), subtreeCtx.getStatementContext(),
                "Parent and subtree should share the same StatementContext");

        // Both should start with privChecked = false
        Assertions.assertFalse(parentCtx.isPrivChecked());
        Assertions.assertFalse(subtreeCtx.isPrivChecked());

        // Setting privChecked on subtree should NOT affect parent
        subtreeCtx.setPrivChecked(true);
        Assertions.assertTrue(subtreeCtx.isPrivChecked(), "subtree privChecked should be true");
        Assertions.assertFalse(parentCtx.isPrivChecked(), "parent privChecked should still be false");

        // Setting privChecked on parent should NOT affect subtree
        parentCtx.setPrivChecked(true);
        Assertions.assertTrue(parentCtx.isPrivChecked());
        Assertions.assertTrue(subtreeCtx.isPrivChecked()); // still true from before
    }

    @Test
    public void testTwoSubtreeContextsAreIndependent() {
        CascadesContext parentCtx = CascadesContext.initTempContext();

        // Simulate CTE consumer and producer: two subtree contexts from same parent
        CascadesContext consumerCtx = CascadesContext.newSubtreeContext(
                Optional.empty(), parentCtx, parentCtx.getRewritePlan(), PhysicalProperties.ANY);
        CascadesContext producerCtx = CascadesContext.newSubtreeContext(
                Optional.empty(), parentCtx, parentCtx.getRewritePlan(), PhysicalProperties.ANY);

        // Consumer runs CheckPrivileges first, sets its flag
        consumerCtx.setPrivChecked(true);

        // Producer should NOT see consumer's flag — this is the bug fix
        Assertions.assertFalse(producerCtx.isPrivChecked(),
                "Producer CascadesContext should have independent privChecked from consumer");
        Assertions.assertFalse(parentCtx.isPrivChecked(),
                "Parent CascadesContext should not be affected by consumer's privChecked");
    }
}
