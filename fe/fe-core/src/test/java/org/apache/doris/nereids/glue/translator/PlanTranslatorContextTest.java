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

package org.apache.doris.nereids.glue.translator;

import org.apache.doris.planner.PlanFragment;
import org.apache.doris.planner.PlanFragmentId;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;

/**
 * The fragment-merging contract of {@link PlanTranslatorContext}.
 *
 * <p>A node that absorbs the fragments of its children must translate those children with exchange
 * elision forbidden ({@link PlanTranslatorContext#forbidExchangeElision()}), because eliding the
 * exchange below an operator removes the last fragment boundary of its olap scan and lets the scan be
 * absorbed into a fragment that already holds another one. Only such a child may be merged, which is
 * what {@link PlanTranslatorContext#markMergeChildFragment(PlanFragment)} records and what
 * {@link PlanTranslatorContext#mergePlanFragment(PlanFragment, PlanFragment)} requires.
 */
public class PlanTranslatorContextTest {

    @Test
    public void testMergePlanFragmentRequiresAMergeChild() {
        PlanTranslatorContext context = new PlanTranslatorContext();
        Assertions.assertThrows(IllegalStateException.class,
                () -> context.mergePlanFragment(mockFragment(0), mockFragment(1)));
    }

    @Test
    public void testOnlyMarkedFragmentsMayBeMerged() {
        PlanTranslatorContext context = new PlanTranslatorContext();
        PlanFragment marked = mockFragment(0);
        context.markMergeChildFragment(marked);
        // The marked child may be merged, another fragment of the same plan may not.
        Assertions.assertThrows(IllegalStateException.class,
                () -> context.mergePlanFragment(mockFragment(1), mockFragment(2)));
        context.mergePlanFragment(marked, mockFragment(2));
    }

    @Test
    public void testExchangeElisionIsForbiddenWhileAMergingNodeTranslatesItsChildren() {
        PlanTranslatorContext context = new PlanTranslatorContext();
        Assertions.assertFalse(context.isExchangeElisionForbidden());
        context.forbidExchangeElision();
        try {
            Assertions.assertTrue(context.isExchangeElisionForbidden());
        } finally {
            context.allowExchangeElision();
        }
        Assertions.assertFalse(context.isExchangeElisionForbidden());
    }

    @Test
    public void testNestedMergingNodesKeepExchangeElisionForbidden() {
        PlanTranslatorContext context = new PlanTranslatorContext();
        context.forbidExchangeElision();
        // A merging node inside a merging node: leaving the inner one keeps the outer declaration.
        context.forbidExchangeElision();
        context.allowExchangeElision();
        Assertions.assertTrue(context.isExchangeElisionForbidden());
        context.allowExchangeElision();
        Assertions.assertFalse(context.isExchangeElisionForbidden());
    }

    private PlanFragment mockFragment(int fragmentId) {
        PlanFragment fragment = Mockito.mock(PlanFragment.class);
        Mockito.when(fragment.getFragmentId()).thenReturn(new PlanFragmentId(fragmentId));
        Mockito.when(fragment.getTargetRuntimeFilterIds()).thenReturn(Collections.emptySet());
        Mockito.when(fragment.getBuilderRuntimeFilterIds()).thenReturn(Collections.emptySet());
        return fragment;
    }
}
