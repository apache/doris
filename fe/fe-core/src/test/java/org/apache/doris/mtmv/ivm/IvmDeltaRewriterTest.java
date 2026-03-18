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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.util.PlanConstructor;

import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IvmDeltaRewriterTest {

    private final LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t1", 0);

    @Test
    void testRewriteThrowsUnsupportedForAnyScanNode(@Mocked MTMV mtmv) {
        IvmDeltaRewriter rewriter = new IvmDeltaRewriter();
        IvmDeltaRewriteContext ctx = new IvmDeltaRewriteContext(mtmv);

        AnalysisException ex = Assertions.assertThrows(AnalysisException.class,
                () -> rewriter.rewrite(scan, ctx));
        Assertions.assertTrue(ex.getMessage().contains("does not yet support"));
    }

    @Test
    void testContextRejectsNullMtmv() {
        Assertions.assertThrows(NullPointerException.class,
                () -> new IvmDeltaRewriteContext(null));
    }
}
