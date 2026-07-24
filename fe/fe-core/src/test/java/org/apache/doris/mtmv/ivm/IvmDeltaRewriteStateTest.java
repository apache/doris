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

import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IvmDeltaRewriteStateTest extends IvmDeltaTestBase {

    @Test
    void testSequenceEncodesRefreshVersionAndDeltaIndex() {
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(ImmutableMap.of(), false, 7L);

        for (int i = 0; i < 5; i++) {
            state.nextSubSeqPrefix();
        }
        Assertions.assertEquals((7L << 11) | (5L << 1), state.toSequence(state.nextSubSeqPrefix()));
    }

    @Test
    void testSequenceRejectsTooManyDeltaScans() {
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(ImmutableMap.of(), false, 1L);

        for (int i = 0; i < 1024; i++) {
            state.nextSubSeqPrefix();
        }
        IvmException exception = Assertions.assertThrows(IvmException.class, state::nextSubSeqPrefix);
        Assertions.assertTrue(exception.getMessage().contains("too many delta scans"));
    }

    @Test
    void testExcludedTableDoesNotCreateDeltaScan() {
        IvmDeltaRewriteState state = new IvmDeltaRewriteState(ImmutableMap.of(), false, 1L);

        Assertions.assertTrue(state.isExcluded(buildScan()));
        Assertions.assertFalse(state.createDeltaScan(buildScan()).isPresent());
    }
}
