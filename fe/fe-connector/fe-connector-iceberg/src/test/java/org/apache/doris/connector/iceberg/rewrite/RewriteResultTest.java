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

package org.apache.doris.connector.iceberg.rewrite;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

/**
 * Tests for the {@link RewriteResult} carrier POJO (P6.4-T05 port). The column order of {@link
 * RewriteResult#toStringList()} is the procedure's result-row contract (rewritten / added / bytes / removed),
 * so it is pinned here.
 */
public class RewriteResultTest {

    @Test
    public void toStringListPreservesColumnOrder() {
        RewriteResult result = new RewriteResult(3, 1, 4096L, 2);
        Assertions.assertEquals(Arrays.asList("3", "1", "4096", "2"), result.toStringList());
    }

    @Test
    public void rewrittenBytesIsRenderedAsLong() {
        // rewrittenBytesCount is a long; a value beyond int range must round-trip as the full long string.
        RewriteResult result = new RewriteResult(0, 0, 3_000_000_000L, 0);
        Assertions.assertEquals("3000000000", result.toStringList().get(2));
    }

    @Test
    public void mergeSumsEachField() {
        RewriteResult a = new RewriteResult(1, 2, 3L, 4);
        a.merge(new RewriteResult(10, 20, 30L, 40));
        Assertions.assertEquals(Arrays.asList("11", "22", "33", "44"), a.toStringList());
    }

    @Test
    public void defaultResultIsAllZero() {
        Assertions.assertEquals(Arrays.asList("0", "0", "0", "0"), new RewriteResult().toStringList());
    }
}
