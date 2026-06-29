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

package org.apache.doris.filesystem.spi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class ObjectStorageGlobTest {

    @Test
    void expandNumericRanges_preservesZeroPadding() {
        Assertions.assertEquals("date=2025-{01,02,03}-01/",
                ObjectStorageGlob.expandNumericRanges("date=2025-{01..03}-01/"));
    }

    @Test
    void expandNumericRanges_skipsOversizedRanges() {
        String pattern = "date={1..10000000}/*";

        Assertions.assertEquals(pattern, ObjectStorageGlob.expandNumericRanges(pattern));
    }

    @Test
    void expandNumericRanges_skipsMixedOversizedRanges() {
        String pattern = "date={x,0..9223372036854775807}/*";

        Assertions.assertEquals(pattern, ObjectStorageGlob.expandNumericRanges(pattern));
    }

    @Test
    void expandedGlobListPrefixes_fallsBackForUnexpandedNumericRanges() {
        Assertions.assertEquals(List.of("date="),
                ObjectStorageGlob.expandedGlobListPrefixes("date={1..10000000}/*"));
    }
}
