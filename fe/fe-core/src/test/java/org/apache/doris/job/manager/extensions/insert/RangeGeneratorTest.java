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

package org.apache.doris.job.manager.extensions.insert;

import org.apache.doris.job.extensions.insert.SQLRangeGenerator;

import org.apache.commons.lang3.Range;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class RangeGeneratorTest {

    /**
     * Tests the generateRanges method under normal conditions and edge cases.
     */
    @Test
    public void testGenerateRanges() {
        // Tests when the range size is less than the shard count
        Assertions.assertIterableEquals(
                Arrays.asList(Range.between(1L, 1L), Range.between(2L, 2L)),
                SQLRangeGenerator.generateRanges(3, 1L, 2L)
        );

        // Tests a case where the range can be evenly divided
        Assertions.assertIterableEquals(
                Arrays.asList(Range.between(1L, 5L), Range.between(6L, 10L)),
                SQLRangeGenerator.generateRanges(2, 1L, 10L)
        );
        Assertions.assertIterableEquals(
                Arrays.asList(Range.between(1L, 5L), Range.between(6L, 11L)),
                SQLRangeGenerator.generateRanges(2, 1L, 11L)
        );
    }
}

