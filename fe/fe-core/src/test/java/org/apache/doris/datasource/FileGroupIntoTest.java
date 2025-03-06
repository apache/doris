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

package org.apache.doris.datasource;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;


public class FileGroupIntoTest {

    private static Stream<Arguments> provideParameters() {
        return Stream.of(
            // 6, 5, 4+1, 3+2, max=6
            Arguments.of(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L), 4, 6),

            // 6+1, 5+2, 4+3, max=7
            Arguments.of(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L), 3, 7),

            // 6+3+1, 5+4+2, max=11
            Arguments.of(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L), 2, 11),

            // 1 group, sum = 21
            Arguments.of(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L), 1, 21),

            // current algorithm is not perfect,
            // perfect partition: 5+4, 3+3+3, max=9
            // current partition: 5+3, 4+3+3, max=10
            Arguments.of(Arrays.asList(3L, 3L, 3L, 4L, 5L), 2, 10),

            // current algorithm is not perfect,
            // perfect partition: 3+3+6, 4+4+4, max=12
            // current partition: 6+4+3, 4+4+3, max=13
            Arguments.of(Arrays.asList(3L, 3L, 4L, 4L, 4L, 6L), 2, 13)
        );
    }

    @ParameterizedTest
    @MethodSource("provideParameters")
    public void testAssignFilesToInstances(List<Long> fileSizes, int numInstances, long expected) {
        List<List<Integer>> groups = FileGroupInfo.assignFilesToInstances(fileSizes, numInstances);
        long max = groups.stream().map(group -> group.stream().mapToLong(fileSizes::get).sum())
                    .max(Long::compare).orElse(0L);
        Assertions.assertEquals(expected, max);
    }
}
