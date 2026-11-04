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

package org.apache.doris.regression.util

import org.junit.jupiter.api.Test

import static org.junit.jupiter.api.Assertions.assertThrows

class ResultUtilsTest {
    @Test
    void preservesHexLookingStrings() {
        for (def values : [["0x41", "A"], ["prefix 0x41", "prefix A"],
                           ["[0x41]", "[A]"], ["{key: 0x41}", "{key: A}"]]) {
            assertThrows(AssertionError) {
                ResultUtils.assertSparkDorisResultEquals([[values[0]]], [[values[1]]])
            }
        }
    }

    @Test
    void preservesHexLookingValuesInMaterializedContainers() {
        assertThrows(AssertionError) {
            ResultUtils.assertSparkDorisResultEquals([[["0x41"]]], [[["A"]]])
        }
        assertThrows(AssertionError) {
            ResultUtils.assertSparkDorisResultEquals([[[key: "0x41"]]], [[[key: "A"]]])
        }
    }

    @Test
    void stillComparesMaterializedBinaryValues() {
        ResultUtils.assertSparkDorisResultEquals(
                [[[0x80, 0x81] as byte[]]], [[[0x80, 0x81] as byte[]]])
        assertThrows(AssertionError) {
            ResultUtils.assertSparkDorisResultEquals([[[0x80] as byte[]]], [[[0x81] as byte[]]])
        }
    }

    @Test
    void encodedBinaryLeavesRemainLossless() {
        ResultUtils.assertSparkDorisResultEquals([["hex:0041", "[hex:80, null]"]],
                [["hex:0041", "[hex:80, null]"]])
        for (def values : [["hex:0041", "hex:41"], ["hex:80", "hex:81"]]) {
            assertThrows(AssertionError) {
                ResultUtils.assertSparkDorisResultEquals([[values[0]]], [[values[1]]])
            }
        }
    }
}
