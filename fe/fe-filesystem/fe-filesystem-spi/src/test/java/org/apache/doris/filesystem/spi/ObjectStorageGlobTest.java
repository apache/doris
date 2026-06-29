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
import java.util.regex.Pattern;

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
    void expandNumericRanges_preservesLiteralSpacesInMixedBraceArms() {
        Assertions.assertEquals("data/{ x,1,2}/*.csv",
                ObjectStorageGlob.expandNumericRanges("data/{ x,1..2}/*.csv"));
    }

    @Test
    void expandedGlobListPrefixes_fallsBackForUnexpandedNumericRanges() {
        Assertions.assertEquals(List.of("date="),
                ObjectStorageGlob.expandedGlobListPrefixes("date={1..10000000}/*"));
    }

    @Test
    void globToRegex_preservesOversizedNumericRangeSemantics() {
        Pattern matcher = Pattern.compile(ObjectStorageGlob.globToRegex("date={1..10000000}/*"));

        Assertions.assertTrue(matcher.matcher("date=123/file.csv").matches());
        Assertions.assertTrue(matcher.matcher("date=10000000/file.csv").matches());
        Assertions.assertFalse(matcher.matcher("date=0/file.csv").matches());
        Assertions.assertFalse(matcher.matcher("date=10000001/file.csv").matches());
        Assertions.assertFalse(matcher.matcher("date=0001/file.csv").matches());
    }

    @Test
    void globToRegex_preservesLiteralSpacesInMixedBraceArms() {
        Pattern matcher = Pattern.compile(ObjectStorageGlob.globToRegex(
                ObjectStorageGlob.expandNumericRanges("data/{ x,1..2}/*.csv")));

        Assertions.assertTrue(matcher.matcher("data/ x/file.csv").matches());
        Assertions.assertTrue(matcher.matcher("data/1/file.csv").matches());
        Assertions.assertFalse(matcher.matcher("data/x/file.csv").matches());
    }

    @Test
    void globToRegex_preservesMixedOversizedNumericRangeSemantics() {
        Pattern matcher = Pattern.compile(ObjectStorageGlob.globToRegex(
                "date={x,0..9223372036854775807}/*"));

        Assertions.assertTrue(matcher.matcher("date=x/file.csv").matches());
        Assertions.assertTrue(matcher.matcher("date=9223372036854775807/file.csv").matches());
        Assertions.assertFalse(matcher.matcher("date=-1/file.csv").matches());
        Assertions.assertFalse(matcher.matcher("date=9223372036854775808/file.csv").matches());
    }

    @Test
    void globToRegex_preservesOversizedZeroPaddedNumericRangeSemantics() {
        Pattern matcher = Pattern.compile(ObjectStorageGlob.globToRegex("date={0001..1000}/*"));

        Assertions.assertTrue(matcher.matcher("date=0001/file.csv").matches());
        Assertions.assertTrue(matcher.matcher("date=0999/file.csv").matches());
        Assertions.assertTrue(matcher.matcher("date=1000/file.csv").matches());
        Assertions.assertFalse(matcher.matcher("date=999/file.csv").matches());
        Assertions.assertFalse(matcher.matcher("date=1001/file.csv").matches());
    }
}
