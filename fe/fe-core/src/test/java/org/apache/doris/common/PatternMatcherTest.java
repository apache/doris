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

package org.apache.doris.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class PatternMatcherTest {
    @Test
    public void testNormal() {
        try {
            PatternMatcher matcher = PatternMatcher.createMysqlPattern("%abc", false);
            Assertions.assertTrue(matcher.match("kljfdljasabc"));
            Assertions.assertTrue(matcher.match("kljfdljasABc"));
            Assertions.assertTrue(matcher.match("ABc"));
            Assertions.assertFalse(matcher.match("kljfdljasABc "));

            matcher = PatternMatcher.createMysqlPattern("ab%c", false);
            Assertions.assertTrue(matcher.match("ab12121dfksjfla c"));
            Assertions.assertTrue(matcher.match("abc"));

            matcher = PatternMatcher.createMysqlPattern("_abc", false);
            Assertions.assertTrue(matcher.match("1ABC"));
            Assertions.assertFalse(matcher.match("12abc"));
            Assertions.assertFalse(matcher.match("abc"));

            matcher = PatternMatcher.createMysqlPattern("a_bc", false);
            Assertions.assertTrue(matcher.match("A1BC"));
            Assertions.assertFalse(matcher.match("abc"));
            Assertions.assertFalse(matcher.match("a12bc"));

            // Escape from MySQL result

            // "abc" like "ab\c" True
            matcher = PatternMatcher.createMysqlPattern("ab\\c", false);
            Assertions.assertTrue(matcher.match("abc"));
            // "ab\c" like "ab\\c"
            matcher = PatternMatcher.createMysqlPattern("ab\\\\c", false);
            Assertions.assertTrue(matcher.match("ab\\c"));
            // "ab\\c" like "ab\\\\c"
            matcher = PatternMatcher.createMysqlPattern("ab\\\\\\\\c", false);
            Assertions.assertTrue(matcher.match("ab\\\\c"));
            // "ab\" like "ab\"
            matcher = PatternMatcher.createMysqlPattern("ab\\", false);
            Assertions.assertTrue(matcher.match("ab\\"));

            // Empty pattern
            matcher = PatternMatcher.createMysqlPattern("", false);
            Assertions.assertTrue(matcher.match(""));
            Assertions.assertFalse(matcher.match(null));
            Assertions.assertFalse(matcher.match(" "));

            matcher = PatternMatcher.createMysqlPattern("192.168.1.%", false);
            Assertions.assertTrue(matcher.match("192.168.1.1"));
            Assertions.assertFalse(matcher.match("192a168.1.1"));

            matcher = PatternMatcher.createMysqlPattern("192.1_8.1.%", false);
            Assertions.assertTrue(matcher.match("192.168.1.1"));
            Assertions.assertTrue(matcher.match("192.158.1.100"));
            Assertions.assertFalse(matcher.match("192.18.1.1"));

            matcher = PatternMatcher.createMysqlPattern("192.1\\_8.1.%", false);
            Assertions.assertTrue(matcher.match("192.1_8.1.1"));
            Assertions.assertFalse(matcher.match("192.158.1.100"));

            matcher = PatternMatcher.createMysqlPattern("192.1\\_8.1.\\%", false);
            Assertions.assertTrue(matcher.match("192.1_8.1.%"));
            Assertions.assertFalse(matcher.match("192.1_8.1.100"));

            matcher = PatternMatcher.createMysqlPattern("192.%", false);
            Assertions.assertTrue(matcher.match("192.1.8.1"));

            matcher = PatternMatcher.createMysqlPattern("192.168.%", false);
            Assertions.assertTrue(matcher.match("192.168.8.1"));

            matcher = PatternMatcher.createMysqlPattern("my-host", false);
            Assertions.assertTrue(matcher.match("my-host"));
            Assertions.assertFalse(matcher.match("my-hostabc"));
            Assertions.assertFalse(matcher.match("abcmy-host"));

            matcher = PatternMatcher.createMysqlPattern("my-%-host", false);
            Assertions.assertTrue(matcher.match("my-abc-host"));
            Assertions.assertFalse(matcher.match("my-abc-hostabc"));
            Assertions.assertFalse(matcher.match("abcmy-abc-host"));
            Assertions.assertTrue(matcher.match("my-%-host"));

            matcher = PatternMatcher.createMysqlPattern("test_dropped_partition_field$partitions", false);
            Assertions.assertTrue(matcher.match("test_dropped_partition_field$partitions"));
            Assertions.assertFalse(matcher.match("test_dropped_partition_fieldapartitions"));
        } catch (Exception e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testAbnormal() {
        try {
            PatternMatcher.createMysqlPattern("^abc", false);
            Assertions.fail();
        } catch (PatternMatcherException e) {
            System.out.println(e.getMessage());
        }

        try {
            PatternMatcher.createMysqlPattern("\\\\(abc", false);
            Assertions.fail();
        } catch (PatternMatcherException e) {
            System.out.println(e.getMessage());
        }

        try {
            PatternMatcher.createMysqlPattern("\\*abc", false);
            Assertions.fail();
        } catch (PatternMatcherException e) {
            System.out.println(e.getMessage());
        }
    }
}
