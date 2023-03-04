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

import org.junit.Assert;
import org.junit.Test;

public class PatternMatcherTest {
    @Test
    public void testNormal() {
        try {
            PatternMatcher matcher = PatternMatcher.createMysqlPattern("%abc", false);
            Assert.assertTrue(matcher.match("kljfdljasabc"));
            Assert.assertTrue(matcher.match("kljfdljasABc"));
            Assert.assertTrue(matcher.match("ABc"));
            Assert.assertFalse(matcher.match("kljfdljasABc "));

            matcher = PatternMatcher.createMysqlPattern("ab%c", false);
            Assert.assertTrue(matcher.match("ab12121dfksjfla c"));
            Assert.assertTrue(matcher.match("abc"));

            matcher = PatternMatcher.createMysqlPattern("_abc", false);
            Assert.assertTrue(matcher.match("1ABC"));
            Assert.assertFalse(matcher.match("12abc"));
            Assert.assertFalse(matcher.match("abc"));

            matcher = PatternMatcher.createMysqlPattern("a_bc", false);
            Assert.assertTrue(matcher.match("A1BC"));
            Assert.assertFalse(matcher.match("abc"));
            Assert.assertFalse(matcher.match("a12bc"));

            // Escape from MySQL result

            // "abc" like "ab\c" True
            matcher = PatternMatcher.createMysqlPattern("ab\\c", false);
            Assert.assertTrue(matcher.match("abc"));
            // "ab\c" like "ab\\c"
            matcher = PatternMatcher.createMysqlPattern("ab\\\\c", false);
            Assert.assertTrue(matcher.match("ab\\c"));
            // "ab\\c" like "ab\\\\c"
            matcher = PatternMatcher.createMysqlPattern("ab\\\\\\\\c", false);
            Assert.assertTrue(matcher.match("ab\\\\c"));
            // "ab\" like "ab\"
            matcher = PatternMatcher.createMysqlPattern("ab\\", false);
            Assert.assertTrue(matcher.match("ab\\"));

            // Empty pattern
            matcher = PatternMatcher.createMysqlPattern("", false);
            Assert.assertTrue(matcher.match(""));
            Assert.assertFalse(matcher.match(null));
            Assert.assertFalse(matcher.match(" "));

            matcher = PatternMatcher.createMysqlPattern("192.168.1.%", false);
            Assert.assertTrue(matcher.match("192.168.1.1"));
            Assert.assertFalse(matcher.match("192a168.1.1"));

            matcher = PatternMatcher.createMysqlPattern("192.1_8.1.%", false);
            Assert.assertTrue(matcher.match("192.168.1.1"));
            Assert.assertTrue(matcher.match("192.158.1.100"));
            Assert.assertFalse(matcher.match("192.18.1.1"));

            matcher = PatternMatcher.createMysqlPattern("192.1\\_8.1.%", false);
            Assert.assertTrue(matcher.match("192.1_8.1.1"));
            Assert.assertFalse(matcher.match("192.158.1.100"));

            matcher = PatternMatcher.createMysqlPattern("192.1\\_8.1.\\%", false);
            Assert.assertTrue(matcher.match("192.1_8.1.%"));
            Assert.assertFalse(matcher.match("192.1_8.1.100"));

            matcher = PatternMatcher.createMysqlPattern("192.%", false);
            Assert.assertTrue(matcher.match("192.1.8.1"));

            matcher = PatternMatcher.createMysqlPattern("192.168.%", false);
            Assert.assertTrue(matcher.match("192.168.8.1"));

            matcher = PatternMatcher.createMysqlPattern("my-host", false);
            Assert.assertTrue(matcher.match("my-host"));
            Assert.assertFalse(matcher.match("my-hostabc"));
            Assert.assertFalse(matcher.match("abcmy-host"));

            matcher = PatternMatcher.createMysqlPattern("my-%-host", false);
            Assert.assertTrue(matcher.match("my-abc-host"));
            Assert.assertFalse(matcher.match("my-abc-hostabc"));
            Assert.assertFalse(matcher.match("abcmy-abc-host"));
            Assert.assertTrue(matcher.match("my-%-host"));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testAbnormal() {
        try {
            PatternMatcher.createMysqlPattern("^abc", false);
            Assert.fail();
        } catch (PatternMatcherException e) {
            System.out.println(e.getMessage());
        }

        try {
            PatternMatcher.createMysqlPattern("\\\\(abc", false);
            Assert.fail();
        } catch (PatternMatcherException e) {
            System.out.println(e.getMessage());
        }

        try {
            PatternMatcher.createMysqlPattern("\\*abc", false);
            Assert.fail();
        } catch (PatternMatcherException e) {
            System.out.println(e.getMessage());
        }
    }
}
