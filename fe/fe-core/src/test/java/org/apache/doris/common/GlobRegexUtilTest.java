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

import com.google.re2j.Pattern;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class GlobRegexUtilTest {

    private void assertGlobToRegex(String globPattern, String expectedRegex) {
        String regex = GlobRegexUtil.globToRegex(globPattern);
        Assertions.assertEquals(expectedRegex, regex, "pattern: " + globPattern);
    }

    @Test
    public void testGlobToRegexBasicTokens() {
        assertGlobToRegex("*", "^.*$");
        assertGlobToRegex("?", "^.$");
        assertGlobToRegex("a?b", "^a.b$");
        assertGlobToRegex("a*b", "^a.*b$");
    }

    @Test
    public void testGlobToRegexRepeatedWildcards() {
        assertGlobToRegex("a**b", "^a.*.*b$");
        assertGlobToRegex("a??b", "^a..b$");
        assertGlobToRegex("?*", "^..*$");
        assertGlobToRegex("*?", "^.*.$");
    }


    @Test
    public void testGlobToRegexEscaping() {
        assertGlobToRegex("a.b", "^a\\.b$");
        assertGlobToRegex("a+b", "^a\\+b$");
        assertGlobToRegex("a{b}", "^a\\{b\\}$");
        assertGlobToRegex("a\\*b", "^a\\*b$");
        assertGlobToRegex("a\\?b", "^a\\?b$");
        assertGlobToRegex("a\\[b", "^a\\[b$");
        assertGlobToRegex("abc\\", "^abc\\\\$");
        assertGlobToRegex("a|b", "^a\\|b$");
        assertGlobToRegex("a(b)c", "^a\\(b\\)c$");
        assertGlobToRegex("a^b", "^a\\^b$");
        assertGlobToRegex("a$b", "^a\\$b$");
    }

    @Test
    public void testGlobToRegexCharacterClasses() {
        assertGlobToRegex("int_[0-9]", "^int_[0-9]$");
        assertGlobToRegex("int_[!0-9]", "^int_[^0-9]$");
        assertGlobToRegex("int_[^0-9]", "^int_[^0-9]$");
        assertGlobToRegex("a[\\-]b", "^a[-]b$");
        assertGlobToRegex("a[b-d]e", "^a[b-d]e$");
        assertGlobToRegex("a[\\]]b", "^a[]]b$");
        assertGlobToRegex("a[\\!]b", "^a[!]b$");
    }

    @Test
    public void testGlobToRegexEmptyPattern() {
        assertGlobToRegex("", "^$");
    }


    @Test
    public void testGlobToRegexWeirdClasses() {
        assertGlobToRegex("a[[]b", "^a[[]b$");
        assertGlobToRegex("a[]b", "^a[]b$");
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> GlobRegexUtil.globToRegex("a[\\]b"));
    }

    @Test
    public void testGlobToRegexUnclosedClass() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> GlobRegexUtil.globToRegex("int_[0-9"));
    }


    @Test
    public void testGlobToRegexMoreWeirdCases() {
        assertGlobToRegex("[]", "^[]$");
        assertGlobToRegex("[!]", "^[^]$");
        assertGlobToRegex("[^]", "^[^]$");
        assertGlobToRegex("\\", "^\\\\$");
        assertGlobToRegex("\\*", "^\\*$");
        assertGlobToRegex("a\\*b", "^a\\*b$");
        assertGlobToRegex("a[!\\]]b", "^a[^]]b$");
    }

    @Test
    public void testGetOrCompilePatternCache() {
        Pattern first = GlobRegexUtil.getOrCompilePattern("num_*");
        Pattern second = GlobRegexUtil.getOrCompilePattern("num_*");
        Assertions.assertSame(first, second);
    }
}
