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

package org.apache.doris.foundation.format;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FormatOptionsTest {

    @Test
    public void testDefaultOptions() {
        FormatOptions options = FormatOptions.getDefault();
        Assertions.assertEquals("\"", options.getNestedStringWrapper());
        Assertions.assertEquals(":", options.getMapKeyDelim());
        Assertions.assertEquals("null", options.getNullFormat());
        Assertions.assertEquals(", ", options.getCollectionDelim());
        Assertions.assertTrue(options.isBoolValueNum());
        Assertions.assertEquals(0, options.level);
    }

    @Test
    public void testPrestoOptions() {
        FormatOptions options = FormatOptions.getForPresto();
        Assertions.assertEquals("", options.getNestedStringWrapper());
        Assertions.assertEquals("=", options.getMapKeyDelim());
        Assertions.assertEquals("NULL", options.getNullFormat());
        Assertions.assertEquals(", ", options.getCollectionDelim());
        Assertions.assertTrue(options.isBoolValueNum());
    }

    @Test
    public void testHiveOptions() {
        FormatOptions options = FormatOptions.getForHive();
        Assertions.assertEquals("\"", options.getNestedStringWrapper());
        Assertions.assertEquals(":", options.getMapKeyDelim());
        Assertions.assertEquals("null", options.getNullFormat());
        Assertions.assertEquals(",", options.getCollectionDelim());
        Assertions.assertFalse(options.isBoolValueNum());
    }
}
