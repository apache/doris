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

package org.apache.doris.datasource.hive;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class HiveMetaStoreClientHelperTest {
    @Test
    public void test() {
        Map<String, String> m = new HashMap<>();
        m.put("a\tb", "a\\tb");
        m.put("a\nb", "a\\nb");
        m.put("a\0b", "a\\0b");
        m.put("a\1b", "a\\1b");
        m.put("a\u0002b", "a\\2b");
        m.put("a\\b", "a\\\\b");
        m.put("a\"b", "a\\\"b");
        m.put("aa\'b", "aa\\'b");
        m.put("a'b", "a\\'b");
        m.put("a;b", "a\\;b");
        m.put("a\46b", "a&b");  // '/' is followed by octal
        m.put("a\u002bb", "a+b"); // '/u' is followed by hexadecimal

        m.forEach((k, v) -> Assert.assertEquals(v, HiveMetaStoreClientHelper.propStringConverter(k)));
    }
}
