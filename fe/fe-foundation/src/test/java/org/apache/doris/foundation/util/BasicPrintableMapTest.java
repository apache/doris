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

package org.apache.doris.foundation.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

class BasicPrintableMapTest {

    @Test
    void testNullMap() {
        BasicPrintableMap<String, String> pm =
                new BasicPrintableMap<>(null, "=", false, false);
        Assertions.assertEquals("", pm.toString());
    }

    @Test
    void testEmptyMap() {
        BasicPrintableMap<String, String> pm =
                new BasicPrintableMap<>(new HashMap<>(), "=", false, false);
        Assertions.assertEquals("", pm.toString());
    }

    @Test
    void testSingleEntryWithoutQuotation() {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put("key1", "value1");
        BasicPrintableMap<String, String> pm =
                new BasicPrintableMap<>(map, "=", false, false);
        Assertions.assertEquals("key1 = value1", pm.toString());
    }

    @Test
    void testSingleEntryWithQuotation() {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put("key1", "value1");
        BasicPrintableMap<String, String> pm =
                new BasicPrintableMap<>(map, "=", true, false);
        Assertions.assertEquals("\"key1\" = \"value1\"", pm.toString());
    }

    @Test
    void testMultipleEntriesWithoutWrapping() {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put("a", "1");
        map.put("b", "2");
        map.put("c", "3");
        BasicPrintableMap<String, String> pm =
                new BasicPrintableMap<>(map, "=", false, false);
        Assertions.assertEquals("a = 1, b = 2, c = 3", pm.toString());
    }

    @Test
    void testMultipleEntriesWithWrapping() {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put("a", "1");
        map.put("b", "2");
        BasicPrintableMap<String, String> pm =
                new BasicPrintableMap<>(map, "=", false, true);
        Assertions.assertEquals("a = 1,\nb = 2", pm.toString());
    }

    @Test
    void testCustomKeyValueSeparator() {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put("host", "localhost");
        BasicPrintableMap<String, String> pm =
                new BasicPrintableMap<>(map, ":", false, false);
        Assertions.assertEquals("host : localhost", pm.toString());
    }

    @Test
    void testCustomEntryDelimiter() {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put("a", "1");
        map.put("b", "2");
        BasicPrintableMap<String, String> pm =
                new BasicPrintableMap<>(map, "=", false, false, ";");
        Assertions.assertEquals("a = 1; b = 2", pm.toString());
    }

    @Test
    void testDefaultEntryDelimiterIsFourParamConstructor() {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put("x", "10");
        map.put("y", "20");
        BasicPrintableMap<String, String> fourParam =
                new BasicPrintableMap<>(map, "=", false, false);
        BasicPrintableMap<String, String> fiveParam =
                new BasicPrintableMap<>(map, "=", false, false, ",");
        Assertions.assertEquals(fiveParam.toString(), fourParam.toString());
    }

    @Test
    void testSubclassOverrideShouldIncludeEntry() {
        LinkedHashMap<String, Integer> map = new LinkedHashMap<>();
        map.put("a", 1);
        map.put("secret", 2);
        map.put("b", 3);

        BasicPrintableMap<String, Integer> pm = new BasicPrintableMap<String, Integer>(
                map, "=", false, false) {
            @Override
            protected boolean shouldIncludeEntry(Map.Entry<String, Integer> entry) {
                return !"secret".equals(entry.getKey());
            }
        };
        Assertions.assertEquals("a = 1, b = 3", pm.toString());
    }

    @Test
    void testSubclassOverrideFormatValue() {
        LinkedHashMap<String, String> map = new LinkedHashMap<>();
        map.put("password", "s3cret");
        map.put("user", "admin");

        BasicPrintableMap<String, String> pm = new BasicPrintableMap<String, String>(
                map, "=", false, false) {
            @Override
            protected String formatValue(Map.Entry<String, String> entry) {
                if ("password".equals(entry.getKey())) {
                    return "***";
                }
                return super.formatValue(entry);
            }
        };
        Assertions.assertEquals("password = ***, user = admin", pm.toString());
    }

    @Test
    void testTreeMapProducesDeterministicOrder() {
        TreeMap<String, String> map = new TreeMap<>();
        map.put("cherry", "3");
        map.put("apple", "1");
        map.put("banana", "2");
        BasicPrintableMap<String, String> pm =
                new BasicPrintableMap<>(map, "=", false, false);
        Assertions.assertEquals("apple = 1, banana = 2, cherry = 3", pm.toString());
    }
}
