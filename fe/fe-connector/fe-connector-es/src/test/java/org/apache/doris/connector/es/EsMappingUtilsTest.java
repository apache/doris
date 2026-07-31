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

package org.apache.doris.connector.es;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class EsMappingUtilsTest {

    // ES 6.x style mapping with explicit mapping type "doc"
    private static final String MAPPING_ES6 = "{"
            + "\"test\": {"
            + "  \"mappings\": {"
            + "    \"doc\": {"
            + "      \"properties\": {"
            + "        \"k1\": { \"type\": \"long\" },"
            + "        \"k2\": { \"type\": \"keyword\" },"
            + "        \"k3\": { \"type\": \"text\", \"fields\": {"
            + "            \"keyword\": { \"type\": \"keyword\" }"
            + "        }},"
            + "        \"k4\": { \"properties\": {"
            + "            \"child1\": { \"type\": \"text\" },"
            + "            \"child2\": { \"type\": \"long\" }"
            + "        }},"
            + "        \"k5\": { \"type\": \"date\" },"
            + "        \"k6\": { \"type\": \"nested\", \"properties\": {"
            + "            \"child3\": { \"type\": \"keyword\" }"
            + "        }}"
            + "      }"
            + "    }"
            + "  }"
            + "}"
            + "}";

    // ES 7+/8+ style mapping without explicit mapping type
    private static final String MAPPING_ES7 = "{"
            + "\"test\": {"
            + "  \"mappings\": {"
            + "    \"properties\": {"
            + "      \"name\": { \"type\": \"keyword\" },"
            + "      \"age\": { \"type\": \"integer\" },"
            + "      \"bio\": { \"type\": \"text\", \"fields\": {"
            + "          \"raw\": { \"type\": \"keyword\" }"
            + "      }},"
            + "      \"created\": { \"type\": \"date\" },"
            + "      \"addr\": { \"properties\": {"
            + "          \"city\": { \"type\": \"keyword\" }"
            + "      }}"
            + "    }"
            + "  }"
            + "}"
            + "}";

    @Test
    public void testColumn2typeMapWithSpecificColumns() {
        List<String> columns = Arrays.asList("k1", "k2", "k3", "k5");
        EsFieldContext ctx = EsMappingUtils.resolveFieldContext(columns, "test", MAPPING_ES6, "doc");

        Map<String, String> typeMap = ctx.getColumn2typeMap();
        Assertions.assertEquals(4, typeMap.size());
        Assertions.assertEquals("long", typeMap.get("k1"));
        Assertions.assertEquals("keyword", typeMap.get("k2"));
        Assertions.assertEquals("text", typeMap.get("k3"));
        Assertions.assertEquals("date", typeMap.get("k5"));
    }

    @Test
    public void testColumn2typeMapObjectFieldsWithoutType() {
        List<String> columns = Arrays.asList("k4");
        EsFieldContext ctx = EsMappingUtils.resolveFieldContext(columns, "test", MAPPING_ES6, "doc");

        Map<String, String> typeMap = ctx.getColumn2typeMap();
        Assertions.assertEquals(1, typeMap.size());
        Assertions.assertEquals("object", typeMap.get("k4"));
    }

    @Test
    public void testColumn2typeMapPopulatedWhenColumnNamesEmpty() {
        // When columnNames is empty, all mapping properties should be resolved
        EsFieldContext ctx = EsMappingUtils.resolveFieldContext(
                Collections.emptyList(), "test", MAPPING_ES6, "doc");

        Map<String, String> typeMap = ctx.getColumn2typeMap();
        Assertions.assertEquals(6, typeMap.size());
        Assertions.assertEquals("long", typeMap.get("k1"));
        Assertions.assertEquals("keyword", typeMap.get("k2"));
        Assertions.assertEquals("text", typeMap.get("k3"));
        Assertions.assertEquals("object", typeMap.get("k4"));
        Assertions.assertEquals("date", typeMap.get("k5"));
        Assertions.assertEquals("nested", typeMap.get("k6"));
    }

    @Test
    public void testColumn2typeMapPopulatedWhenColumnNamesNull() {
        EsFieldContext ctx = EsMappingUtils.resolveFieldContext(
                null, "test", MAPPING_ES6, "doc");

        Map<String, String> typeMap = ctx.getColumn2typeMap();
        Assertions.assertFalse(typeMap.isEmpty());
        Assertions.assertEquals("long", typeMap.get("k1"));
    }

    @Test
    public void testFieldContextKeywordSniffWhenColumnsEmpty() {
        // k3 is "text" with a "keyword" sub-field — should be detected
        EsFieldContext ctx = EsMappingUtils.resolveFieldContext(
                Collections.emptyList(), "test", MAPPING_ES6, "doc");

        Map<String, String> fetchFields = ctx.getFetchFieldsContext();
        Assertions.assertTrue(fetchFields.containsKey("k3"));
        Assertions.assertEquals("k3.keyword", fetchFields.get("k3"));
    }

    @Test
    public void testFieldContextDateCompatWhenColumnsEmpty() {
        // k5 is "date" with no format → needs compat
        EsFieldContext ctx = EsMappingUtils.resolveFieldContext(
                Collections.emptyList(), "test", MAPPING_ES6, "doc");

        List<String> dateFields = ctx.getNeedCompatDateFields();
        Assertions.assertTrue(dateFields.contains("k5"));
    }

    @Test
    public void testColumn2typeMapEs7Mapping() {
        EsFieldContext ctx = EsMappingUtils.resolveFieldContext(
                Collections.emptyList(), "test", MAPPING_ES7, null);

        Map<String, String> typeMap = ctx.getColumn2typeMap();
        Assertions.assertEquals(5, typeMap.size());
        Assertions.assertEquals("keyword", typeMap.get("name"));
        Assertions.assertEquals("integer", typeMap.get("age"));
        Assertions.assertEquals("text", typeMap.get("bio"));
        Assertions.assertEquals("date", typeMap.get("created"));
        Assertions.assertEquals("object", typeMap.get("addr"));
    }

    @Test
    public void testEs7KeywordSniffAndDateCompat() {
        List<String> columns = Arrays.asList("bio", "created");
        EsFieldContext ctx = EsMappingUtils.resolveFieldContext(
                columns, "test", MAPPING_ES7, null);

        // bio is text with keyword sub-field "raw"
        Assertions.assertEquals("bio.raw", ctx.getFetchFieldsContext().get("bio"));
        // created is date with no format
        Assertions.assertTrue(ctx.getNeedCompatDateFields().contains("created"));
    }

    @Test
    public void testIdColumnSkipped() {
        List<String> columns = Arrays.asList("_id", "k1");
        EsFieldContext ctx = EsMappingUtils.resolveFieldContext(
                columns, "test", MAPPING_ES6, "doc");

        Map<String, String> typeMap = ctx.getColumn2typeMap();
        Assertions.assertEquals(1, typeMap.size());
        Assertions.assertFalse(typeMap.containsKey("_id"));
        Assertions.assertEquals("long", typeMap.get("k1"));
    }
}
