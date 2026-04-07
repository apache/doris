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

import org.apache.doris.datasource.hive.HiveConstraintExtractor.HivePrimaryKey;
import org.apache.doris.datasource.hive.HiveConstraintExtractor.HiveUniqueKey;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HiveConstraintExtractorTest {

    // ========================= parseColumnList Tests =========================

    @Test
    public void testParseColumnList_singleColumn() {
        List<String> result = HiveConstraintExtractor.parseColumnList("id");
        Assertions.assertEquals(Collections.singletonList("id"), result);
    }

    @Test
    public void testParseColumnList_multipleColumns() {
        List<String> result = HiveConstraintExtractor.parseColumnList("col1, col2");
        Assertions.assertEquals(Arrays.asList("col1", "col2"), result);
    }

    @Test
    public void testParseColumnList_trimWhitespace() {
        List<String> result = HiveConstraintExtractor.parseColumnList("  col1 , col2 , col3  ");
        Assertions.assertEquals(Arrays.asList("col1", "col2", "col3"), result);
    }

    @Test
    public void testParseColumnList_emptyString() {
        List<String> result = HiveConstraintExtractor.parseColumnList("");
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testParseColumnList_null() {
        List<String> result = HiveConstraintExtractor.parseColumnList(null);
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testParseColumnList_blankString() {
        List<String> result = HiveConstraintExtractor.parseColumnList("   ");
        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testParseColumnList_trailingComma() {
        List<String> result = HiveConstraintExtractor.parseColumnList("col1,col2,");
        Assertions.assertEquals(Arrays.asList("col1", "col2"), result);
    }

    @Test
    public void testParseColumnList_leadingComma() {
        List<String> result = HiveConstraintExtractor.parseColumnList(",col1,col2");
        Assertions.assertEquals(Arrays.asList("col1", "col2"), result);
    }

    @Test
    public void testParseColumnList_consecutiveCommas() {
        List<String> result = HiveConstraintExtractor.parseColumnList("col1,,col2");
        Assertions.assertEquals(Arrays.asList("col1", "col2"), result);
    }

    // ========================= extractPrimaryKey Tests =========================

    @Test
    public void testExtractPrimaryKey_singleColumn() {
        Map<String, String> params = new HashMap<>();
        params.put("primary.keys", "order_id");

        HivePrimaryKey pk = HiveConstraintExtractor.extractPrimaryKey("orders", params);

        Assertions.assertNotNull(pk);
        Assertions.assertEquals("hive_pk_orders", pk.getConstraintName());
        Assertions.assertEquals(Collections.singletonList("order_id"), pk.getColumns());
    }

    @Test
    public void testExtractPrimaryKey_compositeKey() {
        Map<String, String> params = new HashMap<>();
        params.put("primary.keys", "order_id,line_id");

        HivePrimaryKey pk = HiveConstraintExtractor.extractPrimaryKey("order_lines", params);

        Assertions.assertNotNull(pk);
        Assertions.assertEquals("hive_pk_order_lines", pk.getConstraintName());
        Assertions.assertEquals(Arrays.asList("order_id", "line_id"), pk.getColumns());
    }

    @Test
    public void testExtractPrimaryKey_withSpaces() {
        Map<String, String> params = new HashMap<>();
        params.put("primary.keys", " col1 , col2 ");

        HivePrimaryKey pk = HiveConstraintExtractor.extractPrimaryKey("t", params);

        Assertions.assertNotNull(pk);
        Assertions.assertEquals(Arrays.asList("col1", "col2"), pk.getColumns());
    }

    @Test
    public void testExtractPrimaryKey_nullParameters() {
        HivePrimaryKey pk = HiveConstraintExtractor.extractPrimaryKey("t", null);
        Assertions.assertNull(pk);
    }

    @Test
    public void testExtractPrimaryKey_emptyParameters() {
        HivePrimaryKey pk = HiveConstraintExtractor.extractPrimaryKey("t", new HashMap<>());
        Assertions.assertNull(pk);
    }

    @Test
    public void testExtractPrimaryKey_emptyValue() {
        Map<String, String> params = new HashMap<>();
        params.put("primary.keys", "");

        HivePrimaryKey pk = HiveConstraintExtractor.extractPrimaryKey("t", params);
        Assertions.assertNull(pk);
    }

    @Test
    public void testExtractPrimaryKey_blankValue() {
        Map<String, String> params = new HashMap<>();
        params.put("primary.keys", "   ");

        HivePrimaryKey pk = HiveConstraintExtractor.extractPrimaryKey("t", params);
        Assertions.assertNull(pk);
    }

    @Test
    public void testExtractPrimaryKey_columnSet() {
        Map<String, String> params = new HashMap<>();
        params.put("primary.keys", "a,b,c");

        HivePrimaryKey pk = HiveConstraintExtractor.extractPrimaryKey("t", params);

        Assertions.assertNotNull(pk);
        Set<String> columnSet = pk.getColumnSet();
        Assertions.assertEquals(3, columnSet.size());
        Assertions.assertTrue(columnSet.contains("a"));
        Assertions.assertTrue(columnSet.contains("b"));
        Assertions.assertTrue(columnSet.contains("c"));
    }

    // ========================= extractUniqueKeys Tests =========================

    @Test
    public void testExtractUniqueKeys_singleUniqueKey() {
        Map<String, String> params = new HashMap<>();
        params.put("unique.keys", "email");

        List<HiveUniqueKey> uks = HiveConstraintExtractor.extractUniqueKeys("users", params);

        Assertions.assertEquals(1, uks.size());
        Assertions.assertEquals("hive_uk_users", uks.get(0).getConstraintName());
        Assertions.assertEquals(Collections.singletonList("email"), uks.get(0).getColumns());
    }

    @Test
    public void testExtractUniqueKeys_singleCompositeUniqueKey() {
        Map<String, String> params = new HashMap<>();
        params.put("unique.keys", "phone,country_code");

        List<HiveUniqueKey> uks = HiveConstraintExtractor.extractUniqueKeys("users", params);

        Assertions.assertEquals(1, uks.size());
        Assertions.assertEquals(Arrays.asList("phone", "country_code"), uks.get(0).getColumns());
    }

    @Test
    public void testExtractUniqueKeys_numberedKeys() {
        Map<String, String> params = new HashMap<>();
        params.put("unique.keys.0", "email");
        params.put("unique.keys.1", "phone,country_code");

        List<HiveUniqueKey> uks = HiveConstraintExtractor.extractUniqueKeys("users", params);

        Assertions.assertEquals(2, uks.size());

        // Collect all constraint names for order-independent validation
        Set<String> names = uks.stream()
                .map(HiveUniqueKey::getConstraintName)
                .collect(java.util.stream.Collectors.toSet());
        Assertions.assertTrue(names.contains("hive_uk_0_users"));
        Assertions.assertTrue(names.contains("hive_uk_1_users"));
    }

    @Test
    public void testExtractUniqueKeys_bothSingleAndNumbered() {
        Map<String, String> params = new HashMap<>();
        params.put("unique.keys", "email");
        params.put("unique.keys.0", "phone");
        params.put("unique.keys.1", "ssn");

        List<HiveUniqueKey> uks = HiveConstraintExtractor.extractUniqueKeys("users", params);

        // Should find 3: one from unique.keys and two from unique.keys.0, unique.keys.1
        Assertions.assertEquals(3, uks.size());
    }

    @Test
    public void testExtractUniqueKeys_nullParameters() {
        List<HiveUniqueKey> uks = HiveConstraintExtractor.extractUniqueKeys("t", null);
        Assertions.assertNotNull(uks);
        Assertions.assertTrue(uks.isEmpty());
    }

    @Test
    public void testExtractUniqueKeys_emptyParameters() {
        List<HiveUniqueKey> uks = HiveConstraintExtractor.extractUniqueKeys("t", new HashMap<>());
        Assertions.assertNotNull(uks);
        Assertions.assertTrue(uks.isEmpty());
    }

    @Test
    public void testExtractUniqueKeys_emptyValue() {
        Map<String, String> params = new HashMap<>();
        params.put("unique.keys", "");

        List<HiveUniqueKey> uks = HiveConstraintExtractor.extractUniqueKeys("t", params);
        Assertions.assertTrue(uks.isEmpty());
    }

    @Test
    public void testExtractUniqueKeys_numberedWithEmptyValue() {
        Map<String, String> params = new HashMap<>();
        params.put("unique.keys.0", "email");
        params.put("unique.keys.1", "");     // empty → should be skipped
        params.put("unique.keys.2", "phone");

        List<HiveUniqueKey> uks = HiveConstraintExtractor.extractUniqueKeys("t", params);

        Assertions.assertEquals(2, uks.size());
    }

    @Test
    public void testExtractUniqueKeys_columnSet() {
        Map<String, String> params = new HashMap<>();
        params.put("unique.keys", "a,b");

        List<HiveUniqueKey> uks = HiveConstraintExtractor.extractUniqueKeys("t", params);

        Assertions.assertEquals(1, uks.size());
        Set<String> colSet = uks.get(0).getColumnSet();
        Assertions.assertEquals(2, colSet.size());
        Assertions.assertTrue(colSet.contains("a"));
        Assertions.assertTrue(colSet.contains("b"));
    }

    // ========================= hasConstraintProperties Tests =========================

    @Test
    public void testHasConstraintProperties_withPrimaryKey() {
        Map<String, String> params = new HashMap<>();
        params.put("primary.keys", "id");

        Assertions.assertTrue(HiveConstraintExtractor.hasConstraintProperties(params));
    }

    @Test
    public void testHasConstraintProperties_withUniqueKey() {
        Map<String, String> params = new HashMap<>();
        params.put("unique.keys", "email");

        Assertions.assertTrue(HiveConstraintExtractor.hasConstraintProperties(params));
    }

    @Test
    public void testHasConstraintProperties_withNumberedUniqueKey() {
        Map<String, String> params = new HashMap<>();
        params.put("unique.keys.0", "email");

        Assertions.assertTrue(HiveConstraintExtractor.hasConstraintProperties(params));
    }

    @Test
    public void testHasConstraintProperties_noConstraints() {
        Map<String, String> params = new HashMap<>();
        params.put("transient_lastDdlTime", "1234567890");
        params.put("totalSize", "50000");

        Assertions.assertFalse(HiveConstraintExtractor.hasConstraintProperties(params));
    }

    @Test
    public void testHasConstraintProperties_emptyMap() {
        Assertions.assertFalse(HiveConstraintExtractor.hasConstraintProperties(new HashMap<>()));
    }

    @Test
    public void testHasConstraintProperties_null() {
        Assertions.assertFalse(HiveConstraintExtractor.hasConstraintProperties(null));
    }

    @Test
    public void testHasConstraintProperties_allConstraintTypes() {
        Map<String, String> params = new HashMap<>();
        params.put("primary.keys", "id");
        params.put("unique.keys", "email");
        params.put("unique.keys.0", "phone");

        Assertions.assertTrue(HiveConstraintExtractor.hasConstraintProperties(params));
    }

    // ========================= Data Class (toString) Tests =========================

    @Test
    public void testHivePrimaryKey_toString() {
        HivePrimaryKey pk = new HivePrimaryKey("hive_pk_orders", Arrays.asList("order_id", "line_id"));
        String str = pk.toString();

        Assertions.assertTrue(str.contains("hive_pk_orders"));
        Assertions.assertTrue(str.contains("order_id"));
        Assertions.assertTrue(str.contains("line_id"));
    }

    @Test
    public void testHiveUniqueKey_toString() {
        HiveUniqueKey uk = new HiveUniqueKey("hive_uk_users", Collections.singletonList("email"));
        String str = uk.toString();

        Assertions.assertTrue(str.contains("hive_uk_users"));
        Assertions.assertTrue(str.contains("email"));
    }

    // ========================= Data Class Immutability Tests =========================

    @Test
    public void testHivePrimaryKey_columnsAreImmutable() {
        HivePrimaryKey pk = new HivePrimaryKey("pk", Arrays.asList("a", "b"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> pk.getColumns().add("c"));
    }

    @Test
    public void testHiveUniqueKey_columnsAreImmutable() {
        HiveUniqueKey uk = new HiveUniqueKey("uk", Arrays.asList("a", "b"));
        Assertions.assertThrows(UnsupportedOperationException.class, () -> uk.getColumns().add("c"));
    }

    // ========================= Integration-style Tests =========================

    @Test
    public void testFullScenario_ordersTable() {
        // Simulates: CREATE TABLE orders ... TBLPROPERTIES (
        //   'primary.keys' = 'order_id',
        //   'unique.keys.0' = 'email',
        //   'unique.keys.1' = 'customer_id'
        // )
        Map<String, String> params = new HashMap<>();
        params.put("primary.keys", "order_id");
        params.put("unique.keys.0", "email");
        params.put("unique.keys.1", "customer_id");

        // Verify has constraints
        Assertions.assertTrue(HiveConstraintExtractor.hasConstraintProperties(params));

        // Verify PK
        HivePrimaryKey pk = HiveConstraintExtractor.extractPrimaryKey("orders", params);
        Assertions.assertNotNull(pk);
        Assertions.assertEquals("hive_pk_orders", pk.getConstraintName());
        Assertions.assertEquals(Collections.singletonList("order_id"), pk.getColumns());

        // Verify UKs
        List<HiveUniqueKey> uks = HiveConstraintExtractor.extractUniqueKeys("orders", params);
        Assertions.assertEquals(2, uks.size());
    }

    @Test
    public void testFullScenario_noPkOnlyUk() {
        Map<String, String> params = new HashMap<>();
        params.put("unique.keys", "email");

        Assertions.assertTrue(HiveConstraintExtractor.hasConstraintProperties(params));
        Assertions.assertNull(HiveConstraintExtractor.extractPrimaryKey("t", params));

        List<HiveUniqueKey> uks = HiveConstraintExtractor.extractUniqueKeys("t", params);
        Assertions.assertEquals(1, uks.size());
        Assertions.assertEquals(Collections.singletonList("email"), uks.get(0).getColumns());
    }

    @Test
    public void testFullScenario_noConstraints() {
        Map<String, String> params = new HashMap<>();
        params.put("someOtherProp", "value");

        Assertions.assertFalse(HiveConstraintExtractor.hasConstraintProperties(params));
        Assertions.assertNull(HiveConstraintExtractor.extractPrimaryKey("t", params));
        Assertions.assertTrue(HiveConstraintExtractor.extractUniqueKeys("t", params).isEmpty());
    }
}

