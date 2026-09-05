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

package org.apache.doris.catalog;

import org.apache.doris.catalog.info.IndexType;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexTest {

    @Test
    public void testGetColumnUniqueIds() {
        // Create test columns with unique IDs
        List<Column> schema = new ArrayList<>();
        Column col1 = new Column("col1", Type.INT);
        col1.setUniqueId(101);
        Column col2 = new Column("col2", Type.VARCHAR);
        col2.setUniqueId(102);
        Column col3 = new Column("col3", Type.DOUBLE);
        col3.setUniqueId(103);
        Column specialCol = new Column("special-name!@#", Type.STRING);
        specialCol.setUniqueId(104);
        Column mixedCaseCol = new Column("MiXeD_CaSe", Type.BIGINT);
        mixedCaseCol.setUniqueId(105);

        schema.add(col1);
        schema.add(col2);
        schema.add(col3);
        schema.add(specialCol);
        schema.add(mixedCaseCol);

        // Test case 1: Basic column matching
        List<String> indexColumns1 = new ArrayList<>();
        indexColumns1.add("col1");
        indexColumns1.add("col3");
        Index index1 = new Index(1, "test_index1", indexColumns1, IndexType.BITMAP, null, null);

        List<Integer> uniqueIds1 = index1.getColumnUniqueIds(schema);
        Assert.assertEquals(2, uniqueIds1.size());
        Assert.assertEquals(Integer.valueOf(101), uniqueIds1.get(0));
        Assert.assertEquals(Integer.valueOf(103), uniqueIds1.get(1));

        // Test case 2: Case-insensitive matching
        List<String> indexColumns2 = new ArrayList<>();
        indexColumns2.add("CoL1");
        indexColumns2.add("COL3");
        Index index2 = new Index(2, "test_index2", indexColumns2, IndexType.BITMAP, null, null);

        List<Integer> uniqueIds2 = index2.getColumnUniqueIds(schema);
        Assert.assertEquals(2, uniqueIds2.size());
        Assert.assertEquals(Integer.valueOf(101), uniqueIds2.get(0));
        Assert.assertEquals(Integer.valueOf(103), uniqueIds2.get(1));

        // Test case 3: Non-existent column name
        List<String> indexColumns3 = new ArrayList<>();
        indexColumns3.add("col1");
        indexColumns3.add("non_existent_column");
        Index index3 = new Index(3, "test_index3", indexColumns3, IndexType.BITMAP, null, null);

        List<Integer> uniqueIds3 = index3.getColumnUniqueIds(schema);
        Assert.assertEquals(1, uniqueIds3.size());
        Assert.assertEquals(Integer.valueOf(101), uniqueIds3.get(0));

        // Test case 4: Null schema
        List<Integer> uniqueIds4 = index1.getColumnUniqueIds(null);
        Assert.assertEquals(0, uniqueIds4.size());

        // Test case 5: Empty column list
        Index emptyColIndex = new Index(5, "empty_col_index", new ArrayList<>(),
                IndexType.BITMAP, null, null);
        List<Integer> emptyColUniqueIds = emptyColIndex.getColumnUniqueIds(schema);
        Assert.assertEquals(0, emptyColUniqueIds.size());

        // Test case 6: Empty schema (non-null)
        List<Integer> emptySchemaUniqueIds = index1.getColumnUniqueIds(new ArrayList<>());
        Assert.assertEquals(0, emptySchemaUniqueIds.size());

        // Test case 7: Duplicate column names
        List<String> dupColumns = new ArrayList<>();
        dupColumns.add("col1");
        dupColumns.add("col1");  // Duplicated
        dupColumns.add("col2");
        Index dupIndex = new Index(7, "dup_index", dupColumns, IndexType.BITMAP, null, null);

        List<Integer> dupUniqueIds = dupIndex.getColumnUniqueIds(schema);
        Assert.assertEquals(3, dupUniqueIds.size());
        Assert.assertEquals(Integer.valueOf(101), dupUniqueIds.get(0));
        Assert.assertEquals(Integer.valueOf(101), dupUniqueIds.get(1));
        Assert.assertEquals(Integer.valueOf(102), dupUniqueIds.get(2));

        // Test case 8: Special characters in column names
        List<String> specialColList = new ArrayList<>();
        specialColList.add("special-name!@#");
        Index specialIndex = new Index(8, "special_index", specialColList, IndexType.BITMAP, null, null);

        List<Integer> specialUniqueIds = specialIndex.getColumnUniqueIds(schema);
        Assert.assertEquals(1, specialUniqueIds.size());
        Assert.assertEquals(Integer.valueOf(104), specialUniqueIds.get(0));

        // Test case 9: Mixed case column name
        List<String> mixedCaseList = new ArrayList<>();
        mixedCaseList.add("mixed_case");  // Testing case insensitivity with underscores
        Index mixedCaseIndex = new Index(9, "mixed_case_index", mixedCaseList, IndexType.BITMAP, null, null);

        List<Integer> mixedCaseUniqueIds = mixedCaseIndex.getColumnUniqueIds(schema);
        Assert.assertEquals(1, mixedCaseUniqueIds.size());
        Assert.assertEquals(Integer.valueOf(105), mixedCaseUniqueIds.get(0));

        // Test case 10: Large number of columns
        List<String> largeColumnList = new ArrayList<>();
        List<Column> largeSchema = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            Column tempCol = new Column("col" + i, Type.INT);
            tempCol.setUniqueId(1000 + i);
            largeSchema.add(tempCol);

            // Add every other column to the index
            if (i % 2 == 0) {
                largeColumnList.add("col" + i);
            }
        }

        Index largeIndex = new Index(10, "large_index", largeColumnList, IndexType.BITMAP, null, null);
        List<Integer> largeUniqueIds = largeIndex.getColumnUniqueIds(largeSchema);

        Assert.assertEquals(500, largeUniqueIds.size());
        // Check first and last elements
        Assert.assertEquals(Integer.valueOf(1000), largeUniqueIds.get(0));
        Assert.assertEquals(Integer.valueOf(1000 + 998), largeUniqueIds.get(499));

        // Test case 11: Order preservation - ensure column order in index is preserved in IDs
        List<String> reverseOrderColumns = new ArrayList<>();
        reverseOrderColumns.add("col3");
        reverseOrderColumns.add("col2");
        reverseOrderColumns.add("col1");

        Index reverseIndex = new Index(11, "reverse_index", reverseOrderColumns, IndexType.BITMAP, null, null);
        List<Integer> reverseUniqueIds = reverseIndex.getColumnUniqueIds(schema);

        Assert.assertEquals(3, reverseUniqueIds.size());
        Assert.assertEquals(Integer.valueOf(103), reverseUniqueIds.get(0));
        Assert.assertEquals(Integer.valueOf(102), reverseUniqueIds.get(1));
        Assert.assertEquals(Integer.valueOf(101), reverseUniqueIds.get(2));
    }

    private static Map<String, String> invertedProperties(String... keyValues) {
        Map<String, String> properties = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            properties.put(keyValues[i], keyValues[i + 1]);
        }
        return properties;
    }

    private static Index invertedIndex(Map<String, String> properties) {
        List<String> columns = new ArrayList<>();
        columns.add("body");
        return new Index(1, "idx_body", columns, IndexType.INVERTED, properties, "");
    }

    // support_phrase asks the BE to store the position of every term. A position is
    // only observable when a query can supply a second term to match against it, and
    // below should_analyzer() the query string is never split -- so the option is kept
    // exactly where the BE would run an analyzer and dropped everywhere else. Dropping
    // it here keeps the option out of the tablet metadata entirely instead of having
    // every BE consumer second-guess it.
    @Test
    public void testSupportPhraseKeptForTokenizingIndexes() {
        Assert.assertEquals("true",
                invertedIndex(invertedProperties("parser", "english"))
                        .getProperties().get("support_phrase"));
        Assert.assertEquals("true",
                invertedIndex(invertedProperties("analyzer", "my_analyzer"))
                        .getProperties().get("support_phrase"));
        // An explicit value survives untouched.
        Assert.assertEquals("false",
                invertedIndex(invertedProperties("parser", "english", "support_phrase", "false"))
                        .getProperties().get("support_phrase"));
    }

    // A normalizer must keep support_phrase, even though it emits a single token.
    // BE's get_analyzer_name_from_properties() falls back to the "normalizer" key,
    // so should_analyzer() is TRUE for such an index: it gets a FULLTEXT reader,
    // and match.cpp rejects MATCH_PHRASE outright when support_phrase is absent
    // from a FULLTEXT-served index. Dropping the key here would turn a working
    // (degenerate, single-term) phrase query into a hard error.
    @Test
    public void testSupportPhraseKeptForNormalizerIndexes() {
        Assert.assertEquals("true",
                invertedIndex(invertedProperties("normalizer", "my_normalizer"))
                        .getProperties().get("support_phrase"));
        // An analyzer still wins over the normalizer fallback.
        Assert.assertEquals("true",
                invertedIndex(invertedProperties("analyzer", "a", "normalizer", "n"))
                        .getProperties().get("support_phrase"));
    }

    @Test
    public void testSupportPhraseDroppedForNonTokenizingIndexes() {
        // parser=none is the untokenized lane spelled out.
        Assert.assertFalse(invertedIndex(invertedProperties("parser", "none"))
                .getProperties().containsKey("support_phrase"));
        Assert.assertFalse(
                invertedIndex(invertedProperties("parser", "NONE", "support_phrase", "true"))
                        .getProperties().containsKey("support_phrase"));
        // A user-written option on a keyword index is dropped as well.
        Assert.assertFalse(
                invertedIndex(invertedProperties("support_phrase", "true", "ignore_above", "256"))
                        .getProperties().containsKey("support_phrase"));
    }
}
