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

import org.apache.doris.analysis.IndexDef;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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
        Index index1 = new Index(1, "test_index1", indexColumns1, IndexDef.IndexType.BITMAP, null, null);

        List<Integer> uniqueIds1 = index1.getColumnUniqueIds(schema);
        Assert.assertEquals(2, uniqueIds1.size());
        Assert.assertEquals(Integer.valueOf(101), uniqueIds1.get(0));
        Assert.assertEquals(Integer.valueOf(103), uniqueIds1.get(1));

        // Test case 2: Case-insensitive matching
        List<String> indexColumns2 = new ArrayList<>();
        indexColumns2.add("CoL1");
        indexColumns2.add("COL3");
        Index index2 = new Index(2, "test_index2", indexColumns2, IndexDef.IndexType.BITMAP, null, null);

        List<Integer> uniqueIds2 = index2.getColumnUniqueIds(schema);
        Assert.assertEquals(2, uniqueIds2.size());
        Assert.assertEquals(Integer.valueOf(101), uniqueIds2.get(0));
        Assert.assertEquals(Integer.valueOf(103), uniqueIds2.get(1));

        // Test case 3: Non-existent column name
        List<String> indexColumns3 = new ArrayList<>();
        indexColumns3.add("col1");
        indexColumns3.add("non_existent_column");
        Index index3 = new Index(3, "test_index3", indexColumns3, IndexDef.IndexType.BITMAP, null, null);

        List<Integer> uniqueIds3 = index3.getColumnUniqueIds(schema);
        Assert.assertEquals(1, uniqueIds3.size());
        Assert.assertEquals(Integer.valueOf(101), uniqueIds3.get(0));

        // Test case 4: Null schema
        List<Integer> uniqueIds4 = index1.getColumnUniqueIds(null);
        Assert.assertEquals(0, uniqueIds4.size());

        // Test case 5: Empty column list
        Index emptyColIndex = new Index(5, "empty_col_index", new ArrayList<>(),
                IndexDef.IndexType.BITMAP, null, null);
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
        Index dupIndex = new Index(7, "dup_index", dupColumns, IndexDef.IndexType.BITMAP, null, null);

        List<Integer> dupUniqueIds = dupIndex.getColumnUniqueIds(schema);
        Assert.assertEquals(3, dupUniqueIds.size());
        Assert.assertEquals(Integer.valueOf(101), dupUniqueIds.get(0));
        Assert.assertEquals(Integer.valueOf(101), dupUniqueIds.get(1));
        Assert.assertEquals(Integer.valueOf(102), dupUniqueIds.get(2));

        // Test case 8: Special characters in column names
        List<String> specialColList = new ArrayList<>();
        specialColList.add("special-name!@#");
        Index specialIndex = new Index(8, "special_index", specialColList, IndexDef.IndexType.BITMAP, null, null);

        List<Integer> specialUniqueIds = specialIndex.getColumnUniqueIds(schema);
        Assert.assertEquals(1, specialUniqueIds.size());
        Assert.assertEquals(Integer.valueOf(104), specialUniqueIds.get(0));

        // Test case 9: Mixed case column name
        List<String> mixedCaseList = new ArrayList<>();
        mixedCaseList.add("mixed_case");  // Testing case insensitivity with underscores
        Index mixedCaseIndex = new Index(9, "mixed_case_index", mixedCaseList, IndexDef.IndexType.BITMAP, null, null);

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

        Index largeIndex = new Index(10, "large_index", largeColumnList, IndexDef.IndexType.BITMAP, null, null);
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

        Index reverseIndex = new Index(11, "reverse_index", reverseOrderColumns, IndexDef.IndexType.BITMAP, null, null);
        List<Integer> reverseUniqueIds = reverseIndex.getColumnUniqueIds(schema);

        Assert.assertEquals(3, reverseUniqueIds.size());
        Assert.assertEquals(Integer.valueOf(103), reverseUniqueIds.get(0));
        Assert.assertEquals(Integer.valueOf(102), reverseUniqueIds.get(1));
        Assert.assertEquals(Integer.valueOf(101), reverseUniqueIds.get(2));
    }
}
