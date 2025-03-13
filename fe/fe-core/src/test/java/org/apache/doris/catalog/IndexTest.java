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
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.proto.OlapFile;
import org.apache.doris.thrift.TOlapTableIndex;
import org.apache.doris.thrift.TStorageType;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexTest {

    @Test
    public void testConstructorWithNullColumnUniqueIds() {
        // Test constructor with null columnUniqueIds
        Index index = new Index(1, "test_index", Lists.newArrayList("col1", "col2"),
                IndexDef.IndexType.BITMAP, null, "test comment", null);

        // Should initialize columnUniqueIds as empty list, not null
        Assert.assertNotNull(index.getColumnUniqueIds());
        Assert.assertTrue(index.getColumnUniqueIds().isEmpty());
    }

    @Test
    public void testGsonPostProcess() throws IOException {
        // Create JSON without columnUniqueIds field
        String json = "{\"i\":1,\"in\":\"test_index\",\"c\":[\"col1\",\"col2\"],\"it\":\"BITMAP\",\"pt\":{},\"ct\":\"test comment\"}";

        // Deserialize
        Index index = GsonUtils.GSON.fromJson(json, Index.class);

        // Manually call gsonPostProcess to simulate deserialization
        index.gsonPostProcess();

        // columnUniqueIds should be initialized by gsonPostProcess
        Assert.assertNotNull(index.getColumnUniqueIds());
        Assert.assertTrue(index.getColumnUniqueIds().isEmpty());
    }

    @Test
    public void testToThriftWithNullColumnUniqueIds() {
        // Create an index with null columnUniqueIds
        Index index = new Index();
        index.setIndexId(1);
        index.setIndexName("test_index");
        index.setColumns(Lists.newArrayList("col1", "col2"));
        index.setIndexType(IndexDef.IndexType.BITMAP);
        index.setProperties(new HashMap<>());
        index.setColumnUniqueIds(null); // explicitly set to null

        // Should not throw NPE when calling toThrift
        TOlapTableIndex tIndex = index.toThrift();

        // Verify basic fields
        Assert.assertEquals(1, tIndex.getIndexId());
        Assert.assertEquals("test_index", tIndex.getIndexName());
        Assert.assertEquals(2, tIndex.getColumns().size());

        // ColumnUniqueIds should not be set when columnUniqueIds is null
        Assert.assertFalse(tIndex.isSetColumnUniqueIds());
    }

    @Test
    public void testToPbWithNullColumnUniqueIds() {
        // Create an index with null columnUniqueIds
        Index index = new Index();
        index.setIndexId(1);
        index.setIndexName("test_index");
        index.setColumns(Lists.newArrayList("col1", "col2"));
        index.setIndexType(IndexDef.IndexType.BITMAP);
        index.setProperties(new HashMap<>());
        index.setColumnUniqueIds(null); // explicitly set to null

        Map<Integer, Column> columnMap = new HashMap<>();
        columnMap.put(100, new Column("col1", Type.INT));
        columnMap.put(101, new Column("col2", Type.STRING));

        // Should not throw NPE when calling toPb
        OlapFile.TabletIndexPB pb = index.toPb(columnMap);

        // Verify basic fields
        Assert.assertEquals(1, pb.getIndexId());
        Assert.assertEquals("test_index", pb.getIndexName());

        // No column unique IDs should be added when columnUniqueIds is null
        Assert.assertEquals(0, pb.getColUniqueIdCount());
    }

    @Test
    public void testInitIndexColumnUniqueId() {
        // Create columns with unique IDs
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("col1", Type.INT);
        col1.setUniqueId(100);
        Column col2 = new Column("col2", Type.STRING);
        col2.setUniqueId(101);
        columns.add(col1);
        columns.add(col2);

        // Create indexes referencing those columns
        List<Index> indexes = new ArrayList<>();
        Index index1 = new Index(1, "idx1", Lists.newArrayList("col1"),
                IndexDef.IndexType.BITMAP, null, null, null);
        Index index2 = new Index(2, "idx2", Lists.newArrayList("col2"),
                IndexDef.IndexType.INVERTED, null, null, null);
        indexes.add(index1);
        indexes.add(index2);

        MaterializedIndexMeta indexMeta = new MaterializedIndexMeta(1, columns, 1, 1, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS, null, indexes, "testDb");

        // Verify columnUniqueIds is initially empty
        Assert.assertTrue(index1.getColumnUniqueIds().isEmpty());
        Assert.assertTrue(index2.getColumnUniqueIds().isEmpty());

        // Initialize column unique IDs
        indexMeta.initIndexColumnUniqueId();

        // Verify columnUniqueIds is populated correctly
        Assert.assertEquals(1, index1.getColumnUniqueIds().size());
        Assert.assertEquals(Integer.valueOf(100), index1.getColumnUniqueIds().get(0));

        Assert.assertEquals(1, index2.getColumnUniqueIds().size());
        Assert.assertEquals(Integer.valueOf(101), index2.getColumnUniqueIds().get(0));
    }

    @Test
    public void testOlapTableInitIndexColumnUniqueId() {
        // Create columns with unique IDs
        List<Column> columns = new ArrayList<>();
        Column col1 = new Column("col1", Type.INT);
        col1.setUniqueId(100);
        Column col2 = new Column("col2", Type.STRING);
        col2.setUniqueId(101);
        columns.add(col1);
        columns.add(col2);

        // Create indexes referencing those columns
        List<Index> indexes = new ArrayList<>();
        Index index1 = new Index(1, "idx1", Lists.newArrayList("col1"),
                IndexDef.IndexType.BITMAP, null, null, null);
        Index index2 = new Index(2, "idx2", Lists.newArrayList("col2"),
                IndexDef.IndexType.INVERTED, null, null, null);
        indexes.add(index1);
        indexes.add(index2);

        // Create OlapTable with materialized index using these indexes
        OlapTable table = new OlapTable(1, "test_table", columns, KeysType.DUP_KEYS,
                new SinglePartitionInfo(), new RandomDistributionInfo());

        table.setIndexMeta(1, "base_index", columns, 1, 1, (short) 1,
                TStorageType.COLUMN, KeysType.DUP_KEYS, indexes);

        // Verify columnUniqueIds is initially empty
        Assert.assertTrue(index1.getColumnUniqueIds().isEmpty());
        Assert.assertTrue(index2.getColumnUniqueIds().isEmpty());

        // Initialize column unique IDs at table level
        table.initIndexColumnUniqueId();

        // Verify columnUniqueIds is populated correctly
        Assert.assertEquals(1, index1.getColumnUniqueIds().size());
        Assert.assertEquals(Integer.valueOf(100), index1.getColumnUniqueIds().get(0));

        Assert.assertEquals(1, index2.getColumnUniqueIds().size());
        Assert.assertEquals(Integer.valueOf(101), index2.getColumnUniqueIds().get(0));
    }
}
