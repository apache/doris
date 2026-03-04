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

package org.apache.doris.job.util;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.datasource.jdbc.client.JdbcClient;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class StreamingJobUtilsTest {

    @Mock
    private JdbcClient jdbcClient;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetColumnsWithPrimaryKeySorting() throws Exception {
        // Prepare test data
        String database = "test_db";
        String table = "test_table";
        List<String> primaryKeys = Arrays.asList("id", "name");

        // Create mock columns in random order
        List<Column> mockColumns = new ArrayList<>();
        mockColumns.add(new Column("age", ScalarType.createType(PrimitiveType.INT)));
        mockColumns.add(new Column("id", ScalarType.createType(PrimitiveType.BIGINT)));
        mockColumns.add(new Column("email", ScalarType.createVarcharType(100)));
        mockColumns.add(new Column("name", ScalarType.createVarcharType(50)));
        mockColumns.add(new Column("address", ScalarType.createVarcharType(200)));

        Mockito.when(jdbcClient.getColumnsFromJdbc(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(mockColumns);
        List<Column> result = StreamingJobUtils.getColumns(jdbcClient, database, table, primaryKeys);

        // Verify primary keys are at the front in correct order
        Assert.assertEquals(5, result.size());
        Assert.assertEquals("id", result.get(0).getName());
        Assert.assertEquals("name", result.get(1).getName());
        // Verify varchar primary key columns have their length multiplied by 3
        Column nameColumn = result.get(1);
        Assert.assertEquals(150, nameColumn.getType().getLength()); // 50 * 3
        // Verify non-primary key columns follow
        Assert.assertEquals("age", result.get(2).getName());
        Assert.assertEquals("email", result.get(3).getName());
        Assert.assertEquals("address", result.get(4).getName());
        // Verify non-primary key varchar columns also have their length multiplied by 3
        Column emailColumn = result.get(3);
        Assert.assertEquals(300, emailColumn.getType().getLength()); // 100 * 3
        Column addressColumn = result.get(4);
        Assert.assertEquals(600, addressColumn.getType().getLength()); // 200 * 3
    }

    @Test
    public void testGetColumnsWithVarcharTypeConversion() throws Exception {
        String database = "test_db";
        String table = "test_table";
        List<String> primaryKeys = Arrays.asList("id");

        List<Column> mockColumns = new ArrayList<>();
        mockColumns.add(new Column("id", ScalarType.createType(PrimitiveType.INT)));
        mockColumns.add(new Column("short_name", ScalarType.createVarcharType(50)));
        mockColumns.add(new Column("long_name", ScalarType.createVarcharType(20000)));

        Mockito.when(jdbcClient.getColumnsFromJdbc(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(mockColumns);
        List<Column> result = StreamingJobUtils.getColumns(jdbcClient, database, table, primaryKeys);

        // Verify varchar length multiplication by 3
        Column shortName = result.stream()
                .filter(col -> col.getName().equals("short_name"))
                .findFirst()
                .orElse(null);
        Assert.assertNotNull(shortName);
        Assert.assertEquals(150, shortName.getType().getLength()); // 50 * 3

        // Verify long varchar becomes STRING type
        Column longName = result.stream()
                .filter(col -> col.getName().equals("long_name"))
                .findFirst()
                .orElse(null);
        Assert.assertNotNull(longName);
        Assert.assertTrue(longName.getType().isStringType());
    }

    @Test
    public void testGetColumnsWithStringTypeAsPrimaryKey() throws Exception {
        String database = "test_db";
        String table = "test_table";
        List<String> primaryKeys = Arrays.asList("id");

        List<Column> mockColumns = new ArrayList<>();
        mockColumns.add(new Column("id", ScalarType.createStringType()));
        mockColumns.add(new Column("name", ScalarType.createVarcharType(50)));

        Mockito.when(jdbcClient.getColumnsFromJdbc(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(mockColumns);
        List<Column> result = StreamingJobUtils.getColumns(jdbcClient, database, table, primaryKeys);

        // Verify string type primary key is converted to varchar
        Column idColumn = result.stream()
                .filter(col -> col.getName().equals("id"))
                .findFirst()
                .orElse(null);
        Assert.assertNotNull(idColumn);
        Assert.assertTrue(idColumn.getType().isVarchar());
        Assert.assertEquals(ScalarType.MAX_VARCHAR_LENGTH, idColumn.getType().getLength());
    }

    @Test
    public void testGetColumnsWithEmptyPrimaryKeys() throws Exception {
        String database = "test_db";
        String table = "test_table";
        List<String> primaryKeys = new ArrayList<>();

        List<Column> mockColumns = new ArrayList<>();
        mockColumns.add(new Column("col1", ScalarType.createType(PrimitiveType.INT)));
        mockColumns.add(new Column("col2", ScalarType.createVarcharType(100)));
        mockColumns.add(new Column("col3", ScalarType.createType(PrimitiveType.BIGINT)));

        Mockito.when(jdbcClient.getColumnsFromJdbc(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(mockColumns);
        List<Column> result = StreamingJobUtils.getColumns(jdbcClient, database, table, primaryKeys);

        // Verify columns maintain original order when no primary keys
        Assert.assertEquals(3, result.size());
        Assert.assertEquals("col1", result.get(0).getName());
        Assert.assertEquals("col2", result.get(1).getName());
        Assert.assertEquals("col3", result.get(2).getName());
    }

    @Test
    public void testGetColumnsWithMultiplePrimaryKeys() throws Exception {
        String database = "test_db";
        String table = "test_table";
        List<String> primaryKeys = Arrays.asList("pk3", "pk1", "pk2");

        List<Column> mockColumns = new ArrayList<>();
        mockColumns.add(new Column("data1", ScalarType.createType(PrimitiveType.INT)));
        mockColumns.add(new Column("pk1", ScalarType.createType(PrimitiveType.INT)));
        mockColumns.add(new Column("data2", ScalarType.createVarcharType(100)));
        mockColumns.add(new Column("pk2", ScalarType.createType(PrimitiveType.BIGINT)));
        mockColumns.add(new Column("pk3", ScalarType.createType(PrimitiveType.INT)));
        mockColumns.add(new Column("data3", ScalarType.createVarcharType(50)));

        Mockito.when(jdbcClient.getColumnsFromJdbc(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(mockColumns);
        List<Column> result = StreamingJobUtils.getColumns(jdbcClient, database, table, primaryKeys);

        // Verify primary keys are sorted in the order defined in primaryKeys list
        Assert.assertEquals(6, result.size());
        Assert.assertEquals("pk3", result.get(0).getName());
        Assert.assertEquals("pk1", result.get(1).getName());
        Assert.assertEquals("pk2", result.get(2).getName());
        // Verify non-primary keys follow
        Assert.assertEquals("data1", result.get(3).getName());
        Assert.assertEquals("data2", result.get(4).getName());
        Assert.assertEquals("data3", result.get(5).getName());
    }

    @Test
    public void testGetColumnsWithUnsupportedColumnType() throws Exception {
        String database = "test_db";
        String table = "test_table";
        List<String> primaryKeys = Arrays.asList("id");

        List<Column> mockColumns = new ArrayList<>();
        mockColumns.add(new Column("id", ScalarType.createType(PrimitiveType.INT)));
        mockColumns.add(new Column("unsupported_col", new ScalarType(PrimitiveType.UNSUPPORTED)));

        Mockito.when(jdbcClient.getColumnsFromJdbc(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(mockColumns);
        // This should throw IllegalArgumentException due to unsupported column type
        try {
            StreamingJobUtils.getColumns(jdbcClient, database, table, primaryKeys);
            Assert.fail("Expected IllegalArgumentException to be thrown");
        } catch (IllegalArgumentException e) {
            // Verify the exception message contains expected information
            String message = e.getMessage();
            Assert.assertTrue(message.contains("Unsupported column type"));
            Assert.assertTrue(message.contains("test_table"));
            Assert.assertTrue(message.contains("unsupported_col"));
        }
    }

    @Test
    public void testGetColumnsWithVarcharPrimaryKeyLengthMultiplication() throws Exception {
        String database = "test_db";
        String table = "test_table";
        List<String> primaryKeys = Arrays.asList("pk_varchar", "pk_int");

        List<Column> mockColumns = new ArrayList<>();
        mockColumns.add(new Column("pk_int", ScalarType.createType(PrimitiveType.INT)));
        mockColumns.add(new Column("pk_varchar", ScalarType.createVarcharType(100)));
        mockColumns.add(new Column("normal_varchar", ScalarType.createVarcharType(50)));

        Mockito.when(jdbcClient.getColumnsFromJdbc(ArgumentMatchers.anyString(), ArgumentMatchers.anyString())).thenReturn(mockColumns);
        List<Column> result = StreamingJobUtils.getColumns(jdbcClient, database, table, primaryKeys);

        // Verify varchar primary key column has length multiplied by 3
        Column pkVarcharColumn = result.stream()
                .filter(col -> col.getName().equals("pk_varchar"))
                .findFirst()
                .orElse(null);
        Assert.assertNotNull(pkVarcharColumn);
        Assert.assertEquals(300, pkVarcharColumn.getType().getLength()); // 100 * 3

        // Verify normal varchar column also has length multiplied by 3
        Column normalVarcharColumn = result.stream()
                .filter(col -> col.getName().equals("normal_varchar"))
                .findFirst()
                .orElse(null);
        Assert.assertNotNull(normalVarcharColumn);
        Assert.assertEquals(150, normalVarcharColumn.getType().getLength()); // 50 * 3
    }
}
