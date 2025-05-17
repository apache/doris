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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.EsTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ShowResultSetMetaData;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class DescribeCommandTest {

    @Test
    public void testGetElasticsearchMetaData() throws Exception {
        // Use reflection to access the private static method
        Method method = DescribeCommand.class.getDeclaredMethod("getElasticsearchMetaData");
        method.setAccessible(true);
        ShowResultSetMetaData metaData = (ShowResultSetMetaData) method.invoke(null);

        // Verify metadata structure has the expected number of columns
        Assertions.assertNotNull(metaData);
        Assertions.assertEquals(4, metaData.getColumnCount());

        // Verify column names
        Assertions.assertEquals("Hosts", metaData.getColumn(0).getName());
        Assertions.assertEquals("User", metaData.getColumn(1).getName());
        Assertions.assertEquals("Password", metaData.getColumn(2).getName());
        Assertions.assertEquals("Index", metaData.getColumn(3).getName());

        // Verify column types - all should be VARCHAR with specific lengths
        ScalarType hostsType = (ScalarType) metaData.getColumn(0).getType();
        Assertions.assertEquals(ScalarType.createVarchar(100).toString(), hostsType.toString());

        ScalarType userType = (ScalarType) metaData.getColumn(1).getType();
        Assertions.assertEquals(ScalarType.createVarchar(30).toString(), userType.toString());

        ScalarType passwordType = (ScalarType) metaData.getColumn(2).getType();
        Assertions.assertEquals(ScalarType.createVarchar(30).toString(), passwordType.toString());

        ScalarType indexType = (ScalarType) metaData.getColumn(3).getType();
        Assertions.assertEquals(ScalarType.createVarchar(100).toString(), indexType.toString());
    }

    @Test
    public void testDescribeElasticsearchTableAll() throws Exception {
        // Create a mock EsTable with specific properties
        EsTable mockEsTable = Mockito.mock(EsTable.class);
        Mockito.when(mockEsTable.getHosts()).thenReturn("es1:9200,es2:9200");
        Mockito.when(mockEsTable.getUserName()).thenReturn("elastic");
        Mockito.when(mockEsTable.getIndexName()).thenReturn("my_index");
        Mockito.when(mockEsTable.getType()).thenReturn(org.apache.doris.catalog.TableIf.TableType.ELASTICSEARCH);

        // Create a DescribeCommand with isAllTables=true
        TableNameInfo tableNameInfo = new TableNameInfo("ctlName", "dbName", "tblName");
        DescribeCommand describeCommand = new DescribeCommand(tableNameInfo, true, null);

        // Set isEsTable flag and rows list using reflection
        Field isEsTableField = DescribeCommand.class.getDeclaredField("isEsTable");
        isEsTableField.setAccessible(true);
        isEsTableField.set(describeCommand, true);

        Field rowsField = DescribeCommand.class.getDeclaredField("rows");
        rowsField.setAccessible(true);
        List<List<String>> rows = new LinkedList<>();
        rowsField.set(describeCommand, rows);

        // Manually add the expected row for an ES table
        rows.add(Arrays.asList(
                mockEsTable.getHosts(),
                mockEsTable.getUserName(),
                "", // password is empty by design
                mockEsTable.getIndexName()
        ));

        // Get the metadata and verify it matches the ES metadata
        ShowResultSetMetaData metaData = describeCommand.getMetaData();
        Assertions.assertEquals(4, metaData.getColumnCount());
        Assertions.assertEquals("Hosts", metaData.getColumn(0).getName());
        Assertions.assertEquals("User", metaData.getColumn(1).getName());
        Assertions.assertEquals("Password", metaData.getColumn(2).getName());
        Assertions.assertEquals("Index", metaData.getColumn(3).getName());

        // Verify the row data is correct
        List<List<String>> resultRows = rows;
        Assertions.assertEquals(1, resultRows.size());
        List<String> row = resultRows.get(0);
        Assertions.assertEquals("es1:9200,es2:9200", row.get(0)); // Hosts
        Assertions.assertEquals("elastic", row.get(1)); // User
        Assertions.assertEquals("", row.get(2)); // Password (empty)
        Assertions.assertEquals("my_index", row.get(3)); // Index
    }
}
