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

import org.apache.doris.catalog.Column;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;

import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.MockUp;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HMSExternalTableTest {
    @Test
    public void testGetNullParamters(@Injectable Table remoteTable, @Injectable HMSExternalCatalog catalog,
                                     @Injectable HMSExternalDatabase db) {
        new Expectations() {
            {
                remoteTable.getParameters();
                result = null;
            }
        };

        new MockUp<HMSExternalTable>() {
            protected synchronized void makeSureInitialized() {
            }
        };

        HMSExternalTable hmsExternalTable = new HMSExternalTable(1, "test", "test",
                catalog, db);
        hmsExternalTable.setRemoteTable(remoteTable);
        Expression policy = hmsExternalTable.getRowPolicy();
        Assertions.assertNull(policy);
    }

    @Test
    public void testGetEmptyParamters(@Injectable Table remoteTable, @Injectable HMSExternalCatalog catalog,
                                      @Injectable HMSExternalDatabase db) {
        Map<String, String> map = Maps.newHashMap();
        map.put("row_policy", "");
        new Expectations() {
            {
                remoteTable.getParameters();
                result = map;
            }
        };

        new MockUp<HMSExternalTable>() {
            protected synchronized void makeSureInitialized() {
            }
        };

        HMSExternalTable hmsExternalTable = new HMSExternalTable(1, "test", "test",
                catalog, db);
        hmsExternalTable.setRemoteTable(remoteTable);
        Expression policy = hmsExternalTable.getRowPolicy();
        Assertions.assertNull(policy);
    }

    @Test
    public void testGetNullRowPolicy(@Injectable Table remoteTable, @Injectable HMSExternalCatalog catalog,
                                     @Injectable HMSExternalDatabase db) {
        new Expectations() {
            {
                remoteTable.getParameters();
                result = Maps.newHashMap();
            }
        };

        new MockUp<HMSExternalTable>() {
            protected void makeSureInitialized() {
            }
        };

        HMSExternalTable hmsExternalTable = new HMSExternalTable(1, "test", "test",
                catalog, db);
        hmsExternalTable.setRemoteTable(remoteTable);
        Expression policy = hmsExternalTable.getRowPolicy();
        Assertions.assertNull(policy);
    }


    @Test
    public void testGetNormalRowPolicy(@Injectable Table remoteTable, @Injectable HMSExternalCatalog catalog,
                                       @Injectable HMSExternalDatabase db) {
        Map<String, String> map = Maps.newHashMap();
        map.put("row_policy", "(id < 1)");
        new Expectations() {
            {
                remoteTable.getParameters();
                result = map;
            }
        };

        new MockUp<HMSExternalTable>() {
            protected void makeSureInitialized() {
            }
        };

        HMSExternalTable hmsExternalTable = new HMSExternalTable(1, "test", "test",
                catalog, db);
        hmsExternalTable.setRemoteTable(remoteTable);
        Expression policy = hmsExternalTable.getRowPolicy();
        Assertions.assertTrue(policy instanceof LessThan);
        Assertions.assertEquals("(id < 1)", policy.toSql());
    }

    @Test
    public void testGetTableSchemas(@Injectable Table remoteTable, @Injectable HMSExternalCatalog catalog,
                                    @Injectable HMSExternalDatabase db) {
        List<FieldSchema> list = new ArrayList<>();
        FieldSchema fieldSchema1 = new FieldSchema();
        fieldSchema1.setName("name1");
        fieldSchema1.setComment("comment");
        fieldSchema1.setType("int");
        list.add(fieldSchema1);
        FieldSchema fieldSchema2 = new FieldSchema();
        fieldSchema2.setName("name2");
        fieldSchema2.setComment("name2|无权限查看当前列");
        fieldSchema2.setType("int");
        list.add(fieldSchema2);
        new Expectations() {
            {
                remoteTable.getSd().getCols();
                result = list;
            }
        };
        new MockUp<HMSExternalTable>() {
            protected synchronized void makeSureInitialized() {
            }
        };
        HMSExternalTable hmsExternalTable = new HMSExternalTable(1, "test", "test",
                catalog, db);
        hmsExternalTable.setRemoteTable(remoteTable);
        List<Column> baseSchema = hmsExternalTable.getBaseSchema();
        List<Column> fullSchema = hmsExternalTable.getFullSchemaWithPermission();
        Assertions.assertEquals(2, baseSchema.size());
        Assertions.assertEquals(1, fullSchema.size());
    }

    @Test
    public void testGetHiveSchemaWithVoidColumns(@Injectable Table remoteTable, @Injectable HMSExternalCatalog catalog,
                                                 @Injectable HMSExternalDatabase db) {
        List<FieldSchema> list = new ArrayList<>();

        // Add a normal column
        FieldSchema normalField = new FieldSchema();
        normalField.setName("normal_column");
        normalField.setComment("normal column comment");
        normalField.setType("string");
        list.add(normalField);

        // Add a void column (should be skipped)
        FieldSchema voidField = new FieldSchema();
        voidField.setName("void_column");
        voidField.setComment("void column comment");
        voidField.setType("void");
        list.add(voidField);

        // Add another void column with different case (should be skipped)
        FieldSchema voidFieldUpperCase = new FieldSchema();
        voidFieldUpperCase.setName("void_column_upper");
        voidFieldUpperCase.setComment("void column upper case comment");
        voidFieldUpperCase.setType("VOID");
        list.add(voidFieldUpperCase);

        // Add another normal column
        FieldSchema anotherNormalField = new FieldSchema();
        anotherNormalField.setName("another_normal_column");
        anotherNormalField.setComment("another normal column comment");
        anotherNormalField.setType("int");
        list.add(anotherNormalField);

        new Expectations() {
            {
                remoteTable.getSd().getCols();
                result = list;
            }
        };

        new MockUp<HMSExternalTable>() {
            protected synchronized void makeSureInitialized() {
            }
        };

        HMSExternalTable hmsExternalTable = new HMSExternalTable(1, "test", "test",
                catalog, db);
        hmsExternalTable.setRemoteTable(remoteTable);

        List<Column> baseSchema = hmsExternalTable.getBaseSchema();

        // Should only have 2 columns (void columns should be filtered out)
        Assertions.assertEquals(2, baseSchema.size());

        // Verify the remaining columns are the non-void ones
        Assertions.assertEquals("normal_column", baseSchema.get(0).getName());
        Assertions.assertEquals("another_normal_column", baseSchema.get(1).getName());

        // Verify that void columns are not present
        for (Column column : baseSchema) {
            Assertions.assertNotEquals("void_column", column.getName());
            Assertions.assertNotEquals("void_column_upper", column.getName());
        }
    }

    @Test
    public void testGetHiveSchemaWithOnlyVoidColumns(@Injectable Table remoteTable,
                                                     @Injectable HMSExternalCatalog catalog,
                                                     @Injectable HMSExternalDatabase db) {
        List<FieldSchema> list = new ArrayList<>();

        // Add only void columns
        FieldSchema voidField1 = new FieldSchema();
        voidField1.setName("void_column1");
        voidField1.setComment("void column 1 comment");
        voidField1.setType("void");
        list.add(voidField1);

        FieldSchema voidField2 = new FieldSchema();
        voidField2.setName("void_column2");
        voidField2.setComment("void column 2 comment");
        voidField2.setType("VOID");
        list.add(voidField2);

        new Expectations() {
            {
                remoteTable.getSd().getCols();
                result = list;
            }
        };

        new MockUp<HMSExternalTable>() {
            protected synchronized void makeSureInitialized() {
            }
        };

        HMSExternalTable hmsExternalTable = new HMSExternalTable(1, "test", "test",
                catalog, db);
        hmsExternalTable.setRemoteTable(remoteTable);

        List<Column> baseSchema = hmsExternalTable.getBaseSchema();

        // Should have 0 columns (all void columns should be filtered out)
        Assertions.assertEquals(0, baseSchema.size());
    }
}
