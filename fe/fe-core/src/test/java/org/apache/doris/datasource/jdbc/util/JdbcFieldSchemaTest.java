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

package org.apache.doris.datasource.jdbc.util;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.ResultSetMetaData;
import java.sql.Types;

public class JdbcFieldSchemaTest {

    private ResultSetMetaData metaData = Mockito.mock(ResultSetMetaData.class);

    @Test
    public void testUseColumnLabelForQueryAlias() throws Exception {
        Mockito.when(metaData.getColumnLabel(1)).thenReturn("t1");
        Mockito.when(metaData.getColumnType(1)).thenReturn(Types.VARCHAR);
        Mockito.when(metaData.getColumnTypeName(1)).thenReturn("VARCHAR");
        Mockito.when(metaData.getPrecision(1)).thenReturn(64);
        Mockito.when(metaData.getScale(1)).thenReturn(0);

        JdbcFieldSchema schema = new JdbcFieldSchema(metaData, 1);

        Assert.assertEquals("t1", schema.getColumnName());
    }

    @Test
    public void testFallbackToColumnNameWhenLabelMissing() throws Exception {
        Mockito.when(metaData.getColumnLabel(1)).thenReturn("");
        Mockito.when(metaData.getColumnName(1)).thenReturn("username");
        Mockito.when(metaData.getColumnType(1)).thenReturn(Types.VARCHAR);
        Mockito.when(metaData.getColumnTypeName(1)).thenReturn("VARCHAR");
        Mockito.when(metaData.getPrecision(1)).thenReturn(64);
        Mockito.when(metaData.getScale(1)).thenReturn(0);

        JdbcFieldSchema schema = new JdbcFieldSchema(metaData, 1);

        Assert.assertEquals("username", schema.getColumnName());
    }
}
