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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.common.DataSourceType;
import org.apache.doris.job.util.StreamingJobUtils;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CdcStreamTableValuedFunctionTest {

    @Test
    public void testDeleteSignIsExcludedByDefault() throws Exception {
        List<Column> columns = getTableColumns(baseProperties());

        Assert.assertEquals(1, columns.size());
        Assert.assertEquals("id", columns.get(0).getName());
    }

    @Test
    public void testDeleteSignIsIncludedWhenEnabled() throws Exception {
        Map<String, String> properties = baseProperties();
        properties.put(CdcStreamTableValuedFunction.INCLUDE_DELETE_SIGN, "true");

        List<Column> columns = getTableColumns(properties);

        Assert.assertEquals(2, columns.size());
        Column deleteSign = columns.get(1);
        Assert.assertEquals(Column.DELETE_SIGN, deleteSign.getName());
        Assert.assertEquals(PrimitiveType.TINYINT, deleteSign.getType().getPrimitiveType());
        Assert.assertFalse(deleteSign.isAllowNull());
    }

    @Test
    public void testInvalidIncludeDeleteSignIsRejected() {
        Map<String, String> properties = baseProperties();
        properties.put(CdcStreamTableValuedFunction.INCLUDE_DELETE_SIGN, "invalid");

        AnalysisException exception = Assert.assertThrows(AnalysisException.class,
                () -> new CdcStreamTableValuedFunction(properties));

        Assert.assertTrue(exception.getMessage().contains("include_delete_sign"));
    }

    private List<Column> getTableColumns(Map<String, String> properties) throws Exception {
        JdbcClient jdbcClient = Mockito.mock(JdbcClient.class);
        List<Column> sourceColumns = new ArrayList<>();
        sourceColumns.add(new Column("id", PrimitiveType.INT));
        Mockito.when(jdbcClient.isTableExist("test_db", "test_table")).thenReturn(true);
        Mockito.when(jdbcClient.getColumnsFromJdbc("test_db", "test_table")).thenReturn(sourceColumns);

        try (MockedStatic<StreamingJobUtils> utils = Mockito.mockStatic(StreamingJobUtils.class)) {
            utils.when(() -> StreamingJobUtils.getJdbcClient(
                            Mockito.eq(DataSourceType.MYSQL), Mockito.anyMap()))
                    .thenReturn(jdbcClient);
            utils.when(() -> StreamingJobUtils.getRemoteDbName(
                            Mockito.eq(DataSourceType.MYSQL), Mockito.anyMap()))
                    .thenReturn("test_db");
            CdcStreamTableValuedFunction function = new CdcStreamTableValuedFunction(properties);
            return function.getTableColumns();
        }
    }

    private Map<String, String> baseProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put(DataSourceConfigKeys.TYPE, "mysql");
        properties.put(DataSourceConfigKeys.JDBC_URL, "jdbc:mysql://localhost:3306/test_db");
        properties.put(DataSourceConfigKeys.DATABASE, "test_db");
        properties.put(DataSourceConfigKeys.TABLE, "test_table");
        properties.put(DataSourceConfigKeys.OFFSET, "initial");
        return properties;
    }
}
