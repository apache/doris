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

package org.apache.doris.datasource.jdbc.client;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.jdbc.util.JdbcFieldSchema;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.ResultSet;
import java.sql.Types;

public class JdbcUuidMappingTest {
    private JdbcFieldSchema field(String type, int jdbcType, int dimensions) throws Exception {
        ResultSet metadata = Mockito.mock(ResultSet.class);
        Mockito.when(metadata.getString("TYPE_NAME")).thenReturn(type);
        Mockito.when(metadata.getInt("DATA_TYPE")).thenReturn(jdbcType);
        return new JdbcFieldSchema(metadata, dimensions);
    }

    @Test
    public void nativeUuidAndNestedArrays() throws Exception {
        JdbcPostgreSQLClient postgres = Mockito.mock(JdbcPostgreSQLClient.class, Mockito.CALLS_REAL_METHODS);
        JdbcSQLServerClient sqlServer = Mockito.mock(JdbcSQLServerClient.class, Mockito.CALLS_REAL_METHODS);
        JdbcTrinoClient trino = Mockito.mock(JdbcTrinoClient.class, Mockito.CALLS_REAL_METHODS);
        Assert.assertEquals(Type.UUID, postgres.jdbcTypeToDoris(field("uuid", Types.OTHER, 0)));
        Assert.assertEquals(Type.UUID, sqlServer.jdbcTypeToDoris(field("uniqueidentifier", Types.CHAR, 0)));
        Assert.assertEquals(Type.UUID, trino.jdbcTypeToDoris(field("uuid", Types.OTHER, 0)));
        Type nested = ArrayType.create(ArrayType.create(Type.UUID, true), true);
        Assert.assertEquals(nested, postgres.jdbcTypeToDoris(field("_uuid", Types.ARRAY, 2)));
        Assert.assertEquals(nested, trino.jdbcTypeToDoris(field("array(array(uuid))", Types.ARRAY, 2)));
    }

    @Test
    public void logicalUuidIsPreservedInHudiAndTrinoConnector() throws Exception {
        org.apache.avro.Schema uuid = org.apache.avro.LogicalTypes.uuid()
                .addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.STRING));
        Assert.assertEquals(Type.UUID,
                org.apache.doris.datasource.hudi.HudiUtils.fromAvroHudiTypeToDorisType(uuid));
        Assert.assertEquals(ArrayType.create(Type.UUID, true),
                org.apache.doris.datasource.hudi.HudiUtils.fromAvroHudiTypeToDorisType(
                        org.apache.avro.Schema.createArray(uuid)));
        Class<?> tableClass = org.apache.doris.datasource.trinoconnector.TrinoConnectorExternalTable.class;
        java.lang.reflect.Method convert = tableClass.getDeclaredMethod(
                "trinoConnectorTypeToDorisType", io.trino.spi.type.Type.class);
        convert.setAccessible(true);
        Object table = Mockito.mock(tableClass, Mockito.CALLS_REAL_METHODS);
        Assert.assertEquals(Type.UUID, convert.invoke(table, io.trino.spi.type.UuidType.UUID));
        Assert.assertEquals(ArrayType.create(Type.UUID, true),
                convert.invoke(table, new io.trino.spi.type.ArrayType(io.trino.spi.type.UuidType.UUID)));
    }

    @Test
    public void uuidInsertParametersHaveRemoteType() {
        for (String dialect : new String[] {"trino", "presto", "postgresql", "sqlserver"}) {
            org.apache.doris.catalog.JdbcTable table = new org.apache.doris.catalog.JdbcTable(1, "events",
                    java.util.Arrays.asList(new org.apache.doris.catalog.Column("id", Type.INT),
                            new org.apache.doris.catalog.Column("u", Type.UUID)),
                    org.apache.doris.catalog.Table.TableType.JDBC);
            table.setJdbcTypeName(dialect);
            table.setExternalTableName("events");
            String sql = table.getInsertSql(java.util.Arrays.asList("id", "u"));
            Assert.assertTrue(sql, sql.endsWith(dialect.equals("trino") || dialect.equals("presto")
                    ? "VALUES (?, CAST(? AS UUID))" : "VALUES (?, ?)"));
        }
    }

}
