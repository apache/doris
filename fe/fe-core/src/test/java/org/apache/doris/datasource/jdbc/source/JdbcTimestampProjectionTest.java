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


package org.apache.doris.datasource.jdbc.source;

import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.JdbcTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.thrift.TOdbcTableType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class JdbcTimestampProjectionTest {
    @Test
    void testClickHouseNestedInstantsAreProjectedAsEpochMicros() throws Exception {
        JdbcScanNode node = Mockito.mock(JdbcScanNode.class, Mockito.CALLS_REAL_METHODS);
        TupleDescriptor descriptor = Mockito.mock(TupleDescriptor.class);
        SlotDescriptor slot = Mockito.mock(SlotDescriptor.class);
        JdbcTable table = Mockito.mock(JdbcTable.class);
        Mockito.when(descriptor.getSlots()).thenReturn(new ArrayList<>(Collections.singletonList(slot)));
        Mockito.when(slot.getColumn()).thenReturn(new Column("events",
                new ArrayType(new ArrayType(ScalarType.createTimeStampTzType(6)))));
        Mockito.when(table.getProperRemoteColumnName(TOdbcTableType.CLICKHOUSE, "events"))
                .thenReturn("`events`");
        node.setDesc(descriptor);
        List<String> columns = new ArrayList<>();
        setField(node, "columns", columns);
        setField(node, "tbl", table);
        setField(node, "jdbcType", TOdbcTableType.CLICKHOUSE);
        Method create = JdbcScanNode.class.getDeclaredMethod("createJdbcColumns");
        create.setAccessible(true);
        create.invoke(node);
        Assertions.assertEquals(Collections.singletonList(
                "arrayMap(t0 -> arrayMap(t1 -> toUnixTimestamp64Micro(toDateTime64(t1, 6)), t0), `events`) AS `events`"),
                columns);
        setField(node, "query", "SELECT events FROM event_stream;");
        Method tvfQuery = JdbcScanNode.class.getDeclaredMethod("getTvfQuery");
        tvfQuery.setAccessible(true);
        Assertions.assertEquals("SELECT " + columns.get(0)
                + " FROM (SELECT events FROM event_stream) doris_jdbc_source", tvfQuery.invoke(node));
    }

    private static void setField(JdbcScanNode node, String name, Object value) throws Exception {
        Field field = JdbcScanNode.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(node, value);
    }
}
