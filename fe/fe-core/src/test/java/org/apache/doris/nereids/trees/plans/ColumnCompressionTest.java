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

package org.apache.doris.nereids.trees.plans;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.exceptions.ParseException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TCompressionType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class ColumnCompressionTest {
    @Test
    public void testCompressionSyntax() {
        ConnectContext connectContext = new ConnectContext();
        connectContext.setDatabase("test");
        connectContext.setThreadLocalInfo();
        try {
            NereidsParser parser = new NereidsParser();
            Assertions.assertDoesNotThrow(() -> parser.parseSingle("CREATE TABLE test_compression ("
                    + "k INT, v VARCHAR(10) COMPRESSION ZSTD(9) COMMENT 'value') "
                    + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                    + "PROPERTIES ('replication_num' = '1')"));
            Assertions.assertThrows(ParseException.class, () -> parser.parseSingle("CREATE TABLE test_compression ("
                    + "k INT, v VARCHAR(10) COMPRESSION 'zstd:9') "
                    + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                    + "PROPERTIES ('replication_num' = '1')"));
            Assertions.assertThrows(ParseException.class, () -> parser.parseSingle("CREATE TABLE test_compression ("
                    + "k INT, v VARCHAR(10) COMMENT 'value' COMPRESSION ZSTD(9)) "
                    + "DUPLICATE KEY(k) DISTRIBUTED BY HASH(k) BUCKETS 1 "
                    + "PROPERTIES ('replication_num' = '1')"));
        } finally {
            ConnectContext.remove();
        }
    }

    @Test
    public void testLevelOnLz4Rejected() {
        Assertions.assertThrows(AnalysisException.class,
                () -> newColumn().setCompression(TCompressionType.LZ4, 5));
    }

    @Test
    public void testZstdLevelOutOfRange() {
        Assertions.assertThrows(AnalysisException.class,
                () -> newColumn().setCompression(TCompressionType.ZSTD, 99));
    }

    @Test
    public void testLz4hcLevelRange() throws Exception {
        ColumnDefinition column = newColumn();
        column.setCompression(TCompressionType.LZ4HC, 12);
        Assertions.assertEquals(TCompressionType.LZ4HC, column.getCompressionType());
        Assertions.assertEquals(12, column.getCompressionLevel());
        Assertions.assertThrows(AnalysisException.class,
                () -> column.setCompression(TCompressionType.LZ4HC, 13));
    }

    @Test
    public void testCompressionRejectedOnNonOlap() throws Exception {
        ColumnDefinition col = new ColumnDefinition("col1", IntegerType.INSTANCE, false, AggregateType.NONE,
                true, Optional.empty(), "");
        col.setCompression(TCompressionType.ZSTD, 9);
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> col.validate(false, Sets.newHashSet(), Sets.newHashSet(), false, KeysType.DUP_KEYS));
    }

    @Test
    public void testCompressionAllowedOnOlap() throws Exception {
        ColumnDefinition col = new ColumnDefinition("col1", IntegerType.INSTANCE, false, AggregateType.NONE,
                true, Optional.empty(), "");
        col.setCompression(TCompressionType.ZSTD, 9);
        Assertions.assertDoesNotThrow(
                () -> col.validate(true, Sets.newHashSet(), Sets.newHashSet(), false, KeysType.DUP_KEYS));
    }

    @Test
    public void testCompressionRejectedOnComplexType() throws Exception {
        ColumnDefinition col = new ColumnDefinition("col1", ArrayType.of(IntegerType.INSTANCE), false,
                AggregateType.NONE, true, Optional.empty(), "");
        col.setCompression(TCompressionType.ZSTD, 9);
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> col.validate(true, Sets.newHashSet(), Sets.newHashSet(), false, KeysType.DUP_KEYS));
    }

    @Test
    public void testCompressionRejectedOnAggState() throws Exception {
        // AGG_STATE serializes to a function-dependent physical layout (ARRAY/MAP writers for
        // array_agg/map_agg) whose child metas never receive the override, so COMPRESSION must be
        // rejected for all AGG_STATE columns.
        AggStateType aggState = new AggStateType("array_agg",
                ImmutableList.of(IntegerType.INSTANCE), ImmutableList.of(true), true);
        ColumnDefinition col = new ColumnDefinition("col1", aggState, false,
                AggregateType.GENERIC, true, Optional.empty(), "");
        col.setCompression(TCompressionType.ZSTD, 9);
        Assertions.assertThrows(org.apache.doris.nereids.exceptions.AnalysisException.class,
                () -> col.validate(true, Sets.newHashSet(), Sets.newHashSet(), false, KeysType.AGG_KEYS));
    }

    @Test
    public void testToSqlRendersCompressionClause() throws Exception {
        ColumnDefinition withLevel = new ColumnDefinition("c1", IntegerType.INSTANCE, false,
                AggregateType.NONE, true, Optional.empty(), "");
        withLevel.setCompression(TCompressionType.ZSTD, 9);
        Assertions.assertTrue(withLevel.toSql("`c1`").contains("COMPRESSION ZSTD(9)"),
                "toSql should render COMPRESSION with level, got: " + withLevel.toSql("`c1`"));

        ColumnDefinition noLevel = new ColumnDefinition("c2", IntegerType.INSTANCE, false,
                AggregateType.NONE, true, Optional.empty(), "");
        noLevel.setCompression(TCompressionType.ZSTD, -1);
        String sql = noLevel.toSql("`c2`");
        Assertions.assertTrue(sql.contains("COMPRESSION ZSTD") && !sql.contains("ZSTD("),
                "toSql should render COMPRESSION without level, got: " + noLevel.toSql("`c2`"));
    }

    private ColumnDefinition newColumn() {
        return new ColumnDefinition("col1", IntegerType.INSTANCE, false, AggregateType.NONE,
                true, Optional.empty(), "");
    }
}
