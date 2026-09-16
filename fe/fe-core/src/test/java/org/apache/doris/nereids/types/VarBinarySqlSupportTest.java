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

package org.apache.doris.nereids.types;

import org.apache.doris.analysis.ColumnDef;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.proto.OlapFile;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

public class VarBinarySqlSupportTest extends TestWithFeService {
    @Test
    public void testCloudBinaryColumnMetadata() throws Exception {
        Column column = new Column("payload", ScalarType.createVarbinaryType(Integer.MAX_VALUE));
        column.setIsKey(true);
        OlapFile.ColumnPB proto = column.toPb(Collections.emptySet(), Collections.emptyList());
        Assertions.assertEquals("VARBINARY", proto.getType());
        Assertions.assertEquals(Integer.MAX_VALUE, proto.getLength());
        Assertions.assertEquals(PrimitiveType.VARBINARY.getOlapColumnIndexSize(), proto.getIndexLength());
        Column bounded = new Column("bounded", ScalarType.createVarbinaryType(2));
        Assertions.assertEquals(6, bounded.toPb(Collections.emptySet(), Collections.emptyList()).getLength());
        Column nested = new Column("nested", new org.apache.doris.catalog.ArrayType(column.getType(), true));
        Assertions.assertEquals(Integer.MAX_VALUE, nested.toPb(Collections.emptySet(), Collections.emptyList())
                .getChildrenColumns(0).getLength());
    }

    @Test
    public void testDefaultValueUsesByteLength() throws Exception {
        ColumnDef.validateDefaultValue(ScalarType.createVarbinaryType(2), "é", null);
        Assertions.assertThrows(AnalysisException.class,
                () -> ColumnDef.validateDefaultValue(ScalarType.createVarbinaryType(1), "é", null));
        ColumnDef.validateDefaultValue(ScalarType.createVarbinaryType(0), "", null);
    }

    @Test
    public void testDistributionLiteralHashesRawBytes() throws Exception {
        byte[] raw = {0, (byte) 0x80, (byte) 0xff};
        ByteBuffer hashValue = new org.apache.doris.analysis.VarBinaryLiteral(raw)
                .getHashValue(PrimitiveType.VARBINARY);
        byte[] actual = new byte[hashValue.remaining()];
        hashValue.get(actual);
        Assertions.assertArrayEquals(raw, actual);
    }

    @Test
    public void testDeclaredBinaryLength() {
        Assertions.assertEquals(VarBinaryType.createVarBinaryType(2), DataType.convertFromString("varbinary(2)"));
        Assertions.assertEquals(ArrayType.of(VarBinaryType.createVarBinaryType(2)),
                DataType.convertFromString("array<varbinary(2)>"));
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabaseAndUse("binary_sql");
        createTable("create table source_bytes (id int, encoded string) duplicate key(id) "
                + "distributed by hash(id) buckets 1 properties ('replication_num'='1')");
    }

    @Test
    public void testLiteralOrderingKeepsTrailingZeros() throws Exception {
        // Binary equality must distinguish a prefix from the same bytes followed by zero.
        VarBinaryLiteral prefix = new VarBinaryLiteral("AB");
        VarBinaryLiteral extended = new VarBinaryLiteral("AB0001");
        Assertions.assertTrue(prefix.compareTo(extended) < 0);
        Assertions.assertTrue(extended.compareTo(prefix) > 0);
        Assertions.assertTrue(new VarBinaryLiteral("").compareTo(new VarBinaryLiteral("00")) < 0);
        Assertions.assertTrue(new VarBinaryLiteral("7F").compareTo(new VarBinaryLiteral("80")) < 0);
        Assertions.assertTrue(prefix.toLegacyLiteral().compareLiteral(extended.toLegacyLiteral()) < 0);
    }

    @Test
    public void testEqualLiteralsHaveEqualHashes() {
        VarBinaryLiteral first = new VarBinaryLiteral("0080FF0001");
        VarBinaryLiteral second = new VarBinaryLiteral("0080FF0001");
        Assertions.assertEquals(first, second);
        Assertions.assertEquals(first.hashCode(), second.hashCode());
    }

    @Test
    public void testComparison() {
        PlanChecker.from(connectContext).analyze(
                "select id from source_bytes where cast(encoded as varbinary) = X'0080FF'").rewrite();
        PlanChecker.from(connectContext).analyze(
                "select id from source_bytes where cast(encoded as varbinary(16)) < X'0080FF'").rewrite();
        PlanChecker.from(connectContext).analyze(
                "select id from source_bytes where cast(encoded as varbinary) <=> NULL").rewrite();
        PlanChecker.from(connectContext).analyze(
                "select id from source_bytes where cast(encoded as varbinary) in (X'', X'00', NULL)").rewrite();
        PlanChecker.from(connectContext).analyze(
                "select id from source_bytes where cast(encoded as varbinary) = 'abc'").rewrite();
    }

    @Test
    public void testGroupBy() {
        PlanChecker.from(connectContext).analyze(
                "select cast(encoded as varbinary), count(*) from source_bytes "
                        + "group by cast(encoded as varbinary)").rewrite();
    }

    @Test
    public void testJoin() {
        PlanChecker.from(connectContext).analyze("select a.id from source_bytes a join source_bytes b "
                + "on cast(a.encoded as varbinary) = cast(b.encoded as varbinary)").rewrite();
    }

    @Test
    public void testView() throws Exception {
        Assertions.assertEquals("X'0080FF'", new VarBinaryLiteral("0080FF").toSql());
        createView("create view binary_view as select id, cast(encoded as varbinary) as payload from source_bytes");
        Assertions.assertTrue(Env.getCurrentInternalCatalog().getDbOrDdlException("binary_sql")
                .getTableOrDdlException("binary_view").getColumn("payload").getType().isVarbinaryType());
        PlanChecker.from(connectContext).analyze("select * from binary_view where payload = X'00'").rewrite();
    }

    @Test
    public void testCreateTableAsSelect() throws Exception {
        connectContext.setQueryId(new TUniqueId(1, 1));
        connectContext.getState().reset();
        createTable("create table binary_ctas distributed by hash(id) buckets 1 "
                + "properties ('replication_num'='1') "
                + "as select id, cast(encoded as varbinary) as payload from source_bytes");
        Assertions.assertTrue(Env.getCurrentInternalCatalog().getDbOrDdlException("binary_sql")
                .getTableOrDdlException("binary_ctas").getColumn("payload").getType().isVarbinaryType());
        createTable("create table binary_only_ctas distributed by hash(payload) buckets 1 "
                + "properties ('replication_num'='1') "
                + "as select cast(encoded as varbinary) as payload from source_bytes");
        Assertions.assertTrue(Env.getCurrentInternalCatalog().getDbOrDdlException("binary_sql")
                .getTableOrDdlException("binary_only_ctas").getColumn("payload").getType().isVarbinaryType());
    }

    @Test
    public void testMaterializedView() throws Exception {
        createMvByNereids("create materialized view binary_mv build deferred refresh complete on manual "
                + "distributed by hash(id) buckets 1 properties ('replication_num'='1') "
                + "as select id, cast(encoded as varbinary) as payload from source_bytes");
        Assertions.assertTrue(Env.getCurrentInternalCatalog().getDbOrDdlException("binary_sql")
                .getTableOrDdlException("binary_mv").getColumn("payload").getType().isVarbinaryType());
    }
}
