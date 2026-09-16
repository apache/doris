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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.nereids.trees.expressions.literal.VarBinaryLiteral;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

public class VarBinarySqlSupportTest extends TestWithFeService {
    @Test
    public void testNativeVarbinaryRemainsUnsupported() {
        assertVarbinaryRejected(() -> createTable(
                "create table native_binary (id int, payload varbinary(16)) duplicate key(id) "
                        + "distributed by hash(id) buckets 1 properties ('replication_num'='1')"));
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
        assertVarbinaryRejected(() -> createTable(
                "create table binary_ctas distributed by hash(id) buckets 1 "
                + "properties ('replication_num'='1') "
                + "as select id, cast(encoded as varbinary) as payload from source_bytes"));
        assertVarbinaryRejected(() -> createTable(
                "create table binary_only_ctas distributed by hash(payload) buckets 1 "
                + "properties ('replication_num'='1') "
                + "as select cast(encoded as varbinary) as payload from source_bytes"));
    }

    @Test
    public void testMaterializedView() throws Exception {
        assertVarbinaryRejected(() -> createMvByNereids(
                "create materialized view binary_mv build deferred refresh complete on manual "
                + "distributed by hash(id) buckets 1 properties ('replication_num'='1') "
                + "as select id, cast(encoded as varbinary) as payload from source_bytes"));
    }

    private static void assertVarbinaryRejected(org.junit.jupiter.api.function.Executable statement) {
        Exception error = Assertions.assertThrows(Exception.class, statement);
        Assertions.assertTrue(error.getMessage().toLowerCase(java.util.Locale.ROOT).contains("varbinary"),
                error.getMessage());
    }
}
