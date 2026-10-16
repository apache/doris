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

package org.apache.doris.analysis;

import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.datasource.jdbc.source.JdbcScanNode;
import org.apache.doris.thrift.TOdbcTableType;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class UuidLiteralTest {
    @Test
    public void testExternalSqlUsesSingleQuotedCanonicalUuid() throws Exception {
        UuidLiteral literal = new UuidLiteral("00112233445566778899AABBCCDDEEFF");
        Assert.assertEquals("\"00112233-4455-6677-8899-aabbccddeeff\"", literal.toSql());
        for (TableType tableType : Arrays.asList(TableType.JDBC, TableType.JDBC_EXTERNAL_TABLE, TableType.ODBC)) {
            Assert.assertEquals("'00112233-4455-6677-8899-aabbccddeeff'", literal.toExternalSql(tableType, null));
        }
    }

    @Test
    public void testClickHouseEqualityAndInPredicatesQuoteUuidValues() throws Exception {
        SlotRef slot = new SlotRef(null, "u");
        slot.setType(Type.UUID);
        UuidLiteral high = new UuidLiteral("80000000000000000000000000000000");
        UuidLiteral maximum = new UuidLiteral("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF");
        for (BinaryPredicate.Operator op : Arrays.asList(BinaryPredicate.Operator.EQ, BinaryPredicate.Operator.NE)) {
            Expr predicate = new BinaryPredicate(op, slot, high);
            Assert.assertEquals("(u " + op.toString() + " '80000000-0000-0000-0000-000000000000')",
                    JdbcScanNode.conjunctExprToString(TOdbcTableType.CLICKHOUSE, predicate, null));
        }
        for (boolean notIn : Arrays.asList(false, true)) {
            Expr predicate = new InPredicate(slot, Arrays.asList(high, maximum), notIn);
            Assert.assertEquals("u" + (notIn ? " NOT" : "")
                            + " IN ('80000000-0000-0000-0000-000000000000',"
                            + " 'ffffffff-ffff-ffff-ffff-ffffffffffff')",
                    JdbcScanNode.conjunctExprToString(TOdbcTableType.CLICKHOUSE, predicate, null));
        }
    }
}
