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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.parser.ParserTestBase;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConnectorWriteSchemaUtilsTest extends ParserTestBase {

    @Test
    public void connectorHexDefaultParsesIdenticallyInBothBackslashModes() {
        Column column = new Column("path", ScalarType.createStringType());
        column.setConnectorDefaultValueSql("UNHEX('433A5C6E6577')");

        long originalSqlMode = ConnectContext.get().getSessionVariable().getSqlMode();
        try {
            ConnectContext.get().getSessionVariable().setSqlMode(0L);
            Expression defaultMode = ConnectorWriteSchemaUtils.resolveDefault(column);
            ConnectContext.get().getSessionVariable().setSqlMode(SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES);
            Expression noBackslashEscapes = ConnectorWriteSchemaUtils.resolveDefault(column);

            Assertions.assertEquals(defaultMode, noBackslashEscapes);
            Assertions.assertEquals("unhex('433a5c6e6577')", defaultMode.toSql().toLowerCase());
        } finally {
            ConnectContext.get().getSessionVariable().setSqlMode(originalSqlMode);
        }
    }

    @Test
    public void connectorNonFiniteFloatingDefaultsParseAsTypedExpressions() {
        Column column = new Column("value", ScalarType.createStringType());
        for (String defaultSql : new String[] {
                "CAST('NaN' AS FLOAT)",
                "CAST('Infinity' AS DOUBLE)",
                "CAST('-Infinity' AS DOUBLE)",
                "named_struct('float_value', CAST('NaN' AS FLOAT), "
                        + "'double_values', array(CAST('Infinity' AS DOUBLE), "
                        + "CAST('-Infinity' AS DOUBLE)))"}) {
            column.setConnectorDefaultValueSql(defaultSql);
            Expression expression = ConnectorWriteSchemaUtils.resolveDefault(column);
            Assertions.assertTrue(expression.anyMatch(candidate -> candidate instanceof Cast),
                    "non-finite default must remain an explicitly typed cast: " + defaultSql);
            Assertions.assertFalse(expression.anyMatch(candidate -> candidate instanceof UnboundSlot),
                    "non-finite default must not be parsed as an identifier: " + defaultSql);
        }
    }
}
