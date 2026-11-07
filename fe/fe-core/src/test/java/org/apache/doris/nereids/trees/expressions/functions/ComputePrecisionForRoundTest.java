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

package org.apache.doris.nereids.trees.expressions.functions;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConnectionId;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Round;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.math.BigDecimal;

class ComputePrecisionForRoundTest {
    @Test
    void constantScaleDoesNotCreateSession() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext.remove();
        try (MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class)) {
            Round round = new Round(new DecimalV3Literal(new BigDecimal("123.456")),
                    new Cast(new Add(new IntegerLiteral(1), new IntegerLiteral(1)), IntegerType.INSTANCE));
            Assertions.assertEquals(2, ((DecimalV3Type) round.getDataType()).getScale());
            Assertions.assertTrue(contexts.constructed().isEmpty());
            Assertions.assertNull(ConnectContext.get());
        } finally {
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }

    @Test
    void contextualScaleStillUsesExistingStatement() {
        ConnectContext previous = ConnectContext.get();
        ConnectContext context = new ConnectContext();
        context.setConnectionId(2);
        StatementContext statement = new StatementContext(context, new OriginStatement("select round", 0));
        context.setStatementContext(statement);
        context.setThreadLocalInfo();
        try (MockedConstruction<ConnectContext> contexts = Mockito.mockConstruction(ConnectContext.class)) {
            Round round = new Round(new DecimalV3Literal(new BigDecimal("123.456")),
                    new Cast(new ConnectionId(), IntegerType.INSTANCE));
            Assertions.assertEquals(2, ((DecimalV3Type) round.getDataType()).getScale());
            Assertions.assertTrue(contexts.constructed().isEmpty());
            Assertions.assertSame(statement, context.getStatementContext());
            Assertions.assertSame(context, ConnectContext.get());
            Assertions.assertFalse(context.isClosingOrClosed());
        } finally {
            context.cleanup();
            if (previous != null) {
                previous.setThreadLocalInfo();
            }
        }
    }
}
