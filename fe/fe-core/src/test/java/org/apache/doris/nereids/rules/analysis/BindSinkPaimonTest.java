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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.common.Pair;
import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.CTEContext;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundPaimonTableSink;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPaimonTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;

import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BindSinkPaimonTest {

    @Test
    public void testBindPaimonTableSinkBindsFullSchema() throws Exception {
        PaimonExternalDatabase database = Mockito.mock(PaimonExternalDatabase.class);
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        LogicalPlan child = Mockito.mock(LogicalPlan.class);
        List<NamedExpression> output = Arrays.asList(
                new SlotReference("id", IntegerType.INSTANCE),
                new SlotReference("pt", IntegerType.INSTANCE));
        List<Column> schema = Arrays.asList(
                new Column("id", PrimitiveType.INT),
                new Column("pt", PrimitiveType.INT));
        Mockito.when(child.getOutput()).thenReturn((List) output);
        Mockito.when(table.getBaseSchema(true)).thenReturn(schema);
        Mockito.when(table.getFullSchema()).thenReturn(schema);
        Mockito.when(table.getName()).thenReturn("tbl1");

        MatchingContext<UnboundPaimonTableSink<Plan>> ctx = buildContext(table, database, child);
        Plan plan = invokeBindPaimonTableSink(ctx);

        Assertions.assertInstanceOf(LogicalPaimonTableSink.class, plan);
        LogicalPaimonTableSink<?> sink = (LogicalPaimonTableSink<?>) plan;
        Assertions.assertEquals(2, sink.getCols().size());
        Assertions.assertSame(table, sink.getTargetTable());
        Assertions.assertTrue(sink.child() instanceof LogicalProject);
    }

    @Test
    public void testBindPaimonTableSinkRejectsColumnCountMismatch() throws Exception {
        PaimonExternalDatabase database = Mockito.mock(PaimonExternalDatabase.class);
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        LogicalPlan child = Mockito.mock(LogicalPlan.class);
        List<NamedExpression> output = Collections.singletonList(new SlotReference("id", IntegerType.INSTANCE));
        List<Column> schema = Arrays.asList(
                new Column("id", PrimitiveType.INT),
                new Column("pt", PrimitiveType.INT));
        Mockito.when(child.getOutput()).thenReturn((List) output);
        Mockito.when(table.getBaseSchema(true)).thenReturn(schema);
        Mockito.when(table.getName()).thenReturn("tbl1");

        MatchingContext<UnboundPaimonTableSink<Plan>> ctx = buildContext(table, database, child);
        try {
            invokeBindPaimonTableSink(ctx);
            Assertions.fail("expected analysis exception");
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            Assertions.assertTrue(cause.getMessage().contains(
                    "insert into cols should be corresponding to the query output"));
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private MatchingContext<UnboundPaimonTableSink<Plan>> buildContext(
            PaimonExternalTable table, PaimonExternalDatabase database, LogicalPlan child) {
        ConnectContext connectContext = new ConnectContext();
        CascadesContext cascadesContext = Mockito.mock(CascadesContext.class);
        Mockito.when(cascadesContext.getStatementContext()).thenReturn(Mockito.mock(StatementContext.class));
        Mockito.when(cascadesContext.getConnectContext()).thenReturn(connectContext);
        Mockito.when(cascadesContext.getCteContext()).thenReturn(Mockito.mock(CTEContext.class));

        new MockUp<RelationUtil>() {
            @Mock
            public List<String> getQualifierName(ConnectContext ctx, List<String> nameParts) {
                return nameParts;
            }

            @Mock
            public Pair getDbAndTable(List<String> tableQualifier, Object env, java.util.Optional qualifier) {
                return Pair.of(database, table);
            }
        };

        UnboundPaimonTableSink<Plan> sink = new UnboundPaimonTableSink<>(
                Arrays.asList("ctl", "db1", "tbl1"),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                DMLCommandType.NONE,
                java.util.Optional.empty(),
                java.util.Optional.empty(),
                child);
        return new MatchingContext<>(sink, null, cascadesContext);
    }

    private Plan invokeBindPaimonTableSink(MatchingContext<UnboundPaimonTableSink<Plan>> ctx) throws Exception {
        Method method = BindSink.class.getDeclaredMethod("bindPaimonTableSink", MatchingContext.class);
        method.setAccessible(true);
        return (Plan) method.invoke(new BindSink(), ctx);
    }
}
