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

import org.apache.doris.datasource.paimon.PaimonExternalDatabase;
import org.apache.doris.datasource.paimon.PaimonExternalTable;
import org.apache.doris.nereids.analyzer.UnboundPaimonTableSink;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalEmptyRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPaimonTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.IntegerType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Lightweight tests around Paimon sink plan wiring that avoid the full
 * Nereids analyzer. The Paimon insert binding rule is indirectly exercised
 * by end-to-end regression tests; here we focus on the constructor wiring
 * and helper accessors of the Paimon sink plan nodes.
 */
public class BindSinkPaimonTest {

    @Test
    public void testUnboundPaimonTableSinkKeepsQualifierAndChild() {
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(table.getName()).thenReturn("tbl1");
        List<NamedExpression> output = Arrays.asList(
                new SlotReference("id", IntegerType.INSTANCE),
                new SlotReference("pt", IntegerType.INSTANCE));
        LogicalPlan child = new LogicalEmptyRelation(new RelationId(1), output);

        UnboundPaimonTableSink<Plan> sink = new UnboundPaimonTableSink<>(
                Arrays.asList("internal", "db1", "tbl1"),
                Collections.emptyList(),
                Collections.emptyList(),
                Collections.emptyList(),
                child);

        Assertions.assertEquals(Arrays.asList("internal", "db1", "tbl1"), sink.getNameParts());
        Assertions.assertEquals(DMLCommandType.NONE, sink.getDMLCommandType());
        Assertions.assertSame(child, sink.child());
        Assertions.assertTrue(sink.getColNames().isEmpty());
    }

    @Test
    public void testLogicalPaimonTableSinkExposesBoundTableAndCols() {
        PaimonExternalDatabase database = Mockito.mock(PaimonExternalDatabase.class);
        PaimonExternalTable table = Mockito.mock(PaimonExternalTable.class);
        Mockito.when(table.getName()).thenReturn("tbl1");
        List<NamedExpression> output = Arrays.asList(
                new SlotReference("id", IntegerType.INSTANCE),
                new SlotReference("pt", IntegerType.INSTANCE));
        LogicalPlan child = new LogicalEmptyRelation(new RelationId(2), output);

        org.apache.doris.catalog.Column idCol = new org.apache.doris.catalog.Column(
                "id", org.apache.doris.catalog.PrimitiveType.INT);
        org.apache.doris.catalog.Column ptCol = new org.apache.doris.catalog.Column(
                "pt", org.apache.doris.catalog.PrimitiveType.INT);
        LogicalPaimonTableSink<LogicalPlan> sink = new LogicalPaimonTableSink<>(
                database, table,
                Arrays.asList(idCol, ptCol),
                output,
                DMLCommandType.INSERT,
                Optional.empty(),
                Optional.empty(),
                child);

        Assertions.assertSame(table, sink.getTargetTable());
        Assertions.assertSame(database, sink.getDatabase());
        Assertions.assertEquals(2, sink.getCols().size());
        Assertions.assertEquals(DMLCommandType.INSERT, sink.getDmlCommandType());
        Assertions.assertSame(child, sink.child());
    }
}
