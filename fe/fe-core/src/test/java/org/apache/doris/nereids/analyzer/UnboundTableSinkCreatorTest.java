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

package org.apache.doris.nereids.analyzer;

import org.apache.doris.catalog.Env;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.hive.HMSExternalCatalog;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

public class UnboundTableSinkCreatorTest {

    private static final List<String> NAME_PARTS = ImmutableList.of("cat", "db", "tbl");

    @Test
    public void staticPartitionIsRejectedForOlapTables() {
        for (CatalogIf<?> catalog : ImmutableList.of(Mockito.mock(InternalCatalog.class))) {
            withCatalog(catalog, () -> {
                LogicalOneRowRelation query = new LogicalOneRowRelation(new RelationId(1),
                        ImmutableList.of(new Alias(Literal.of(1))));
                Map<String, Expression> staticPartitions = ImmutableMap.of("dt", Literal.of("2026-11-04"));
                AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                        () -> UnboundTableSinkCreator.createUnboundTableSink(
                                NAME_PARTS, ImmutableList.of(), ImmutableList.of(), false,
                                ImmutableList.of(), false, null, DMLCommandType.INSERT, query, staticPartitions));
                Assertions.assertTrue(exception.getMessage().contains(
                        "Static partition syntax PARTITION (column=value) is not supported for OLAP tables"));
                for (boolean overwrite : new boolean[] {false, true}) {
                    AnalysisException overwriteException = Assertions.assertThrows(AnalysisException.class,
                            () -> UnboundTableSinkCreator.createUnboundTableSinkMaybeOverwrite(
                                    NAME_PARTS, ImmutableList.of(), ImmutableList.of(), false,
                                    ImmutableList.of(), false, overwrite, false, null,
                                    DMLCommandType.INSERT, query, staticPartitions));
                    Assertions.assertEquals(exception.getMessage(), overwriteException.getMessage());
                }
            });
        }
    }

    @Test
    public void staticPartitionIsPreservedForHiveTables() {
        withCatalog(Mockito.mock(HMSExternalCatalog.class), () -> {
            LogicalOneRowRelation query = new LogicalOneRowRelation(new RelationId(1),
                    ImmutableList.of(new Alias(Literal.of(1))));
            Map<String, Expression> staticPartitions = ImmutableMap.of("dt", Literal.of("2026-11-04"));
            UnboundHiveTableSink<?> sink = (UnboundHiveTableSink<?>)
                    UnboundTableSinkCreator.createUnboundTableSink(
                            NAME_PARTS, ImmutableList.of(), ImmutableList.of(), false,
                            ImmutableList.of(), false, null, DMLCommandType.INSERT, query, staticPartitions);
            Assertions.assertEquals(staticPartitions, sink.getStaticPartitionKeyValues());
            for (boolean overwrite : new boolean[] {false, true}) {
                UnboundHiveTableSink<?> overwriteSink = (UnboundHiveTableSink<?>)
                        UnboundTableSinkCreator.createUnboundTableSinkMaybeOverwrite(
                                NAME_PARTS, ImmutableList.of(), ImmutableList.of(), false,
                                ImmutableList.of(), false, overwrite, false, null,
                                DMLCommandType.INSERT, query, staticPartitions);
                Assertions.assertEquals(staticPartitions, overwriteSink.getStaticPartitionKeyValues());
            }
        });
    }

    private void withCatalog(CatalogIf<?> catalog, Runnable action) {
        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.<Object>when(catalogMgr.getCatalog("cat")).thenReturn(catalog);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            action.run();
        }
    }

}
