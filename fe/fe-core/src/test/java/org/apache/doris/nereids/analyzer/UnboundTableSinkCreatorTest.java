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
import org.apache.doris.common.UserException;
import org.apache.doris.connector.spi.Connector;
import org.apache.doris.connector.spi.handle.WriteOperation;
import org.apache.doris.connector.spi.write.ConnectorWritePlanProvider;
import org.apache.doris.datasource.CatalogMgr;
import org.apache.doris.datasource.plugin.PluginDrivenExternalCatalog;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalSink;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * CTAS destination admission (upstream #66112).
 *
 * <p>{@code CreateTableCommand} calls this 5-arg overload BEFORE it publishes the table metadata, so an
 * unsupported destination is refused while nothing has been created yet. Its rollback path can otherwise only
 * drop the failed CTAS table BY NAME, which cannot tell this statement's table from a concurrent creator's
 * table of the same name — so a late refusal risks dropping someone else's table.
 *
 * <p>Every {@code PluginDrivenExternalCatalog} builds the same generic {@code UnboundConnectorTableSink}, so
 * "can this connector be written to at all" has to be asked of the connector here; the equivalent refusal in
 * {@code PhysicalPlanTranslator} fires only at translation time, long after the remote table exists.
 */
public class UnboundTableSinkCreatorTest {

    private static final List<String> NAME_PARTS = ImmutableList.of("cat", "db", "tbl");

    @Test
    public void ctasIsRefusedWhenTheConnectorDeclaresNoWritePathAtAll() {
        // paimon / es / hudi / trino: no write plan provider at all.
        UserException ex = Assertions.assertThrows(UserException.class,
                () -> createSinkFor(null));
        Assertions.assertTrue(ex.getMessage().contains("does not support INSERT operations"),
                "the refusal must name the missing capability; got: " + ex.getMessage());
    }

    @Test
    public void ctasIsRefusedWhenTheConnectorWritesButCannotInsert() {
        // A connector that only declares row-level DML is still not a CTAS destination.
        UserException ex = Assertions.assertThrows(UserException.class,
                () -> createSinkFor(EnumSet.of(WriteOperation.DELETE, WriteOperation.MERGE)));
        Assertions.assertTrue(ex.getMessage().contains("does not support INSERT operations"),
                "declaring DELETE/MERGE must not admit a CTAS; got: " + ex.getMessage());
    }

    @Test
    public void ctasIsAdmittedWhenTheConnectorDeclaresInsert() throws Exception {
        // iceberg / hive / jdbc / maxcompute: unchanged behaviour, the generic connector sink is built.
        LogicalSink<? extends Plan> sink = createSinkFor(EnumSet.of(WriteOperation.INSERT));
        Assertions.assertTrue(sink instanceof UnboundConnectorTableSink,
                "a write-capable connector must still get the generic plugin sink");
    }

    private LogicalSink<? extends Plan> createSinkFor(Set<WriteOperation> declaredOps) throws UserException {
        PluginDrivenExternalCatalog catalog = Mockito.mock(PluginDrivenExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("cat");
        Mockito.when(catalog.getType()).thenReturn("paimon");
        Connector connector = Mockito.mock(Connector.class);
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        if (declaredOps != null) {
            ConnectorWritePlanProvider provider = Mockito.mock(ConnectorWritePlanProvider.class);
            Mockito.when(provider.supportedOperations()).thenReturn(declaredOps);
            Mockito.when(connector.getWritePlanProvider()).thenReturn(provider);
        }

        CatalogMgr catalogMgr = Mockito.mock(CatalogMgr.class);
        Mockito.<Object>when(catalogMgr.getCatalog("cat")).thenReturn(catalog);
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getCatalogMgr()).thenReturn(catalogMgr);
        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class)) {
            envMock.when(Env::getCurrentEnv).thenReturn(env);
            Plan query = new LogicalOneRowRelation(new RelationId(1),
                    ImmutableList.of(new Alias(Literal.of(1))));
            return UnboundTableSinkCreator.createUnboundTableSink(NAME_PARTS,
                    ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), query);
        }
    }
}
