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

package org.apache.doris.connector.trino;

import org.apache.doris.connector.spi.ConnectorTableSchema;
import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorIsNull;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.trinoconnector.TrinoConnectorPluginManager;

import io.trino.FeaturesConfig;
import io.trino.metadata.HandleResolver;
import io.trino.metadata.TypeRegistry;
import io.trino.plugin.mongodb.MongoPlugin;
import io.trino.plugin.mongodb.ObjectIdType;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarcharType;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MongoObjectIdTest {
    @Test
    public void registersPluginDefinedTypesBeforeReadingSchemas() {
        TypeRegistry registry = new TypeRegistry(new TypeOperators(), new FeaturesConfig());
        TrinoConnectorPluginManager manager = new TrinoConnectorPluginManager(
                (loader, factory) -> { }, registry, new HandleResolver());
        manager.installPlugin(new MongoPlugin());

        Assertions.assertSame(ObjectIdType.OBJECT_ID,
                Assertions.assertDoesNotThrow(() -> registry.fromSqlType("ObjectId")));
        Assertions.assertEquals(new ArrayType(ObjectIdType.OBJECT_ID), registry.fromSqlType("array(ObjectId)"));
    }

    @Test
    public void registersFrontendPluginDefinedTypes() {
        TypeRegistry registry = new TypeRegistry(new TypeOperators(), new FeaturesConfig());
        TrinoPluginManager manager = new TrinoPluginManager(
                (loader, factory) -> { }, registry, new HandleResolver());
        manager.installPlugin(new MongoPlugin());

        Assertions.assertSame(ObjectIdType.OBJECT_ID,
                Assertions.assertDoesNotThrow(() -> registry.fromSqlType("ObjectId")));
    }

    @Test
    public void preservesObjectIdAsTwelveBytes() {
        ConnectorType type = Assertions.assertDoesNotThrow(
                () -> TrinoTypeMapping.toConnectorType(ObjectIdType.OBJECT_ID));
        Assertions.assertEquals("VARBINARY", type.getTypeName());
        Assertions.assertEquals(12, type.getPrecision());
    }

    @Test
    public void preservesObjectIdsInsideArraysAndRows() {
        ConnectorType type = Assertions.assertDoesNotThrow(() -> TrinoTypeMapping.toConnectorType(RowType.rowType(
                RowType.field("owner", ObjectIdType.OBJECT_ID),
                RowType.field("members", new ArrayType(ObjectIdType.OBJECT_ID)))));
        Assertions.assertEquals("VARBINARY", type.getChildren().get(0).getTypeName());
        Assertions.assertEquals(12, type.getChildren().get(0).getPrecision());
        Assertions.assertEquals("VARBINARY", type.getChildren().get(1).getChildren().get(0).getTypeName());
        Assertions.assertEquals(12, type.getChildren().get(1).getChildren().get(0).getPrecision());
    }

    @Test
    public void exposesOnlyTheMongoIdentifierAmongHiddenColumns() {
        Map<ColumnHandle, ColumnMetadata> fields = new LinkedHashMap<>();
        fields.put(new ColumnHandle() { }, ColumnMetadata.builder().setName("_id")
                .setType(ObjectIdType.OBJECT_ID).setHidden(true).build());
        fields.put(new ColumnHandle() { }, ColumnMetadata.builder().setName("hidden_ref")
                .setType(ObjectIdType.OBJECT_ID).setHidden(true).build());
        fields.put(new ColumnHandle() { }, ColumnMetadata.builder().setName("$path")
                .setType(VarcharType.VARCHAR).setHidden(true).build());
        fields.put(new ColumnHandle() { }, new ColumnMetadata("name", VarcharType.VARCHAR));
        Map<String, ColumnHandle> handles = new LinkedHashMap<>();
        fields.forEach((handle, metadata) -> handles.put(metadata.getName(), handle));
        ConnectorMetadata source = new ConnectorMetadata() {
            @Override
            public ColumnMetadata getColumnMetadata(ConnectorSession session,
                    ConnectorTableHandle table, ColumnHandle column) {
                return fields.get(column);
            }
        };
        Connector connector = new Connector() {
            @Override
            public ConnectorTransactionHandle beginTransaction(IsolationLevel isolation,
                    boolean readOnly, boolean autoCommit) {
                return new ConnectorTransactionHandle() { };
            }

            @Override
            public ConnectorMetadata getMetadata(ConnectorSession session, ConnectorTransactionHandle transaction) {
                return source;
            }

            @Override
            public void commit(ConnectorTransactionHandle transaction) {
            }
        };
        TrinoConnectorDorisMetadata metadata = new TrinoConnectorDorisMetadata(connector,
                TestingSession.testSessionBuilder().build(),
                CatalogHandle.createRootCatalogHandle("mongodb", new CatalogHandle.CatalogVersion("test")));
        ConnectorTableSchema schema = metadata.getTableSchema(null,
                new TrinoTableHandle("fixture", "records", null, handles, null));
        List<String> names = schema.getColumns().stream().map(column -> column.getName()).collect(Collectors.toList());
        Assertions.assertEquals(Arrays.asList("_id", "name"), names);
    }

    @Test
    public void nullPredicateRetainsTheSourceObjectIdType() {
        ColumnHandle handle = new ColumnHandle() { };
        TrinoPredicateConverter converter = new TrinoPredicateConverter(Collections.singletonMap("owner", handle),
                Collections.singletonMap("owner", new ColumnMetadata("owner", ObjectIdType.OBJECT_ID)));
        ConnectorColumnRef column = new ConnectorColumnRef("owner", ConnectorType.of("VARBINARY"));
        Assertions.assertEquals(TupleDomain.withColumnDomains(Collections.singletonMap(handle,
                Domain.onlyNull(ObjectIdType.OBJECT_ID))), converter.convert(new ConnectorIsNull(column, false)));
    }

    @Test
    public void textBoundsAreNotPushedAsBsonObjectIds() {
        ColumnHandle handle = new ColumnHandle() { };
        TrinoPredicateConverter converter = new TrinoPredicateConverter(Collections.singletonMap("owner", handle),
                Collections.singletonMap("owner", new ColumnMetadata("owner", ObjectIdType.OBJECT_ID)));
        ConnectorColumnRef column = new ConnectorColumnRef("owner", ConnectorType.of("VARBINARY"));
        Assertions.assertTrue(converter.convert(new ConnectorComparison(ConnectorComparison.Operator.EQ,
                column, ConnectorLiteral.ofString("000000000000000000000001"))).isAll());
    }

}
