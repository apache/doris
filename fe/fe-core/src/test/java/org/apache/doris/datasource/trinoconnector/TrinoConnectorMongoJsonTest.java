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

package org.apache.doris.datasource.trinoconnector;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.trinoconnector.source.TrinoConnectorPredicateConverter;
import org.apache.doris.trinoconnector.TrinoConnectorPluginManager;

import io.trino.FeaturesConfig;
import io.trino.Session;
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
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import io.trino.type.JsonType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class TrinoConnectorMongoJsonTest {
    private Type map(io.trino.spi.type.Type type) throws Exception {
        Method mapping = TrinoConnectorExternalTable.class.getDeclaredMethod(
                "trinoConnectorTypeToDorisType", io.trino.spi.type.Type.class);
        mapping.setAccessible(true);
        return (Type) mapping.invoke(
                Mockito.mock(TrinoConnectorExternalTable.class, Mockito.CALLS_REAL_METHODS), type);
    }

    @Test
    public void registersPluginDefinedTypesBeforeReadingSchemas() {
        TypeRegistry registry = new TypeRegistry(new TypeOperators(), new FeaturesConfig());
        TrinoConnectorPluginManager manager = new TrinoConnectorPluginManager(
                (loader, factory) -> { }, registry, new HandleResolver());
        manager.installPlugin(new MongoPlugin());
        Assertions.assertSame(ObjectIdType.OBJECT_ID, registry.fromSqlType("ObjectId"));
        Assertions.assertEquals(new io.trino.spi.type.ArrayType(ObjectIdType.OBJECT_ID),
                registry.fromSqlType("array(ObjectId)"));
    }

    @Test
    public void preservesObjectIdAndJsonRecursively() throws Exception {
        Type objectId = ScalarType.createVarbinaryType(12);
        Assertions.assertEquals(objectId, map(ObjectIdType.OBJECT_ID));
        Assertions.assertEquals(ScalarType.createVarbinaryType(ScalarType.MAX_VARBINARY_LENGTH),
                map(VarbinaryType.VARBINARY));
        Assertions.assertEquals(Type.JSONB, map(JsonType.JSON));
        for (io.trino.spi.type.Type child : Arrays.asList(ObjectIdType.OBJECT_ID, JsonType.JSON)) {
            Type expected = child == JsonType.JSON ? Type.JSONB : objectId;
            Assertions.assertEquals(ArrayType.create(expected, true), map(new io.trino.spi.type.ArrayType(child)));
            MapType mapped = (MapType) map(new io.trino.spi.type.MapType(
                    VarcharType.VARCHAR, child, new TypeOperators()));
            Assertions.assertEquals(expected, mapped.getValueType());
            StructType row = (StructType) map(RowType.rowType(RowType.field("payload", child)));
            Assertions.assertEquals("payload", row.getFields().get(0).getName());
            Assertions.assertEquals(expected, row.getFields().get(0).getType());
        }
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
        ConnectorTableHandle tableHandle = new ConnectorTableHandle() { };
        ConnectorMetadata metadata = new ConnectorMetadata() {
            @Override
            public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName name,
                    Optional<io.trino.spi.connector.ConnectorTableVersion> start,
                    Optional<io.trino.spi.connector.ConnectorTableVersion> end) {
                return tableHandle;
            }

            @Override
            public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
                return handles;
            }

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
                return metadata;
            }
        };
        TrinoConnectorExternalCatalog catalog = Mockito.mock(TrinoConnectorExternalCatalog.class);
        Mockito.when(catalog.getName()).thenReturn("mongodb");
        Mockito.when(catalog.getConnector()).thenReturn(connector);
        Session session = Mockito.mock(Session.class);
        Mockito.when(catalog.getTrinoSession()).thenReturn(session);
        CatalogHandle catalogHandle = CatalogHandle.createRootCatalogHandle(
                "mongodb", new CatalogHandle.CatalogVersion("test"));
        Mockito.when(catalog.getTrinoCatalogHandle()).thenReturn(catalogHandle);
        TrinoConnectorExternalDatabase db = Mockito.mock(TrinoConnectorExternalDatabase.class);
        Mockito.when(db.getFullName()).thenReturn("fixture");
        Mockito.when(db.getRemoteName()).thenReturn("fixture");
        TrinoConnectorExternalTable table = new TrinoConnectorExternalTable(1, "records", "records", catalog, db);
        List<Column> schema = table.initSchema().get().getSchema();
        Assertions.assertEquals(Arrays.asList("_id", "name"),
                schema.stream().map(Column::getName).collect(Collectors.toList()));
        Assertions.assertTrue(schema.get(0).isVisible());
        Assertions.assertEquals(ScalarType.createVarbinaryType(12), schema.get(0).getType());
    }

    @Test
    public void predicatesRetainObjectIdTypeAndRejectTextBounds() throws Exception {
        ColumnHandle handle = new ColumnHandle() { };
        TrinoConnectorPredicateConverter converter = new TrinoConnectorPredicateConverter(
                Collections.singletonMap("owner", handle),
                Collections.singletonMap("owner", new ColumnMetadata("owner", ObjectIdType.OBJECT_ID)));
        SlotRef slot = new SlotRef(new TableName("records"), "owner");
        Assertions.assertEquals(TupleDomain.withColumnDomains(Collections.singletonMap(handle,
                Domain.onlyNull(ObjectIdType.OBJECT_ID))),
                converter.convertExprToTrinoTupleDomain(new IsNullPredicate(slot, false)));
        BinaryPredicate textBound = new BinaryPredicate(BinaryPredicate.Operator.EQ, slot,
                new StringLiteral("000000000000000000000001"));
        Assertions.assertThrows(AnalysisException.class, () -> converter.convertExprToTrinoTupleDomain(textBound));
    }
}
