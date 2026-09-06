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

package org.apache.doris.connector.iceberg.dlf;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import shade.doris.hive.org.apache.thrift.TException;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class HiveCompatibleCatalogTest {

    @Test
    public void listTablesFiltersNonIcebergTablesByDefault() {
        Table iceberg = table("iceberg_table", "ICEBERG");
        Table hive = table("hive_table", "HIVE");
        TestCatalog catalog = catalog(client((method, args) -> {
            if (method.equals("getAllTables")) {
                return List.of("iceberg_table", "hive_table");
            }
            if (method.equals("getTableObjectsByName")) {
                return List.of(iceberg, hive);
            }
            return null;
        }));

        Assertions.assertEquals(List.of(TableIdentifier.of("db", "iceberg_table")),
                catalog.listTables(Namespace.of("db")));
    }

    @Test
    public void listTablesCanRetainHiveCompatibleListAllBehavior() throws Exception {
        TestCatalog catalog = catalog(client((method, args) -> method.equals("getAllTables")
                ? List.of("iceberg_table", "hive_table") : null));
        Field listAllTables = HiveCompatibleCatalog.class.getDeclaredField("listAllTables");
        listAllTables.setAccessible(true);
        listAllTables.set(catalog, true);

        Assertions.assertEquals(List.of(
                        TableIdentifier.of("db", "iceberg_table"),
                        TableIdentifier.of("db", "hive_table")),
                catalog.listTables(Namespace.of("db")));
    }

    @Test
    public void loadNamespaceMetadataPreservesLocationAndComment() {
        Database database = new Database();
        database.setName("db");
        database.setLocationUri("oss://bucket/db");
        database.setDescription("description");
        database.setParameters(Map.of("key", "value"));
        TestCatalog catalog = catalog(client((method, args) -> database));

        Assertions.assertEquals(Map.of(
                        "key", "value",
                        "location", "oss://bucket/db",
                        "comment", "description"),
                catalog.loadNamespaceMetadata(Namespace.of("db")));
    }

    @Test
    public void missingDatabaseMapsToNoSuchNamespace() {
        TestCatalog catalog = catalog(client((method, args) -> {
            throw new NoSuchObjectException("missing");
        }));

        Assertions.assertThrows(NoSuchNamespaceException.class,
                () -> catalog.loadNamespaceMetadata(Namespace.of("missing")));
    }

    @Test
    public void interruptedMetastoreCallRestoresInterruptFlag() {
        TestCatalog catalog = catalog(new InterruptingClientPool());
        try {
            Assertions.assertThrows(RuntimeException.class,
                    () -> catalog.listTables(Namespace.of("db")));
            Assertions.assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    public void buildTableRejectsUnsupportedCreateBeforeTableOperations() {
        TestCatalog catalog = catalog(client((method, args) -> null));
        Schema schema = new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));

        Assertions.assertThrows(UnsupportedOperationException.class,
                () -> catalog.buildTable(TableIdentifier.of("db", "table"), schema));
    }

    @Test
    public void closeReleasesFileIoAndClientPoolOnlyOnce() throws Exception {
        AtomicInteger fileIoCloses = new AtomicInteger();
        FileIO fileIO = (FileIO) Proxy.newProxyInstance(
                FileIO.class.getClassLoader(), new Class<?>[] {FileIO.class}, (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                        fileIoCloses.incrementAndGet();
                    }
                    return null;
                });
        RecordingClientPool pool = new RecordingClientPool(client((method, args) -> null));
        TestCatalog catalog = new TestCatalog();
        catalog.initialize("test", fileIO, pool);

        catalog.close();
        catalog.close();

        Assertions.assertEquals(1, fileIoCloses.get());
        Assertions.assertEquals(1, pool.closes.get());
    }

    @Test
    public void closeHandlesPartiallyInitializedResources() throws Exception {
        AtomicInteger fileIoCloses = new AtomicInteger();
        FileIO fileIO = (FileIO) Proxy.newProxyInstance(
                FileIO.class.getClassLoader(), new Class<?>[] {FileIO.class}, (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                        fileIoCloses.incrementAndGet();
                    }
                    return null;
                });
        TestCatalog fileIoOnly = new TestCatalog();
        fileIoOnly.initialize("file-io-only", fileIO, null);

        RecordingClientPool pool = new RecordingClientPool(client((method, args) -> null));
        TestCatalog clientOnly = new TestCatalog();
        clientOnly.initialize("client-only", null, pool);

        fileIoOnly.close();
        clientOnly.close();

        Assertions.assertEquals(1, fileIoCloses.get());
        Assertions.assertEquals(1, pool.closes.get());
    }

    private static Table table(String name, String type) {
        Table table = new Table();
        table.setTableName(name);
        table.setParameters(Map.of(BaseMetastoreTableOperations.TABLE_TYPE_PROP, type));
        return table;
    }

    private static TestCatalog catalog(ClientPool<IMetaStoreClient, TException> pool) {
        TestCatalog catalog = new TestCatalog();
        catalog.initialize("test", null, pool);
        return catalog;
    }

    private static TestCatalog catalog(IMetaStoreClient client) {
        return catalog(new RecordingClientPool(client));
    }

    private static IMetaStoreClient client(ClientAction action) {
        return (IMetaStoreClient) Proxy.newProxyInstance(
                IMetaStoreClient.class.getClassLoader(), new Class<?>[] {IMetaStoreClient.class},
                (proxy, method, args) -> action.invoke(method.getName(), args));
    }

    private interface ClientAction {
        Object invoke(String method, Object[] args) throws Throwable;
    }

    private static class RecordingClientPool implements ClientPool<IMetaStoreClient, TException>, AutoCloseable {
        private final IMetaStoreClient client;
        private final AtomicInteger closes = new AtomicInteger();

        private RecordingClientPool(IMetaStoreClient client) {
            this.client = client;
        }

        @Override
        public <R> R run(Action<R, IMetaStoreClient, TException> action) throws TException, InterruptedException {
            return action.run(client);
        }

        @Override
        public <R> R run(Action<R, IMetaStoreClient, TException> action, boolean retry)
                throws TException, InterruptedException {
            return run(action);
        }

        @Override
        public void close() {
            closes.incrementAndGet();
        }
    }

    private static final class InterruptingClientPool extends RecordingClientPool {
        private InterruptingClientPool() {
            super(null);
        }

        @Override
        public <R> R run(Action<R, IMetaStoreClient, TException> action) throws InterruptedException {
            throw new InterruptedException("interrupted");
        }
    }

    private static final class TestCatalog extends HiveCompatibleCatalog {
        @Override
        protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
            return null;
        }
    }
}
