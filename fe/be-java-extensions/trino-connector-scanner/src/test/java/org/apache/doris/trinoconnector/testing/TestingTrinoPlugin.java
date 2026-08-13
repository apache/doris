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

package org.apache.doris.trinoconnector.testing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.Page;
import io.trino.spi.Plugin;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.transaction.IsolationLevel;
import io.trino.spi.type.BigintType;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A whole Trino connector, small enough to read, so that the plugin tests can exercise the real
 * loading path instead of a stub of it.
 *
 * <p>It is deliberately written against nothing but {@code io.trino.spi} and jackson's annotations,
 * because those are the two package prefixes a Trino plugin classloader delegates to its parent.
 * Anything else it referenced would have to be inside the plugin jar - which is exactly the
 * property under test, and a fixture that needed it would be testing the fixture.
 *
 * <p>The tests package these classes into a directory of their own and point the scanner at it, so
 * at runtime this class exists twice: once on the test classpath and once inside that jar, loaded
 * by the nested classloader. Those are different classes, and the tests assert as much.
 */
public class TestingTrinoPlugin implements Plugin {

    /** The name a catalog reaches this connector by, as {@code connector.name}. */
    public static final String CONNECTOR_NAME = "testing";

    /** The single column the connector serves, named so a scan can ask for it by name. */
    public static final String COLUMN_NAME = "id";

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories() {
        return Collections.singletonList(new Factory());
    }

    /**
     * Builds the handles a scan is made of. Called reflectively from the test, because the point of
     * calling it is that these objects come from <em>this</em> copy of the class - the one the
     * nested classloader defined - which is what makes them serialize under that classloader's id
     * and deserialize back into it.
     */
    public static Map<String, Object> handles(int rows) {
        Map<String, Object> handles = new LinkedHashMap<>();
        handles.put("split", new Split(rows));
        handles.put("table", new TableHandle("t"));
        handles.put("column", new Column(COLUMN_NAME));
        handles.put("transaction", new Transaction("txn"));
        return handles;
    }

    public static class Factory implements ConnectorFactory {
        @Override
        public String getName() {
            return CONNECTOR_NAME;
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config,
                ConnectorContext context) {
            return new TestingConnector();
        }
    }

    public static class TestingConnector implements Connector {
        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel,
                boolean readOnly, boolean autoCommit) {
            return new Transaction("txn");
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() {
            return new PageSources();
        }
    }

    /** Rows are generated from the split, so the row count travels through the serialized form. */
    public static class PageSources implements ConnectorPageSourceProvider {
        @Override
        public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction,
                ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table,
                List<ColumnHandle> columns, DynamicFilter dynamicFilter) {
            int rows = ((Split) split).getRows();
            BlockBuilder builder = BigintType.BIGINT.createBlockBuilder(null, rows);
            for (int i = 0; i < rows; i++) {
                BigintType.BIGINT.writeLong(builder, i + 1);
            }
            return new FixedPageSource(Collections.singletonList(new Page(builder.build())));
        }
    }

    public static class Split implements ConnectorSplit {
        private final int rows;

        @JsonCreator
        public Split(@JsonProperty("rows") int rows) {
            this.rows = rows;
        }

        @JsonProperty
        public int getRows() {
            return rows;
        }

        @Override
        public Object getInfo() {
            return Collections.emptyMap();
        }
    }

    /**
     * Handles carry a property each because jackson refuses to serialize a bean with none, and a
     * real connector's handles always carry the table they point at.
     */
    public static class TableHandle implements ConnectorTableHandle {
        private final String table;

        @JsonCreator
        public TableHandle(@JsonProperty("table") String table) {
            this.table = table;
        }

        @JsonProperty
        public String getTable() {
            return table;
        }
    }

    public static class Column implements ColumnHandle {
        private final String name;

        @JsonCreator
        public Column(@JsonProperty("name") String name) {
            this.name = name;
        }

        @JsonProperty
        public String getName() {
            return name;
        }
    }

    public static class Transaction implements ConnectorTransactionHandle {
        private final String id;

        @JsonCreator
        public Transaction(@JsonProperty("id") String id) {
            this.id = id;
        }

        @JsonProperty
        public String getId() {
            return id;
        }
    }
}
