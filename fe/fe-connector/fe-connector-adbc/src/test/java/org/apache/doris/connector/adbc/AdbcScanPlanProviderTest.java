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

package org.apache.doris.connector.adbc;

import org.apache.doris.connector.spi.ConnectorType;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.handle.NamedColumnHandle;
import org.apache.doris.connector.spi.handle.PassthroughQueryTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.spi.pushdown.ConnectorComparison;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.pushdown.ConnectorLiteral;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Planning one scan: the statement, the single range, and the properties the scan node reads before there
 * are any ranges.
 *
 * <p>The provider is built with a client supplier that throws, which is not a shortcut -- it pins that
 * planning a scan needs no remote call once the dialect is known. A provider that reached out per query
 * would put a network round trip on every plan.
 */
class AdbcScanPlanProviderTest {

    private static final AdbcTableHandle T1 =
            new AdbcTableHandle(new AdbcNamespace("main", ""), "t1");

    private static AdbcScanPlanProvider provider() {
        return provider(Map.of(AdbcConnectorProperties.URI, "file:/tmp/x.db"));
    }

    private static AdbcScanPlanProvider provider(Map<String, String> properties) {
        AdbcDialectSelector selector = new AdbcDialectSelector(
                Map.of(AdbcConnectorProperties.SQL_DIALECT, AnsiDialect.NAME));
        return new AdbcScanPlanProvider(properties,
                Paths.get("/opt/doris/plugins/adbc_drivers/libadbc_driver_sqlite.so"), selector,
                () -> {
                    throw new AssertionError("planning must not open a connection");
                });
    }

    private static List<ConnectorColumnHandle> columns(String... names) {
        List<ConnectorColumnHandle> handles = new ArrayList<>(names.length);
        for (String name : names) {
            handles.add(new NamedColumnHandle(name));
        }
        return handles;
    }

    private static ConnectorExpression greaterThan(long value) {
        return new ConnectorComparison(ConnectorComparison.Operator.GT,
                new ConnectorColumnRef("a", ConnectorType.of("INT")), ConnectorLiteral.ofLong(value));
    }

    private static String querySqlOf(ConnectorScanRange range) {
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        range.populateRangeParams(formatDesc, new TFileRangeDesc());
        return formatDesc.getAdbcParams().get("query_sql");
    }

    @Test
    void plansExactlyOneRangeCarryingTheStatement() {
        // One range because BE can only run a statement, not a driver partition. Planning several would
        // hand out work nothing can execute.
        List<ConnectorScanRange> ranges = provider().planScan(null,
                ConnectorScanRequest.builder(T1, columns("a", "b"))
                        .filter(Optional.of(greaterThan(10))).limit(7).build());

        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals("SELECT \"a\", \"b\" FROM \"main\".\"t1\" WHERE (\"a\" > 10) LIMIT 7",
                querySqlOf(ranges.get(0)));
    }

    @Test
    void carriesTheCatalogConnectionOntoTheRange() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(AdbcConnectorProperties.URI, "file:/tmp/x.db");
        properties.put(AdbcConnectorProperties.USER, "alice");
        properties.put(AdbcConnectorProperties.PASSWORD, "secret");
        properties.put("adbc.snowflake.sql.db", "MYDB");

        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        provider(properties).planScan(null, ConnectorScanRequest.builder(T1, columns("a")).build())
                .get(0).populateRangeParams(formatDesc, new TFileRangeDesc());
        Map<String, String> params = formatDesc.getAdbcParams();

        Assertions.assertEquals("/opt/doris/plugins/adbc_drivers/libadbc_driver_sqlite.so",
                params.get("driver_path"));
        Assertions.assertEquals("file:/tmp/x.db", params.get("uri"));
        Assertions.assertEquals("alice", params.get("username"));
        Assertions.assertEquals("secret", params.get("password"));
        Assertions.assertEquals("MYDB", params.get("adbc.snowflake.sql.db"));
    }

    @Test
    void namesTheArrowReaderInTheScanNodeProperties() {
        // The scan node reads the format from here, not from the range. Leaving it out routes the scan to
        // the JNI scanner, which has no ADBC branch.
        Assertions.assertEquals("arrow",
                provider().getScanNodeProperties(null, T1, columns("a"), Optional.empty())
                        .get(ScanNodePropertyKeys.FILE_FORMAT_TYPE));
    }

    @Test
    void explainsTheSameStatementItWillRun() {
        // EXPLAIN never calls planScan, so the statement it shows is generated a second time. If the two
        // ever diverge, EXPLAIN describes a query that is not the one executed.
        List<ConnectorColumnHandle> cols = columns("a", "b");
        Optional<ConnectorExpression> filter = Optional.of(greaterThan(10));

        String planned = querySqlOf(provider().planScan(null,
                ConnectorScanRequest.builder(T1, cols).filter(filter).build()).get(0));
        String explained = provider().getScanNodeProperties(null, T1, cols, filter)
                .get(ScanNodePropertyKeys.REMOTE_QUERY);

        Assertions.assertEquals(planned, explained);
    }

    @Test
    void explainsWithoutARowLimitBecauseItHasNoneToShow() {
        String explained = provider().getScanNodeProperties(null, T1, columns("a"), Optional.empty())
                .get(ScanNodePropertyKeys.REMOTE_QUERY);
        Assertions.assertFalse(explained.contains("LIMIT"), explained);
    }

    @Test
    void refusesAHandleItDidNotCreate() {
        // A table-valued function forwarding raw SQL. Not supported yet, and the message has to say which
        // handle arrived or the failure reads as an internal cast error.
        ConnectorTableHandle passthrough = new PassthroughQueryTableHandle("SELECT 1");
        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> provider().planScan(null,
                        ConnectorScanRequest.builder(passthrough, columns("a")).build()));
        Assertions.assertTrue(
                failure.getMessage().contains(PassthroughQueryTableHandle.class.getSimpleName()),
                failure.getMessage());
    }
}
