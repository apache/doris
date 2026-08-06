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

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.PartitionDescriptor;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Planning one scan: the remote work it produces, and the properties the scan node reads before there are
 * any ranges.
 *
 * <p>Two shapes are planned. Without partitioned execution a scan is one range carrying a statement, the
 * shape the JDBC connector has always had. With it, the driver splits the query and each partition becomes
 * its own range for its own backend -- and the split call has already run the query remotely, which is what
 * makes the failure modes here worth pinning individually.
 *
 * <p>Where a provider is built with a client supplier that throws, that is not a shortcut: it pins that the
 * path under test needs no remote call at all.
 */
class AdbcScanPlanProviderTest {

    private static final AdbcTableHandle T1 =
            new AdbcTableHandle(new AdbcNamespace("main", ""), "t1");

    private static final String DRIVER =
            "/opt/doris/plugins/adbc_drivers/libadbc_driver_sqlite.so";

    /** Plans statements: partitioned execution off, so no client is needed at all. */
    private static AdbcScanPlanProvider statementProvider() {
        return statementProvider(Map.of(AdbcConnectorProperties.URI, "file:/tmp/x.db"));
    }

    private static AdbcScanPlanProvider statementProvider(Map<String, String> properties) {
        Map<String, String> withoutPartitions = new LinkedHashMap<>(properties);
        withoutPartitions.put(AdbcConnectorProperties.PARTITIONED_READ, "disabled");
        return provider(withoutPartitions, () -> {
            throw new AssertionError("planning a statement must not open a connection");
        }, new AdbcPartitionedReadSupport());
    }

    private static AdbcScanPlanProvider provider(Map<String, String> properties,
            Supplier<AdbcClient> clientSupplier, AdbcPartitionedReadSupport partitionedRead) {
        AdbcDialectSelector selector = new AdbcDialectSelector(
                Map.of(AdbcConnectorProperties.SQL_DIALECT, AnsiDialect.NAME));
        return new AdbcScanPlanProvider(properties, Paths.get(DRIVER), selector, clientSupplier,
                partitionedRead);
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

    private static Map<String, String> paramsOf(ConnectorScanRange range) {
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        range.populateRangeParams(formatDesc, new TFileRangeDesc());
        return formatDesc.getAdbcParams();
    }

    private static String querySqlOf(ConnectorScanRange range) {
        return paramsOf(range).get("query_sql");
    }

    private static List<ConnectorScanRange> planAll(AdbcScanPlanProvider provider) {
        return provider.planScan(null, ConnectorScanRequest.builder(T1, columns("a")).build());
    }

    // -- statement planning --------------------------------------------------------------------------

    @Test
    void plansOneRangeCarryingTheStatementWhenTheDriverCannotPartition() {
        List<ConnectorScanRange> ranges = statementProvider().planScan(null,
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

        Map<String, String> params = paramsOf(planAll(statementProvider(properties)).get(0));

        Assertions.assertEquals(DRIVER, params.get("driver_path"));
        Assertions.assertEquals("file:/tmp/x.db", params.get("uri"));
        Assertions.assertEquals("alice", params.get("username"));
        Assertions.assertEquals("secret", params.get("password"));
        Assertions.assertEquals("MYDB", params.get("adbc.snowflake.sql.db"));
    }

    @Test
    void refusesAHandleItDidNotCreate() {
        // A table-valued function forwarding raw SQL. Not supported yet, and the message has to say which
        // handle arrived or the failure reads as an internal cast error.
        ConnectorTableHandle passthrough = new PassthroughQueryTableHandle("SELECT 1");
        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> statementProvider().planScan(null,
                        ConnectorScanRequest.builder(passthrough, columns("a")).build()));
        Assertions.assertTrue(
                failure.getMessage().contains(PassthroughQueryTableHandle.class.getSimpleName()),
                failure.getMessage());
    }

    // -- partition planning --------------------------------------------------------------------------

    @Test
    void plansOneRangePerPartitionTheDriverReports() {
        // Three partitions, three ranges: this is what lets one remote query be read by three backends.
        FakeClient client = FakeClient.partitioning("p0", "p1", "p2");
        List<ConnectorScanRange> ranges = planAll(provider(
                Map.of(AdbcConnectorProperties.URI, "grpc://remote:9090"), () -> client,
                new AdbcPartitionedReadSupport()));

        Assertions.assertEquals(3, ranges.size());
        List<String> descriptors = new ArrayList<>();
        for (ConnectorScanRange range : ranges) {
            Map<String, String> params = paramsOf(range);
            descriptors.add(params.get("partition_descriptor"));
            // Each range still carries the connection: a partition is read on a fresh connection made by
            // whichever backend gets it, not on the one that planned.
            Assertions.assertEquals("grpc://remote:9090", params.get("uri"));
            Assertions.assertNull(params.get("query_sql"));
        }
        Assertions.assertEquals(List.of(encode("p0"), encode("p1"), encode("p2")), descriptors);
    }

    @Test
    void splitsTheSameStatementItWouldOtherwiseHaveSent() {
        // The pushed-down statement is what gets partitioned. Splitting an unfiltered query instead would
        // make the source materialize the whole table and Doris filter it afterwards.
        FakeClient client = FakeClient.partitioning("p0");
        provider(Map.of(AdbcConnectorProperties.URI, "grpc://remote:9090"), () -> client,
                new AdbcPartitionedReadSupport()).planScan(null,
                        ConnectorScanRequest.builder(T1, columns("a"))
                                .filter(Optional.of(greaterThan(10))).limit(7).build());

        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\" WHERE (\"a\" > 10) LIMIT 7",
                client.lastSql);
    }

    @Test
    void fallsBackToOneStatementAndStopsAskingADriverThatCannotPartition() {
        FakeClient client = FakeClient.notImplemented();
        AdbcPartitionedReadSupport support = new AdbcPartitionedReadSupport();
        AdbcScanPlanProvider planner = provider(
                Map.of(AdbcConnectorProperties.URI, "file:/tmp/x.db"), () -> client, support);

        List<ConnectorScanRange> first = planAll(planner);
        Assertions.assertEquals(1, first.size());
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\"", querySqlOf(first.get(0)));
        Assertions.assertTrue(support.isKnownUnsupported());

        // The answer is a property of the driver, so a second scan must not pay for the same round trip.
        Assertions.assertEquals(1, planAll(planner).size());
        Assertions.assertEquals(1, client.attempts);
    }

    @Test
    void letsADriverFailureThatIsNotAMissingMethodThrough() {
        // Only NOT_IMPLEMENTED means "this driver cannot". Anything else means the driver tried and the
        // query or the source is at fault; planning a statement instead would hide that behind a slower
        // query AND run the statement on the source a second time.
        FakeClient client = FakeClient.failing(AdbcStatusCode.IO);
        AdbcPartitionedReadSupport support = new AdbcPartitionedReadSupport();

        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> planAll(provider(Map.of(AdbcConnectorProperties.URI, "grpc://remote:9090"),
                        () -> client, support)));

        Assertions.assertTrue(failure.getMessage().contains("SELECT \"a\""), failure.getMessage());
        Assertions.assertFalse(support.isKnownUnsupported());

        // A source that rejects the statement is the first thing anyone pointing this connector at
        // something other than Doris will hit, and the rejection arrives in the source's own words --
        // usually a syntax error about a quote character. Nothing in that says Doris generated the SQL,
        // let alone that which SQL it generates is a property the user can set. So the message has to.
        Assertions.assertTrue(failure.getMessage().contains(AdbcConnectorProperties.SQL_DIALECT),
                failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains(AnsiDialect.NAME), failure.getMessage());
    }

    @Test
    void failsRatherThanPlanningNothingWhenTheDriverReportsNoPartitions() {
        // Zero ranges would be a scan that returns no rows, silently. The partition count reflects the
        // source's parallelism, not its cardinality, so it is never a legitimate way to say "empty".
        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> planAll(provider(Map.of(AdbcConnectorProperties.URI, "grpc://remote:9090"),
                        FakeClient::partitioningNothing, new AdbcPartitionedReadSupport())));
        Assertions.assertTrue(failure.getMessage().contains("no partitions"), failure.getMessage());
    }

    @Test
    void failsWhenThereAreMorePartitionsThanTheLimitAllows() {
        // Not a fallback to one statement: the source has already executed the query to produce these
        // descriptors, so re-planning it as a statement would execute it a second time.
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(AdbcConnectorProperties.URI, "grpc://remote:9090");
        properties.put(AdbcConnectorProperties.MAX_PARTITIONS, "2");
        FakeClient client = FakeClient.partitioning("p0", "p1", "p2");

        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> planAll(provider(properties, () -> client, new AdbcPartitionedReadSupport())));

        Assertions.assertTrue(failure.getMessage().contains("3 partitions"), failure.getMessage());
        Assertions.assertTrue(failure.getMessage().contains(AdbcConnectorProperties.MAX_PARTITIONS),
                failure.getMessage());
    }

    @Test
    void skipsThePartitionRoundTripWhenTheCatalogTurnedItOff() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(AdbcConnectorProperties.URI, "grpc://remote:9090");
        properties.put(AdbcConnectorProperties.PARTITIONED_READ, "disabled");
        FakeClient client = FakeClient.partitioning("p0", "p1");

        List<ConnectorScanRange> ranges = planAll(
                provider(properties, () -> client, new AdbcPartitionedReadSupport()));

        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\"", querySqlOf(ranges.get(0)));
        Assertions.assertEquals(0, client.attempts);
    }

    // -- scan node properties (the EXPLAIN path) -----------------------------------------------------

    @Test
    void namesTheArrowReaderInTheScanNodeProperties() {
        // The scan node reads the format from here, not from the range. Leaving it out routes the scan to
        // the JNI scanner, which has no ADBC branch.
        Assertions.assertEquals("arrow",
                statementProvider().getScanNodeProperties(null, T1, columns("a"), Optional.empty())
                        .get(ScanNodePropertyKeys.FILE_FORMAT_TYPE));
    }

    @Test
    void explainsTheSameStatementItWillRun() {
        // EXPLAIN never calls planScan, so the statement it shows is generated a second time. If the two
        // ever diverge, EXPLAIN describes a query that is not the one executed.
        List<ConnectorColumnHandle> cols = columns("a", "b");
        Optional<ConnectorExpression> filter = Optional.of(greaterThan(10));

        String planned = querySqlOf(statementProvider().planScan(null,
                ConnectorScanRequest.builder(T1, cols).filter(filter).build()).get(0));
        String explained = statementProvider().getScanNodeProperties(null, T1, cols, filter)
                .get(ScanNodePropertyKeys.REMOTE_QUERY);

        Assertions.assertEquals(planned, explained);
    }

    @Test
    void explainsWithoutARowLimitBecauseItHasNoneToShow() {
        String explained = statementProvider()
                .getScanNodeProperties(null, T1, columns("a"), Optional.empty())
                .get(ScanNodePropertyKeys.REMOTE_QUERY);
        Assertions.assertFalse(explained.contains("LIMIT"), explained);
    }

    @Test
    void requiredModeFailsRatherThanDowngradingWhenTheDriverCannotPartition() {
        // The whole point of the mode: a downgrade is invisible in the result -- the same rows arrive,
        // from one backend instead of many -- so a test written for the partitioned path would pass while
        // exercising the fallback, and the pass would be indistinguishable from the real thing.
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(AdbcConnectorProperties.URI, "grpc://remote:9090");
        properties.put(AdbcConnectorProperties.PARTITIONED_READ, "required");
        FakeClient client = FakeClient.notImplemented();
        AdbcPartitionedReadSupport support = new AdbcPartitionedReadSupport();
        AdbcScanPlanProvider planner = provider(properties, () -> client, support);

        DorisConnectorException failure =
                Assertions.assertThrows(DorisConnectorException.class, () -> planAll(planner));
        Assertions.assertTrue(failure.getMessage().contains("required"), failure.getMessage());

        // And it keeps failing without asking the driver again -- the answer is a property of the driver,
        // so a second scan must neither pay for the round trip nor quietly succeed.
        Assertions.assertThrows(DorisConnectorException.class, () -> planAll(planner));
        Assertions.assertEquals(1, client.attempts);
    }

    @Test
    void requiredModeCarriesTheDriversOwnAnswerIntoTheFailure() {
        // Without it the message says only that partitioning is unavailable, and which layer refused --
        // driver, driver manager, or the JNI bridge -- has to be found by hand.
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(AdbcConnectorProperties.URI, "grpc://remote:9090");
        properties.put(AdbcConnectorProperties.PARTITIONED_READ, "required");

        DorisConnectorException failure = Assertions.assertThrows(DorisConnectorException.class,
                () -> planAll(provider(properties, FakeClient::notImplemented,
                        new AdbcPartitionedReadSupport())));

        Assertions.assertTrue(failure.getMessage().contains("scripted failure"), failure.getMessage());
    }

    @Test
    void requiredModePlansPartitionsWhenTheDriverHasThem() {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(AdbcConnectorProperties.URI, "grpc://remote:9090");
        properties.put(AdbcConnectorProperties.PARTITIONED_READ, "required");

        Assertions.assertEquals(2, planAll(provider(properties,
                () -> FakeClient.partitioning("p0", "p1"), new AdbcPartitionedReadSupport())).size());
    }

    @Test
    void explainStillWorksUnderRequiredBecauseItNeverPartitions() {
        // EXPLAIN deliberately does not ask for partitions, so it must not be failed for not having any:
        // refusing to describe a query would help nobody, and describing it costs the source nothing.
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put(AdbcConnectorProperties.URI, "grpc://remote:9090");
        properties.put(AdbcConnectorProperties.PARTITIONED_READ, "required");
        AdbcScanPlanProvider planner = provider(properties, () -> {
            throw new AssertionError("EXPLAIN must not reach the ADBC driver");
        }, new AdbcPartitionedReadSupport());

        List<ConnectorScanRange> ranges = planner.planScan(null,
                ConnectorScanRequest.builder(T1, columns("a")).explainOnly(true).build());
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\"", querySqlOf(ranges.get(0)));
    }

    @Test
    void planningAnExplainAsksForNoPartitionsAndShowsTheStatement() {
        // EXPLAIN reaches planScan too -- that is where its inputSplitNum comes from -- and asking the
        // driver to partition IS executing the query on a Flight SQL source. An EXPLAIN that ran the
        // query it was asked to describe would double the work with nothing on screen to show for it.
        FakeClient client = FakeClient.partitioning("p0", "p1");
        List<ConnectorScanRange> ranges = provider(
                Map.of(AdbcConnectorProperties.URI, "grpc://remote:9090"), () -> client,
                new AdbcPartitionedReadSupport()).planScan(null,
                        ConnectorScanRequest.builder(T1, columns("a")).explainOnly(true).build());

        Assertions.assertEquals(0, client.attempts, "EXPLAIN executed the query on the source");
        // Still the statement a real scan would send, so what EXPLAIN shows is not fiction.
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertEquals("SELECT \"a\" FROM \"main\".\"t1\"", querySqlOf(ranges.get(0)));
    }

    @Test
    void neverAsksForPartitionsWhileExplaining() {
        // Asking for partitions IS executing the query on a Flight SQL source, so an EXPLAIN that took
        // this path would run the very query the user asked only to have described. Partitioned read is
        // ON here, and the client still must not be touched.
        AdbcScanPlanProvider planner = provider(Map.of(AdbcConnectorProperties.URI, "grpc://remote:9090"),
                () -> {
                    throw new AssertionError("EXPLAIN must not reach the ADBC driver");
                }, new AdbcPartitionedReadSupport());

        Assertions.assertDoesNotThrow(
                () -> planner.getScanNodeProperties(null, T1, columns("a"), Optional.empty()));
    }

    private static String encode(String descriptor) {
        return Base64.getEncoder().encodeToString(descriptor.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * An {@link AdbcClient} that opens nothing and answers {@code executePartitioned} from a script.
     *
     * <p>Hand-written rather than mocked because it also has to enforce the negatives: any call planning
     * is not allowed to make -- executing a query, updating -- fails the test where it happens.
     */
    private static final class FakeClient extends AdbcClient {

        private final List<String> descriptors;
        private final AdbcStatusCode failure;
        private String lastSql;
        private int attempts;

        private FakeClient(List<String> descriptors, AdbcStatusCode failure) {
            super(Paths.get(DRIVER), "libadbc_driver_sqlite.so", null, "file:/tmp/x.db", null, null,
                    Map.of());
            this.descriptors = descriptors;
            this.failure = failure;
        }

        static FakeClient partitioning(String... descriptors) {
            return new FakeClient(List.of(descriptors), null);
        }

        static FakeClient partitioningNothing() {
            return new FakeClient(Collections.emptyList(), null);
        }

        static FakeClient notImplemented() {
            return new FakeClient(null, AdbcStatusCode.NOT_IMPLEMENTED);
        }

        static FakeClient failing(AdbcStatusCode status) {
            return new FakeClient(null, status);
        }

        @Override
        public <T> T withConnection(AdbcConnectionCall<T> body) {
            try {
                return body.apply(new FakeConnection(this));
            } catch (AdbcException e) {
                throw AdbcClient.translate(e, "ADBC operation failed");
            } catch (DorisConnectorException e) {
                throw e;
            } catch (Exception e) {
                throw new DorisConnectorException("ADBC operation failed: " + e.getMessage(), e);
            }
        }
    }

    private static final class FakeConnection implements AdbcConnection {

        private final FakeClient client;

        private FakeConnection(FakeClient client) {
            this.client = client;
        }

        @Override
        public AdbcStatement createStatement() {
            return new FakeStatement(client);
        }

        @Override
        public ArrowReader getInfo(int[] infoCodes) {
            throw new AssertionError("planning a scan must not probe the driver for info");
        }

        @Override
        public void close() {
        }
    }

    private static final class FakeStatement implements AdbcStatement {

        private final FakeClient client;

        private FakeStatement(FakeClient client) {
            this.client = client;
        }

        @Override
        public void setSqlQuery(String query) {
            client.lastSql = query;
        }

        @Override
        public PartitionResult executePartitioned() throws AdbcException {
            client.attempts++;
            if (client.failure != null) {
                throw new AdbcException("scripted failure", null, client.failure, null, 0);
            }
            List<PartitionDescriptor> result = new ArrayList<>(client.descriptors.size());
            for (String descriptor : client.descriptors) {
                result.add(new PartitionDescriptor(
                        ByteBuffer.wrap(descriptor.getBytes(StandardCharsets.UTF_8))));
            }
            return new PartitionResult(new Schema(Collections.emptyList()), -1, result);
        }

        @Override
        public QueryResult executeQuery() {
            throw new AssertionError("planning a scan must not execute a query on FE");
        }

        @Override
        public UpdateResult executeUpdate() {
            throw new AssertionError("planning a scan must not write to the source");
        }

        @Override
        public void prepare() {
            throw new AssertionError("planning a scan must not prepare a statement");
        }

        @Override
        public void close() {
        }
    }
}
