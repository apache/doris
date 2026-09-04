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

import org.apache.doris.connector.spi.ConnectorSession;
import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.handle.ConnectorColumnHandle;
import org.apache.doris.connector.spi.handle.ConnectorTableHandle;
import org.apache.doris.connector.spi.pushdown.ConnectorExpression;
import org.apache.doris.connector.spi.scan.ConnectorScanPlanProvider;
import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;
import org.apache.doris.connector.spi.scan.ScanNodePropertyKeys;

import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.core.AdbcStatement;
import org.apache.arrow.adbc.core.AdbcStatusCode;
import org.apache.arrow.adbc.core.PartitionDescriptor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Turns a scan into the remote work that serves it: either the driver's partitions, one per backend, or
 * the single statement one backend runs.
 *
 * <p>The partitioned path is what makes an ADBC catalog read faster than one connection can. It costs a
 * remote round trip at planning time, and on a Flight SQL source that round trip <b>is</b> the query's
 * execution -- the descriptors it returns identify result sets the source is already producing. That is why
 * nothing else in this class may take it: see {@link #getScanNodeProperties}.
 */
public class AdbcScanPlanProvider implements ConnectorScanPlanProvider {

    private static final Logger LOG = LogManager.getLogger(AdbcScanPlanProvider.class);

    /** Names the reader on BE. Kept next to {@link AdbcScanRange#getFileFormat()}, which must agree. */
    private static final String FILE_FORMAT = "arrow";

    private final AdbcCatalogProperties props;
    private final Path driverPath;
    private final AdbcDialectSelector dialectSelector;
    private final Supplier<AdbcClient> clientSupplier;
    private final AdbcPartitionedReadSupport partitionedRead;

    public AdbcScanPlanProvider(AdbcCatalogProperties props, Path driverPath,
            AdbcDialectSelector dialectSelector, Supplier<AdbcClient> clientSupplier,
            AdbcPartitionedReadSupport partitionedRead) {
        this.props = props;
        this.driverPath = driverPath;
        this.dialectSelector = dialectSelector;
        this.clientSupplier = clientSupplier;
        this.partitionedRead = partitionedRead;
    }

    @Override
    public List<ConnectorScanRange> planScan(ConnectorSession session, ConnectorScanRequest request) {
        AdbcTableHandle handle = adbcHandle(request.getTableHandle());
        AdbcQueryBuilder.BuiltQuery query = buildQuery(handle, request.getColumns(),
                request.getFilter(), request.getLimit());
        LOG.debug("ADBC scan of {}.{}: {}", handle.getDorisDbName(), handle.getRemoteTable(),
                query.getSql());
        // EXPLAIN plans a scan for real, so this is reached while describing a query as well as while
        // running one -- and asking the driver to partition a statement EXECUTES it on the source. An
        // EXPLAIN must not do that, so it is shown the statement instead, whatever the mode: refusing to
        // describe a query because the driver cannot partition would help nobody. The statement is what a
        // partitioned scan splits, so what EXPLAIN shows still describes the real scan; only the range
        // count differs, and a range count for a query that was never run means nothing anyway.
        AdbcCatalogProperties.PartitionedReadMode mode = props.getPartitionedReadMode();
        if (!request.isExplainOnly() && mode != AdbcCatalogProperties.PartitionedReadMode.DISABLED) {
            if (partitionedRead.isKnownUnsupported()) {
                // Already asked once, on this catalog, and the driver said no.
                refuseIfRequired(mode, partitionedRead.getRefusal());
            } else {
                List<ConnectorScanRange> partitioned = planPartitions(query.getSql(), mode);
                if (partitioned != null) {
                    return partitioned;
                }
            }
        }
        return Collections.singletonList(rangeBuilder().querySql(query.getSql()).build());
    }

    /**
     * Fails when the catalog asked for partitioned execution and cannot have it.
     *
     * <p>Without this the loss is invisible: the scan still returns the right rows, just from one backend
     * instead of many. A test written to exercise the partitioned path would go green while exercising the
     * fallback -- the failure and the pass look identical -- and a deployment sized for the parallelism
     * would simply be slow.
     */
    private static void refuseIfRequired(AdbcCatalogProperties.PartitionedReadMode mode,
            String driverAnswer) {
        if (mode != AdbcCatalogProperties.PartitionedReadMode.REQUIRED) {
            return;
        }
        throw new DorisConnectorException("This catalog sets '"
                + AdbcCatalogProperties.PARTITIONED_READ + "'='required', but its ADBC driver does not"
                + " implement partitioned execution, so the scan cannot be split across backends."
                + " The driver answered: " + (driverAnswer == null || driverAnswer.isEmpty()
                        ? "(no message)" : driverAnswer)
                + ". Use a driver that partitions, or set the property to 'auto' to allow reading through"
                + " a single statement instead.");
    }

    /**
     * Asks the driver to split this statement, and returns one range per partition -- or null when the
     * driver has no partitioned execution and the caller should plan the statement itself.
     *
     * <p>Only {@code NOT_IMPLEMENTED} means "this driver cannot do it". Any other status means the driver
     * tried and the query or the source is at fault, and swallowing that into a single-range plan would
     * hide a real failure behind a slower query -- and would run the statement remotely a second time.
     */
    private List<ConnectorScanRange> planPartitions(String sql,
            AdbcCatalogProperties.PartitionedReadMode mode) {
        // Set by the NOT_IMPLEMENTED branch so the downgrade can be logged with what the driver
        // actually said. Without it the log says only that SOMETHING answered NOT_IMPLEMENTED, and
        // the layer that did -- driver, driver manager, or JNI bridge -- has to be found by hand.
        AdbcException[] refusal = new AdbcException[1];
        List<PartitionDescriptor> descriptors = clientSupplier.get().withConnection(connection -> {
            try (AdbcStatement statement = connection.createStatement()) {
                statement.setSqlQuery(sql);
                try {
                    return statement.executePartitioned().getPartitionDescriptors();
                } catch (AdbcException e) {
                    if (e.getStatus() != AdbcStatusCode.NOT_IMPLEMENTED) {
                        // Naming the dialect is the point of this message. The source answers in its own
                        // words -- typically a syntax error about a quote character -- and nothing in that
                        // hints that Doris wrote the statement, or that which SQL it writes is settable.
                        throw AdbcClient.translate(e,
                                "Failed to plan a partitioned ADBC scan of [" + sql + "], generated in the '"
                                        + dialectSelector.select(clientSupplier).name() + "' dialect (set '"
                                        + AdbcCatalogProperties.SQL_DIALECT + "' on this catalog if the"
                                        + " source expects different SQL)");
                    }
                    refusal[0] = e;
                    return null;
                }
            }
        });
        if (descriptors == null) {
            String answer = refusal[0] == null ? "" : refusal[0].toString();
            if (partitionedRead.markUnsupported(answer)) {
                LOG.info("The ADBC driver does not implement partitioned execution, so scans of this"
                        + " catalog run as one range on one backend. Set '{}'='disabled' to stop asking,"
                        + " or 'required' to fail instead. The driver answered: {}",
                        AdbcCatalogProperties.PARTITIONED_READ,
                        answer.isEmpty() ? "(no exception)" : answer);
            }
            refuseIfRequired(mode, answer);
            return null;
        }
        if (descriptors.isEmpty()) {
            // Not read as "no rows": the partition count reflects the source's parallelism, not its
            // cardinality -- an empty table still yields a partition that returns nothing. Planning zero
            // ranges for a driver that answered with nothing would report an empty result for a query that
            // has one, which is the one failure a user cannot see.
            throw new DorisConnectorException("The ADBC driver reported no partitions for ["
                    + sql + "]. Doris cannot tell that apart from a lost result set, so the query fails"
                    + " rather than returning no rows. Set '"
                    + AdbcCatalogProperties.PARTITIONED_READ
                    + "'='disabled' on this catalog to read it through a single statement instead.");
        }
        int limit = props.getMaxPartitions();
        if (descriptors.size() > limit) {
            // Deliberately not a fallback to the single-range path: the source has already executed the
            // statement to produce these descriptors, so re-planning it as a statement would execute it a
            // second time while this result set sits unread until the source times it out.
            throw new DorisConnectorException("The ADBC driver split [" + sql + "] into "
                    + descriptors.size() + " partitions, over the '"
                    + AdbcCatalogProperties.MAX_PARTITIONS + "' limit of " + limit
                    + ". Raise that property, narrow the query, or set '"
                    + AdbcCatalogProperties.PARTITIONED_READ + "'='disabled' on this catalog.");
        }
        List<ConnectorScanRange> ranges = new ArrayList<>(descriptors.size());
        for (PartitionDescriptor descriptor : descriptors) {
            ranges.add(rangeBuilder().partitionDescriptor(encode(descriptor)).build());
        }
        LOG.debug("ADBC scan planned into {} partitions: {}", ranges.size(), sql);
        return ranges;
    }

    /**
     * No {@code getHosts()} goes with these ranges: the descriptor is opaque bytes FE does not decode, so
     * it has no location to prefer and Doris assigns them by its own policy. Decoding a Flight endpoint's
     * location to schedule for affinity is a later optimization, not a correctness matter.
     */
    private static String encode(PartitionDescriptor descriptor) {
        ByteBuffer buffer = descriptor.getDescriptor();
        byte[] bytes = new byte[buffer.remaining()];
        // duplicate() so reading the bytes does not consume the descriptor's own buffer.
        buffer.duplicate().get(bytes);
        return Base64.getEncoder().encodeToString(bytes);
    }

    /**
     * The properties the scan node reads before it has any ranges: the reader to use, and the statement to
     * show in {@code EXPLAIN}.
     *
     * <p>It regenerates the statement rather than reusing one, because {@code EXPLAIN} never calls
     * {@link #planScan}. The two must agree or {@code EXPLAIN} describes a query that will not be run; a
     * test pins that they do.
     *
     * <p><b>This path must never ask for partitions</b> -- and neither may {@link #planScan} when it is
     * planning an {@code EXPLAIN}, which it also is. On a Flight SQL source that call executes the query,
     * so either one would run the very query the user asked only to have described. Tests pin both by
     * planning with a client that refuses to be opened.
     */
    @Override
    public Map<String, String> getScanNodeProperties(ConnectorSession session,
            ConnectorTableHandle handle, List<ConnectorColumnHandle> columns,
            Optional<ConnectorExpression> filter) {
        Map<String, String> props = new HashMap<>();
        props.put(ScanNodePropertyKeys.FILE_FORMAT_TYPE, FILE_FORMAT);
        // No row limit here: EXPLAIN has none to show, and planScan applies its own.
        props.put(ScanNodePropertyKeys.REMOTE_QUERY,
                buildQuery(adbcHandle(handle), columns, filter, -1L).getSql());
        return props;
    }

    private AdbcQueryBuilder.BuiltQuery buildQuery(AdbcTableHandle handle,
            List<ConnectorColumnHandle> columns, Optional<ConnectorExpression> filter, long limit) {
        return AdbcQueryBuilder.build(dialectSelector.select(clientSupplier), handle, columns,
                filter, limit);
    }

    /** Everything a range needs except the work itself; the caller adds a statement or a partition. */
    private AdbcScanRange.Builder rangeBuilder() {
        return new AdbcScanRange.Builder()
                .driverPath(driverPath.toString())
                .driverEntrypoint(props.getDriverEntrypoint())
                .uri(props.getUri())
                .username(props.getUser())
                .password(props.getPassword())
                .driverOptions(props.getDriverOptions());
    }

    private static AdbcTableHandle adbcHandle(ConnectorTableHandle handle) {
        if (handle instanceof AdbcTableHandle) {
            return (AdbcTableHandle) handle;
        }
        // Reached by a passthrough-SQL table handle (a table-valued function forwarding raw SQL), which
        // this connector does not accept yet. Naming the handle keeps the message useful when it does.
        throw new DorisConnectorException("An adbc catalog cannot scan through a "
                + handle.getClass().getSimpleName() + "; only its own tables are readable.");
    }
}
