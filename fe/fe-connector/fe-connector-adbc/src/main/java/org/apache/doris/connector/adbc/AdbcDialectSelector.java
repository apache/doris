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

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcInfoCode;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Picks one catalog's SQL dialect, once, and keeps it.
 *
 * <p>Order: the {@code sql_dialect} property, then the vendor the driver reports, then ANSI. The property
 * comes first because it is the user's escape hatch for a source whose vendor string is unhelpful or shared,
 * and a probe that could override it would make the property advisory.
 *
 * <p><b>The probe runs at most once per catalog.</b> It is a remote call on a query's planning path, and the
 * answer is a property of the driver rather than of the query. It also has to be allowed to fail: the
 * metadata phase measured drivers implementing these context calls one at a time -- the SQLite driver
 * answers {@code getCurrentCatalog} but rejects {@code getCurrentDbSchema} -- so a failed probe means
 * "unknown vendor", never a failed query.
 */
public final class AdbcDialectSelector {

    private static final Logger LOG = LogManager.getLogger(AdbcDialectSelector.class);

    private final String configuredName;
    private final AtomicReference<AdbcDialect> resolved = new AtomicReference<>();

    public AdbcDialectSelector(Map<String, String> properties) {
        String value = properties.get(AdbcConnectorProperties.SQL_DIALECT);
        this.configuredName = value == null || value.trim().isEmpty() ? null : value.trim();
    }

    /**
     * Validates the configured name without touching the source, so a misspelled {@code sql_dialect} is
     * rejected when the catalog is created rather than when a query first scans.
     */
    public void validateConfiguredName() {
        if (configuredName != null) {
            AdbcDialectRegistry.require(configuredName);
        }
    }

    /**
     * The dialect for this catalog. {@code connectionSource} supplies a connection for the vendor probe and
     * is not used once the answer is known.
     */
    public AdbcDialect select(Supplier<AdbcClient> connectionSource) {
        AdbcDialect known = resolved.get();
        if (known != null) {
            return known;
        }
        AdbcDialect selected = resolve(connectionSource);
        // A racing probe would have reached the same answer, so whichever lands first wins.
        resolved.compareAndSet(null, selected);
        return resolved.get();
    }

    private AdbcDialect resolve(Supplier<AdbcClient> connectionSource) {
        if (configuredName != null) {
            return AdbcDialectRegistry.require(configuredName);
        }
        Optional<String> vendor = probeVendorName(connectionSource);
        if (vendor.isPresent()) {
            Optional<AdbcDialect> byVendor = AdbcDialectRegistry.forVendor(vendor.get());
            if (byVendor.isPresent()) {
                LOG.info("ADBC catalog uses the '{}' SQL dialect, matched on vendor '{}'",
                        byVendor.get().name(), vendor.get());
                return byVendor.get();
            }
            LOG.info("ADBC vendor '{}' has no registered SQL dialect; using '{}'",
                    vendor.get(), AdbcDialectRegistry.defaultDialect().name());
        }
        return AdbcDialectRegistry.defaultDialect();
    }

    private Optional<String> probeVendorName(Supplier<AdbcClient> connectionSource) {
        try {
            return connectionSource.get().withConnection(AdbcDialectSelector::readVendorName);
        } catch (Exception e) {
            // Includes the driver refusing getInfo outright. Not knowing the vendor is the normal case for
            // an arbitrary ADBC source, and ANSI is exactly the answer for it.
            LOG.info("ADBC driver did not report its vendor, so the default SQL dialect is used: {}",
                    e.toString());
            return Optional.empty();
        }
    }

    /**
     * Reads {@code VENDOR_NAME} out of the {@code getInfo} result.
     *
     * <p>The result is a two-column table -- an integer info code and a dense union holding the value in
     * whichever of six types that code uses. Both columns are read through the generic
     * {@code ValueVector.getObject} rather than by casting to a concrete vector class: the union's child
     * layout is the driver's to choose, and a cast that assumed one would fail on a driver that laid it out
     * differently, for a probe whose whole purpose is to tolerate not being answered.
     *
     * <p>The requested code is re-checked per row because a driver may answer with more rows than were
     * asked for.
     */
    private static Optional<String> readVendorName(AdbcConnection connection) throws Exception {
        try (ArrowReader reader = connection.getInfo(new AdbcInfoCode[] {AdbcInfoCode.VENDOR_NAME})) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                FieldVector codes = root.getVector("info_name");
                FieldVector values = root.getVector("info_value");
                if (codes == null || values == null) {
                    return Optional.empty();
                }
                for (int row = 0; row < root.getRowCount(); row++) {
                    Object code = codes.getObject(row);
                    if (!(code instanceof Number)
                            || ((Number) code).intValue() != AdbcInfoCode.VENDOR_NAME.getValue()) {
                        continue;
                    }
                    Object value = values.getObject(row);
                    if (value != null) {
                        return Optional.of(String.valueOf(value));
                    }
                }
            }
        }
        return Optional.empty();
    }
}
