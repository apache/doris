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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    /**
     * @param configuredSqlDialect the catalog's {@code sql_dialect}, empty when it set none. Taken as a
     *                             value rather than read out of the property map, so that the map has
     *                             exactly one reader ({@link AdbcCatalogProperties}) and the blank-means-
     *                             unset rule cannot be spelled differently in two places.
     */
    public AdbcDialectSelector(String configuredSqlDialect) {
        this.configuredName = configuredSqlDialect == null || configuredSqlDialect.trim().isEmpty()
                ? null : configuredSqlDialect.trim();
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
            return connectionSource.get().vendorName();
        } catch (Exception e) {
            // Includes the driver refusing getInfo outright. Not knowing the vendor is the normal case for
            // an arbitrary ADBC source, and ANSI is exactly the answer for it.
            LOG.info("ADBC driver did not report its vendor, so the default SQL dialect is used: {}",
                    e.toString());
            return Optional.empty();
        }
    }
}
