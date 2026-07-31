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

import org.apache.doris.connector.spi.scan.ConnectorScanRange;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * One unit of ADBC work: the connection parameters plus either the statement to run or the one partition
 * to read.
 *
 * <p>A scan plans one range carrying a statement when the driver has no partitioned execution, and
 * otherwise one range per partition the driver reported -- which is how a single remote query ends up read
 * by several backends at once.
 *
 * <p><b>The parameter names below are a contract with BE.</b> Its reader looks them up literally
 * ({@code be/src/format_v2/table/adbc_reader.cpp}, the {@code kParam*} constants), so a rename on either
 * side has to happen on both. Two are easy to get wrong: the credentials travel as {@code username} /
 * {@code password} -- ADBC's own option names, not this connector's {@code user} property -- and an
 * {@code adbc.}-prefixed option keeps its prefix, because the prefix is part of the ADBC option name.
 */
public class AdbcScanRange implements ConnectorScanRange {

    private static final long serialVersionUID = 1L;

    /** Selects BE's ADBC reader. Must match the string BE's FileScannerV2 dispatches on. */
    private static final String TABLE_FORMAT_TYPE = "adbc";

    /**
     * There is no file. The path is carried anyway because the generic scan machinery expects one; a
     * scheme-less placeholder is used rather than something like {@code adbc://...} so nothing tries to
     * resolve it as a filesystem. Mirrors the remote_doris scan node, which reads Arrow the same way.
     */
    private static final String VIRTUAL_PATH = "/dummyPath";

    static final String PARAM_DRIVER_PATH = "driver_path";
    static final String PARAM_DRIVER_ENTRYPOINT = "driver_entrypoint";
    static final String PARAM_URI = "uri";
    static final String PARAM_USERNAME = "username";
    static final String PARAM_PASSWORD = "password";
    static final String PARAM_QUERY_SQL = "query_sql";
    static final String PARAM_PARTITION_DESCRIPTOR = "partition_descriptor";

    private final Map<String, String> properties;

    private AdbcScanRange(Map<String, String> properties) {
        this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
    }

    @Override
    public Optional<String> getPath() {
        return Optional.of(VIRTUAL_PATH);
    }

    /**
     * Arrow, because the rows arrive as Arrow record batches from the driver rather than from a file. This
     * is what routes the scan to BE's Arrow reader path; the JNI default would land it in a scanner with no
     * ADBC branch.
     */
    @Override
    public String getFileFormat() {
        return "arrow";
    }

    @Override
    public String getTableFormatType() {
        return TABLE_FORMAT_TYPE;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    /**
     * Writes the parameters into the ADBC slot of the range descriptor.
     *
     * <p>Overriding this is not optional: the inherited implementation writes to {@code jdbc_params}, which
     * BE's ADBC reader never reads, so the scan would reach BE with no connection parameters at all and
     * fail on a missing driver path.
     */
    @Override
    public void populateRangeParams(TTableFormatFileDesc formatDesc, TFileRangeDesc rangeDesc) {
        formatDesc.setAdbcParams(new LinkedHashMap<>(properties));
    }

    /** Collects the parameters for one range. */
    public static class Builder {

        private final Map<String, String> props = new LinkedHashMap<>();

        /**
         * The absolute path of the driver library, as FE resolved it.
         *
         * <p>BE loads this path verbatim -- it has no drivers directory of its own to resolve a bare name
         * against -- so FE and every BE must have the same driver file at the same path. That is what the
         * deployment asks for, and it matches how a JDBC catalog's driver reference already works.
         */
        public Builder driverPath(String path) {
            props.put(PARAM_DRIVER_PATH, path);
            return this;
        }

        /** Optional; empty lets the driver manager infer the init symbol. */
        public Builder driverEntrypoint(String entrypoint) {
            putIfPresent(PARAM_DRIVER_ENTRYPOINT, entrypoint);
            return this;
        }

        public Builder uri(String uri) {
            props.put(PARAM_URI, uri);
            return this;
        }

        /** The connector's {@code user} property, under ADBC's name for it. */
        public Builder username(String user) {
            putIfPresent(PARAM_USERNAME, user);
            return this;
        }

        public Builder password(String password) {
            putIfPresent(PARAM_PASSWORD, password);
            return this;
        }

        public Builder querySql(String sql) {
            props.put(PARAM_QUERY_SQL, sql);
            return this;
        }

        /**
         * One partition of an already-executed remote query, base64 of the driver's opaque descriptor.
         *
         * <p>Base64 because the descriptor is arbitrary bytes (a serialized protobuf, for a Flight SQL
         * driver) and the range parameters are a string map. It is opaque on this side on purpose: only the
         * driver that produced it can read it, which is why FE and BE must load the same driver library.
         */
        public Builder partitionDescriptor(String base64Descriptor) {
            props.put(PARAM_PARTITION_DESCRIPTOR, base64Descriptor);
            return this;
        }

        /** Driver options, names unchanged -- the {@code adbc.} prefix is part of the option name. */
        public Builder driverOptions(Map<String, String> options) {
            props.putAll(options);
            return this;
        }

        /**
         * @throws IllegalStateException if the range would carry both kinds of work or neither. BE fails
         *         the same way on the same condition; catching it here names the planning bug instead of
         *         letting one backend report it halfway through a query.
         */
        public AdbcScanRange build() {
            boolean hasStatement = props.containsKey(PARAM_QUERY_SQL);
            boolean hasPartition = props.containsKey(PARAM_PARTITION_DESCRIPTOR);
            if (hasStatement == hasPartition) {
                throw new IllegalStateException("An ADBC scan range runs either a statement or one"
                        + " partition of an already-executed query, but this one carries "
                        + (hasStatement ? "both" : "neither"));
            }
            return new AdbcScanRange(props);
        }

        private void putIfPresent(String key, String value) {
            // An empty value is left out rather than sent as "": BE skips empty credentials but hands any
            // present entrypoint to dlsym, where "" is not the same as "use the default".
            if (value != null && !value.isEmpty()) {
                props.put(key, value);
            }
        }
    }
}
