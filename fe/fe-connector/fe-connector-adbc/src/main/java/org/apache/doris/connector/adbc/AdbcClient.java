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

import org.apache.doris.connector.spi.DorisConnectorException;

import org.apache.arrow.adbc.core.AdbcConnection;
import org.apache.arrow.adbc.core.AdbcDatabase;
import org.apache.arrow.adbc.core.AdbcDriver;
import org.apache.arrow.adbc.core.AdbcException;
import org.apache.arrow.adbc.driver.jni.JniDriver;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.Closeable;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Owns the ADBC objects for one catalog: the Arrow allocator, the {@code AdbcDatabase}, and the short-lived
 * connections metadata operations borrow.
 *
 * <p><b>Why the JNI driver and not a pure-Java one.</b> The JNI bridge wraps the same C driver manager BE
 * links statically and dlopens the same driver library, so anything opaque that crosses between FE and BE
 * (partition descriptors) is produced and consumed by one implementation. The pure-Java Flight SQL driver
 * would be lighter on FE, but it serializes partition descriptors as a different protobuf message than the
 * Go driver expects, and protobuf mis-parses it silently rather than failing -- and it does not implement
 * {@code getTableSchema} at all, which would force FE to derive column types from XDBC type codes while BE
 * reads real Arrow types.
 *
 * <p><b>Nothing is opened eagerly.</b> Driver loading is deferred to first use rather than done in the
 * constructor: an FE follower replaying the edit log builds every catalog, and its filesystem layout need
 * not match the leader's, so a missing driver file would otherwise stop FE from starting instead of
 * failing the one catalog that cannot work.
 */
public class AdbcClient implements Closeable {

    private final Path driverPath;
    private final String driverUrl;
    private final String entrypoint;
    private final String uri;
    private final String user;
    private final String password;
    private final Map<String, String> driverOptions;

    private volatile BufferAllocator allocator;
    private volatile AdbcDatabase database;
    private volatile boolean closed;

    public AdbcClient(Path driverPath, String driverUrl, String entrypoint, String uri,
            String user, String password, Map<String, String> driverOptions) {
        this.driverPath = driverPath;
        this.driverUrl = driverUrl;
        this.entrypoint = entrypoint;
        this.uri = uri;
        this.user = user;
        this.password = password;
        this.driverOptions = driverOptions;
    }

    /**
     * Runs {@code body} on a fresh connection and closes it afterwards.
     *
     * <p>One connection per operation, deliberately: connection reuse only starts paying off once scans
     * hold connections across a query, and a pool that outlives a statement would have to answer for
     * per-user authorization on borrowed connections. Metadata calls are infrequent enough that the open
     * cost does not show.
     */
    public <T> T withConnection(AdbcConnectionCall<T> body) {
        AdbcDatabase db = getOrOpenDatabase();
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            // The plugin loads child-first and the ADBC/Arrow classes live in it; pin the context
            // classloader so any name-based lookup underneath resolves the plugin's copies rather than a
            // parent one (which would ClassCast against the child-loaded objects it is handed).
            Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
            try (AdbcConnection connection = db.connect()) {
                return body.apply(connection);
            }
        } catch (AdbcException e) {
            throw translate(e, "ADBC operation failed");
        } catch (DorisConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new DorisConnectorException("ADBC operation failed: " + e.getMessage(), e);
        } finally {
            Thread.currentThread().setContextClassLoader(previous);
        }
    }

    private AdbcDatabase getOrOpenDatabase() {
        if (closed) {
            throw new DorisConnectorException("AdbcClient has been closed");
        }
        AdbcDatabase db = database;
        if (db != null) {
            return db;
        }
        synchronized (this) {
            if (closed) {
                throw new DorisConnectorException("AdbcClient has been closed");
            }
            if (database == null) {
                AdbcDriverPathResolver.checkExists(driverPath, driverUrl);
                ClassLoader previous = Thread.currentThread().getContextClassLoader();
                try {
                    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
                    if (allocator == null) {
                        allocator = new RootAllocator();
                    }
                    database = new JniDriver(allocator).open(buildParameters());
                } catch (AdbcException e) {
                    throw translate(e, "Failed to open the ADBC driver " + driverPath);
                } catch (UnsatisfiedLinkError e) {
                    throw new DorisConnectorException("Failed to load the ADBC JNI bridge."
                            + " FE loads it from the directory named by the JVM property"
                            + " arrow.adbc.driver.jni.library.path (set in fe.conf, normally"
                            + " ${DORIS_HOME}/lib): " + e.getMessage(), e);
                } finally {
                    Thread.currentThread().setContextClassLoader(previous);
                }
            }
            return database;
        }
    }

    private Map<String, Object> buildParameters() {
        Map<String, Object> params = new HashMap<>();
        JniDriver.PARAM_DRIVER.set(params, driverPath.toString());
        AdbcDriver.PARAM_URI.set(params, uri);
        if (user != null && !user.isEmpty()) {
            AdbcDriver.PARAM_USERNAME.set(params, user);
        }
        if (password != null && !password.isEmpty()) {
            AdbcDriver.PARAM_PASSWORD.set(params, password);
        }
        if (entrypoint != null && !entrypoint.trim().isEmpty()) {
            // The C driver manager reads this key to pick the init symbol to dlsym; verified against the
            // SQLite driver, where a bogus value fails with "dlsym(...) failed: undefined symbol".
            params.put("entrypoint", entrypoint.trim());
        }
        // Option names keep their "adbc." prefix; see AdbcConnectorProperties.DRIVER_OPTION_PREFIX.
        params.putAll(driverOptions);
        return params;
    }

    /**
     * Carries the ADBC status/SQLSTATE/vendor code into a Doris error.
     *
     * <p>The driver's own message is appended only when it says something: the SQLite driver answers
     * {@code NOT_IMPLEMENTED} with the literal text {@code (unknown error)}, so a message built by
     * forwarding {@code getMessage()} would tell a user nothing at all.
     */
    static DorisConnectorException translate(AdbcException e, String context) {
        StringBuilder sb = new StringBuilder(context);
        sb.append(" [status=").append(e.getStatus());
        if (e.getSqlState() != null && !e.getSqlState().isEmpty()) {
            sb.append(", sqlState=").append(e.getSqlState());
        }
        if (e.getVendorCode() != 0) {
            sb.append(", vendorCode=").append(e.getVendorCode());
        }
        sb.append(']');
        String message = e.getMessage();
        if (message != null && !message.isEmpty() && !"(unknown error)".equals(message)) {
            sb.append(": ").append(message);
        }
        return new DorisConnectorException(sb.toString(), e);
    }

    @Override
    public synchronized void close() {
        closed = true;
        try {
            if (database != null) {
                database.close();
            }
        } catch (Exception e) {
            throw new DorisConnectorException("Failed to close the ADBC database: " + e.getMessage(), e);
        } finally {
            database = null;
            if (allocator != null) {
                allocator.close();
                allocator = null;
            }
        }
    }

    /** A body that runs against a borrowed ADBC connection. */
    @FunctionalInterface
    public interface AdbcConnectionCall<T> {
        T apply(AdbcConnection connection) throws Exception;
    }
}
