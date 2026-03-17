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

package org.apache.doris.jdbc;

import org.apache.doris.cloud.security.SecurityChecker;
import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

/**
 * JdbcConnectionTester is a lightweight JNI-invocable class for testing JDBC connections.
 * It extends JniScanner to reuse the JniConnector infrastructure on the C++ side.
 *
 * <p>Usage: C++ creates a JniConnector with this class name and connection params,
 * calls open() to test the connection, then close() to clean up.
 *
 * <p>The getNext() method is a no-op since this class is only used for testing.
 *
 * <p>Parameters:
 * <ul>
 *   <li>jdbc_url, jdbc_user, jdbc_password, jdbc_driver_class, jdbc_driver_url</li>
 *   <li>query_sql — the test query to run</li>
 *   <li>catalog_id, connection_pool_min_size, connection_pool_max_size, etc.</li>
 *   <li>clean_datasource — if "true", close the datasource pool on close()</li>
 * </ul>
 */
public class JdbcConnectionTester extends JniScanner {
    private static final Logger LOG = Logger.getLogger(JdbcConnectionTester.class);

    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;
    private final String jdbcDriverClass;
    private final String jdbcDriverUrl;
    private final String querySql;
    private final long catalogId;
    private final int connectionPoolMinSize;
    private final int connectionPoolMaxSize;
    private final int connectionPoolMaxWaitTime;
    private final int connectionPoolMaxLifeTime;
    private final boolean connectionPoolKeepAlive;
    private final boolean cleanDatasource;
    private final JdbcTypeHandler typeHandler;

    private HikariDataSource hikariDataSource = null;
    private Connection conn = null;
    private PreparedStatement stmt = null;
    private ClassLoader classLoader = null;

    public JdbcConnectionTester(int batchSize, Map<String, String> params) {
        this.jdbcUrl = params.getOrDefault("jdbc_url", "");
        this.jdbcUser = params.getOrDefault("jdbc_user", "");
        this.jdbcPassword = params.getOrDefault("jdbc_password", "");
        this.jdbcDriverClass = params.getOrDefault("jdbc_driver_class", "");
        this.jdbcDriverUrl = params.getOrDefault("jdbc_driver_url", "");
        this.querySql = params.getOrDefault("query_sql", "SELECT 1");
        this.catalogId = Long.parseLong(params.getOrDefault("catalog_id", "0"));
        this.connectionPoolMinSize = Integer.parseInt(
                params.getOrDefault("connection_pool_min_size", "1"));
        this.connectionPoolMaxSize = Integer.parseInt(
                params.getOrDefault("connection_pool_max_size", "10"));
        this.connectionPoolMaxWaitTime = Integer.parseInt(
                params.getOrDefault("connection_pool_max_wait_time", "5000"));
        this.connectionPoolMaxLifeTime = Integer.parseInt(
                params.getOrDefault("connection_pool_max_life_time", "1800000"));
        this.connectionPoolKeepAlive = "true".equalsIgnoreCase(
                params.getOrDefault("connection_pool_keep_alive", "false"));
        this.cleanDatasource = "true".equalsIgnoreCase(
                params.getOrDefault("clean_datasource", "false"));

        // Select database-specific type handler for validation query
        String tableType = params.getOrDefault("table_type", "");
        this.typeHandler = JdbcTypeHandlerFactory.create(tableType);

        // Initialize with a dummy schema since this is only for connection testing
        initTableInfo(new ColumnType[] {ColumnType.parseType("result", "int")},
                new String[] {"result"}, batchSize);
    }

    /**
     * Open the connection and execute the test query to verify connectivity.
     */
    @Override
    public void open() throws IOException {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            URL[] urls = {new URL(jdbcDriverUrl)};
            ClassLoader parent = getClass().getClassLoader();
            this.classLoader = URLClassLoader.newInstance(urls, parent);
            Thread.currentThread().setContextClassLoader(classLoader);

            String cacheKey = createCacheKey();
            hikariDataSource = JdbcDataSource.getDataSource().getSource(cacheKey);
            if (hikariDataSource == null) {
                synchronized (JdbcConnectionTester.class) {
                    hikariDataSource = JdbcDataSource.getDataSource().getSource(cacheKey);
                    if (hikariDataSource == null) {
                        HikariDataSource ds = new HikariDataSource();
                        ds.setDriverClassName(jdbcDriverClass);
                        ds.setJdbcUrl(SecurityChecker.getInstance().getSafeJdbcUrl(jdbcUrl));
                        ds.setUsername(jdbcUser);
                        ds.setPassword(jdbcPassword);
                        ds.setMinimumIdle(connectionPoolMinSize);
                        ds.setMaximumPoolSize(connectionPoolMaxSize);
                        ds.setConnectionTimeout(connectionPoolMaxWaitTime);
                        ds.setMaxLifetime(connectionPoolMaxLifeTime);
                        ds.setIdleTimeout(connectionPoolMaxLifeTime / 2L);
                        // Use type handler for database-specific validation query
                        // (e.g. Oracle: "SELECT 1 FROM dual", DB2: "select 1 from sysibm.sysdummy1")
                        typeHandler.setValidationQuery(ds);
                        if (connectionPoolKeepAlive) {
                            ds.setKeepaliveTime(connectionPoolMaxLifeTime / 5L);
                        }
                        hikariDataSource = ds;
                        JdbcDataSource.getDataSource().putSource(cacheKey, hikariDataSource);
                    }
                }
            }

            conn = hikariDataSource.getConnection();
            stmt = conn.prepareStatement(querySql);
            ResultSet rs = stmt.executeQuery();
            if (!rs.next()) {
                throw new IOException(
                        "Failed to test connection: query executed but returned no results.");
            }
            rs.close();
            LOG.info("JdbcConnectionTester: connection test succeeded for " + jdbcUrl);
        } catch (Exception e) {
            throw new IOException("Failed to test JDBC connection: " + e.getMessage(), e);
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }

    /**
     * No-op: connection tester does not read data.
     */
    @Override
    protected int getNext() throws IOException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        try {
            if (stmt != null && !stmt.isClosed()) {
                stmt.close();
            }
            if (conn != null && !conn.isClosed()) {
                conn.close();
            }
        } catch (SQLException e) {
            LOG.warn("JdbcConnectionTester close error: " + e.getMessage(), e);
        } finally {
            stmt = null;
            conn = null;
            if (cleanDatasource && hikariDataSource != null) {
                hikariDataSource.close();
                JdbcDataSource.getDataSource().getSourcesMap().remove(createCacheKey());
                hikariDataSource = null;
            }
        }
    }

    @Override
    public Map<String, String> getStatistics() {
        return Collections.emptyMap();
    }

    private String createCacheKey() {
        return catalogId + "#" + jdbcUrl + "#" + jdbcUser + "#" + jdbcDriverClass;
    }
}
