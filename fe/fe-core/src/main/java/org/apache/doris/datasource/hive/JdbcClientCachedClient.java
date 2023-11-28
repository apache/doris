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

package org.apache.doris.datasource.hive;

import org.apache.doris.catalog.JdbcResource;
import org.apache.doris.datasource.jdbc.client.JdbcClient;
import org.apache.doris.datasource.jdbc.client.JdbcClientConfig;
import org.apache.doris.datasource.jdbc.client.JdbcClientException;

import com.alibaba.druid.pool.DruidDataSource;
import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;

public abstract class JdbcClientCachedClient extends CachedClient {
    private static final Logger LOG = LogManager.getLogger(JdbcClientCachedClient.class);

    protected String dbType;
    protected String jdbcUser;
    protected DruidDataSource dataSource = null;

    protected JdbcClientConfig jdbcClientConfig;

    protected JdbcClientCachedClient(PooledHiveMetaStoreClient pooledHiveMetaStoreClient,
            JdbcClientConfig jdbcClientConfig) {
        super(pooledHiveMetaStoreClient);
        Preconditions.checkNotNull(jdbcClientConfig, "JdbcClientConfig can not be null");
        this.jdbcClientConfig = jdbcClientConfig;
        this.jdbcUser = jdbcClientConfig.getUser();
        String jdbcUrl = jdbcClientConfig.getJdbcUrl();
        this.dbType = JdbcClient.parseDbType(jdbcUrl);
        initializeDataSource(jdbcClientConfig.getPassword(), jdbcUrl, jdbcClientConfig.getDriverUrl(),
                jdbcClientConfig.getDriverClass());
    }

    // Initialize DruidDataSource
    private void initializeDataSource(String password, String jdbcUrl, String driverUrl, String driverClass) {
        ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            // TODO(ftw): The problem here is that the jar package is handled by FE
            //  and URLClassLoader may load the jar package directly into memory
            URL[] urls = {new URL(JdbcResource.getFullDriverUrl(driverUrl))};
            // set parent ClassLoader to null, we can achieve class loading isolation.
            ClassLoader parent = getClass().getClassLoader();
            ClassLoader classLoader = URLClassLoader.newInstance(urls, parent);
            LOG.debug("parent ClassLoader: {}, old ClassLoader: {}, class Loader: {}.",
                    parent, oldClassLoader, classLoader);
            Thread.currentThread().setContextClassLoader(classLoader);
            dataSource = new DruidDataSource();
            dataSource.setDriverClassLoader(classLoader);
            dataSource.setDriverClassName(driverClass);
            dataSource.setUrl(jdbcUrl);
            dataSource.setUsername(jdbcUser);
            dataSource.setPassword(password);
            dataSource.setMinIdle(1);
            dataSource.setInitialSize(1);
            dataSource.setMaxActive(100);
            dataSource.setTimeBetweenEvictionRunsMillis(600000);
            dataSource.setMinEvictableIdleTimeMillis(300000);
            // set connection timeout to 5s.
            // The default is 30s, which is too long.
            // Because when querying information_schema db, BE will call thrift rpc(default timeout is 30s)
            // to FE to get schema info, and may create connection here, if we set it too long and the url is invalid,
            // it may cause the thrift rpc timeout.
            dataSource.setMaxWait(5000);
        } catch (MalformedURLException e) {
            throw new JdbcClientException("MalformedURLException to load class about " + driverUrl, e);
        } finally {
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }

    protected Connection getConnection() throws JdbcClientException {
        Connection conn;
        try {
            conn = dataSource.getConnection();
        } catch (Exception e) {
            String errorMessage = String.format("Can not connect to jdbc due to error: %s", e.getMessage());
            throw new JdbcClientException(errorMessage, e);
        }
        return conn;
    }

    protected void close(AutoCloseable... closeables) {
        for (AutoCloseable closeable : closeables) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (Exception  e) {
                    throw new JdbcClientException("Can not close : ", e);
                }
            }
        }
    }

    @Override
    protected void closeRealClient() {
        dataSource.close();
    }
}
