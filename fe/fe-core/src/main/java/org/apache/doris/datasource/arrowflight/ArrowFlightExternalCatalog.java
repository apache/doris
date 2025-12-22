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

package org.apache.doris.datasource.arrowflight;

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.constants.ArrowFlightProperties;

import com.google.common.collect.ImmutableList;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class ArrowFlightExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(ArrowFlightExternalCatalog.class);

    private FlightSqlClientLoadBalancer flightSqlClientLoadBalancer;

    // Flight SQL protocol-defined standard metadata column names.
    public static final String CATALOG_NAME = "catalog_name";
    public static final String DB_SCHEMA_NAME = "db_schema_name";
    public static final String TABLE_NAME = "table_name";
    public static final String TABLE_TYPE = "table_type";
    public static final String TABLE_SCHEMA = "table_schema";

    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            ArrowFlightProperties.HOSTS,
            ArrowFlightProperties.USER,
            ArrowFlightProperties.PASSWORD,
            ArrowFlightProperties.FLIGHT_SQL_CATALOG_NAME
    );

    /**
     * Default constructor for ArrowFlightExternalCatalog.
     */
    public ArrowFlightExternalCatalog(long catalogId, String name, String resource,
                                      Map<String, String> props, String comment) {
        super(catalogId, name, InitCatalogLog.Type.ARROW_FLIGHT, comment);
        this.catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();

        for (String requiredProperty : REQUIRED_PROPERTIES) {
            if (!catalogProperty.getProperties().containsKey(requiredProperty)) {
                throw new DdlException("Required property '" + requiredProperty + "' is missing");
            }
        }
    }

    public List<String> getHosts() {
        String hosts = catalogProperty.getOrDefault(ArrowFlightProperties.HOSTS, "");
        return Arrays.asList(hosts.trim().split(","));
    }

    public String getUsername() {
        return catalogProperty.getOrDefault(ArrowFlightProperties.USER, "");
    }

    public String getPassword() {
        return catalogProperty.getOrDefault(ArrowFlightProperties.PASSWORD, "");
    }

    public int getQueryRetryCount() {
        return Integer.parseInt(catalogProperty.getOrDefault(ArrowFlightProperties.QUERY_RETRY_COUNT, "3"));
    }

    public long getQueryTimeoutMs() {
        return Long.parseLong(catalogProperty.getOrDefault(ArrowFlightProperties.QUERY_TIMEOUT_MS, "10000"));
    }

    public String getFlightSqlCatalogName() {
        return catalogProperty.getOrDefault(ArrowFlightProperties.FLIGHT_SQL_CATALOG_NAME, "");
    }

    public Map<String, String> getSessionProperties() {
        return parseSessionProperties(getProperties());
    }

    public FlightSqlClientLoadBalancer getFlightSqlClientLoadBalancer() {
        return flightSqlClientLoadBalancer;
    }

    @Override
    protected void initLocalObjectsImpl() {
        this.flightSqlClientLoadBalancer = new FlightSqlClientLoadBalancer(getHosts(), getUsername(), getPassword(),
            getQueryTimeoutMs(), getSessionProperties());
    }

    protected List<String> listDatabaseNames() {
        makeSureInitialized();

        for (int retryCount = 0; retryCount < getQueryRetryCount(); retryCount++) {
            try {
                FlightSqlClientLoadBalancer.FlightSqlClientWithOptions clientWithOptions =
                        flightSqlClientLoadBalancer.randomClient();
                FlightSqlClient flightSqlClient = clientWithOptions.getFlightSqlClient();
                CallOption[] callOptions = clientWithOptions.getCallOptions();

                FlightInfo info = flightSqlClient.getSchemas(
                        getFlightSqlCatalogName(),
                        null,
                        callOptions);
                List<String> databaseNames = new ArrayList<>();
                processFlightInfo(info, flightSqlClient, callOptions, root -> {
                    try (VarCharVector nameVector = (VarCharVector) root.getVector(DB_SCHEMA_NAME)) {
                        for (int i = 0; i < root.getRowCount(); i++) {
                            databaseNames.add(nameVector.getObject(i).toString());
                        }
                    }
                });

                return databaseNames;
            } catch (FlightRuntimeException e) {
                LOG.warn("listDatabaseNames init failed, catalog name: {}, retry count: {}", getName(), retryCount, e);
            }
        }

        throw new RuntimeException("listDatabaseNames init failed, catalog name: " + getName());
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();

        for (int retryCount = 0; retryCount < getQueryRetryCount(); retryCount++) {
            try {
                FlightSqlClientLoadBalancer.FlightSqlClientWithOptions clientWithOptions =
                        flightSqlClientLoadBalancer.randomClient();
                FlightSqlClient flightSqlClient = clientWithOptions.getFlightSqlClient();
                CallOption[] callOptions = clientWithOptions.getCallOptions();

                FlightInfo info = flightSqlClient.getTables(
                        getFlightSqlCatalogName(),
                        dbName,
                        null,
                        null,
                        false,
                        callOptions);

                List<String> tableNames = new ArrayList<>();
                processFlightInfo(info, flightSqlClient, callOptions, root -> {
                    try (VarCharVector tableNameVector = (VarCharVector) root.getVector(TABLE_NAME)) {
                        for (int i = 0; i < root.getRowCount(); i++) {
                            tableNames.add(tableNameVector.getObject(i).toString());
                        }
                    }
                });

                return tableNames;
            } catch (FlightRuntimeException e) {
                LOG.warn("listTableNames init failed, catalog name: {}, retry count: {}", getName(), retryCount, e);
            }
        }

        throw new RuntimeException("listTableNames init failed, catalog name: " + getName());
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        for (int retryCount = 0; retryCount < getQueryRetryCount(); retryCount++) {
            try {
                FlightSqlClientLoadBalancer.FlightSqlClientWithOptions clientWithOptions =
                        flightSqlClientLoadBalancer.randomClient();
                FlightSqlClient flightSqlClient = clientWithOptions.getFlightSqlClient();
                CallOption[] callOptions = clientWithOptions.getCallOptions();

                FlightInfo info = flightSqlClient.getTables(
                        getFlightSqlCatalogName(),
                        dbName,
                        tblName,
                        null,
                        false,
                        callOptions);

                AtomicBoolean tableExist = new AtomicBoolean(false);
                processFlightInfo(info, flightSqlClient, callOptions, root -> {
                    try (VarCharVector tableNameVector = (VarCharVector) root.getVector(TABLE_NAME)) {
                        for (int i = 0; i < root.getRowCount(); i++) {
                            if (tblName.equals(tableNameVector.getObject(i).toString())) {
                                tableExist.set(true);
                            }
                        }
                    }
                });
                return tableExist.get();
            } catch (FlightRuntimeException e) {
                LOG.warn("tableExist execution failed, catalog name: {}, retry count: {}", getName(), retryCount, e);
            }
        }

        throw new RuntimeException("tableExist execution failed, catalog name: " + getName());
    }

    @Override
    public void onClose() {
        super.onClose();
        try {
            if (flightSqlClientLoadBalancer != null) {
                flightSqlClientLoadBalancer.close();
            }
        } catch (IOException e) {
            LOG.error("arrow flight catalog client close error.", e);
        }
    }

    private Map<String, String> parseSessionProperties(Map<String, String> props) {
        Map<String, String> sessionProperties = new HashMap<>();
        props.forEach((key, value) -> {
            if (key.startsWith(ArrowFlightProperties.FLIGHT_SQL_SESSION_PRE)) {
                sessionProperties.put(key.substring(ArrowFlightProperties.FLIGHT_SQL_SESSION_PRE.length()), value);
            }
        });
        return sessionProperties;
    }

    private void processFlightInfo(FlightInfo info, FlightSqlClient flightSqlClient, CallOption[] callOptions,
                                   Consumer<VectorSchemaRoot> processStream) {
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = flightSqlClient.getStream(endpoint.getTicket(), callOptions)) {
                while (stream.next()) {
                    try (VectorSchemaRoot root = stream.getRoot()) {
                        processStream.accept(root);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException("process FlightInfo error, catalog name: " + getName(), e);
            }
        }
    }
}
