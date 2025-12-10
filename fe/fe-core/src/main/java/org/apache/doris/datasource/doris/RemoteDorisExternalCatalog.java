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

package org.apache.doris.datasource.doris;

import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.constants.RemoteDorisProperties;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RemoteDorisExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(RemoteDorisExternalCatalog.class);

    private RemoteDorisRestClient dorisRestClient;
    private FeServiceClient client;
    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            RemoteDorisProperties.FE_THRIFT_HOSTS,
            RemoteDorisProperties.FE_HTTP_HOSTS,
            RemoteDorisProperties.FE_ARROW_HOSTS,
            RemoteDorisProperties.USER,
            RemoteDorisProperties.PASSWORD,
            RemoteDorisProperties.USE_ARROW_FLIGHT
    );

    /**
     * Default constructor for DorisExternalCatalog.
     */
    public RemoteDorisExternalCatalog(long catalogId, String name, String resource,
                                      Map<String, String> props, String comment) {
        super(catalogId, name, InitCatalogLog.Type.REMOTE_DORIS, comment);
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
        if (!useArrowFlight() && Config.isCloudMode()) {
            // TODO we not validate it in cloud mode, so currently not support it
            throw new DdlException("Cloud mode is not supported when "
                    + RemoteDorisProperties.USE_ARROW_FLIGHT + " is false");
        }
    }

    public List<String> getFeNodes() {
        return parseHttpHosts(catalogProperty.getOrDefault(RemoteDorisProperties.FE_HTTP_HOSTS, ""));
    }

    public List<String> getFeArrowNodes() {
        return parseArrowHosts(catalogProperty.getOrDefault(RemoteDorisProperties.FE_ARROW_HOSTS, ""));
    }

    public List<TNetworkAddress> getFeThriftNodes() {
        String addresses = catalogProperty.getOrDefault(RemoteDorisProperties.FE_THRIFT_HOSTS, "");
        List<TNetworkAddress> tAddresses = new ArrayList<>();
        for (String address : addresses.split(",")) {
            int index = address.lastIndexOf(":");
            String host = address.substring(0, index);
            int port = Integer.parseInt(address.substring(index + 1));
            TNetworkAddress thriftAddress = new TNetworkAddress(host, port);
            tAddresses.add(thriftAddress);
        }
        return tAddresses;
    }

    public String getUsername() {
        return catalogProperty.getOrDefault(RemoteDorisProperties.USER, "");
    }

    public String getPassword() {
        return catalogProperty.getOrDefault(RemoteDorisProperties.PASSWORD, "");
    }

    public boolean enableSsl() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(RemoteDorisProperties.METADATA_HTTP_SSL_ENABLED,
            "false"));
    }

    public boolean isCompatible() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(RemoteDorisProperties.COMPATIBLE,
            "false"));
    }

    public boolean enableParallelResultSink() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(RemoteDorisProperties.ENABLE_PARALLEL_RESULT_SINK,
            "true"));
    }

    public int getQueryRetryCount() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.QUERY_RETRY_COUNT,
            "3"));
    }

    public int getQueryTimeoutSec() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.QUERY_TIMEOUT_SEC,
            "15"));
    }

    public int getMetadataSyncRetryCount() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.METADATA_SYNC_RETRIES_COUNT,
            "3"));
    }

    public int getMetadataMaxIdleConnections() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.METADATA_MAX_IDLE_CONNECTIONS,
            "5"));
    }

    public int getMetadataKeepAliveDurationSec() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.METADATA_KEEP_ALIVE_DURATION_SEC,
            "300"));
    }

    public int getMetadataConnectTimeoutSec() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.METADATA_CONNECT_TIMEOUT_SEC,
            "10"));
    }

    public int getMetadataReadTimeoutSec() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.METADATA_READ_TIMEOUT_SEC,
            "10"));
    }

    public int getMetadataWriteTimeoutSec() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.METADATA_WRITE_TIMEOUT_SEC,
            "10"));
    }

    public int getMetadataCallTimeoutSec() {
        return Integer.parseInt(catalogProperty.getOrDefault(RemoteDorisProperties.METADATA_CALL_TIMEOUT_SEC,
            "0"));
    }

    public boolean useArrowFlight() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(RemoteDorisProperties.USE_ARROW_FLIGHT,
                "true"));
    }

    @Override
    protected void initLocalObjectsImpl() {
        if (isCompatible()) {
            dorisRestClient = new RemoteDorisCompatibleRestClient(
                getFeNodes(), getUsername(), getPassword(), enableSsl(), getMetadataSyncRetryCount(),
                getMetadataMaxIdleConnections(), getMetadataKeepAliveDurationSec(), getMetadataConnectTimeoutSec(),
                getMetadataReadTimeoutSec(), getMetadataWriteTimeoutSec(), getMetadataCallTimeoutSec()
                );
        } else {
            dorisRestClient = new RemoteDorisRestClient(
                getFeNodes(), getUsername(), getPassword(), enableSsl(), getMetadataSyncRetryCount(),
                getMetadataMaxIdleConnections(), getMetadataKeepAliveDurationSec(), getMetadataConnectTimeoutSec(),
                getMetadataReadTimeoutSec(), getMetadataWriteTimeoutSec(), getMetadataCallTimeoutSec());
        }

        if (!dorisRestClient.health()) {
            throw new RuntimeException("Failed to connect to Doris cluster,"
                + " please check your Doris cluster or your Doris catalog configuration.");
        }
        client = new FeServiceClient(name, getFeThriftNodes(), getUsername(), getPassword(),
                getMetadataSyncRetryCount(), getMetadataReadTimeoutSec());
    }

    protected List<String> listDatabaseNames() {
        makeSureInitialized();
        return dorisRestClient.getDatabaseNameList();
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        return dorisRestClient.getTablesNameList(dbName);
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        return dorisRestClient.isTableExist(dbName, tblName);
    }

    public RemoteDorisRestClient getDorisRestClient() {
        return dorisRestClient;
    }

    public FeServiceClient getFeServiceClient() {
        return client;
    }

    private List<String> parseHttpHosts(String hosts) {
        String[] hostUrls = hosts.trim().split(",");
        fillUrlsWithSchema(hostUrls, enableSsl());
        return Arrays.asList(hostUrls);
    }

    private void fillUrlsWithSchema(String[] urls, boolean isSslEnabled) {
        for (int i = 0; i < urls.length; i++) {
            String seed = urls[i].trim();
            if (!seed.startsWith("http://") && !seed.startsWith("https://")) {
                urls[i] = (isSslEnabled ? "https://" : "http://") + seed;
            }
        }
    }

    private List<String> parseArrowHosts(String hosts) {
        return Arrays.asList(hosts.trim().split(","));
    }
}
