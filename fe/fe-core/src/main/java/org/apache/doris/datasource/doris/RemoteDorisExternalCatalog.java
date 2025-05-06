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

import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.constants.RemoteDorisProperties;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RemoteDorisExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(RemoteDorisExternalCatalog.class);

    private RemoteDorisRestClient dorisRestClient;
    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            RemoteDorisProperties.FE_HTTP_HOSTS,
            RemoteDorisProperties.FE_ARROW_HOSTS,
            RemoteDorisProperties.USER,
            RemoteDorisProperties.PASSWORD
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
    }

    public List<String> getFeNodes() {
        return parseHttpHosts(catalogProperty.getOrDefault(RemoteDorisProperties.FE_HTTP_HOSTS, ""));
    }

    public List<String> getFeArrowNodes() {
        return parseArrowHosts(catalogProperty.getOrDefault(RemoteDorisProperties.FE_ARROW_HOSTS, ""));
    }

    public String getUsername() {
        return catalogProperty.getOrDefault(RemoteDorisProperties.USER, "");
    }

    public String getPassword() {
        return catalogProperty.getOrDefault(RemoteDorisProperties.PASSWORD, "");
    }

    public boolean enableSsl() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(RemoteDorisProperties.HTTP_SSL_ENABLED,
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

    @Override
    protected void initLocalObjectsImpl() {
        if (isCompatible()) {
            dorisRestClient =
                    new RemoteDorisCompatibleRestClient(getFeNodes(), getUsername(), getPassword(), enableSsl());
        } else {
            dorisRestClient = new RemoteDorisRestClient(getFeNodes(), getUsername(), getPassword(), enableSsl());
        }

        if (!dorisRestClient.health()) {
            throw new RuntimeException("Failed to connect to Doris cluster,"
                + " please check your Doris cluster or your Doris catalog configuration.");
        }
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
