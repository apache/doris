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

import org.apache.doris.catalog.DorisResource;
import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *
 * timed_refresh_catalog
 */
@Getter
public class DorisExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = LogManager.getLogger(DorisExternalCatalog.class);

    private DorisRestClient dorisRestClient;
    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            DorisResource.FE_HOSTS,
            DorisResource.FE_ARROW_HOSTS,
            DorisResource.USER,
            DorisResource.PASSWORD
    );

    /**
     * Default constructor for DorisExternalCatalog.
     */
    public DorisExternalCatalog(long catalogId, String name, String resource,
                                Map<String, String> props, String comment) {
        super(catalogId, name, InitCatalogLog.Type.DORIS, comment);
        this.catalogProperty = new CatalogProperty(resource, processCompatibleProperties(props));
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

    private Map<String, String> processCompatibleProperties(Map<String, String> props) {
        Map<String, String> properties = Maps.newHashMap();
        for (Map.Entry<String, String> kv : props.entrySet()) {
            properties.put(StringUtils.removeStart(kv.getKey(), DorisResource.DORIS_PROPERTIES_PREFIX), kv.getValue());
        }
        return properties;
    }

    public List<String> getFeNodes() {
        return parseHttpHosts(catalogProperty.getOrDefault(DorisResource.FE_HOSTS, ""));
    }

    public List<String> getFeArrowNodes() {
        return parseArrowHosts(catalogProperty.getOrDefault(DorisResource.FE_ARROW_HOSTS, ""));
    }

    public List<String> getBeNodes() {
        return parseHttpHosts(catalogProperty.getOrDefault(DorisResource.BE_HOSTS, ""));
    }

    public String getUsername() {
        return catalogProperty.getOrDefault(DorisResource.USER, "");
    }

    public String getPassword() {
        return catalogProperty.getOrDefault(DorisResource.PASSWORD, "");
    }

    public Integer getMaxExecBeNum() {
        return Integer.parseInt(catalogProperty.getOrDefault(DorisResource.MAX_EXEC_BE_NUM,
            DorisResource.MAX_EXEC_BE_NUM_DEFAULT_VALUE));
    }

    public boolean enableSsl() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(DorisResource.HTTP_SSL_ENABLED,
            DorisResource.HTTP_SSL_ENABLED_DEFAULT_VALUE));
    }

    public boolean isCompatible() {
        return Boolean.parseBoolean(catalogProperty.getOrDefault(DorisResource.COMPATIBLE,
            DorisResource.COMPATIBLE_DEFAULT_VALUE));
    }

    @Override
    protected void initLocalObjectsImpl() {
        if (isCompatible()) {
            dorisRestClient = new DorisCompatibleRestClient(getFeNodes(), getUsername(), getPassword(), enableSsl());
        } else {
            dorisRestClient = new DorisLastedRestClient(getFeNodes(), getUsername(), getPassword(), enableSsl());
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

    private List<String> parseHttpHosts(String hosts) {
        String sslEnabled = catalogProperty.getOrDefault(DorisResource.HTTP_SSL_ENABLED,
                    DorisResource.HTTP_SSL_ENABLED_DEFAULT_VALUE);
        String[] hostUrls = hosts.trim().split(",");
        DorisResource.fillUrlsWithSchema(hostUrls, Boolean.parseBoolean(sslEnabled));
        return Arrays.asList(hostUrls);
    }

    private List<String> parseArrowHosts(String hosts) {
        return Arrays.asList(hosts.trim().split(","));
    }
}
