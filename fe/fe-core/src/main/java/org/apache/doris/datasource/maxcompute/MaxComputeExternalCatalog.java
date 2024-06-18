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

package org.apache.doris.datasource.maxcompute;


import org.apache.doris.common.DdlException;
import org.apache.doris.common.credentials.CloudCredential;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.InitCatalogLog;
import org.apache.doris.datasource.SessionContext;
import org.apache.doris.datasource.property.constants.MCProperties;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.Partition;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MaxComputeExternalCatalog extends ExternalCatalog {
    private Odps odps;
    private TableTunnel tunnel;
    @SerializedName(value = "region")
    private String region;
    @SerializedName(value = "accessKey")
    private String accessKey;
    @SerializedName(value = "secretKey")
    private String secretKey;
    @SerializedName(value = "publicAccess")
    private boolean enablePublicAccess;
    private static final String odpsUrlTemplate = "http://service.{}.maxcompute.aliyun-inc.com/api";
    private static final String tunnelUrlTemplate = "http://dt.{}.maxcompute.aliyun-inc.com";
    private static String odpsUrl;
    private static String tunnelUrl;
    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            MCProperties.REGION,
            MCProperties.PROJECT
    );

    public MaxComputeExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
                                     String comment) {
        super(catalogId, name, InitCatalogLog.Type.MAX_COMPUTE, comment);
        catalogProperty = new CatalogProperty(resource, props);
        odpsUrl = props.getOrDefault(MCProperties.ODPS_ENDPOINT, "");
        tunnelUrl = props.getOrDefault(MCProperties.TUNNEL_SDK_ENDPOINT, "");
    }

    @Override
    protected void initLocalObjectsImpl() {
        Map<String, String> props = catalogProperty.getProperties();
        String region = props.get(MCProperties.REGION);
        String defaultProject = props.get(MCProperties.PROJECT);
        if (Strings.isNullOrEmpty(region)) {
            throw new IllegalArgumentException("Missing required property '" + MCProperties.REGION + "'.");
        }
        if (Strings.isNullOrEmpty(defaultProject)) {
            throw new IllegalArgumentException("Missing required property '" + MCProperties.PROJECT + "'.");
        }
        if (region.startsWith("oss-")) {
            // may use oss-cn-beijing, ensure compatible
            region = region.replace("oss-", "");
        }
        this.region = region;
        CloudCredential credential = MCProperties.getCredential(props);
        if (!credential.isWhole()) {
            throw new IllegalArgumentException("Max-Compute credential properties '"
                    + MCProperties.ACCESS_KEY + "' and  '" + MCProperties.SECRET_KEY + "' are required.");
        }
        accessKey = credential.getAccessKey();
        secretKey = credential.getSecretKey();
        Account account = new AliyunAccount(accessKey, secretKey);
        this.odps = new Odps(account);
        enablePublicAccess = Boolean.parseBoolean(props.getOrDefault(MCProperties.PUBLIC_ACCESS, "false"));
        setOdpsUrl(region);
        odps.setDefaultProject(defaultProject);
        tunnel = new TableTunnel(odps);
        setTunnelUrl(region);
    }

    private void setOdpsUrl(String region) {
        if (StringUtils.isEmpty(odpsUrl)) {
            odpsUrl = odpsUrlTemplate.replace("{}", region);
            if (enablePublicAccess) {
                odpsUrl = odpsUrl.replace("-inc", "");
            }
        }
        odps.setEndpoint(odpsUrl);
    }

    private void setTunnelUrl(String region) {
        if (StringUtils.isEmpty(tunnelUrl)) {
            tunnelUrl = tunnelUrlTemplate.replace("{}", region);
            if (enablePublicAccess) {
                tunnelUrl = tunnelUrl.replace("-inc", "");
            }
        }
        tunnel.setEndpoint(tunnelUrl);
    }

    public TableTunnel getTableTunnel() {
        makeSureInitialized();
        return tunnel;
    }

    public Odps getClient() {
        makeSureInitialized();
        return odps;
    }

    protected List<String> listDatabaseNames() {
        List<String> result = new ArrayList<>();
        try {
            // TODO: How to get all privileged project from max compute as databases?
            // Now only have permission to show default project.
            result.add(odps.projects().get(odps.getDefaultProject()).getName());
        } catch (OdpsException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        try {
            return odps.tables().exists(tblName);
        } catch (OdpsException e) {
            throw new RuntimeException(e);
        }
    }

    public List<String> listPartitionNames(String dbName, String tbl) {
        return listPartitionNames(dbName, tbl, 0, -1);
    }

    public List<String> listPartitionNames(String dbName, String tbl, long skip, long limit) {
        try {
            if (getClient().projects().exists(dbName)) {
                List<Partition> parts;
                if (limit < 0) {
                    parts = getClient().tables().get(tbl).getPartitions();
                } else {
                    skip = skip < 0 ? 0 : skip;
                    parts = new ArrayList<>();
                    Iterator<Partition> it = getClient().tables().get(tbl).getPartitionIterator();
                    int count = 0;
                    while (it.hasNext()) {
                        if (count < skip) {
                            count++;
                            it.next();
                        } else if (parts.size() >= limit) {
                            break;
                        } else {
                            parts.add(it.next());
                        }
                    }
                }
                return parts.stream().map(p -> p.getPartitionSpec().toString(false, true))
                        .collect(Collectors.toList());
            } else {
                throw new OdpsException("Max compute project: " + dbName + " not exists.");
            }
        } catch (OdpsException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        List<String> result = new ArrayList<>();
        odps.tables().forEach(e -> result.add(e.getName()));
        return result;
    }

    /**
     * use region to create data tunnel url
     * @return region, required by jni scanner.
     */
    public String getRegion() {
        makeSureInitialized();
        return region;
    }

    public String getAccessKey() {
        makeSureInitialized();
        return accessKey;
    }

    public String getSecretKey() {
        makeSureInitialized();
        return secretKey;
    }

    public boolean enablePublicAccess() {
        makeSureInitialized();
        return enablePublicAccess;
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

    public String getOdpsUrl() {
        return odpsUrl;
    }

    public String getTunnelUrl() {
        return tunnelUrl;
    }
}
