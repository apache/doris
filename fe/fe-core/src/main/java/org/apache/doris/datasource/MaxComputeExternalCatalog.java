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

package org.apache.doris.datasource;


import org.apache.doris.common.DdlException;
import org.apache.doris.datasource.credentials.CloudCredential;
import org.apache.doris.datasource.property.constants.MCProperties;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TunnelException;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

public class MaxComputeExternalCatalog extends ExternalCatalog {
    private Odps odps;
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
    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            MCProperties.REGION,
            MCProperties.PROJECT
    );

    public MaxComputeExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
                                     String comment) {
        super(catalogId, name, InitCatalogLog.Type.MAX_COMPUTE, comment);
        catalogProperty = new CatalogProperty(resource, props);
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
        String odpsUrl = odpsUrlTemplate.replace("{}", region);
        if (enablePublicAccess) {
            odpsUrl = odpsUrl.replace("-inc", "");
        }
        odps.setEndpoint(odpsUrl);
        odps.setDefaultProject(defaultProject);
    }

    public long getTotalRows(String project, String table, List<String> partitionConjuncts) throws TunnelException {
        makeSureInitialized();
        TableTunnel tunnel = new TableTunnel(odps);
        String tunnelUrl = tunnelUrlTemplate.replace("{}", region);
        if (enablePublicAccess) {
            tunnelUrl = tunnelUrl.replace("-inc", "");
        }
        TableTunnel.DownloadSession downloadSession;
        String downloadId = "DORIS_MC_TOTAL_ROWS_" + System.currentTimeMillis();
        tunnel.setEndpoint(tunnelUrl);
        if (partitionConjuncts.isEmpty()) {
            downloadSession = tunnel.getDownloadSession(project, table, downloadId);
        } else {
            StringJoiner partitionStr = new StringJoiner(",");
            partitionConjuncts.forEach(partitionStr::add);
            PartitionSpec partitionSpec = new PartitionSpec(partitionStr.toString());
            downloadSession = tunnel.getDownloadSession(project, table, partitionSpec, downloadId);
        }
        return downloadSession.getRecordCount();
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
}
