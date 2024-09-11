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
import com.aliyun.odps.Project;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.security.SecurityManager;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.aliyun.odps.utils.StringUtils;
import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MaxComputeExternalCatalog extends ExternalCatalog {
    private Odps odps;
    private String accessKey;
    private String secretKey;
    private String endpoint;
    private String catalogOwner;
    private String defaultProject;
    private String quota;
    private EnvironmentSettings settings;

    private String splitStrategy;
    private SplitOptions splitOptions;
    private long splitRowCount;
    private long splitByteSize;

    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            MCProperties.PROJECT,
            MCProperties.ENDPOINT
    );

    public MaxComputeExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
                                     String comment) {
        super(catalogId, name, InitCatalogLog.Type.MAX_COMPUTE, comment);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initLocalObjectsImpl() {
        Map<String, String> props = catalogProperty.getProperties();

        endpoint = props.get(MCProperties.ENDPOINT);
        defaultProject = props.get(MCProperties.PROJECT);
        quota = props.getOrDefault(MCProperties.QUOTA, MCProperties.DEFAULT_QUOTA);


        splitStrategy = props.getOrDefault(MCProperties.SPLIT_STRATEGY, MCProperties.DEFAULT_SPLIT_STRATEGY);
        if (splitStrategy.equals(MCProperties.SPLIT_BY_BYTE_SIZE_STRATEGY)) {
            splitByteSize = Long.parseLong(props.getOrDefault(MCProperties.SPLIT_BYTE_SIZE,
                    MCProperties.DEFAULT_SPLIT_BYTE_SIZE));

            splitOptions = SplitOptions.newBuilder()
                    .SplitByByteSize(splitByteSize)
                    .withCrossPartition(false)
                    .build();
        } else {
            splitRowCount = Long.parseLong(props.getOrDefault(MCProperties.SPLIT_ROW_COUNT,
                    MCProperties.DEFAULT_SPLIT_ROW_COUNT));
            splitOptions = SplitOptions.newBuilder()
                    .SplitByRowOffset()
                    .withCrossPartition(false)
                    .build();
        }


        CloudCredential credential = MCProperties.getCredential(props);
        accessKey = credential.getAccessKey();
        secretKey = credential.getSecretKey();



        Account account = new AliyunAccount(accessKey, secretKey);
        this.odps = new Odps(account);
        odps.setDefaultProject(defaultProject);
        odps.setEndpoint(endpoint);
        Credentials credentials = Credentials.newBuilder().withAccount(odps.getAccount())
                .withAppAccount(odps.getAppAccount()).build();

        settings = EnvironmentSettings.newBuilder()
                .withCredentials(credentials)
                .withServiceEndpoint(odps.getEndpoint())
                .withQuotaName(quota)
                .build();
    }

    public Odps getClient() {
        makeSureInitialized();
        return odps;
    }

    protected List<String> listDatabaseNames() {
        List<String> result = new ArrayList<>();
        try {
            result.add(defaultProject);
            if (StringUtils.isNullOrEmpty(catalogOwner)) {
                SecurityManager sm = odps.projects().get().getSecurityManager();
                String whoami = sm.runQuery("whoami", false);

                JsonObject js = JsonParser.parseString(whoami).getAsJsonObject();
                catalogOwner = js.get("DisplayName").getAsString();
            }
            Iterator<Project> iterator = odps.projects().iterator(catalogOwner);
            while (iterator.hasNext()) {
                Project project = iterator.next();
                if (!project.getName().equals(defaultProject)) {
                    result.add(project.getName());
                }
            }
        } catch (OdpsException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        try {
            return getClient().tables().exists(dbName, tblName);
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
        getClient().tables().forEach(e -> result.add(e.getName()));
        return result;
    }

    public String getAccessKey() {
        makeSureInitialized();
        return accessKey;
    }

    public String getSecretKey() {
        makeSureInitialized();
        return secretKey;
    }

    public String getEndpoint() {
        makeSureInitialized();
        return endpoint;
    }

    public String getDefaultProject() {
        makeSureInitialized();
        return defaultProject;
    }

    public String getQuota() {
        return quota;
    }

    public SplitOptions getSplitOption() {
        return splitOptions;
    }

    public EnvironmentSettings getSettings() {
        return settings;
    }

    public String getSplitStrategy() {
        return splitStrategy;
    }

    public long getSplitRowCount() {
        return splitRowCount;
    }


    public long getSplitByteSize() {
        return splitByteSize;
    }

    @Override
    public void checkProperties() throws DdlException {
        super.checkProperties();
        Map<String, String> props = catalogProperty.getProperties();
        for (String requiredProperty : REQUIRED_PROPERTIES) {
            if (!props.containsKey(requiredProperty)) {
                throw new DdlException("Required property '" + requiredProperty + "' is missing");
            }
        }

        try {
            splitStrategy = props.getOrDefault(MCProperties.SPLIT_STRATEGY, MCProperties.DEFAULT_SPLIT_STRATEGY);
            if (splitStrategy.equals(MCProperties.SPLIT_BY_BYTE_SIZE_STRATEGY)) {
                splitByteSize = Long.parseLong(props.getOrDefault(MCProperties.SPLIT_BYTE_SIZE,
                        MCProperties.DEFAULT_SPLIT_BYTE_SIZE));

                if (splitByteSize < 10485760L) {
                    throw new DdlException(MCProperties.SPLIT_ROW_COUNT + " must be greater than or equal to 10485760");
                }

            } else if (splitStrategy.equals(MCProperties.SPLIT_BY_ROW_COUNT_STRATEGY)) {
                splitRowCount = Long.parseLong(props.getOrDefault(MCProperties.SPLIT_ROW_COUNT,
                        MCProperties.DEFAULT_SPLIT_ROW_COUNT));
                if (splitRowCount <= 0) {
                    throw new DdlException(MCProperties.SPLIT_ROW_COUNT + " must be greater than 0");
                }

            } else {
                throw new DdlException("property " + MCProperties.SPLIT_STRATEGY + "must is "
                        + MCProperties.SPLIT_BY_BYTE_SIZE_STRATEGY + " or " + MCProperties.SPLIT_BY_ROW_COUNT_STRATEGY);
            }
        } catch (NumberFormatException e) {
            throw new DdlException("property " + MCProperties.SPLIT_BYTE_SIZE + "/"
                    + MCProperties.SPLIT_ROW_COUNT + "must be an integer");
        }

        CloudCredential credential = MCProperties.getCredential(props);
        if (!credential.isWhole()) {
            throw new DdlException("Max-Compute credential properties '"
                    + MCProperties.ACCESS_KEY + "' and  '" + MCProperties.SECRET_KEY + "' are required.");
        }
    }
}
