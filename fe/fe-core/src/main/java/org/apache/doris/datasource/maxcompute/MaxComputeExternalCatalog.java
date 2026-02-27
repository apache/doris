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
import org.apache.doris.datasource.operations.ExternalMetadataOperations;
import org.apache.doris.datasource.property.constants.MCProperties;
import org.apache.doris.transaction.TransactionManagerFactory;

import com.aliyun.odps.Odps;
import com.aliyun.odps.Partition;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AccountFormat;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.table.TableIdentifier;
import com.aliyun.odps.table.configuration.RestOptions;
import com.aliyun.odps.table.configuration.SplitOptions;
import com.aliyun.odps.table.enviroment.Credentials;
import com.aliyun.odps.table.enviroment.EnvironmentSettings;
import com.google.common.collect.ImmutableList;
import org.apache.log4j.Logger;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MaxComputeExternalCatalog extends ExternalCatalog {
    private static final Logger LOG = Logger.getLogger(MaxComputeExternalCatalog.class);

    // you can ref : https://help.aliyun.com/zh/maxcompute/user-guide/endpoints
    private static final String endpointTemplate = "http://service.{}.maxcompute.aliyun-inc.com/api";

    private Odps odps;
    private String accessKey;
    private String secretKey;
    private String endpoint;
    private String defaultProject;
    private String quota;
    private EnvironmentSettings settings;

    private String splitStrategy;
    private SplitOptions splitOptions;
    private long splitRowCount;
    private long splitByteSize;

    private int connectTimeout;
    private int readTimeout;
    private int retryTimes;

    public boolean dateTimePredicatePushDown;

    AccountFormat accountFormat = AccountFormat.DISPLAYNAME;

    private McStructureHelper mcStructureHelper = null;

    private static final Map<String, ZoneId> REGION_ZONE_MAP;
    private static final List<String> REQUIRED_PROPERTIES = ImmutableList.of(
            MCProperties.PROJECT,
            MCProperties.ENDPOINT
    );

    static {
        Map<String, ZoneId> map = new HashMap<>();

        map.put("cn-hangzhou", ZoneId.of("Asia/Shanghai"));
        map.put("cn-shanghai", ZoneId.of("Asia/Shanghai"));
        map.put("cn-shanghai-finance-1", ZoneId.of("Asia/Shanghai"));
        map.put("cn-beijing", ZoneId.of("Asia/Shanghai"));
        map.put("cn-north-2-gov-1", ZoneId.of("Asia/Shanghai"));
        map.put("cn-zhangjiakou", ZoneId.of("Asia/Shanghai"));
        map.put("cn-wulanchabu", ZoneId.of("Asia/Shanghai"));
        map.put("cn-shenzhen", ZoneId.of("Asia/Shanghai"));
        map.put("cn-shenzhen-finance-1", ZoneId.of("Asia/Shanghai"));
        map.put("cn-chengdu", ZoneId.of("Asia/Shanghai"));
        map.put("cn-hongkong", ZoneId.of("Asia/Shanghai"));
        map.put("ap-southeast-1", ZoneId.of("Asia/Singapore"));
        map.put("ap-southeast-2", ZoneId.of("Australia/Sydney"));
        map.put("ap-southeast-3", ZoneId.of("Asia/Kuala_Lumpur"));
        map.put("ap-southeast-5", ZoneId.of("Asia/Jakarta"));
        map.put("ap-northeast-1", ZoneId.of("Asia/Tokyo"));
        map.put("eu-central-1", ZoneId.of("Europe/Berlin"));
        map.put("eu-west-1", ZoneId.of("Europe/London"));
        map.put("us-west-1", ZoneId.of("America/Los_Angeles"));
        map.put("us-east-1", ZoneId.of("America/New_York"));
        map.put("me-east-1", ZoneId.of("Asia/Dubai"));

        REGION_ZONE_MAP = Collections.unmodifiableMap(map);
    }


    public MaxComputeExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
                                     String comment) {
        super(catalogId, name, InitCatalogLog.Type.MAX_COMPUTE, comment);
        catalogProperty = new CatalogProperty(resource, props);
    }

    //Compatible with existing catalogs in previous versions.
    protected void generatorEndpoint() {
        Map<String, String> props = catalogProperty.getProperties();

        if (props.containsKey(MCProperties.ENDPOINT)) {
            // This is a new version of the property, so no parsing conversion is required.
            endpoint = props.get(MCProperties.ENDPOINT);
        } else if (props.containsKey(MCProperties.TUNNEL_SDK_ENDPOINT)) {
            // If customized `mc.tunnel_endpoint` before,
            // need to convert the value of this property because used the `tunnel API` before.
            String tunnelEndpoint = props.get(MCProperties.TUNNEL_SDK_ENDPOINT);
            endpoint = tunnelEndpoint.replace("//dt", "//service") + "/api";
        } else if (props.containsKey(MCProperties.ODPS_ENDPOINT)) {
            // If you customized `mc.odps_endpoint` before,
            // this value is equivalent to the new version of `mc.endpoint`, so you can use it directly
            endpoint = props.get(MCProperties.ODPS_ENDPOINT);
        } else if (props.containsKey(MCProperties.REGION)) {
            //Copied from original logic.
            String region = props.get(MCProperties.REGION);
            if (region.startsWith("oss-")) {
                // may use oss-cn-beijing, ensure compatible
                region = region.replace("oss-", "");
            }
            boolean enablePublicAccess = Boolean.parseBoolean(props.getOrDefault(MCProperties.PUBLIC_ACCESS,
                    MCProperties.DEFAULT_PUBLIC_ACCESS));
            endpoint = endpointTemplate.replace("{}", region);
            if (enablePublicAccess) {
                endpoint = endpoint.replace("-inc", "");
            }
        }
        /*
            Since MCProperties.REGION is a REQUIRED_PROPERTIES in previous versions
            and MCProperties.ENDPOINT is a REQUIRED_PROPERTIES in current versions,
            `else {}` is not needed here.
         */
    }


    @Override
    protected void initLocalObjectsImpl() {
        Map<String, String> props = catalogProperty.getProperties();

        generatorEndpoint();

        defaultProject = props.get(MCProperties.PROJECT);
        quota = props.getOrDefault(MCProperties.QUOTA, MCProperties.DEFAULT_QUOTA);

        boolean splitCrossPartition =
                Boolean.parseBoolean(props.getOrDefault(MCProperties.SPLIT_CROSS_PARTITION,
                MCProperties.DEFAULT_SPLIT_CROSS_PARTITION));

        splitStrategy = props.getOrDefault(MCProperties.SPLIT_STRATEGY, MCProperties.DEFAULT_SPLIT_STRATEGY);
        if (splitStrategy.equals(MCProperties.SPLIT_BY_BYTE_SIZE_STRATEGY)) {
            splitByteSize = Long.parseLong(props.getOrDefault(MCProperties.SPLIT_BYTE_SIZE,
                    MCProperties.DEFAULT_SPLIT_BYTE_SIZE));
            splitOptions = SplitOptions.newBuilder()
                    .SplitByByteSize(splitByteSize)
                    .withCrossPartition(splitCrossPartition)
                    .build();
        } else {
            splitRowCount = Long.parseLong(props.getOrDefault(MCProperties.SPLIT_ROW_COUNT,
                    MCProperties.DEFAULT_SPLIT_ROW_COUNT));
            splitOptions = SplitOptions.newBuilder()
                    .SplitByRowOffset()
                    .withCrossPartition(splitCrossPartition)
                    .build();
        }

        connectTimeout = Integer.parseInt(
                props.getOrDefault(MCProperties.CONNECT_TIMEOUT, MCProperties.DEFAULT_CONNECT_TIMEOUT));
        readTimeout = Integer.parseInt(
                props.getOrDefault(MCProperties.READ_TIMEOUT, MCProperties.DEFAULT_READ_TIMEOUT));
        retryTimes = Integer.parseInt(
                props.getOrDefault(MCProperties.RETRY_COUNT, MCProperties.DEFAULT_RETRY_COUNT));

        RestOptions restOptions = RestOptions.newBuilder()
                .withConnectTimeout(connectTimeout)
                .withReadTimeout(readTimeout)
                .withRetryTimes(retryTimes).build();

        CloudCredential credential = MCProperties.getCredential(props);
        accessKey = credential.getAccessKey();
        secretKey = credential.getSecretKey();

        dateTimePredicatePushDown = Boolean.parseBoolean(
                props.getOrDefault(MCProperties.DATETIME_PREDICATE_PUSH_DOWN,
                        MCProperties.DEFAULT_DATETIME_PREDICATE_PUSH_DOWN));

        Account account = new AliyunAccount(accessKey, secretKey);
        this.odps = new Odps(account);
        odps.setDefaultProject(defaultProject);
        odps.setEndpoint(endpoint);

        String accountFormatProp = props.getOrDefault(MCProperties.ACCOUNT_FORMAT, MCProperties.DEFAULT_ACCOUNT_FORMAT);
        if (accountFormatProp.equals(MCProperties.ACCOUNT_FORMAT_NAME)) {
            accountFormat = AccountFormat.DISPLAYNAME;
        } else if (accountFormatProp.equals(MCProperties.ACCOUNT_FORMAT_ID)) {
            accountFormat = AccountFormat.ID;
        }
        odps.setAccountFormat(accountFormat);
        Credentials credentials = Credentials.newBuilder().withAccount(odps.getAccount())
                .withAppAccount(odps.getAppAccount()).build();

        settings = EnvironmentSettings.newBuilder()
                .withCredentials(credentials)
                .withServiceEndpoint(odps.getEndpoint())
                .withQuotaName(quota)
                .withRestOptions(restOptions)
                .build();

        boolean enableNamespaceSchema = Boolean.parseBoolean(
                props.getOrDefault(MCProperties.ENABLE_NAMESPACE_SCHEMA, MCProperties.DEFAULT_ENABLE_NAMESPACE_SCHEMA));
        mcStructureHelper = McStructureHelper.getHelper(enableNamespaceSchema, defaultProject);

        initPreExecutionAuthenticator();
        metadataOps = ExternalMetadataOperations.newMaxComputeMetadataOps(this, odps);
        transactionManager = TransactionManagerFactory.createMCTransactionManager(this);
    }

    public Odps getClient() {
        makeSureInitialized();
        return odps;
    }

    public McStructureHelper getMcStructureHelper() {
        makeSureInitialized();
        return mcStructureHelper;
    }

    protected List<String> listDatabaseNames() {
        makeSureInitialized();
        return mcStructureHelper.listDatabaseNames(getClient(), getDefaultProject());
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        return mcStructureHelper.tableExist(getClient(), dbName, tblName);

    }

    public List<String> listPartitionNames(String dbName, String tbl) {
        return listPartitionNames(dbName, tbl, 0, -1);
    }

    public List<String> listPartitionNames(String dbName, String tbl, long skip, long limit) {
        if (mcStructureHelper.databaseExist(getClient(), dbName)) {
            List<Partition> parts;
            if (limit < 0) {
                parts = mcStructureHelper.getPartitions(getClient(), dbName, tbl);
            } else {
                skip = skip < 0 ? 0 : skip;
                parts = new ArrayList<>();
                Iterator<Partition> it = mcStructureHelper.getPartitionIterator(getClient(), dbName, tbl);
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
            throw new RuntimeException("MaxCompute schema/project: " + dbName + " not exists.");
        }
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        return mcStructureHelper.listTableNames(getClient(), dbName);
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

    public int getRetryTimes() {
        makeSureInitialized();
        return retryTimes;
    }

    public int getConnectTimeout() {
        makeSureInitialized();
        return connectTimeout;
    }

    public int getReadTimeout() {
        makeSureInitialized();
        return readTimeout;
    }

    public boolean getDateTimePredicatePushDown() {
        return dateTimePredicatePushDown;
    }

    public ZoneId getProjectDateTimeZone() {
        makeSureInitialized();

        String[] endpointSplit = endpoint.split("\\.");
        if (endpointSplit.length >= 2) {
            // http://service.cn-hangzhou-vpc.maxcompute.aliyun-inc.com/api => cn-hangzhou-vpc
            String regionAndSuffix = endpointSplit[1];

            //remove `-vpc` and `-intranet` suffix.
            String region = regionAndSuffix.replace("-vpc", "").replace("-intranet", "");
            if (REGION_ZONE_MAP.containsKey(region)) {
                return REGION_ZONE_MAP.get(region);
            }
            LOG.warn("Not exist region. region = " + region + ". endpoint = " + endpoint + ". use systemDefault.");
            return ZoneId.systemDefault();
        }
        LOG.warn("Split EndPoint " + endpoint + "fill. use systemDefault.");
        return ZoneId.systemDefault();
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

    public com.aliyun.odps.Table getOdpsTable(String dbName, String tableName) {
        return mcStructureHelper.getOdpsTable(getClient(), dbName, tableName);
    }

    public TableIdentifier getOdpsTableIdentifier(String dbName, String tableName) {
        return mcStructureHelper.getTableIdentifier(dbName, tableName);
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

        String accountFormatProp = props.getOrDefault(MCProperties.ACCOUNT_FORMAT, MCProperties.DEFAULT_ACCOUNT_FORMAT);
        if (accountFormatProp.equals(MCProperties.ACCOUNT_FORMAT_NAME)) {
            accountFormat = AccountFormat.DISPLAYNAME;
        } else if (accountFormatProp.equals(MCProperties.ACCOUNT_FORMAT_ID)) {
            accountFormat = AccountFormat.ID;
        } else {
            throw new DdlException("property " + MCProperties.ACCOUNT_FORMAT + "only support name and id");
        }

        try {
            connectTimeout = Integer.parseInt(
                    props.getOrDefault(MCProperties.CONNECT_TIMEOUT, MCProperties.DEFAULT_CONNECT_TIMEOUT));
            readTimeout = Integer.parseInt(
                    props.getOrDefault(MCProperties.READ_TIMEOUT, MCProperties.DEFAULT_READ_TIMEOUT));
            retryTimes = Integer.parseInt(
                    props.getOrDefault(MCProperties.RETRY_COUNT, MCProperties.DEFAULT_RETRY_COUNT));
            if (connectTimeout <= 0) {
                throw new DdlException(MCProperties.CONNECT_TIMEOUT + " must be greater than 0");
            }

            if (readTimeout <= 0) {
                throw new DdlException(MCProperties.READ_TIMEOUT + " must be greater than 0");
            }

            if (retryTimes <= 0) {
                throw new DdlException(MCProperties.RETRY_COUNT + " must be greater than 0");
            }

        } catch (NumberFormatException e) {
            throw new DdlException("property " + MCProperties.CONNECT_TIMEOUT + "/"
                    + MCProperties.READ_TIMEOUT + "/" + MCProperties.RETRY_COUNT + "must be an integer");
        }

        CloudCredential credential = MCProperties.getCredential(props);
        if (!credential.isWhole()) {
            throw new DdlException("Max-Compute credential properties '"
                    + MCProperties.ACCESS_KEY + "' and  '" + MCProperties.SECRET_KEY + "' are required.");
        }
    }
}
