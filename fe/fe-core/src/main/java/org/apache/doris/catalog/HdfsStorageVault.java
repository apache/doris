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

package org.apache.doris.catalog;

import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.cloud.rpc.MetaServiceProxy;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.rpc.RpcException;

import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * HDFS resource
 * <p>
 * Syntax:
 * CREATE STORAGE VAULT "remote_hdfs"
 * PROPERTIES
 * (
 * "type" = "hdfs",
 * "fs.defaultFS" = "hdfs://10.220.147.151:8020",
 * "hadoop.username" = "root"
 * );
 */
public class HdfsStorageVault extends StorageVault {
    private static final Logger LOG = LogManager.getLogger(HdfsStorageVault.class);
    public static final String HADOOP_FS_PREFIX = "dfs.";
    public static String HADOOP_FS_NAME = "fs.defaultFS";
    public static String HADOOP_SHORT_CIRCUIT = "dfs.client.read.shortcircuit";
    public static String HADOOP_SOCKET_PATH = "dfs.domain.socket.path";
    public static String DSF_NAMESERVICES = "dfs.nameservices";
    public static final String HDFS_PREFIX = "hdfs:";
    public static final String HDFS_FILE_PREFIX = "hdfs://";

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public HdfsStorageVault(String name, boolean ifNotExists) {
        super(name, StorageVault.StorageVaultType.HDFS, ifNotExists);
        properties = Maps.newHashMap();
    }

    @Override
    public void alterMetaService() throws DdlException {
        Cloud.HdfsParams hdfsParams = generateHdfsParam(properties);
        Cloud.AlterHdfsParams.Builder alterHdfsParamsBuilder = Cloud.AlterHdfsParams.newBuilder();
        alterHdfsParamsBuilder.setVaultName(name);
        alterHdfsParamsBuilder.setHdfs(hdfsParams);
        Cloud.AlterObjStoreInfoRequest.Builder requestBuilder
                = Cloud.AlterObjStoreInfoRequest.newBuilder();
        requestBuilder.setOp(Cloud.AlterObjStoreInfoRequest.Operation.ADD_HDFS_INFO);
        requestBuilder.setHdfs(alterHdfsParamsBuilder.build());
        try {
            Cloud.AlterObjStoreInfoResponse response =
                    MetaServiceProxy.getInstance().alterObjStoreInfo(requestBuilder.build());
            if (!response.hasStatus() || !response.getStatus().hasCode()) {
                if (response.getStatus().getCode() == Cloud.MetaServiceCode.ALREADY_EXISTED
                        && ifNotExists()) {
                    return;
                }
                if (response.getStatus().getCode() != Cloud.MetaServiceCode.OK) {
                    LOG.warn("failed to alter storage vault response: {} ", response);
                    throw new DdlException(response.getStatus().getMsg());
                }
            }
        } catch (RpcException e) {
            LOG.warn("failed to alter storage vault due to RpcException: {}", e);
            throw new DdlException(e.getMessage());
        }
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
        }
        super.modifyProperties(properties);
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        // `dfs.client.read.shortcircuit` and `dfs.domain.socket.path` should be both set to enable short circuit read.
        // We should disable short circuit read if they are not both set because it will cause performance down.
        if (!(enableShortCircuitRead(properties))) {
            properties.put(HADOOP_SHORT_CIRCUIT, "false");
        }
        this.properties = properties;
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return Maps.newHashMap(properties);
    }

    @Override
    protected void getProcNodeData(BaseProcResult result) {
        // String lowerCaseType = type.name().toLowerCase();
        // for (Map.Entry<String, String> entry : properties.entrySet()) {
        //     result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        // }
    }

    public static boolean enableShortCircuitRead(Map<String, String> properties) {
        return "true".equalsIgnoreCase(properties.getOrDefault(HADOOP_SHORT_CIRCUIT, "false"))
                    && properties.containsKey(HADOOP_SOCKET_PATH);
    }

    public static Cloud.HdfsParams generateHdfsParam(Map<String, String> properties) {
        Cloud.HdfsParams.Builder hdfsParamsBuilder =
                    Cloud.HdfsParams.newBuilder();
        for (Map.Entry<String, String> property : properties.entrySet()) {
            if (property.getKey().equalsIgnoreCase(HADOOP_FS_NAME)) {
                hdfsParamsBuilder.setFsName(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(AuthenticationConfig.HADOOP_USER_NAME)) {
                hdfsParamsBuilder.setUser(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(AuthenticationConfig.HADOOP_KERBEROS_PRINCIPAL)) {
                hdfsParamsBuilder.setHdfsKerberosPrincipal(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(AuthenticationConfig.HADOOP_KERBEROS_KEYTAB)) {
                hdfsParamsBuilder.setHdfsKerberosKeytab(property.getValue());
            } else {
                Cloud.HdfsConf.Builder hdfsConfBuilder = Cloud.HdfsConf.newBuilder();
                hdfsConfBuilder.setKey(property.getKey());
                hdfsConfBuilder.setValue(property.getValue());
                hdfsParamsBuilder.addHdfsConfs(hdfsConfBuilder.build());
            }
        }
        return hdfsParamsBuilder.build();
    }
}
