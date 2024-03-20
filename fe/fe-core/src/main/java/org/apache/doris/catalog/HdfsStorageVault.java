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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.security.authentication.AuthenticationConfig;

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
 * "fs.prefix" = "",
 * "hadoop.username" = "root"
 * );
 */
public class HdfsStorageVault extends StorageVault {
    private static final Logger LOG = LogManager.getLogger(HdfsStorageVault.class);
    public static final String HADOOP_FS_PREFIX = "dfs.";
    public static String HADOOP_FS_NAME = "fs.defaultFS";
    public static String HADOOP_PREFIX = "fs.prefix";
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
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
        }
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

    public static boolean enableShortCircuitRead(Map<String, String> properties) {
        return "true".equalsIgnoreCase(properties.getOrDefault(HADOOP_SHORT_CIRCUIT, "false"))
                    && properties.containsKey(HADOOP_SOCKET_PATH);
    }

    public static Cloud.HdfsVaultInfo generateHdfsParam(Map<String, String> properties) {
        Cloud.HdfsVaultInfo.Builder hdfsVaultInfoBuilder =
                    Cloud.HdfsVaultInfo.newBuilder();
        Cloud.HdfsBuildConf.Builder hdfsConfBuilder = Cloud.HdfsBuildConf.newBuilder();
        for (Map.Entry<String, String> property : properties.entrySet()) {
            if (property.getKey().equalsIgnoreCase(HADOOP_FS_NAME)) {
                hdfsConfBuilder.setFsName(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(HADOOP_PREFIX)) {
                hdfsVaultInfoBuilder.setPrefix(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(AuthenticationConfig.HADOOP_USER_NAME)) {
                hdfsConfBuilder.setUser(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(AuthenticationConfig.HADOOP_KERBEROS_PRINCIPAL)) {
                hdfsConfBuilder.setHdfsKerberosPrincipal(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(AuthenticationConfig.HADOOP_KERBEROS_KEYTAB)) {
                hdfsConfBuilder.setHdfsKerberosKeytab(property.getValue());
            } else {
                Cloud.HdfsBuildConf.HdfsConfKVPair.Builder conf = Cloud.HdfsBuildConf.HdfsConfKVPair.newBuilder();
                conf.setKey(property.getKey());
                conf.setValue(property.getValue());
                hdfsConfBuilder.addHdfsConfs(conf.build());
            }
        }
        return hdfsVaultInfoBuilder.setBuildConf(hdfsConfBuilder.build()).build();
    }
}
