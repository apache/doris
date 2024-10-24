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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * HDFS resource
 * <p>
 * Syntax:
 * CREATE STORAGE VAULT "remote_hdfs"
 * PROPERTIES
 * (
 * "type" = "hdfs",
 * "fs.defaultFS" = "hdfs://10.220.147.151:8020",
 * "path_prefix" = "/path/to/data",
 * "hadoop.username" = "root"
 * );
 */
public class HdfsStorageVault extends StorageVault {
    private static final Logger LOG = LogManager.getLogger(HdfsStorageVault.class);

    public static final String VAULT_TYPE = "type";
    public static final String HADOOP_FS_PREFIX = "dfs.";
    public static String HADOOP_FS_NAME = "fs.defaultFS";
    public static String VAULT_PATH_PREFIX = "path_prefix";
    public static String HADOOP_SHORT_CIRCUIT = "dfs.client.read.shortcircuit";
    public static String HADOOP_SOCKET_PATH = "dfs.domain.socket.path";
    public static String DSF_NAMESERVICES = "dfs.nameservices";
    public static final String HDFS_PREFIX = "hdfs:";
    public static final String HDFS_FILE_PREFIX = "hdfs://";

    public static final HashSet<String> FORBID_CHECK_PROPERTIES = new HashSet<>(Arrays.asList(
            VAULT_PATH_PREFIX,
            HADOOP_FS_NAME
    ));

    /**
     * Property keys used by Doris, and should not be put in HDFS client configs,
     * such as `type`, `path_prefix`, etc.
     */
    private static final Set<String> nonHdfsConfPropertyKeys = ImmutableSet.of(VAULT_TYPE, VAULT_PATH_PREFIX)
            .stream().map(String::toLowerCase)
            .collect(ImmutableSet.toImmutableSet());

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public HdfsStorageVault(String name, boolean ifNotExists, boolean setAsDefault) {
        super(name, StorageVault.StorageVaultType.HDFS, ifNotExists, setAsDefault);
        properties = Maps.newHashMap();
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
        }
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return Maps.newHashMap(properties);
    }

    public static Cloud.HdfsVaultInfo generateHdfsParam(Map<String, String> properties) {
        Cloud.HdfsVaultInfo.Builder hdfsVaultInfoBuilder =
                    Cloud.HdfsVaultInfo.newBuilder();
        Cloud.HdfsBuildConf.Builder hdfsConfBuilder = Cloud.HdfsBuildConf.newBuilder();
        for (Map.Entry<String, String> property : properties.entrySet()) {
            if (property.getKey().equalsIgnoreCase(HADOOP_FS_NAME)) {
                hdfsConfBuilder.setFsName(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(VAULT_PATH_PREFIX)) {
                hdfsVaultInfoBuilder.setPrefix(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(AuthenticationConfig.HADOOP_USER_NAME)) {
                hdfsConfBuilder.setUser(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(AuthenticationConfig.HADOOP_KERBEROS_PRINCIPAL)) {
                hdfsConfBuilder.setHdfsKerberosPrincipal(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(AuthenticationConfig.HADOOP_KERBEROS_KEYTAB)) {
                hdfsConfBuilder.setHdfsKerberosKeytab(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(VAULT_NAME)) {
                continue;
            } else {
                if (!nonHdfsConfPropertyKeys.contains(property.getKey().toLowerCase())) {
                    Cloud.HdfsBuildConf.HdfsConfKVPair.Builder conf = Cloud.HdfsBuildConf.HdfsConfKVPair.newBuilder();
                    conf.setKey(property.getKey());
                    conf.setValue(property.getValue());
                    hdfsConfBuilder.addHdfsConfs(conf.build());
                }
            }
        }
        return hdfsVaultInfoBuilder.setBuildConf(hdfsConfBuilder.build()).build();
    }
}
