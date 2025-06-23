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

import org.apache.doris.backup.Status;
import org.apache.doris.cloud.proto.Cloud;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.property.constants.S3Properties;
import org.apache.doris.datasource.property.storage.HdfsCompatibleProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    public static final String HADOOP_FS_PREFIX = "dfs.";
    public static String HADOOP_SHORT_CIRCUIT = "dfs.client.read.shortcircuit";
    public static String HADOOP_SOCKET_PATH = "dfs.domain.socket.path";
    public static String DSF_NAMESERVICES = "dfs.nameservices";
    public static final String HDFS_PREFIX = "hdfs:";
    public static final String HDFS_FILE_PREFIX = "hdfs://";

    public static class PropertyKey {
        public static String HADOOP_FS_NAME = "fs.defaultFS";
        public static String VAULT_PATH_PREFIX = "path_prefix";
        public static String HADOOP_USER_NAME = AuthenticationConfig.HADOOP_USER_NAME;
        public static String HADOOP_SECURITY_AUTHENTICATION =
                CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION;
        public static String HADOOP_KERBEROS_KEYTAB = AuthenticationConfig.HADOOP_KERBEROS_KEYTAB;
        public static String HADOOP_KERBEROS_PRINCIPAL = AuthenticationConfig.HADOOP_KERBEROS_PRINCIPAL;
    }

    public static final HashSet<String> FORBID_ALTER_PROPERTIES = new HashSet<>(Arrays.asList(
            PropertyKey.VAULT_PATH_PREFIX,
            PropertyKey.HADOOP_FS_NAME
    ));

    /**
     * Property keys used by Doris, and should not be put in HDFS client configs,
     * such as `type`, `path_prefix`, etc.
     */
    private static final Set<String> NON_HDFS_CONF_PROPERTY_KEYS =
            ImmutableSet.of(StorageVault.PropertyKey.TYPE, PropertyKey.VAULT_PATH_PREFIX, S3Properties.VALIDITY_CHECK)
                    .stream().map(String::toLowerCase)
                    .collect(ImmutableSet.toImmutableSet());

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public HdfsStorageVault(String name, boolean ifNotExists, boolean setAsDefault) {
        super(name, StorageVault.StorageVaultType.HDFS, ifNotExists, setAsDefault);
        properties = Maps.newHashMap();
    }

    @Override
    public void modifyProperties(ImmutableMap<String, String> newProperties) throws DdlException {
        for (Map.Entry<String, String> kv : newProperties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
        }
        checkConnectivity(this.properties);
    }

    @Override
    public Map<String, String> getCopiedProperties() {
        return Maps.newHashMap(properties);
    }

    public static void checkConnectivity(Map<String, String> newProperties) throws DdlException {
        if (newProperties.containsKey(S3Properties.VALIDITY_CHECK)
                && newProperties.get(S3Properties.VALIDITY_CHECK).equalsIgnoreCase("false")) {
            return;
        }

        String hadoopFsName = null;
        String pathPrefix = null;
        for (Map.Entry<String, String> property : newProperties.entrySet()) {
            if (property.getKey().equalsIgnoreCase(PropertyKey.HADOOP_FS_NAME)) {
                hadoopFsName = property.getValue();
            } else if (property.getKey().equalsIgnoreCase(PropertyKey.VAULT_PATH_PREFIX)) {
                pathPrefix = property.getValue();
            }
        }
        Preconditions.checkArgument(
                !Strings.isNullOrEmpty(hadoopFsName), "%s is null or empty", PropertyKey.HADOOP_FS_NAME);
        Preconditions.checkArgument(
                !Strings.isNullOrEmpty(pathPrefix), "%s is null or empty", PropertyKey.VAULT_PATH_PREFIX);

        try (DFSFileSystem dfsFileSystem = new DFSFileSystem((HdfsCompatibleProperties) StorageProperties
                .createPrimary(newProperties))) {
            Long timestamp = System.currentTimeMillis();
            String remotePath = hadoopFsName + "/" + pathPrefix + "/doris-check-connectivity" + timestamp.toString();

            Status st = dfsFileSystem.makeDir(remotePath);
            if (st != Status.OK) {
                throw new DdlException(
                        "checkConnectivity(makeDir) failed, status: " + st + ", properties: " + new PrintableMap<>(
                                newProperties, "=", true, false, true, false));
            }

            st = dfsFileSystem.exists(remotePath);
            if (st != Status.OK) {
                throw new DdlException(
                        "checkConnectivity(exist) failed, status: " + st + ", properties: " + new PrintableMap<>(
                                newProperties, "=", true, false, true, false));
            }

            st = dfsFileSystem.delete(remotePath);
            if (st != Status.OK) {
                throw new DdlException(
                        "checkConnectivity(exist) failed, status: " + st + ", properties: " + new PrintableMap<>(
                                newProperties, "=", true, false, true, false));
            }
        } catch (IOException e) {
            LOG.warn("checkConnectivity failed, properties:{}", new PrintableMap<>(
                    newProperties, "=", true, false, true, false), e);
            throw new DdlException("checkConnectivity failed, properties: " + new PrintableMap<>(
                    newProperties, "=", true, false, true, false), e);
        }
    }

    public static Cloud.HdfsVaultInfo generateHdfsParam(Map<String, String> properties) {
        Cloud.HdfsVaultInfo.Builder hdfsVaultInfoBuilder =
                    Cloud.HdfsVaultInfo.newBuilder();
        Cloud.HdfsBuildConf.Builder hdfsConfBuilder = Cloud.HdfsBuildConf.newBuilder();

        Set<String> lowerCaseKeys = properties.keySet().stream().map(String::toLowerCase)
                .collect(Collectors.toSet());

        for (Map.Entry<String, String> property : properties.entrySet()) {
            if (property.getKey().equalsIgnoreCase(PropertyKey.HADOOP_FS_NAME)) {
                Preconditions.checkArgument(!Strings.isNullOrEmpty(property.getValue()),
                        "%s is null or empty", property.getKey());
                hdfsConfBuilder.setFsName(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(PropertyKey.VAULT_PATH_PREFIX)) {
                hdfsVaultInfoBuilder.setPrefix(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(PropertyKey.HADOOP_USER_NAME)) {
                Preconditions.checkArgument(!Strings.isNullOrEmpty(property.getValue()),
                        "%s is null or empty", property.getKey());
                hdfsConfBuilder.setUser(property.getValue());
            } else if (property.getKey()
                    .equalsIgnoreCase(PropertyKey.HADOOP_SECURITY_AUTHENTICATION)) {
                Preconditions.checkArgument(lowerCaseKeys.contains(PropertyKey.HADOOP_KERBEROS_PRINCIPAL),
                        "%s is required for kerberos", PropertyKey.HADOOP_KERBEROS_PRINCIPAL);
                Preconditions.checkArgument(lowerCaseKeys.contains(PropertyKey.HADOOP_KERBEROS_KEYTAB),
                        "%s is required for kerberos", PropertyKey.HADOOP_KERBEROS_KEYTAB);
            } else if (property.getKey().equalsIgnoreCase(PropertyKey.HADOOP_KERBEROS_PRINCIPAL)) {
                Preconditions.checkArgument(!Strings.isNullOrEmpty(property.getValue()),
                        "%s is null or empty", property.getKey());
                hdfsConfBuilder.setHdfsKerberosPrincipal(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(PropertyKey.HADOOP_KERBEROS_KEYTAB)) {
                Preconditions.checkArgument(!Strings.isNullOrEmpty(property.getValue()),
                        "%s is null or empty", property.getKey());
                hdfsConfBuilder.setHdfsKerberosKeytab(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(StorageVault.PropertyKey.VAULT_NAME)) {
                continue;
            } else {
                // Get rid of copy and paste from create s3 vault stmt
                Preconditions.checkArgument(
                        !property.getKey().toLowerCase().contains(S3StorageVault.PropertyKey.REGION),
                        "Invalid argument %s", property.getKey());
                Preconditions.checkArgument(
                        !property.getKey().toLowerCase().contains(S3StorageVault.PropertyKey.ENDPOINT),
                        "Invalid argument %s", property.getKey());
                Preconditions.checkArgument(
                        !property.getKey().toLowerCase().contains(S3StorageVault.PropertyKey.ROOT_PATH),
                        "Invalid argument %s", property.getKey());
                Preconditions.checkArgument(
                        !property.getKey().toLowerCase().contains(S3StorageVault.PropertyKey.PROVIDER),
                        "Invalid argument %s", property.getKey());
                Preconditions.checkArgument(
                        !property.getKey().toLowerCase().contains(S3StorageVault.PropertyKey.BUCKET),
                        "Invalid argument %s", property.getKey());

                if (!NON_HDFS_CONF_PROPERTY_KEYS.contains(property.getKey().toLowerCase())) {
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
