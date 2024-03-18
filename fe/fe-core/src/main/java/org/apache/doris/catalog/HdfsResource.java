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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.security.authentication.AuthenticationConfig;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.fs.remote.dfs.DFSFileSystem;
import org.apache.doris.thrift.THdfsConf;
import org.apache.doris.thrift.THdfsParams;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * HDFS resource
 * <p>
 * Syntax:
 * CREATE RESOURCE "remote_hdfs"
 * PROPERTIES
 * (
 * "type" = "hdfs",
 * "fs.defaultFS" = "hdfs://10.220.147.151:8020",
 * "hadoop.username" = "root"
 * );
 */
public class HdfsResource extends Resource {
    private static final Logger LOG = LogManager.getLogger(HdfsResource.class);
    public static final String HADOOP_FS_PREFIX = "dfs.";
    public static String HADOOP_FS_NAME = "fs.defaultFS";
    public static String HADOOP_SHORT_CIRCUIT = "dfs.client.read.shortcircuit";
    public static String HADOOP_SOCKET_PATH = "dfs.domain.socket.path";
    public static String DSF_NAMESERVICES = "dfs.nameservices";
    public static final String HDFS_PREFIX = "hdfs:";
    public static final String HDFS_FILE_PREFIX = "hdfs://";
    public static final String VALIDITY_CHECK = "hdfs_validity_check";

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public HdfsResource() {
        super();
    }

    public HdfsResource(String name) {
        super(name, Resource.ResourceType.HDFS);
        properties = Maps.newHashMap();
    }

    private static void pingHdfs(String hdfsUri, Map<String, String> properties) throws DdlException {
        String testFile = hdfsUri + "/test_hdfs_valid.txt";
        Map<String, String> propertiesPing = new HashMap<>();
        propertiesPing.put(HADOOP_FS_NAME, hdfsUri);
        properties.putAll(propertiesPing);
        DFSFileSystem dfsSystem = new DFSFileSystem(properties);
        String content = "doris hdfs valid";
        Status status = Status.OK;
        try {
            status = dfsSystem.directUpload(content, testFile);
            if (status != Status.OK) {
                throw new DdlException("ping hdfs failed(upload), status: " + status + new PrintableMap<>(
                    propertiesPing, "=", true, false, true, false));
            }
        } finally {
            if (status.ok()) {
                Status delete = dfsSystem.delete(testFile);
                if (delete != Status.OK) {
                    LOG.warn("delete test file failed, status: {}, properties: {}", delete, new PrintableMap<>(
                            propertiesPing, "=", true, false, true, false));
                }
            }
        }
        LOG.info("success to ping hdfs");
    }

    @Override
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        boolean needCheck = isNeedCheck(properties);
        if (needCheck) {
            Map<String, String> changedProperties = new HashMap<>(this.properties);
            changedProperties.putAll(properties);
            String hdfsUri = properties.getOrDefault(HADOOP_FS_NAME, this.properties.get(HADOOP_FS_NAME));
            pingHdfs(hdfsUri, changedProperties);
        }
        for (Map.Entry<String, String> kv : properties.entrySet()) {
            replaceIfEffectiveValue(this.properties, kv.getKey(), kv.getValue());
        }
        super.modifyProperties(properties);
    }

    private boolean isNeedCheck(Map<String, String> newProperties) {
        boolean needCheck = !this.properties.containsKey(VALIDITY_CHECK)
                || Boolean.parseBoolean(this.properties.get(VALIDITY_CHECK));
        if (newProperties != null && newProperties.containsKey(VALIDITY_CHECK)) {
            needCheck = Boolean.parseBoolean(newProperties.get(VALIDITY_CHECK));
        }
        return needCheck;
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        boolean needCheck = isNeedCheck(properties);
        if (needCheck) {
            String hdfsUri = properties.getOrDefault(HADOOP_FS_NAME, this.properties.get(HADOOP_FS_NAME));
            pingHdfs(hdfsUri, properties);
        }
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
        String lowerCaseType = type.name().toLowerCase();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            result.addRow(Lists.newArrayList(name, lowerCaseType, entry.getKey(), entry.getValue()));
        }
    }

    public static boolean enableShortCircuitRead(Map<String, String> properties) {
        return "true".equalsIgnoreCase(properties.getOrDefault(HADOOP_SHORT_CIRCUIT, "false"))
                    && properties.containsKey(HADOOP_SOCKET_PATH);
    }

    // Will be removed after BE unified storage params
    public static THdfsParams generateHdfsParam(Map<String, String> properties) {
        THdfsParams tHdfsParams = new THdfsParams();
        tHdfsParams.setHdfsConf(new ArrayList<>());
        for (Map.Entry<String, String> property : properties.entrySet()) {
            if (property.getKey().equalsIgnoreCase(HADOOP_FS_NAME)) {
                tHdfsParams.setFsName(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(AuthenticationConfig.HADOOP_USER_NAME)) {
                tHdfsParams.setUser(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(AuthenticationConfig.HADOOP_KERBEROS_PRINCIPAL)) {
                tHdfsParams.setHdfsKerberosPrincipal(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(AuthenticationConfig.HADOOP_KERBEROS_KEYTAB)) {
                tHdfsParams.setHdfsKerberosKeytab(property.getValue());
            } else {
                THdfsConf hdfsConf = new THdfsConf();
                hdfsConf.setKey(property.getKey());
                hdfsConf.setValue(property.getValue());
                tHdfsParams.hdfs_conf.add(hdfsConf);
            }
        }
        return tHdfsParams;
    }
}
