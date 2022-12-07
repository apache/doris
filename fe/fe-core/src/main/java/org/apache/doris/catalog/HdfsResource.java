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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.thrift.THdfsConf;
import org.apache.doris.thrift.THdfsParams;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    public static final String HADOOP_FS_PREFIX = "dfs.";
    public static String HADOOP_FS_NAME = "fs.defaultFS";
    // simple or kerberos
    public static String HADOOP_USER_NAME = "hadoop.username";
    public static String HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication";
    public static String HADOOP_KERBEROS_PRINCIPAL = "hadoop.kerberos.principal";
    public static String HADOOP_KERBEROS_KEYTAB = "hadoop.kerberos.keytab";
    public static String HADOOP_SHORT_CIRCUIT = "dfs.client.read.shortcircuit";
    public static String HADOOP_SOCKET_PATH = "dfs.domain.socket.path";
    public static List<String> REQUIRED_FIELDS = Collections.singletonList(HADOOP_FS_NAME);

    @SerializedName(value = "properties")
    private Map<String, String> properties;

    public HdfsResource(String name) {
        super(name, Resource.ResourceType.HDFS);
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
        for (String field : REQUIRED_FIELDS) {
            if (!properties.containsKey(field)) {
                throw new DdlException("Missing [" + field + "] in properties.");
            }
        }
        // `dfs.client.read.shortcircuit` and `dfs.domain.socket.path` should be both set to enable short circuit read.
        // We should disable short circuit read if they are not both set because it will cause performance down.
        if (!properties.containsKey(HADOOP_SHORT_CIRCUIT) || !properties.containsKey(HADOOP_SOCKET_PATH)) {
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

    // Will be removed after BE unified storage params
    public static THdfsParams generateHdfsParam(Map<String, String> properties) {
        THdfsParams tHdfsParams = new THdfsParams();
        tHdfsParams.setHdfsConf(new ArrayList<>());
        for (Map.Entry<String, String> property : properties.entrySet()) {
            if (property.getKey().equalsIgnoreCase(HADOOP_FS_NAME)) {
                tHdfsParams.setFsName(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(HADOOP_USER_NAME)) {
                tHdfsParams.setUser(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(HADOOP_KERBEROS_PRINCIPAL)) {
                tHdfsParams.setHdfsKerberosPrincipal(property.getValue());
            } else if (property.getKey().equalsIgnoreCase(HADOOP_KERBEROS_KEYTAB)) {
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
