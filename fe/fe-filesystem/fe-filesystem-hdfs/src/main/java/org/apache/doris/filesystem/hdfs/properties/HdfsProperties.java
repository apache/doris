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

package org.apache.doris.filesystem.hdfs.properties;

import org.apache.doris.foundation.property.ConnectorProperty;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Plain HDFS storage properties for fe-filesystem.
 *
 * <p>Self-contained port of the kernel {@code HdfsProperties} chain, sitting on the shared
 * {@link HdfsCompatibleProperties} base. It binds {@code @ConnectorProperty} fields via
 * fe-foundation, translates the typed authentication parameters into Hadoop configuration keys,
 * loads xml resources, and injects defaults — with zero fe-core / fe-common dependency.</p>
 */
public class HdfsProperties extends HdfsCompatibleProperties {

    private static final Set<String> SUPPORT_SCHEMA = ImmutableSet.of("hdfs", "viewfs");

    @ConnectorProperty(names = {"hdfs.authentication.type", "hadoop.security.authentication"},
            required = false,
            description = "The authentication type of HDFS. The default value is 'none'.")
    private String hdfsAuthenticationType = "simple";

    @ConnectorProperty(names = {"hdfs.authentication.kerberos.principal", "hadoop.kerberos.principal"},
            required = false,
            description = "The principal of the kerberos authentication.")
    private String hdfsKerberosPrincipal = "";

    @ConnectorProperty(names = {"hdfs.authentication.kerberos.keytab", "hadoop.kerberos.keytab"},
            required = false,
            description = "The keytab of the kerberos authentication.")
    private String hdfsKerberosKeytab = "";

    @ConnectorProperty(names = {"hadoop.username"},
            required = false,
            description = "The username of Hadoop. Doris will user this user to access HDFS")
    private String hadoopUsername = "";

    @ConnectorProperty(names = {"hdfs.impersonation.enabled"},
            required = false,
            supported = false,
            description = "Whether to enable the impersonation of HDFS.")
    private boolean hdfsImpersonationEnabled = false;

    @ConnectorProperty(names = {"ipc.client.fallback-to-simple-auth-allowed"},
            required = false,
            description = "Whether to allow fallback to simple authentication.")
    private String allowFallbackToSimpleAuth = "";

    @ConnectorProperty(names = {"fs.defaultFS"}, required = false, description = "")
    private String fsDefaultFS = "";

    @ConnectorProperty(names = {"hadoop.config.resources"},
            required = false,
            description = "The xml files of Hadoop configuration.")
    private String hadoopConfigResources = "";

    private Map<String, String> userOverriddenHdfsConfig;

    public HdfsProperties(Map<String, String> origProps) {
        super(origProps);
    }

    @Override
    protected void checkRequiredProperties() {
        super.checkRequiredProperties();
        if ("kerberos".equalsIgnoreCase(hdfsAuthenticationType) && (Strings.isNullOrEmpty(hdfsKerberosPrincipal)
                || Strings.isNullOrEmpty(hdfsKerberosKeytab))) {
            throw new IllegalArgumentException("HDFS authentication type is kerberos, "
                    + "but principal or keytab is not set.");
        }
    }

    @Override
    protected void doInitNormalizeAndCheckProps() {
        if (StringUtils.isBlank(fsDefaultFS)) {
            this.fsDefaultFS = HdfsPropertiesUtils.extractDefaultFsFromUri(origProps, SUPPORT_SCHEMA);
        }
        extractUserOverriddenHdfsConfig(origProps);
        initBackendConfigProperties();
        HdfsPropertiesUtils.checkHaConfig(backendConfigProperties);
    }

    private void extractUserOverriddenHdfsConfig(Map<String, String> origProps) {
        if (MapUtils.isEmpty(origProps)) {
            return;
        }
        userOverriddenHdfsConfig = new HashMap<>();
        origProps.forEach((key, value) -> {
            if (key.startsWith("hadoop.") || key.startsWith("dfs.") || key.startsWith("fs.")
                    || key.startsWith("juicefs.")) {
                userOverriddenHdfsConfig.put(key, value);
            }
        });
    }

    private void initBackendConfigProperties() {
        Map<String, String> props = loadConfigFromFile(hadoopConfigResources);
        if (MapUtils.isNotEmpty(userOverriddenHdfsConfig)) {
            props.putAll(userOverriddenHdfsConfig);
        }
        if (StringUtils.isNotBlank(fsDefaultFS)) {
            props.put(HDFS_DEFAULT_FS_NAME, fsDefaultFS);
        }
        if (StringUtils.isNotBlank(allowFallbackToSimpleAuth)) {
            props.put("ipc.client.fallback-to-simple-auth-allowed", allowFallbackToSimpleAuth);
        } else {
            props.put("ipc.client.fallback-to-simple-auth-allowed", "true");
        }
        props.put("hdfs.security.authentication", hdfsAuthenticationType);
        if ("kerberos".equalsIgnoreCase(hdfsAuthenticationType)) {
            props.put("hadoop.security.authentication", "kerberos");
            props.put("hadoop.kerberos.principal", hdfsKerberosPrincipal);
            props.put("hadoop.kerberos.keytab", hdfsKerberosKeytab);
        }
        if (StringUtils.isNotBlank(hadoopUsername)) {
            props.put("hadoop.username", hadoopUsername);
        }
        if (StringUtils.isBlank(fsDefaultFS)) {
            this.fsDefaultFS = props.getOrDefault(HDFS_DEFAULT_FS_NAME, "");
        }
        this.backendConfigProperties = props;
    }
}
