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

package org.apache.doris.filesystem.jfs.properties;

import org.apache.doris.filesystem.hdfs.properties.HdfsCompatibleProperties;
import org.apache.doris.filesystem.hdfs.properties.HdfsPropertiesUtils;
import org.apache.doris.foundation.property.ConnectorProperty;

import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * JuiceFS ({@code jfs://}) storage properties. HDFS-compatible: JuiceFS ships a Hadoop FileSystem
 * impl resolved by Hadoop config, so this only needs to derive {@code fs.defaultFS} from a
 * {@code jfs://} uri and forward the {@code juicefs.*} / Hadoop keys to {@code DFSFileSystem}.
 */
public class JfsProperties extends HdfsCompatibleProperties {

    private static final Set<String> SUPPORT_SCHEMA = ImmutableSet.of("jfs");

    @ConnectorProperty(names = {"fs.defaultFS"}, required = false, description = "")
    private String fsDefaultFS = "";

    @ConnectorProperty(names = {"hadoop.config.resources"}, required = false,
            description = "The xml files of Hadoop configuration.")
    private String hadoopConfigResources = "";

    @ConnectorProperty(names = {"hadoop.username"}, required = false,
            description = "The username used to access JuiceFS.")
    private String hadoopUsername = "";

    private Map<String, String> userOverriddenConfig;

    public JfsProperties(Map<String, String> origProps) {
        super(origProps);
    }

    @Override
    protected void doInitNormalizeAndCheckProps() {
        if (StringUtils.isBlank(fsDefaultFS)) {
            this.fsDefaultFS = HdfsPropertiesUtils.extractDefaultFsFromUri(origProps, SUPPORT_SCHEMA);
        }
        extractUserOverriddenConfig(origProps);
        initBackendConfigProperties();
    }

    private void extractUserOverriddenConfig(Map<String, String> origProps) {
        if (MapUtils.isEmpty(origProps)) {
            return;
        }
        userOverriddenConfig = new HashMap<>();
        origProps.forEach((key, value) -> {
            if (key.startsWith("hadoop.") || key.startsWith("dfs.") || key.startsWith("fs.")
                    || key.startsWith("juicefs.")) {
                userOverriddenConfig.put(key, value);
            }
        });
    }

    private void initBackendConfigProperties() {
        Map<String, String> props = loadConfigFromFile(hadoopConfigResources);
        if (MapUtils.isNotEmpty(userOverriddenConfig)) {
            props.putAll(userOverriddenConfig);
        }
        if (StringUtils.isNotBlank(fsDefaultFS)) {
            props.put(HDFS_DEFAULT_FS_NAME, fsDefaultFS);
        }
        if (StringUtils.isNotBlank(hadoopUsername)) {
            props.put("hadoop.username", hadoopUsername);
        }
        this.backendConfigProperties = props;
    }
}
