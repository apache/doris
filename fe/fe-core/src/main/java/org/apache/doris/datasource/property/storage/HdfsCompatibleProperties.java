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

package org.apache.doris.datasource.property.storage;

import org.apache.doris.datasource.property.ConnectorProperty;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class HdfsCompatibleProperties extends StorageProperties {


    @ConnectorProperty(names = {"fs.defaultFS"}, required = false, description = "")
    protected String fsDefaultFS = "";

    @ConnectorProperty(names = {"hadoop.config.resources"},
            required = false,
            description = "The xml files of Hadoop configuration.")
    protected String hadoopConfigResources = "";

    protected static final List<String> HDFS_COMPATIBLE_PROPERTIES_KEYS = Arrays.asList("fs.defaultFS",
            "hadoop.config.resources");

    protected Configuration configuration;

    public static final String HDFS_DEFAULT_FS_NAME = "fs.defaultFS";

    abstract Set<String> getSupportedSchemas();

    @Override
    protected void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        // If fsDefaultFS is not explicitly provided, we attempt to infer it from the 'uri' field.
        // However, the 'uri' is not a dedicated HDFS-specific property and may be present
        // even when the user is configuring multiple storage backends.
        // Additionally, since we are not using FileSystem.get(Configuration conf),
        // fsDefaultFS is not strictly required here.
        // This is a best-effort fallback to populate fsDefaultFS when possible.
        if (StringUtils.isBlank(fsDefaultFS)) {
            this.fsDefaultFS = HdfsPropertiesUtils.extractDefaultFsFromUri(origProps, getSupportedSchemas());
        }
    }

    protected HdfsCompatibleProperties(Type type, Map<String, String> origProps) {
        super(type, origProps);
    }

    public abstract Configuration getHadoopConfiguration();


    @Override
    protected String getResourceConfigPropName() {
        return "hadoop.config.resources";
    }
}
