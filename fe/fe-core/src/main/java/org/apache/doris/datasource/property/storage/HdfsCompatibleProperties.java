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

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public abstract class HdfsCompatibleProperties extends StorageProperties {


    public static final String HDFS_DEFAULT_FS_NAME = "fs.defaultFS";

    protected HdfsCompatibleProperties(Type type, Map<String, String> origProps) {
        super(type, origProps);
    }

    protected Configuration configuration;

    @Override
    protected String getResourceConfigPropName() {
        return "hadoop.config.resources";
    }

    public Configuration getHadoopConfiguration() {
        return configuration;
    }
}
