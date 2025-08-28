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

import org.apache.doris.common.security.authentication.HadoopAuthenticator;
import org.apache.doris.common.security.authentication.HadoopSimpleAuthenticator;
import org.apache.doris.common.security.authentication.SimpleAuthenticationConfig;

import lombok.Getter;

import java.util.Map;

public abstract class HdfsCompatibleProperties extends StorageProperties {


    public static final String HDFS_DEFAULT_FS_NAME = "fs.defaultFS";

    protected Map<String, String> backendConfigProperties;

    protected HdfsCompatibleProperties(Type type, Map<String, String> origProps) {
        super(type, origProps);
    }

    @Getter
    protected HadoopAuthenticator hadoopAuthenticator = new HadoopSimpleAuthenticator(new SimpleAuthenticationConfig());

    @Override
    protected String getResourceConfigPropName() {
        return "hadoop.config.resources";
    }

    @Override
    public void initializeHadoopStorageConfig() {
        //nothing to do
    }

}
