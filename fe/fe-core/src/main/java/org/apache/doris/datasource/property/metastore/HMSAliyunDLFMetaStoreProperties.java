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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.datasource.property.storage.OSSProperties;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import org.apache.hadoop.hive.conf.HiveConf;

import java.util.Map;

public class HMSAliyunDLFMetaStoreProperties extends AbstractHMSProperties {

    private AliyunDLFBaseProperties baseProperties;

    private OSSProperties ossProperties;

    public HMSAliyunDLFMetaStoreProperties(Map<String, String> origProps) {
        super(Type.DLF, origProps);
    }

    @Override
    public void initNormalizeAndCheckProps() {
        super.initNormalizeAndCheckProps();
        ossProperties = OSSProperties.of(origProps);
        baseProperties = AliyunDLFBaseProperties.of(origProps);
        initHiveConf();
    }

    private void initHiveConf() {
        // @see com.aliyun.datalake.metastore.hive.common.utils.ConfigUtils
        // todo support other parameters
        hiveConf = new HiveConf();
        hiveConf.addResource(ossProperties.hadoopStorageConfig);
        hiveConf.set(DataLakeConfig.CATALOG_ACCESS_KEY_ID, baseProperties.dlfAccessKey);
        hiveConf.set(DataLakeConfig.CATALOG_ACCESS_KEY_SECRET, baseProperties.dlfSecretKey);
        hiveConf.set(DataLakeConfig.CATALOG_ENDPOINT, baseProperties.dlfEndpoint);
        hiveConf.set(DataLakeConfig.CATALOG_REGION_ID, baseProperties.dlfRegion);
        hiveConf.set(DataLakeConfig.CATALOG_SECURITY_TOKEN, baseProperties.dlfSessionToken);
        hiveConf.set(DataLakeConfig.CATALOG_USER_ID, baseProperties.dlfUid);
        hiveConf.set(DataLakeConfig.CATALOG_ID, baseProperties.dlfCatalogId);
        hiveConf.set(DataLakeConfig.CATALOG_PROXY_MODE, baseProperties.dlfProxyMode);
        hiveConf.set("hive.metastore.type", "dlf");
        hiveConf.set("type", "hms");
    }

}
