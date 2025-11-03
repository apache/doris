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

import org.apache.doris.common.UserException;

import com.aliyun.datalake.metastore.common.DataLakeConfig;
import com.google.common.collect.Maps;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class HMSAliyunDLFMetaStorePropertiesTest {

    @Test
    public void testCreate() throws UserException {
        Map<String, String> props = Maps.newHashMap();
        props.put("type", "hms");
        props.put("hive.metastore.type", "dlf");
        props.put("dlf.endpoint", "dlf.cn-shanghai.aliyuncs.com");
        props.put("dlf.region", "cn-shanghai");
        props.put("dlf.access_key", "xxx");
        props.put("dlf.secret_key", "xxx");
        props.put("dlf.catalog_id", "5789");
        HiveAliyunDLFMetaStoreProperties hmsAliyunDLFMetaStoreProperties =
                (HiveAliyunDLFMetaStoreProperties) MetastoreProperties.create(props);
        Assertions.assertEquals("xxx", hmsAliyunDLFMetaStoreProperties.getHiveConf().get(DataLakeConfig.CATALOG_ACCESS_KEY_ID));
        Assertions.assertEquals("xxx", hmsAliyunDLFMetaStoreProperties.getHiveConf().get(DataLakeConfig.CATALOG_ACCESS_KEY_SECRET));
        Assertions.assertEquals("5789", hmsAliyunDLFMetaStoreProperties.getHiveConf().get(DataLakeConfig.CATALOG_ID));
        Assertions.assertEquals("cn-shanghai", hmsAliyunDLFMetaStoreProperties.getHiveConf().get(DataLakeConfig.CATALOG_REGION_ID));
        Assertions.assertEquals("dlf.cn-shanghai.aliyuncs.com", hmsAliyunDLFMetaStoreProperties.getHiveConf().get(DataLakeConfig.CATALOG_ENDPOINT));
        Assertions.assertEquals("dlf", hmsAliyunDLFMetaStoreProperties.getHiveConf().get("hive.metastore.type"));
        Assertions.assertEquals("hms", hmsAliyunDLFMetaStoreProperties.getHiveConf().get("type"));
    }
}
