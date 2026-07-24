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

package org.apache.doris.fs;

import org.apache.doris.common.Config;
import org.apache.doris.datasource.property.storage.HdfsProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class StoragePropertiesConverterHdfsDirTest {

    @Test
    public void hdfsMapCarriesInjectedConfigDir() {
        Map<String, String> raw = new HashMap<>();
        raw.put("fs.defaultFS", "hdfs://ns");
        HdfsProperties props = new HdfsProperties(raw);
        props.initNormalizeAndCheckProps();

        Map<String, String> map = StoragePropertiesConverter.toMap(props);

        Assertions.assertEquals("HDFS", map.get("_STORAGE_TYPE_"));
        Assertions.assertEquals(Config.hadoop_config_dir, map.get("_HADOOP_CONFIG_DIR_"));
    }

    @Test
    public void ossHdfsMapCarriesDistinctStorageType() {
        // OSS-HDFS extends the HDFS-compatible base but must carry the OSS_HDFS marker so the
        // fe-filesystem OssHdfsFileSystemProvider (not the plain HDFS one) is selected.
        Map<String, String> raw = new HashMap<>();
        raw.put("oss.hdfs.endpoint", "cn-beijing.oss-dls.aliyuncs.com");
        raw.put("oss.hdfs.access_key", "ak");
        raw.put("oss.hdfs.secret_key", "sk");
        StorageProperties props = StorageProperties.createPrimary(raw);

        Map<String, String> map = StoragePropertiesConverter.toMap(props);

        Assertions.assertEquals("OSS_HDFS", map.get("_STORAGE_TYPE_"));
        Assertions.assertEquals(Config.hadoop_config_dir, map.get("_HADOOP_CONFIG_DIR_"));
    }
}
