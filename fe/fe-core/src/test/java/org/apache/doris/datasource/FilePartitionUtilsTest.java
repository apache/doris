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

package org.apache.doris.datasource;

import org.apache.doris.datasource.FilePartitionUtils.ParsedColumnsFromPath;
import org.apache.doris.datasource.hive.HiveMetaStoreCache;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FilePartitionUtilsTest {

    @Test
    public void testParseColumnsFromPathPreservesNullMetadataAndInputKeys() throws Exception {
        List<String> partitionKeys = new ArrayList<>(Arrays.asList("region", "dt"));
        ParsedColumnsFromPath parsed = FilePartitionUtils.parseColumnsFromPathWithNullInfo(
                "hdfs://host/table/Region=cn/Dt=" + HiveMetaStoreCache.HIVE_DEFAULT_PARTITION
                        + "/data.parquet",
                partitionKeys, false, false);

        Assert.assertEquals(Arrays.asList("cn", ""), parsed.getValues());
        Assert.assertEquals(Arrays.asList(false, true), parsed.getIsNull());
        Assert.assertEquals(Arrays.asList("region", "dt"), partitionKeys);
    }

    @Test
    public void testParseColumnsFromPathPreservesLiteralBackslashN() throws Exception {
        ParsedColumnsFromPath parsed = FilePartitionUtils.parseColumnsFromPathWithNullInfo(
                "hdfs://host/table/p=\\N/data.orc", Arrays.asList("p"), true, false);

        Assert.assertEquals(Arrays.asList("\\N"), parsed.getValues());
        Assert.assertEquals(Arrays.asList(false), parsed.getIsNull());
    }

    @Test
    public void testParseAcidColumnsFromPath() throws Exception {
        ParsedColumnsFromPath parsed = FilePartitionUtils.parseColumnsFromPathWithNullInfo(
                "hdfs://host/table/p=value/delta_1_1/bucket_00000", Arrays.asList("p"), true, true);

        Assert.assertEquals(Arrays.asList("value"), parsed.getValues());
        Assert.assertEquals(Arrays.asList(false), parsed.getIsNull());
    }

    @Test
    public void testNormalizeColumnsFromPath() {
        ParsedColumnsFromPath parsed = FilePartitionUtils.normalizeColumnsFromPath(
                Arrays.asList(null, HiveMetaStoreCache.HIVE_DEFAULT_PARTITION, "", "\\N"));

        Assert.assertEquals(Arrays.asList("", "", "", "\\N"), parsed.getValues());
        Assert.assertEquals(Arrays.asList(true, true, false, false), parsed.getIsNull());
    }
}
