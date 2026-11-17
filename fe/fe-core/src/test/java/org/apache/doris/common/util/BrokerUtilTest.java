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

package org.apache.doris.common.util;

import org.apache.doris.common.UserException;
import org.apache.doris.connector.spi.scan.ConnectorPartitionValues;
import org.apache.doris.datasource.scan.FilePartitionUtils;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

public class BrokerUtilTest {

    @Test
    public void parseColumnsFromPath() {
        String path = "/path/to/dir/k1=v1/xxx.csv";
        try {
            List<String> columns = FilePartitionUtils.parseColumnsFromPath(path, Collections.singletonList("k1"));
            Assertions.assertEquals(1, columns.size());
            Assertions.assertEquals(Collections.singletonList("v1"), columns);
        } catch (UserException e) {
            Assertions.fail();
        }

        path = "/path/to/dir/k1/xxx.csv";
        try {
            FilePartitionUtils.parseColumnsFromPath(path, Collections.singletonList("k1"));
            Assertions.fail();
        } catch (UserException ignored) {
            // CHECKSTYLE IGNORE THIS LINE
        }

        path = "/path/to/dir/k1=v1/xxx.csv";
        try {
            FilePartitionUtils.parseColumnsFromPath(path, Collections.singletonList("k2"));
            Assertions.fail();
        } catch (UserException ignored) {
            // CHECKSTYLE IGNORE THIS LINE
        }

        path = "/path/to/dir/k1=v2/k1=v1/xxx.csv";
        try {
            List<String> columns = FilePartitionUtils.parseColumnsFromPath(path, Collections.singletonList("k1"));
            Assertions.assertEquals(1, columns.size());
            Assertions.assertEquals(Collections.singletonList("v1"), columns);
        } catch (UserException e) {
            Assertions.fail();
        }

        path = "/path/to/dir/k2=v2/k1=v1/xxx.csv";
        try {
            List<String> columns = FilePartitionUtils.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            Assertions.assertEquals(2, columns.size());
            Assertions.assertEquals(Lists.newArrayList("v1", "v2"), columns);
        } catch (UserException e) {
            Assertions.fail();
        }

        path = "/path/to/dir/k2=v2/a/k1=v1/xxx.csv";
        try {
            FilePartitionUtils.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            Assertions.fail();
        } catch (UserException ignored) {
            // CHECKSTYLE IGNORE THIS LINE
        }

        path = "/path/to/dir/k2=v2/k1=v1/xxx.csv";
        try {
            FilePartitionUtils.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2", "k3"));
            Assertions.fail();
        } catch (UserException ignored) {
            // CHECKSTYLE IGNORE THIS LINE
        }

        path = "/path/to/dir/k2=v2//k1=v1//xxx.csv";
        try {
            List<String> columns = FilePartitionUtils.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            Assertions.assertEquals(2, columns.size());
            Assertions.assertEquals(Lists.newArrayList("v1", "v2"), columns);
        } catch (UserException e) {
            Assertions.fail();
        }

        path = "/path/to/dir/k2==v2=//k1=v1//xxx.csv";
        try {
            List<String> columns = FilePartitionUtils.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            Assertions.assertEquals(2, columns.size());
            Assertions.assertEquals(Lists.newArrayList("v1", "=v2="), columns);
        } catch (UserException e) {
            Assertions.fail();
        }

        path = "/path/to/dir/k2==v2=//k1=v1/";
        try {
            FilePartitionUtils.parseColumnsFromPath(path, Lists.newArrayList("k1", "k2"));
            Assertions.fail();
        } catch (UserException ignored) {
            // CHECKSTYLE IGNORE THIS LINE
        }

        path = "/path/to/dir/k1=2/a/xxx.csv";
        try {
            FilePartitionUtils.parseColumnsFromPath(path, Collections.singletonList("k1"));
        } catch (UserException ignored) {
            Assertions.fail();
        }

    }

    @Test
    public void parseColumnsFromPathPreservesNullMetadataAndInputKeys() throws Exception {
        // A case-insensitive parse lowercases its own copy of the keys, not the caller's list, and turns the Hive
        // default partition into SQL NULL.
        List<String> partitionKeys = Lists.newArrayList("Region", "Dt");
        FilePartitionUtils.ParsedColumnsFromPath parsed = FilePartitionUtils.parseColumnsFromPathWithNullInfo(
                "hdfs://host/table/REGION=cn/dt=" + ConnectorPartitionValues.NULL_PARTITION_NAME + "/data.parquet",
                partitionKeys, false);
        Assertions.assertEquals(Lists.newArrayList("cn", ""), parsed.getValues());
        Assertions.assertEquals(Lists.newArrayList(false, true), parsed.getIsNull());
        Assertions.assertEquals(Lists.newArrayList("Region", "Dt"), partitionKeys);

        // A literal \N is data on the query path.
        parsed = FilePartitionUtils.parseColumnsFromPathWithNullInfo(
                "hdfs://host/table/p=\\N/data.orc", Collections.singletonList("p"), true);
        Assertions.assertEquals(Collections.singletonList("\\N"), parsed.getValues());
        Assertions.assertEquals(Collections.singletonList(false), parsed.getIsNull());
    }

    @Test
    public void parseColumnsFromPathForLoadKeepsLegacyNullSemantics() throws Exception {
        FilePartitionUtils.ParsedColumnsFromPath parsed = FilePartitionUtils.parseColumnsFromPathWithNullInfoForLoad(
                "hdfs://host/table/p=\\N/data.orc", Collections.singletonList("p"), true);
        Assertions.assertEquals(Collections.singletonList(""), parsed.getValues());
        Assertions.assertEquals(Collections.singletonList(true), parsed.getIsNull());

        // Other values, including the Hive default partition, parse as on the query path.
        parsed = FilePartitionUtils.parseColumnsFromPathWithNullInfoForLoad(
                "hdfs://host/table/p=a/q=" + ConnectorPartitionValues.NULL_PARTITION_NAME + "/data.orc",
                Lists.newArrayList("p", "q"), true);
        Assertions.assertEquals(Lists.newArrayList("a", ""), parsed.getValues());
        Assertions.assertEquals(Lists.newArrayList(false, true), parsed.getIsNull());
    }

}
