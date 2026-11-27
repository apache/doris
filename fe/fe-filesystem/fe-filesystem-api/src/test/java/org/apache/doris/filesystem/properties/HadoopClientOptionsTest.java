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

package org.apache.doris.filesystem.properties;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Map;

class HadoopClientOptionsTest {
    @Test
    void selectsStatementOptionsWithoutChangingInput() {
        Map<String, String> input = Map.of("format", "parquet", "broker.dfs.replication", "2",
                "fs.client.unspecified", "unused", "dfs.client.socket-timeout", "3000");
        Assertions.assertEquals(Map.of("format", "parquet", "broker.dfs.replication", "2",
                "dfs.client.socket-timeout", "3000"), HadoopClientOptions.forStatement(input));
        Assertions.assertEquals(4, input.size());
    }

    @Test
    void validatesImplementationsAndProviders() {
        Assertions.assertTrue(HadoopClientOptions.isHdfsOption("fs.hdfs.impl",
                "org.apache.hadoop.hdfs.DistributedFileSystem"));
        Assertions.assertTrue(HadoopClientOptions.isHdfsOption("dfs.client.failover.proxy.provider.ns",
                "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HadoopClientOptions.isHdfsOption("fs.hdfs.impl", "test.Unconfigured"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HadoopClientOptions.isHdfsOption("dfs.client.failover.proxy.provider.ns", "test.Unconfigured"));
        Assertions.assertFalse(HadoopClientOptions.isHdfsOption("dfs.replication ", "2"));
    }

    @Test
    void checksEachMountTarget() {
        String key = "fs.viewfs.mounttable.cluster.one.linkMerge./warehouse";
        Assertions.assertTrue(HadoopClientOptions.isHdfsOption(key, "hdfs://one/data,hdfs://two/data"));
        Assertions.assertTrue(HadoopClientOptions.isHdfsOption("fs.viewfs.mounttable.cluster.homedir", "/users"));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HadoopClientOptions.isHdfsOption(key, "hdfs://one/data,relative"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"_HADOOP_CONFIG_DIR_", "broker.hadoop.config.resources", "fs.test.support"})
    void keepsStatementRoutingFieldsReserved(String key) {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HadoopClientOptions.forStatement(Map.of(key, "fixture")));
    }

    @Test
    void requiresQualifiedDefaultFilesystem() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> HadoopClientOptions.forStatement(Map.of("fs.defaultFS", "relative")));
        Map<String, String> input = Map.of("fs.defaultFS", "hdfs://cluster");
        Assertions.assertEquals(input, HadoopClientOptions.forStatement(input));
    }
}
