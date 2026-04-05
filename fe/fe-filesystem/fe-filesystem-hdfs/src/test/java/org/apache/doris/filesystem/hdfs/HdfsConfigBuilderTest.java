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

package org.apache.doris.filesystem.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class HdfsConfigBuilderTest {

    @Test
    void buildSetsDisableCache() {
        Configuration conf = HdfsConfigBuilder.build(Map.of());
        Assertions.assertTrue(conf.getBoolean("fs.hdfs.impl.disable.cache", false));
        Assertions.assertTrue(conf.getBoolean("fs.AbstractFileSystem.hdfs.impl.disable.cache", false));
    }

    @Test
    void buildSetsProvidedProperties() {
        Configuration conf = HdfsConfigBuilder.build(Map.of(
                "fs.defaultFS", "hdfs://namenode:8020",
                "dfs.replication", "3"
        ));
        Assertions.assertEquals("hdfs://namenode:8020", conf.get("fs.defaultFS"));
        Assertions.assertEquals("3", conf.get("dfs.replication"));
    }

    @Test
    void buildIgnoresNullAndEmptyValues() {
        Map<String, String> props = new java.util.HashMap<>();
        props.put("good.key", "good.value");
        props.put("empty.key", "");
        props.put("null.key", null);

        Configuration conf = HdfsConfigBuilder.build(props);
        Assertions.assertEquals("good.value", conf.get("good.key"));
        // empty and null values should not be set
        Assertions.assertFalse(conf.iterator().hasNext()
                && "empty.key".equals(conf.get("empty.key")));
    }

    @Test
    void isKerberosEnabledBothPresent() {
        Assertions.assertTrue(HdfsConfigBuilder.isKerberosEnabled(Map.of(
                "hadoop.kerberos.principal", "doris@REALM",
                "hadoop.kerberos.keytab", "/path/to/keytab"
        )));
    }

    @Test
    void isKerberosEnabledMissingPrincipal() {
        Assertions.assertFalse(HdfsConfigBuilder.isKerberosEnabled(Map.of(
                "hadoop.kerberos.keytab", "/path/to/keytab"
        )));
    }

    @Test
    void isKerberosEnabledMissingKeytab() {
        Assertions.assertFalse(HdfsConfigBuilder.isKerberosEnabled(Map.of(
                "hadoop.kerberos.principal", "doris@REALM"
        )));
    }

    @Test
    void isKerberosEnabledNeitherPresent() {
        Assertions.assertFalse(HdfsConfigBuilder.isKerberosEnabled(Map.of()));
    }
}
