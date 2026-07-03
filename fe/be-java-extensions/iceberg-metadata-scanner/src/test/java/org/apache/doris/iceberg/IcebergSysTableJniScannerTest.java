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

package org.apache.doris.iceberg;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class IcebergSysTableJniScannerTest {
    @Test
    public void testGetHadoopOptionParamsStripsTransportPrefix() {
        Map<String, String> params = new HashMap<>();
        params.put("serialized_split", "split");
        params.put("required_fields", "snapshot_id");
        params.put("required_types", "bigint");
        params.put("time_zone", "UTC");
        params.put("hadoop.hadoop.security.authentication", "kerberos");
        params.put("hadoop.hadoop.kerberos.principal", "principal");
        params.put("hadoop.hadoop.kerberos.keytab", "keytab");
        params.put("hadoop.fs.defaultFS", "hdfs://nameservice1");

        Map<String, String> hadoopOptionParams = IcebergSysTableJniScanner.getHadoopOptionParams(params);

        Assert.assertEquals(4, hadoopOptionParams.size());
        Assert.assertEquals("kerberos", hadoopOptionParams.get("hadoop.security.authentication"));
        Assert.assertEquals("principal", hadoopOptionParams.get("hadoop.kerberos.principal"));
        Assert.assertEquals("keytab", hadoopOptionParams.get("hadoop.kerberos.keytab"));
        Assert.assertEquals("hdfs://nameservice1", hadoopOptionParams.get("fs.defaultFS"));
        Assert.assertFalse(hadoopOptionParams.containsKey("serialized_split"));
        Assert.assertFalse(hadoopOptionParams.containsKey("time_zone"));
        Assert.assertFalse(hadoopOptionParams.containsKey("hadoop.hadoop.security.authentication"));
    }
}
