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

package org.apache.doris.alter;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.ExceptionChecker;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ModifyPartitionStoragePolicyTest extends TestWithFeService {

    @Override
    protected void beforeCluster() {
        Config.enable_storage_policy = true;
        FeConstants.runningUnitTest = true;
    }

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_cir_19357");
        useDatabase("test_cir_19357");

        executeSql("CREATE RESOURCE IF NOT EXISTS \"cir_19357_resource_a\" "
                + "PROPERTIES("
                + "\"type\"=\"s3\","
                + "\"AWS_REGION\"=\"bj\","
                + "\"AWS_ENDPOINT\"=\"bj.s3.comaaaa\","
                + "\"AWS_ROOT_PATH\"=\"path/to/rootaaaa\","
                + "\"AWS_SECRET_KEY\"=\"aaaa\","
                + "\"AWS_ACCESS_KEY\"=\"bbba\","
                + "\"AWS_BUCKET\"=\"test-bucket-a\","
                + "\"s3_validity_check\"=\"false\")");
        executeSql("CREATE RESOURCE IF NOT EXISTS \"cir_19357_resource_b\" "
                + "PROPERTIES("
                + "\"type\"=\"s3\","
                + "\"AWS_REGION\"=\"bj\","
                + "\"AWS_ENDPOINT\"=\"bj.s3.comaaaa\","
                + "\"AWS_ROOT_PATH\"=\"path/to/rootaaaa\","
                + "\"AWS_SECRET_KEY\"=\"aaaa\","
                + "\"AWS_ACCESS_KEY\"=\"bbba\","
                + "\"AWS_BUCKET\"=\"test-bucket-b\","
                + "\"s3_validity_check\"=\"false\")");
        executeSql("CREATE STORAGE POLICY IF NOT EXISTS cir_19357_policy_a "
                + "PROPERTIES("
                + "\"storage_resource\"=\"cir_19357_resource_a\","
                + "\"cooldown_datetime\"=\"2999-01-01 00:00:00\")");
        executeSql("CREATE STORAGE POLICY IF NOT EXISTS cir_19357_policy_b "
                + "PROPERTIES("
                + "\"storage_resource\"=\"cir_19357_resource_b\","
                + "\"cooldown_datetime\"=\"2999-01-01 00:00:00\")");

        createTable("CREATE TABLE tbl_with_policy (k1 int, k2 int) "
                + "PARTITION BY RANGE(k1) ("
                + "PARTITION p1 VALUES LESS THAN (\"10\"),"
                + "PARTITION p2 VALUES LESS THAN (\"20\")) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1 "
                + "PROPERTIES(\"replication_num\"=\"1\", \"storage_policy\"=\"cir_19357_policy_a\")");
        createTable("CREATE TABLE tbl_without_policy (k1 int, k2 int) "
                + "PARTITION BY RANGE(k1) ("
                + "PARTITION p1 VALUES LESS THAN (\"10\"),"
                + "PARTITION p2 VALUES LESS THAN (\"20\")) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 1 "
                + "PROPERTIES(\"replication_num\"=\"1\")");
    }

    @Test
    public void testRejectDifferentResourcePolicyBeforeBeUpdate() throws Exception {
        assertModifyPartitionRejectedBeforeBeUpdate(
                "ALTER TABLE tbl_with_policy MODIFY PARTITION (*) "
                        + "SET (\"storage_policy\"=\"cir_19357_policy_b\")",
                "currently do not support change origin storage policy to another one with different resource");
    }

    @Test
    public void testRejectUninitializedDefaultPolicyBeforeBeUpdate() throws Exception {
        assertModifyPartitionRejectedBeforeBeUpdate(
                "ALTER TABLE tbl_without_policy MODIFY PARTITION (*) "
                        + "SET (\"storage_policy\"=\"default_storage_policy\")",
                "Use default storage policy, but not give s3 info");
    }

    private void assertModifyPartitionRejectedBeforeBeUpdate(String sql, String errorMessage) throws Exception {
        Alter alter = Env.getCurrentEnv().getAlterInstance();
        SchemaChangeHandler originHandler = (SchemaChangeHandler) alter.getSchemaChangeHandler();
        SchemaChangeHandler spyHandler = Mockito.spy(originHandler);
        Deencapsulation.setField(alter, "schemaChangeHandler", spyHandler);
        try {
            ExceptionChecker.expectThrowsWithMsg(IllegalStateException.class, errorMessage,
                    () -> executeSql(sql));
            Mockito.verify(spyHandler, Mockito.never()).updatePartitionsProperties(
                    Mockito.any(), Mockito.anyString(), Mockito.anyList(), Mockito.anyMap());
        } finally {
            Deencapsulation.setField(alter, "schemaChangeHandler", originHandler);
        }
    }
}
