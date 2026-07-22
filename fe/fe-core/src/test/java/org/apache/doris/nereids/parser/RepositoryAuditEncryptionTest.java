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

package org.apache.doris.nereids.parser;

import org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that NeedAuditEncryption statements mask credential properties before they reach the audit log.
 */
public class RepositoryAuditEncryptionTest {

    @BeforeEach
    public void setUpConnectContext() {
        // Some Nereids command parsing paths read the current database from thread-local context.
        ConnectContext connectContext = new ConnectContext();
        connectContext.setDatabase("");
        connectContext.setThreadLocalInfo();
    }

    @AfterEach
    public void tearDownConnectContext() {
        ConnectContext.remove();
    }

    private String encrypt(String sql) {
        LogicalPlan plan = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(plan instanceof NeedAuditEncryption,
                "command should be NeedAuditEncryption: " + plan.getClass().getName());
        NeedAuditEncryption cmd = (NeedAuditEncryption) plan;
        Assertions.assertTrue(cmd.needAuditEncryption());
        return cmd.geneEncryptionSQL(sql);
    }

    @Test
    public void testCreateRepositoryMasksSecretKey() {
        String sql = "CREATE REPOSITORY `repo` WITH S3 ON LOCATION 's3://bkt/repo' "
                + "PROPERTIES ("
                + "'s3.endpoint' = 'oss-cn-hongkong.aliyuncs.com', "
                + "'s3.access_key' = 'AK123', "
                + "'s3.secret_key' = 'SUPERSECRET')";
        String masked = encrypt(sql);
        Assertions.assertFalse(masked.contains("SUPERSECRET"), "secret_key must be masked: " + masked);
        Assertions.assertTrue(masked.contains("*XXX"), "expected mask token: " + masked);
        // Non-credential values are preserved (only properties marked sensitive are masked;
        // for the legacy S3Properties both access_key and secret_key are sensitive).
        Assertions.assertTrue(masked.contains("oss-cn-hongkong.aliyuncs.com"), masked);
    }

    @Test
    public void testAlterRepositoryMasksSecretKey() {
        String sql = "ALTER REPOSITORY `repo` PROPERTIES ('s3.secret_key' = 'SUPERSECRET')";
        String masked = encrypt(sql);
        Assertions.assertFalse(masked.contains("SUPERSECRET"), "secret_key must be masked: " + masked);
        Assertions.assertTrue(masked.contains("*XXX"), "expected mask token: " + masked);
    }

    @Test
    public void testCreateResourceMasksAiApiKey() {
        String sql = "CREATE EXTERNAL RESOURCE \"ai_resource\" PROPERTIES ("
                + "\"type\" = \"ai\", "
                + "\"ai.api_key\" = \"sk-test\", "
                + "\"ai.endpoint\" = \"https://api.test\")";
        String masked = encrypt(sql);
        Assertions.assertFalse(masked.contains("sk-test"), masked);
        Assertions.assertTrue(masked.contains("*XXX"), masked);
        Assertions.assertTrue(masked.contains("https://api.test"), masked);
    }

    @Test
    public void testAlterResourceMasksAiApiKey() {
        String sql = "ALTER RESOURCE \"ai_resource\" PROPERTIES ("
                + "\"ai.api_key\" = \"sk-test\", "
                + "\"ai.endpoint\" = \"https://api.test\")";
        String masked = encrypt(sql);
        Assertions.assertFalse(masked.contains("sk-test"), masked);
        Assertions.assertTrue(masked.contains("*XXX"), masked);
        Assertions.assertTrue(masked.contains("https://api.test"), masked);
    }

    @Test
    public void testCreateDatabaseMasksJdbcPassword() {
        String sql = "CREATE DATABASE test_db PROPERTIES ("
                + "\"iceberg.jdbc.password\" = \"jdbc-secret\", "
                + "\"iceberg.jdbc.user\" = \"root\")";
        String masked = encrypt(sql);
        Assertions.assertFalse(masked.contains("jdbc-secret"), masked);
        Assertions.assertTrue(masked.contains("*XXX"), masked);
        Assertions.assertTrue(masked.contains("root"), masked);
    }

    @Test
    public void testCreateStageMasksAkSk() {
        String sql = "CREATE STAGE ex_stage PROPERTIES ("
                + "\"bucket\" = \"tmp-bucket\", "
                + "\"endpoint\" = \"cos.ap-beijing.myqcloud.com\", "
                + "\"provider\" = \"cos\", "
                + "\"prefix\" = \"tmp_prefix\", "
                + "\"sk\" = \"tmp_sk\", "
                + "\"ak\" = \"tmp_ak\", "
                + "\"access_type\" = \"aksk\", "
                + "\"region\" = \"ap-beijing\")";
        String masked = encrypt(sql);
        Assertions.assertFalse(masked.contains("tmp_ak"), masked);
        Assertions.assertFalse(masked.contains("tmp_sk"), masked);
        Assertions.assertTrue(masked.contains("*XXX"), masked);
    }

    @Test
    public void testCreateRoutineLoadMasksKafkaSecrets() {
        String sql = "CREATE ROUTINE LOAD test_db.job1 ON tbl1 "
                + "PROPERTIES(\"desired_concurrent_number\" = \"1\") "
                + "FROM KAFKA("
                + "\"kafka_broker_list\" = \"127.0.0.1:9092\", "
                + "\"kafka_topic\" = \"topic1\", "
                + "\"property.sasl.jaas.config\" = \"plain-secret\", "
                + "\"property.aws.secret_key\" = \"aws-secret\")";
        String masked = encrypt(sql);
        Assertions.assertFalse(masked.contains("plain-secret"), masked);
        Assertions.assertFalse(masked.contains("aws-secret"), masked);
        Assertions.assertTrue(masked.contains("*XXX"), masked);
    }

    @Test
    public void testAlterRoutineLoadMasksKafkaSecrets() {
        String sql = "ALTER ROUTINE LOAD FOR job1 FROM KAFKA("
                + "\"property.aws.access_key\" = \"aws-ak\", "
                + "\"property.aws.secret_key\" = \"aws-sk\", "
                + "\"property.aws.external_id\" = \"external-id\")";
        String masked = encrypt(sql);
        Assertions.assertFalse(masked.contains("aws-ak"), masked);
        Assertions.assertFalse(masked.contains("aws-sk"), masked);
        Assertions.assertTrue(masked.contains("external-id"), masked);
        Assertions.assertTrue(masked.contains("*XXX"), masked);
    }

    @Test
    public void testCreateRoutineLoadKeepsKinesisExternalId() {
        String sql = "CREATE ROUTINE LOAD test_db.job_kinesis ON tbl1 "
                + "PROPERTIES(\"desired_concurrent_number\" = \"1\") "
                + "FROM KINESIS("
                + "\"aws.region\" = \"ap-southeast-1\", "
                + "\"kinesis_stream\" = \"stream1\", "
                + "\"property.aws.role_arn\" = \"arn:aws:iam::123456789012:role/test\", "
                + "\"property.aws.external.id\" = \"kinesis-external-id\")";
        String masked = encrypt(sql);
        Assertions.assertTrue(masked.contains("kinesis-external-id"), masked);
    }

    @Test
    public void testAlterRoutineLoadKeepsKinesisExternalId() {
        String sql = "ALTER ROUTINE LOAD FOR job1 FROM KINESIS("
                + "\"property.aws.role_arn\" = \"arn:aws:iam::123456789012:role/test\", "
                + "\"property.aws.external.id\" = \"kinesis-external-id\")";
        String masked = encrypt(sql);
        Assertions.assertTrue(masked.contains("kinesis-external-id"), masked);
    }
}
