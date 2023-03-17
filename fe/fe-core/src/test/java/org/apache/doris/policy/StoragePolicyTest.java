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

package org.apache.doris.policy;

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.S3Resource;
import org.apache.doris.catalog.SparkResource;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Test for StoragePolicy.
 **/
@TestInstance(Lifecycle.PER_METHOD)
public class StoragePolicyTest {

    public static final String STORAGE_RESOURCE = "storage_resource";
    public static final String COOLDOWN_DATETIME = "cooldown_datetime";
    public static final String COOLDOWN_TTL = "cooldown_ttl";
    private static final String S3_ROOT_PATH = "AWS_ROOT_PATH";
    private static final String S3_BUCKET = "AWS_BUCKET";
    private static final String S3_VALIDITY_CHECK = "s3_validity_check";
    private static final long POLICY_ID = 1;
    private static final String POLICY_NAME = "policy_test";
    private static final String S3_RESOURCE_NAME = "s3_resource_test";
    private static final String SPARK_RESOURCE_NAME = "spark_resource_test";

    @BeforeAll
    protected static final void beforeAll() throws DdlException {
        S3Resource s3Resource = new S3Resource(S3_RESOURCE_NAME);
        S3Resource s3ResourceNoRootpath = new S3Resource("s3_resource_test_no_rootpath");
        S3Resource s3ResourceNoBucket = new S3Resource("s3_resource_test_no_bucket");
        Map<String, String> properties = new HashMap<>();
        properties.put(S3_VALIDITY_CHECK, "false");
        properties.put(S3Resource.S3_ENDPOINT, "test");
        properties.put(S3Resource.S3_ACCESS_KEY, "ak");
        properties.put(S3Resource.S3_SECRET_KEY, "sk");
        s3ResourceNoRootpath.modifyProperties(properties);
        Env.getCurrentEnv().getResourceMgr().createResource(s3ResourceNoRootpath, false);
        properties.put(S3Resource.S3_ROOT_PATH, "root_path");
        s3ResourceNoBucket.modifyProperties(properties);
        Env.getCurrentEnv().getResourceMgr().createResource(s3ResourceNoBucket, false);
        properties.put(S3Resource.S3_BUCKET, "bucket");
        s3Resource.modifyProperties(properties);
        Env.getCurrentEnv().getResourceMgr().createResource(s3Resource, false);
        Resource sparkResource = new SparkResource(SPARK_RESOURCE_NAME);
        Env.getCurrentEnv().getResourceMgr().createResource(sparkResource, false);
    }

    @Test
    public void testInit() {
        StoragePolicy storagePolicy = new StoragePolicy(POLICY_ID, POLICY_NAME);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> storagePolicy.init(null, false));
        Assertions.assertEquals("errCode = 2, detailMessage = properties config is required", exception.getMessage());
        Map<String, String> props = new HashMap<>();
        exception = Assertions.assertThrows(AnalysisException.class, () -> storagePolicy.init(props, false));
        Assertions.assertEquals("errCode = 2, detailMessage = Missing [storage_resource] in properties.",
                exception.getMessage());
        props.put(STORAGE_RESOURCE, S3_RESOURCE_NAME);
        exception = Assertions.assertThrows(AnalysisException.class, () -> storagePolicy.init(props, false));
        Assertions.assertEquals("errCode = 2, detailMessage = cooldown_datetime or cooldown_ttl must be set",
                exception.getMessage());
        props.put(COOLDOWN_DATETIME, "2023-01-01 00:00:00");
        props.put(COOLDOWN_TTL, "10d");
        exception = Assertions.assertThrows(AnalysisException.class, () -> storagePolicy.init(props, false));
        Assertions.assertEquals("errCode = 2, detailMessage = cooldown_datetime and cooldown_ttl "
                + "can't be set together.", exception.getMessage());
        props.remove(COOLDOWN_TTL);
        props.put(COOLDOWN_DATETIME, "test");
        exception = Assertions.assertThrows(AnalysisException.class, () -> storagePolicy.init(props, false));
        Assertions.assertEquals("errCode = 2, detailMessage = cooldown_datetime format error: test",
                exception.getMessage());
        props.put(COOLDOWN_TTL, "10d");
        props.remove(COOLDOWN_DATETIME);
        props.put(STORAGE_RESOURCE, "test");
        exception = Assertions.assertThrows(AnalysisException.class, () -> storagePolicy.init(props, false));
        Assertions.assertEquals("errCode = 2, detailMessage = storage resource doesn't exist: test",
                exception.getMessage());
        props.put(STORAGE_RESOURCE, SPARK_RESOURCE_NAME);
        exception = Assertions.assertThrows(AnalysisException.class, () -> storagePolicy.init(props, false));
        Assertions.assertEquals("errCode = 2, detailMessage = current storage policy just support "
                + "resource type S3_COOLDOWN", exception.getMessage());
        props.put(STORAGE_RESOURCE, "s3_resource_test_no_rootpath");
        exception = Assertions.assertThrows(AnalysisException.class, () -> storagePolicy.init(props, false));
        Assertions.assertEquals("errCode = 2, detailMessage = Missing [AWS_ROOT_PATH] in "
                + "'s3_resource_test_no_rootpath' resource", exception.getMessage());
        props.put(STORAGE_RESOURCE, "s3_resource_test_no_bucket");
        exception = Assertions.assertThrows(AnalysisException.class, () -> storagePolicy.init(props, false));
        Assertions.assertEquals("errCode = 2, detailMessage = Missing [AWS_BUCKET] in "
                + "'s3_resource_test_no_bucket' resource", exception.getMessage());
        props.put(STORAGE_RESOURCE, S3_RESOURCE_NAME);
        Assertions.assertDoesNotThrow(() -> storagePolicy.init(props, false));
        Assertions.assertEquals(POLICY_NAME, storagePolicy.getPolicyName());
        Assertions.assertEquals(S3_RESOURCE_NAME, storagePolicy.getStorageResource());
        Assertions.assertEquals(864000, storagePolicy.getCooldownTtl());
        Assertions.assertEquals(-1, storagePolicy.getCooldownTimestampMs());
        StoragePolicy storagePolicy2 = new StoragePolicy(POLICY_ID, "policy2");
        props.remove(COOLDOWN_TTL);
        props.put(COOLDOWN_DATETIME, "2023-01-01 00:00:00");
        Assertions.assertDoesNotThrow(() -> storagePolicy2.init(props, false));
        Assertions.assertEquals("policy2", storagePolicy2.getPolicyName());
        Assertions.assertEquals(S3_RESOURCE_NAME, storagePolicy2.getStorageResource());
        Assertions.assertEquals(-1, storagePolicy2.getCooldownTtl());
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Long cooldownTimestampMs = -1L;
        try {
            cooldownTimestampMs = df.parse("2023-01-01 00:00:00").getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        Assertions.assertEquals(cooldownTimestampMs, storagePolicy2.getCooldownTimestampMs());
    }

    @Test
    public void testModify() {
        StoragePolicy storagePolicy = new StoragePolicy(POLICY_ID, "policy3");
        Map<String, String> props = new HashMap<>();
        props.put(STORAGE_RESOURCE, S3_RESOURCE_NAME);
        props.put(COOLDOWN_TTL, "10d");
        Assertions.assertDoesNotThrow(() -> storagePolicy.init(props, false));
        Assertions.assertEquals("policy3", storagePolicy.getPolicyName());
        Assertions.assertEquals(S3_RESOURCE_NAME, storagePolicy.getStorageResource());
        Assertions.assertEquals(864000, storagePolicy.getCooldownTtl());
        Assertions.assertEquals(-1, storagePolicy.getCooldownTimestampMs());

        props.clear();
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> storagePolicy.modifyProperties(props));
        Assertions.assertEquals("errCode = 2, detailMessage = cooldown_datetime or cooldown_ttl must be set",
                exception.getMessage());
        props.put(COOLDOWN_DATETIME, "2023-01-01 00:00:00");
        props.put(COOLDOWN_TTL, "10d");
        exception = Assertions.assertThrows(AnalysisException.class, () -> storagePolicy.modifyProperties(props));
        Assertions.assertEquals("errCode = 2, detailMessage = cooldown_datetime and cooldown_ttl "
                + "can't be set together.", exception.getMessage());
        props.remove(COOLDOWN_TTL);
        props.put(COOLDOWN_DATETIME, "test");
        exception = Assertions.assertThrows(AnalysisException.class, () -> storagePolicy.modifyProperties(props));
        Assertions.assertEquals("errCode = 2, detailMessage = cooldown_datetime format error: test",
                exception.getMessage());
        props.put(COOLDOWN_TTL, "1h");
        props.remove(COOLDOWN_DATETIME);
        props.put(STORAGE_RESOURCE, "test");
        exception = Assertions.assertThrows(AnalysisException.class, () -> storagePolicy.modifyProperties(props));
        Assertions.assertEquals("errCode = 2, detailMessage = storage resource doesn't exist: test",
                exception.getMessage());
        props.put(STORAGE_RESOURCE, SPARK_RESOURCE_NAME);
        exception = Assertions.assertThrows(AnalysisException.class, () -> storagePolicy.modifyProperties(props));
        Assertions.assertEquals("errCode = 2, detailMessage = current storage policy just support "
                + "resource type S3_COOLDOWN", exception.getMessage());
        props.put(STORAGE_RESOURCE, "s3_resource_test_no_rootpath");
        exception = Assertions.assertThrows(AnalysisException.class, () -> storagePolicy.modifyProperties(props));
        Assertions.assertEquals("errCode = 2, detailMessage = Missing [AWS_ROOT_PATH] in "
                + "'s3_resource_test_no_rootpath' resource", exception.getMessage());
        props.put(STORAGE_RESOURCE, "s3_resource_test_no_bucket");
        exception = Assertions.assertThrows(AnalysisException.class, () -> storagePolicy.modifyProperties(props));
        Assertions.assertEquals("errCode = 2, detailMessage = Missing [AWS_BUCKET] in "
                + "'s3_resource_test_no_bucket' resource", exception.getMessage());
        props.put(STORAGE_RESOURCE, S3_RESOURCE_NAME);
        Assertions.assertDoesNotThrow(() -> storagePolicy.modifyProperties(props));
        Assertions.assertEquals("policy3", storagePolicy.getPolicyName());
        Assertions.assertEquals(S3_RESOURCE_NAME, storagePolicy.getStorageResource());
        Assertions.assertEquals(3600, storagePolicy.getCooldownTtl());
        Assertions.assertEquals(-1, storagePolicy.getCooldownTimestampMs());
        props.remove(COOLDOWN_TTL);
        props.put(COOLDOWN_DATETIME, "2023-01-01 00:00:00");
        Assertions.assertDoesNotThrow(() -> storagePolicy.modifyProperties(props));
        Assertions.assertEquals(-1, storagePolicy.getCooldownTtl());
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Long cooldownTimestampMs = -1L;
        try {
            cooldownTimestampMs = df.parse("2023-01-01 00:00:00").getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        Assertions.assertEquals(cooldownTimestampMs, storagePolicy.getCooldownTimestampMs());
    }
}
