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

package org.apache.doris.connector.paimon;

import org.apache.doris.thrift.TPaimonTableDesc;

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.JsonSerdeUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PaimonTableDescriptorTest {
    @Test
    public void testCommonDescriptorPreservesPreparedSchema() {
        Map<String, String> options = new HashMap<>();
        options.put("branch", "audit");
        options.put("read.batch-size", "37");
        TableSchema schema = new TableSchema(7,
                Arrays.asList(DataTypes.FIELD(3, "id", DataTypes.INT()),
                        DataTypes.FIELD(8, "value", DataTypes.STRING())),
                12, Collections.emptyList(), Collections.emptyList(), options, "schema comment");
        Map<String, String> storage = s3Properties();
        Map<String, String> original = new HashMap<>(storage);

        TPaimonTableDesc descriptor = PaimonTableDescriptor.describe(
                "s3://bucket/db.db/t", schema, "db", "t", storage);
        TableSchema restored = JsonSerdeUtil.fromJson(descriptor.getSchemaJson(), TableSchema.class);
        Assertions.assertEquals(schema.id(), restored.id());
        Assertions.assertEquals(schema.highestFieldId(), restored.highestFieldId());
        Assertions.assertEquals(schema.fields(), restored.fields());
        Assertions.assertEquals(schema.options(), restored.options());
        Assertions.assertEquals("audit", descriptor.getBranch());
        Assertions.assertEquals("db", descriptor.getDatabaseName());
        Assertions.assertEquals("t", descriptor.getTableName());
        Assertions.assertEquals("s3://bucket/db.db/t", descriptor.getTablePath());
        Assertions.assertFalse(descriptor.isSetCatalogContext());
        Assertions.assertFalse(restored.options().containsKey("write.parquet-buffer-size"));
        Assertions.assertEquals(original, storage);
    }

    @Test
    public void testS3AndS3aAliases() {
        for (String scheme : Arrays.asList("s3", "s3a")) {
            Map<String, String> normalized = PaimonTableDescriptor.normalizeStorage(
                    scheme + "://bucket/t", s3Properties());
            Assertions.assertEquals("access", normalized.get("s3.access-key"));
            Assertions.assertEquals("secret", normalized.get("s3.secret-key"));
            Assertions.assertEquals("token", normalized.get("s3.session.token"));
            Assertions.assertEquals("https://endpoint", normalized.get("s3.endpoint"));
            Assertions.assertEquals("region", normalized.get("s3.region"));
            Assertions.assertEquals("true", normalized.get("s3.path-style-access"));
            Assertions.assertFalse(normalized.containsKey("s3.anonymous"));
            Assertions.assertFalse(normalized.containsKey("s3.assumed.role.arn"));
        }
        Map<String, String> storage = new HashMap<>();
        storage.put("fs.s3a.access.key", "hadoop-access");
        storage.put("fs.s3a.secret.key", "hadoop-secret");
        storage.put("fs.s3a.endpoint.region", "hadoop-region");
        Map<String, String> normalized = PaimonTableDescriptor.normalizeStorage("s3a://bucket/t", storage);
        Assertions.assertEquals("hadoop-access", normalized.get("s3.access-key"));
        Assertions.assertEquals("hadoop-secret", normalized.get("s3.secret-key"));
        Assertions.assertEquals("hadoop-region", normalized.get("s3.region"));
    }

    @Test
    public void testAnonymousAndAssumeRoleAliases() {
        Map<String, String> storage = s3Properties();
        storage.put("AWS_CREDENTIALS_PROVIDER_TYPE", "ANONYMOUS");
        storage.put("AWS_ROLE_ARN", "arn:aws:iam::123:role/reader");
        storage.put("AWS_EXTERNAL_ID", "external-123");
        Map<String, String> normalized = PaimonTableDescriptor.normalizeStorage("s3://bucket/t", storage);
        Assertions.assertEquals("true", normalized.get("s3.anonymous"));
        Assertions.assertEquals("arn:aws:iam::123:role/reader", normalized.get("s3.assumed.role.arn"));
        Assertions.assertEquals("external-123", normalized.get("s3.assumed.role.externalId"));
    }

    @Test
    public void testOssUsesItsOwnOptionFamily() {
        Map<String, String> storage = s3Properties();
        Map<String, String> normalized = PaimonTableDescriptor.normalizeStorage("oss://bucket/t", storage);
        Assertions.assertEquals("access", normalized.get("fs.oss.accessKeyId"));
        Assertions.assertEquals("secret", normalized.get("fs.oss.accessKeySecret"));
        Assertions.assertEquals("token", normalized.get("fs.oss.securityToken"));
        Assertions.assertEquals("https://endpoint", normalized.get("fs.oss.endpoint"));
        Assertions.assertFalse(normalized.containsKey("s3.access-key"));
        Assertions.assertFalse(normalized.containsKey("s3.endpoint"));

        storage.put("fs.oss.accessKeyId", "native-access");
        storage.put("fs.oss.endpoint", "https://native-endpoint");
        normalized = PaimonTableDescriptor.normalizeStorage("oss://bucket/t", storage);
        Assertions.assertEquals("native-access", normalized.get("fs.oss.accessKeyId"));
        Assertions.assertEquals("https://native-endpoint", normalized.get("fs.oss.endpoint"));
    }

    @Test
    public void testNativeS3OptionsWinAndEmptyAliasesAreIgnored() {
        Map<String, String> storage = s3Properties();
        storage.put("s3.access-key", "native-access");
        storage.put("s3.path-style-access", "false");
        storage.put("AWS_TOKEN", "");
        Map<String, String> normalized = PaimonTableDescriptor.normalizeStorage("s3://bucket/t", storage);
        Assertions.assertEquals("native-access", normalized.get("s3.access-key"));
        Assertions.assertEquals("false", normalized.get("s3.path-style-access"));
        Assertions.assertFalse(normalized.containsKey("s3.session.token"));
    }

    @Test
    public void testOssAliasesOnS3Tables() {
        Map<String, String> storage = new HashMap<>();
        storage.put("fs.oss.accessKeyId", "oss-access");
        storage.put("fs.oss.accessKeySecret", "oss-secret");
        storage.put("fs.oss.sessionToken", "oss-token");
        storage.put("fs.oss.endpoint", "https://oss-endpoint");
        storage.put("fs.oss.region", "oss-region");
        Map<String, String> normalized = PaimonTableDescriptor.normalizeStorage("s3://bucket/t", storage);
        Assertions.assertEquals("oss-access", normalized.get("s3.access-key"));
        Assertions.assertEquals("oss-secret", normalized.get("s3.secret-key"));
        Assertions.assertEquals("oss-token", normalized.get("s3.session.token"));
        Assertions.assertEquals("https://oss-endpoint", normalized.get("s3.endpoint"));
        Assertions.assertEquals("oss-region", normalized.get("s3.region"));
    }

    @Test
    public void testOtherSchemesPassThrough() {
        Map<String, String> storage = s3Properties();
        storage.put("fs.defaultFS", "hdfs://namenode");
        for (String location : Arrays.asList("cosn://bucket/t", "cos://bucket/t", "obs://bucket/t",
                "gs://bucket/t", "gcs://bucket/t", "abfs://bucket/t", "abfss://bucket/t",
                "az://bucket/t", "azure://bucket/t", "hdfs://namenode/t", "file:///tmp/t", "/tmp/t")) {
            Assertions.assertEquals(storage, PaimonTableDescriptor.normalizeStorage(location, storage), location);
        }
    }

    private static Map<String, String> s3Properties() {
        Map<String, String> storage = new HashMap<>();
        storage.put("AWS_ACCESS_KEY", "access");
        storage.put("AWS_SECRET_KEY", "secret");
        storage.put("AWS_TOKEN", "token");
        storage.put("AWS_ENDPOINT", "https://endpoint");
        storage.put("AWS_REGION", "region");
        storage.put("use_path_style", "true");
        return storage;
    }
}
