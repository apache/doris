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

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class IcebergSysTableFileIOResolverTest {
    private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

    @Test
    void rebindsAllManifestsTaskToAzureSasFileIo() throws Exception {
        FileScanTask task = allManifestsTask();
        Map<String, String> storageProperties = new HashMap<>();
        storageProperties.put("provider", "azure");
        storageProperties.put("AZURE_AUTH_TYPE", "SAS");
        storageProperties.put("AZURE_ENDPOINT", "https://account.blob.core.windows.net");
        storageProperties.put("AZURE_ACCOUNT_NAME", "account");
        storageProperties.put("AZURE_SAS_TOKEN", "sv=2024-01-01&sig=temporary");
        storageProperties.put("AZURE_SAS_EXPIRY_MS", "4102444800000");

        FileScanTask rebound = IcebergSysTableFileIOResolver.resolve(task, storageProperties);
        FileIO fileIO = taskFileIO(rebound);

        Assertions.assertEquals("org.apache.iceberg.azure.adlsv2.ADLSFileIO", fileIO.getClass().getName());
        Assertions.assertEquals("sv=2024-01-01&sig=temporary",
                fileIO.properties().get("adls.sas-token.account.dfs.core.windows.net"));
        Assertions.assertEquals("4102444800000",
                fileIO.properties().get("adls.sas-token-expires-at-ms.account.dfs.core.windows.net"));
        Assertions.assertEquals(task.residual(), rebound.residual());
    }

    @Test
    void keepsNonAzureAndOAuthTasksUnchanged() throws Exception {
        FileScanTask task = allManifestsTask();
        Assertions.assertSame(task, IcebergSysTableFileIOResolver.resolve(task,
                Map.of("provider", "s3", "AWS_ACCESS_KEY", "access")));
        Assertions.assertSame(task, IcebergSysTableFileIOResolver.resolve(task,
                Map.of("provider", "azure", "AZURE_AUTH_TYPE", "OAuth2",
                        "AZURE_ACCOUNT_NAME", "account")));
    }

    @Test
    void createsAzureSharedKeyFileIoProperties() throws Exception {
        Map<String, String> properties = Map.of(
                "provider", "azure",
                "AZURE_AUTH_TYPE", "SHARED_KEY",
                "AZURE_ACCOUNT_NAME", "account",
                "AZURE_ACCOUNT_KEY", "base64-key");

        Assertions.assertEquals(Map.of(
                        "adls.auth.shared-key.account.name", "account",
                        "adls.auth.shared-key.account.key", "base64-key"),
                IcebergSysTableFileIOResolver.azureFileIOProperties(allManifestsTask(), properties));
    }

    @Test
    void rejectsExpiredAzureSasBeforeOpeningTheReader() throws Exception {
        Map<String, String> properties = Map.of(
                "provider", "azure",
                "AZURE_AUTH_TYPE", "SAS",
                "AZURE_ACCOUNT_NAME", "account",
                "AZURE_SAS_TOKEN", "sv=2024-01-01&sig=expired",
                "AZURE_SAS_EXPIRY_MS", "1");

        IllegalArgumentException error = Assertions.assertThrows(IllegalArgumentException.class,
                () -> IcebergSysTableFileIOResolver.resolve(allManifestsTask(), properties));
        Assertions.assertEquals("Azure SAS credential is expired", error.getMessage());
        Assertions.assertFalse(error.getMessage().contains("sig="), "the error must not expose the SAS token");
    }

    private static FileScanTask allManifestsTask() throws Exception {
        Class<?> taskClass = Class.forName("org.apache.iceberg.AllManifestsTable$ManifestListReadTask");
        Constructor<?> constructor = taskClass.getDeclaredConstructor(
                Schema.class, FileIO.class, Schema.class, Map.class, String.class, Expression.class, long.class);
        constructor.setAccessible(true);
        return (FileScanTask) constructor.newInstance(
                SCHEMA, new PropertiesOnlyFileIO(), SCHEMA,
                Map.of(PartitionSpec.unpartitioned().specId(), PartitionSpec.unpartitioned()),
                "abfss://container@account.dfs.core.windows.net/metadata/snap-1.avro",
                Expressions.alwaysTrue(), 1L);
    }

    private static FileIO taskFileIO(FileScanTask task) throws Exception {
        Field io = task.getClass().getDeclaredField("io");
        io.setAccessible(true);
        return (FileIO) io.get(task);
    }

    private static final class PropertiesOnlyFileIO implements FileIO {
        @Override
        public Map<String, String> properties() {
            return Collections.emptyMap();
        }

        @Override
        public InputFile newInputFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public OutputFile newOutputFile(String path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void deleteFile(String path) {
            throw new UnsupportedOperationException();
        }
    }
}
