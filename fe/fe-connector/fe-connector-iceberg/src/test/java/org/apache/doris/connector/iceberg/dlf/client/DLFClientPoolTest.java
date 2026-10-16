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

package org.apache.doris.connector.iceberg.dlf.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.hive.RuntimeMetaException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class DLFClientPoolTest {
    @Test
    void storageFormatDescriptorsKeepPublicClassNames() throws Exception {
        Class<?> factoryClass = Class.forName("org.apache.hadoop.hive.ql.io.StorageFormatFactory");
        Object factory = factoryClass.getConstructor().newInstance();
        Class<?> descriptorClass = Class.forName("org.apache.hadoop.hive.ql.io.StorageFormatDescriptor");
        Map<String, String> expectedInputs = Map.of(
                "TEXTFILE", "org.apache.hadoop.mapred.TextInputFormat",
                "SEQUENCEFILE", "org.apache.hadoop.mapred.SequenceFileInputFormat",
                "RCFILE", "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
                "AVRO", "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
                "ORC", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
                "PARQUET", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat");
        for (Map.Entry<String, String> entry : expectedInputs.entrySet()) {
            Object descriptor = factoryClass.getMethod("get", String.class).invoke(factory, entry.getKey());
            Assertions.assertNotNull(descriptor, entry.getKey());
            Assertions.assertEquals(entry.getValue(), descriptorClass.getMethod("getInputFormat").invoke(descriptor));
            String output = (String) descriptorClass.getMethod("getOutputFormat").invoke(descriptor);
            Assertions.assertTrue(output.startsWith("org.apache.hadoop."), output);
        }
        // Custom symlink-table metadata also uses the public Hive class name.
        Assertions.assertNotNull(getClass().getClassLoader().getResource(
                "org/apache/hadoop/hive/ql/io/SymlinkTextInputFormat.class"));
    }

    @Test
    void constructsDlfClientWithoutContactingService() {
        Configuration conf = new Configuration(false);
        conf.set("dlf.catalog.proxyMode", "DLF_ONLY");
        conf.set("dlf.catalog.akMode", "MANUAL");
        conf.set("dlf.catalog.accessKeyId", "test-key");
        conf.set("dlf.catalog.accessKeySecret", "test-secret");
        conf.set("dlf.catalog.endpoint", "dlf.example.invalid");
        conf.set("dlf.catalog.region", "test-region");
        // Exercise the real constructor and local dependencies without its default-database RPC.
        conf.setBoolean("dlf.catalog.createDefaultDBIfNotExist", false);
        new DLFClientPool(1, conf).newClient().close();
    }

    @Test
    void invokesDlfConstructorBeforeValidatingConfiguration() {
        Configuration conf = new Configuration(false);
        // Invalid mode stops inside the real vendor constructor before any remote client is created.
        conf.set("dlf.catalog.proxyMode", "invalid-test-mode");
        DLFClientPool pool = new DLFClientPool(1, conf);
        Throwable cause = Assertions.assertThrows(RuntimeMetaException.class, pool::newClient);
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        Assertions.assertInstanceOf(IllegalArgumentException.class, cause);
        Assertions.assertTrue(cause.getMessage().contains("invalid-test-mode"), cause.toString());
    }
}
