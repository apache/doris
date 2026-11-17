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

import org.apache.doris.extension.loader.ChildFirstClassLoader;

import com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

class PaimonDlfExpressionProxyTest {
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
    void constructsDlfClientWithoutContactingService() throws Exception {
        HiveConf conf = new HiveConf();
        conf.set("dlf.catalog.proxyMode", "DLF_ONLY");
        conf.set("dlf.catalog.akMode", "MANUAL");
        conf.set("dlf.catalog.accessKeyId", "test-key");
        conf.set("dlf.catalog.accessKeySecret", "test-secret");
        conf.set("dlf.catalog.endpoint", "dlf.example.invalid");
        conf.set("dlf.catalog.region", "test-region");
        // The vendor constructor normally checks the remote default database; keep this test offline.
        conf.setBoolean("dlf.catalog.createDefaultDBIfNotExist", false);
        new ProxyMetaStoreClient(conf).close();
    }

    @Test
    void expressionProxyDoesNotDelegateToHostHiveQl() throws Exception {
        ClassLoader host = new ClassLoader(getClass().getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
                if (name.equals("org.apache.hadoop.hive.ql.optimizer.ppr.PartitionExpressionForMetastore")) {
                    // FE's Hive 3 QL implementation cannot link the plugin-only metastore interface.
                    throw new NoClassDefFoundError("org/apache/hadoop/hive/metastore/PartitionExpressionProxy");
                }
                if (name.startsWith("org.apache.hadoop.hive.")) {
                    throw new ClassNotFoundException(name);
                }
                return super.loadClass(name, resolve);
            }
        };
        URL[] pluginUrls = {getClass().getProtectionDomain().getCodeSource().getLocation(),
                HiveConf.class.getProtectionDomain().getCodeSource().getLocation()};
        try (ChildFirstClassLoader plugin = new ChildFirstClassLoader(pluginUrls, host,
                List.of("java.", "org.apache.hadoop.", "org.junit.jupiter.", "org.slf4j."))) {
            Class<?> testClass = plugin.loadClass(getClass().getName());
            var constructor = testClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            var verify = testClass.getDeclaredMethod("defaultProxyConvertsAndFiltersPartitions");
            verify.setAccessible(true);
            // Metadata operations enter through the same TCCL-pinning context in production.
            TcclPinningConnectorContext context = new TcclPinningConnectorContext(
                    new RecordingConnectorContext(), plugin, () -> null);
            context.executeAuthenticated(() -> verify.invoke(constructor.newInstance()));
        }
    }

    @Test
    void defaultProxyConvertsAndFiltersPartitions() throws Exception {
        HiveConf conf = new HiveConf();
        String proxyName = conf.getVar(HiveConf.ConfVars.METASTORE_EXPRESSION_PROXY_CLASS);
        // Follow the vendor client's default configuration, including the relocated class-name string.
        Class<?> proxyClass = Class.forName(proxyName);
        PartitionExpressionProxy proxy = (PartitionExpressionProxy) proxyClass.getConstructor().newInstance();
        Assertions.assertSame(PartitionExpressionProxy.class.getClassLoader(), proxyClass.getClassLoader());

        String ql = proxyName.substring(0, proxyName.indexOf(".optimizer."));
        Class<?> columnClass = Class.forName(ql + ".plan.ExprNodeColumnDesc");
        Object column = columnClass.getConstructor(TypeInfo.class, String.class, String.class, boolean.class)
                .newInstance(TypeInfoFactory.intTypeInfo, "part", "table", true);
        Object constant = Class.forName(ql + ".plan.ExprNodeConstantDesc")
                .getConstructor(Object.class).newInstance(1);
        Class<?> udfClass = Class.forName(ql + ".udf.generic.GenericUDF");
        Object equal = Class.forName(ql + ".udf.generic.GenericUDFOPEqual").getConstructor().newInstance();
        Class<?> expressionClass = Class.forName(ql + ".plan.ExprNodeGenericFuncDesc");
        Object expression = expressionClass.getMethod("newInstance", udfClass, List.class)
                .invoke(null, equal, new ArrayList<>(List.of(column, constant)));
        byte[] serialized = (byte[]) Class.forName(ql + ".exec.SerializationUtilities")
                .getMethod("serializeExpressionToKryo", expressionClass).invoke(null, expression);

        Assertions.assertTrue(proxy.convertExprToFilter(serialized).contains("part"));
        List<String> partitions = new ArrayList<>(List.of("part=1", "part=2"));
        Assertions.assertFalse(proxy.filterPartitionsByExpr(List.of("part"),
                List.of(TypeInfoFactory.intTypeInfo), serialized, "__HIVE_DEFAULT_PARTITION__", partitions));
        Assertions.assertEquals(List.of("part=1"), partitions);
    }
}
