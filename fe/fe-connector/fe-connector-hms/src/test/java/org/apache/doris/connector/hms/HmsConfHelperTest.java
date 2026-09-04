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

package org.apache.doris.connector.hms;

import org.apache.doris.connector.spi.ConnectorContext;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Tests {@link HmsConfHelper#createHiveConf(Map)} — the HiveConf builder for the hms connector's
 * metastore Thrift client ({@code ThriftHmsClient} constructs its conf here).
 *
 * <p>WHY: a kerberized HMS only accepts a SASL-negotiated Thrift transport. The legacy fe-core
 * {@code HMSBaseProperties.initHadoopAuthenticator} auto-enabled {@code hive.metastore.sasl.enabled}
 * whenever the metastore/hadoop auth was kerberos (HMSBaseProperties.java:162,179). The SPI connector's
 * builder previously only copied raw properties, so a catalog that declared kerberos auth but did not
 * spell out {@code hive.metastore.sasl.enabled} opened a plain TSocket the metastore dropped with
 * {@code TTransportException} (test_single_hive_kerberos / test_two_hive_kerberos). These tests pin the
 * restored auto-injection and its opposite — non-kerberos auth must NOT flip SASL on (Rule 9: encode WHY
 * the injection is gated on kerberos, not just that it happens).</p>
 */
public class HmsConfHelperTest {

    @TempDir
    Path tempDir;

    private static String saslOf(Map<String, String> props) {
        HiveConf conf = HmsConfHelper.createHiveConf(props);
        return conf.get("hive.metastore.sasl.enabled");
    }

    @Test
    public void hadoopSecurityAuthKerberosEnablesSasl() {
        Map<String, String> props = new HashMap<>();
        props.put("hive.metastore.uris", "thrift://host:9583");
        props.put("hadoop.security.authentication", "kerberos");
        props.put("hive.metastore.kerberos.principal", "hive/hadoop-master@LABS.TERADATA.COM");
        // No explicit hive.metastore.sasl.enabled — the kerberized catalog relies on auto-injection.
        Assertions.assertEquals("true", saslOf(props));
    }

    @Test
    public void hiveMetastoreAuthTypeKerberosEnablesSasl() {
        Map<String, String> props = new HashMap<>();
        props.put("hive.metastore.uris", "thrift://host:9583");
        props.put("hive.metastore.authentication.type", "kerberos");
        Assertions.assertEquals("true", saslOf(props));
    }

    @Test
    public void kerberosAuthIsCaseInsensitive() {
        Map<String, String> props = new HashMap<>();
        props.put("hadoop.security.authentication", "KERBEROS");
        Assertions.assertEquals("true", saslOf(props));
    }

    @Test
    public void simpleAuthDoesNotEnableSasl() {
        Map<String, String> props = new HashMap<>();
        props.put("hive.metastore.uris", "thrift://host:9083");
        props.put("hadoop.security.authentication", "simple");
        Assertions.assertNotEquals("true", saslOf(props));
    }

    @Test
    public void explicitSimpleHmsOverridesKerberosStorageForSasl() {
        Map<String, String> props = new HashMap<>();
        props.put("hive.metastore.authentication.type", "simple");
        props.put("hadoop.security.authentication", "kerberos");
        Assertions.assertNotEquals("true", saslOf(props));
    }

    @Test
    public void explicitSimpleHmsOverridesSaslFromHiveConfResource() throws Exception {
        Path resource = tempDir.resolve("hive-site.xml");
        Files.writeString(resource, "<configuration><property>"
                + "<name>hive.metastore.sasl.enabled</name><value>true</value>"
                + "</property></configuration>");
        String previous = System.setProperty("doris.hadoop.config.dir", tempDir + "/");
        try {
            HiveConf conf = HmsConfHelper.createHiveConfWithResources(resource.getFileName().toString(),
                    Map.of("hive.metastore.authentication.type", "simple"));
            Assertions.assertEquals("false", conf.get("hive.metastore.sasl.enabled"));
        } finally {
            if (previous == null) {
                System.clearProperty("doris.hadoop.config.dir");
            } else {
                System.setProperty("doris.hadoop.config.dir", previous);
            }
        }
    }

    @Test
    public void noAuthPropertyDoesNotEnableSasl() {
        Map<String, String> props = new HashMap<>();
        props.put("hive.metastore.uris", "thrift://host:9083");
        Assertions.assertNotEquals("true", saslOf(props));
    }

    @Test
    public void arbitraryPropertiesArePassedThrough() {
        Map<String, String> props = new HashMap<>();
        props.put("hive.metastore.uris", "thrift://host:9083");
        props.put("some.custom.key", "custom-value");
        HiveConf conf = HmsConfHelper.createHiveConf(props);
        Assertions.assertEquals("thrift://host:9083", conf.get("hive.metastore.uris"));
        Assertions.assertEquals("custom-value", conf.get("some.custom.key"));
    }

    @Test
    public void canonicalOverridesPreserveUnrelatedKeysAndDropBlankUser() {
        Map<String, String> raw = new HashMap<>();
        raw.put("some.custom.key", "custom-value");
        raw.put("hadoop.username", "  ");
        Map<String, String> overrides = new HashMap<>();
        overrides.put("hive.metastore.uris", "thrift://host:9083");

        Map<String, String> merged = HmsConfHelper.mergeCatalogProperties(raw, overrides);
        Assertions.assertEquals("custom-value", merged.get("some.custom.key"));
        Assertions.assertEquals("thrift://host:9083", merged.get("hive.metastore.uris"));
        Assertions.assertFalse(merged.containsKey("hadoop.username"));
    }

    @Test
    public void metastoreTimeoutFallsBackToFeEnvironment() {
        ConnectorContext context = new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }

            @Override
            public Map<String, String> getEnvironment() {
                return Map.of("hive_metastore_client_timeout_second", "47");
            }
        };
        Assertions.assertEquals("47", HmsConfHelper.metastoreClientTimeoutSecond(context));
    }
}
