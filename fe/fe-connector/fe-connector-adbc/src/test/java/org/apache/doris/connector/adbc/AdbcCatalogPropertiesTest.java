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

package org.apache.doris.connector.adbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What {@link AdbcCatalogProperties#of(Map)} guarantees, which is the whole reason the holder exists:
 * an instance that exists has valid properties. Everything downstream reads getters instead of the map
 * on the strength of that, so each rule below is load-bearing rather than descriptive.
 */
class AdbcCatalogPropertiesTest {

    private static Map<String, String> minimal() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put(AdbcCatalogProperties.DRIVER_URL, "libadbc_driver_flightsql.so");
        m.put(AdbcCatalogProperties.URI, "grpc://host:31337");
        return m;
    }

    @Test
    void bindsEveryKeyAndDefaults() {
        Map<String, String> m = minimal();
        m.put(AdbcCatalogProperties.USER, "u");
        m.put(AdbcCatalogProperties.PASSWORD, "secret-p");
        m.put(AdbcCatalogProperties.SQL_DIALECT, "postgresql");
        AdbcCatalogProperties p = AdbcCatalogProperties.of(m);
        Assertions.assertEquals("libadbc_driver_flightsql.so", p.getDriverUrl());
        Assertions.assertEquals("grpc://host:31337", p.getUri());
        Assertions.assertEquals("u", p.getUser());
        Assertions.assertEquals("secret-p", p.getPassword());
        Assertions.assertEquals("postgresql", p.getSqlDialect());
        Assertions.assertEquals("", p.getDriverChecksum());
        Assertions.assertEquals("", p.getDriverEntrypoint());
        Assertions.assertEquals(AdbcCatalogProperties.PartitionedReadMode.AUTO, p.getPartitionedReadMode());
        Assertions.assertEquals(1024, p.getMaxPartitions());
    }

    @Test
    void missingDriverUrlFailsNamingTheKey() {
        Map<String, String> m = minimal();
        m.remove(AdbcCatalogProperties.DRIVER_URL);
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> AdbcCatalogProperties.of(m));
        Assertions.assertTrue(e.getMessage().contains(AdbcCatalogProperties.DRIVER_URL));
    }

    @Test
    void blankRequiredValueCountsAsMissing() {
        Map<String, String> m = minimal();
        m.put(AdbcCatalogProperties.URI, "   ");
        Assertions.assertThrows(IllegalArgumentException.class, () -> AdbcCatalogProperties.of(m));
    }

    /**
     * <b>of() rejects bad values, never unknown keys.</b> The catalog property map is shared ground: it
     * also carries engine keys (type, meta.cache.*) and storage keys (s3.*), and {@code ALTER CATALOG}
     * merges properties -- it can overwrite a key but never remove one. So a key refused here would be a
     * catalog nobody can repair, which is a worse failure than the typo it would have caught.
     */
    @Test
    void unknownKeysAreTolerated() {
        Map<String, String> m = minimal();
        m.put("some_future_key", "x");
        m.put("s3.endpoint", "http://minio:9000");
        m.put("type", "adbc");
        Assertions.assertDoesNotThrow(() -> AdbcCatalogProperties.of(m));
    }

    @Test
    void partitionedReadTypoFailsLoud() {
        Map<String, String> m = minimal();
        m.put(AdbcCatalogProperties.PARTITIONED_READ, "requird");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> AdbcCatalogProperties.of(m));
        Assertions.assertTrue(e.getMessage().contains("must be one of"));
    }

    @Test
    void partitionedReadParsesCaseInsensitive() {
        Map<String, String> m = minimal();
        m.put(AdbcCatalogProperties.PARTITIONED_READ, "REQUIRED");
        Assertions.assertEquals(AdbcCatalogProperties.PartitionedReadMode.REQUIRED,
                AdbcCatalogProperties.of(m).getPartitionedReadMode());
    }

    @Test
    void maxPartitionsGarbageFails() {
        Map<String, String> m = minimal();
        m.put(AdbcCatalogProperties.MAX_PARTITIONS, "abc");
        Assertions.assertThrows(IllegalArgumentException.class, () -> AdbcCatalogProperties.of(m));
    }

    @Test
    void maxPartitionsZeroFails() {
        Map<String, String> m = minimal();
        m.put(AdbcCatalogProperties.MAX_PARTITIONS, "0");
        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> AdbcCatalogProperties.of(m));
        Assertions.assertTrue(e.getMessage().contains("at least 1"));
    }

    @Test
    void driverOptionsKeepPrefixAndOrder() {
        Map<String, String> m = minimal();
        m.put("adbc.adbc.snowflake.sql.db", "d1");
        m.put("adbc.custom.flag", "on");
        AdbcCatalogProperties p = AdbcCatalogProperties.of(m);
        Assertions.assertEquals(2, p.getDriverOptions().size());
        // The prefix is part of the option name and is NOT stripped; BE applies the same rule to its own
        // parameter map, and a catalog whose two sides disagreed would plan against different settings
        // than it reads with.
        Assertions.assertEquals("d1", p.getDriverOptions().get("adbc.adbc.snowflake.sql.db"));
        Assertions.assertEquals("[adbc.adbc.snowflake.sql.db, adbc.custom.flag]",
                p.getDriverOptions().keySet().toString());
    }

    /**
     * The password is annotated {@code sensitive}, and that annotation is the only thing standing between
     * a credential and any log line that renders this object. Masking has to be asserted rather than
     * assumed, because the failure -- a password in a log file -- leaves no trace at the call site.
     */
    @Test
    void toStringMasksPassword() {
        Map<String, String> m = minimal();
        m.put(AdbcCatalogProperties.PASSWORD, "secret-p");
        String s = AdbcCatalogProperties.of(m).toString();
        Assertions.assertFalse(s.contains("secret-p"), s);
    }

    @Test
    void metaCacheGarbageTtlFailsAtCreate() {
        Map<String, String> m = minimal();
        m.put(AdbcMetadataCache.propertySpec().getTtlKey(), "not-a-number");
        Assertions.assertThrows(IllegalArgumentException.class, () -> AdbcCatalogProperties.of(m));
    }
}
