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

class AdbcConnectorPropertiesTest {

    private static Map<String, String> minimalProperties() {
        Map<String, String> props = new LinkedHashMap<>();
        props.put(AdbcConnectorProperties.DRIVER_URL, "libadbc_driver_flightsql.so");
        props.put(AdbcConnectorProperties.URI, "grpc://remote-fe:9090");
        return props;
    }

    @Test
    void driverOptionsKeepTheAdbcPrefix() {
        // The prefix is part of the option name, NOT a namespace to strip: ADBC's own option names already
        // begin with "adbc." (e.g. adbc.snowflake.sql.db), so stripping would send the driver a name it
        // does not know and the option would be silently ignored. BE applies the same rule; if the two
        // sides ever disagree, a catalog would plan against different driver settings than it reads with.
        Map<String, String> props = minimalProperties();
        props.put("adbc.adbc.snowflake.sql.db", "MYDB");
        props.put("adbc.custom.option", "v");

        Map<String, String> options = AdbcConnectorProperties.driverOptions(props);

        Assertions.assertEquals(Map.of("adbc.adbc.snowflake.sql.db", "MYDB", "adbc.custom.option", "v"),
                options);
    }

    @Test
    void nonPrefixedPropertiesAreNotPassedToTheDriver() {
        Map<String, String> props = minimalProperties();
        props.put("password", "secret");

        Assertions.assertEquals(Map.of(), AdbcConnectorProperties.driverOptions(props));
    }

    @Test
    void partitionedReadIsAutomaticUnlessTheCatalogSaysOtherwise() {
        Assertions.assertEquals(AdbcConnectorProperties.PartitionedReadMode.AUTO,
                AdbcConnectorProperties.partitionedReadMode(minimalProperties()));

        for (AdbcConnectorProperties.PartitionedReadMode mode
                : AdbcConnectorProperties.PartitionedReadMode.values()) {
            Map<String, String> props = minimalProperties();
            // Spelled the way a user writes it, and case-insensitively.
            props.put(AdbcConnectorProperties.PARTITIONED_READ, mode.name().toLowerCase());
            Assertions.assertEquals(mode, AdbcConnectorProperties.partitionedReadMode(props));
        }
    }

    @Test
    void anUnreadableModeFailsInsteadOfMeaningTheDefault() {
        // Falling back to AUTO on a typo would be the worst answer for the one mode that exists to stop
        // a silent downgrade: 'requred' would quietly permit exactly what 'required' forbids.
        Map<String, String> props = minimalProperties();
        props.put(AdbcConnectorProperties.PARTITIONED_READ, "requred");

        IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                () -> AdbcConnectorProperties.partitionedReadMode(props));
        Assertions.assertTrue(e.getMessage().contains(AdbcConnectorProperties.PARTITIONED_READ),
                e.getMessage());
    }

    @Test
    void thePartitionLimitDefaultsAndRejectsValuesThatCannotBeOne() {
        Assertions.assertEquals(1024, AdbcConnectorProperties.maxPartitions(minimalProperties()));

        Map<String, String> raised = minimalProperties();
        raised.put(AdbcConnectorProperties.MAX_PARTITIONS, " 4096 ");
        Assertions.assertEquals(4096, AdbcConnectorProperties.maxPartitions(raised));

        for (String bad : new String[] {"0", "-1", "many"}) {
            Map<String, String> props = minimalProperties();
            props.put(AdbcConnectorProperties.MAX_PARTITIONS, bad);
            IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> AdbcConnectorProperties.maxPartitions(props), bad);
            Assertions.assertTrue(e.getMessage().contains(AdbcConnectorProperties.MAX_PARTITIONS),
                    e.getMessage());
        }
    }

    @Test
    void providerRejectsAnUnreadablePartitionSettingAtCreateTime() {
        // These decide how every scan is planned, so a typo has to fail at CREATE CATALOG rather than
        // changing the plan shape silently from the first query onwards.
        AdbcConnectorProvider provider = new AdbcConnectorProvider();
        for (String[] bad : new String[][] {
                {AdbcConnectorProperties.PARTITIONED_READ, "yes"},
                {AdbcConnectorProperties.MAX_PARTITIONS, "0"}}) {
            Map<String, String> props = minimalProperties();
            props.put(bad[0], bad[1]);
            IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> provider.validateProperties(props));
            Assertions.assertTrue(e.getMessage().contains(bad[0]), e.getMessage());
        }
    }

    /**
     * A cache knob is read through {@code CacheSpec}, which answers an unparseable value with the default
     * rather than an error. Silently caching for ten minutes when the operator wrote {@code ttl-second=6O0}
     * is exactly the kind of thing nobody notices until the metadata is wrong, so the value has to be
     * refused where the operator is still looking at it.
     */
    @Test
    void providerRejectsAnUnreadableCacheSettingAtCreateTime() {
        AdbcConnectorProvider provider = new AdbcConnectorProvider();
        for (String[] bad : new String[][] {
                {"meta.cache.adbc.metadata.enable", "no"},
                {"meta.cache.adbc.metadata.ttl-second", "6O0"},
                {"meta.cache.adbc.metadata.ttl-second", "-2"},
                {"meta.cache.adbc.metadata.capacity", "-1"}}) {
            Map<String, String> props = minimalProperties();
            props.put(bad[0], bad[1]);
            IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> provider.validateProperties(props), bad[0] + "=" + bad[1]);
            Assertions.assertTrue(e.getMessage().contains(bad[0]), e.getMessage());
        }

        // -1 is the framework's "never expire", not a typo.
        Map<String, String> noExpiry = minimalProperties();
        noExpiry.put("meta.cache.adbc.metadata.ttl-second", "-1");
        Assertions.assertDoesNotThrow(() -> provider.validateProperties(noExpiry));
    }

    @Test
    void providerRejectsAMissingDriverUrlOrUri() {
        AdbcConnectorProvider provider = new AdbcConnectorProvider();
        Assertions.assertDoesNotThrow(() -> provider.validateProperties(minimalProperties()));

        for (String required : new String[] {
                AdbcConnectorProperties.DRIVER_URL, AdbcConnectorProperties.URI}) {
            Map<String, String> props = minimalProperties();
            props.remove(required);
            IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> provider.validateProperties(props));
            Assertions.assertTrue(e.getMessage().contains(required), e.getMessage());

            Map<String, String> blank = minimalProperties();
            blank.put(required, "   ");
            IllegalArgumentException blankError = Assertions.assertThrows(IllegalArgumentException.class,
                    () -> provider.validateProperties(blank));
            Assertions.assertTrue(blankError.getMessage().contains(required), blankError.getMessage());
        }
    }
}
