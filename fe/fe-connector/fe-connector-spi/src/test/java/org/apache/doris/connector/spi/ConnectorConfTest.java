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

package org.apache.doris.connector.spi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;

public class ConnectorConfTest {

    private static ConnectorContext context(Map<String, String> conf, Map<String, String> env) {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test_catalog";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }

            @Override
            public Map<String, String> getConnectorConfig() {
                return conf;
            }

            @Override
            public Map<String, String> getEnvironment() {
                return env;
            }
        };
    }

    @Test
    public void pluginConfWinsOverFeConf() {
        // The whole point of the channel: an administrator who sets the key in the plugin's conf gets
        // that value, whatever fe.conf still says. Reverse this precedence and migrating a deployment
        // to the new channel silently does nothing.
        ConnectorContext ctx = context(Collections.singletonMap("drivers_dir", "/from/plugin/conf"),
                Collections.singletonMap("jdbc_drivers_dir", "/from/fe/conf"));
        Assertions.assertEquals("/from/plugin/conf",
                ConnectorConf.get(ctx, "drivers_dir", "jdbc_drivers_dir", "/default"));
    }

    @Test
    public void fallsBackToFeConfWhenThePluginConfHasNoSuchKey() {
        // This is what makes the migration non-breaking: an untouched deployment ships no plugin conf,
        // so every one of these settings must still resolve to the fe.conf value it resolved to before.
        ConnectorContext ctx = context(Collections.emptyMap(),
                Collections.singletonMap("jdbc_drivers_dir", "/from/fe/conf"));
        Assertions.assertEquals("/from/fe/conf",
                ConnectorConf.get(ctx, "drivers_dir", "jdbc_drivers_dir", "/default"));
    }

    @Test
    public void blankInThePluginConfFallsBackRatherThanMaskingFeConf() {
        // 'drivers_dir=' in a conf file reads as "I have not configured this", not "configure it to the
        // empty string". Treating it as a set value would let one stray line hide the value actually in
        // effect, and the operator would have no way to tell from either file which one won.
        ConnectorContext ctx = context(Collections.singletonMap("drivers_dir", "   "),
                Collections.singletonMap("jdbc_drivers_dir", "/from/fe/conf"));
        Assertions.assertEquals("/from/fe/conf",
                ConnectorConf.get(ctx, "drivers_dir", "jdbc_drivers_dir", "/default"));
    }

    @Test
    public void blankInFeConfFallsBackToTheDefault() {
        ConnectorContext ctx = context(Collections.emptyMap(),
                Collections.singletonMap("jdbc_drivers_dir", ""));
        Assertions.assertEquals("/default",
                ConnectorConf.get(ctx, "drivers_dir", "jdbc_drivers_dir", "/default"));
    }

    @Test
    public void defaultWhenNeitherChannelHasIt() {
        ConnectorContext ctx = context(Collections.emptyMap(), Collections.emptyMap());
        Assertions.assertEquals("/default",
                ConnectorConf.get(ctx, "drivers_dir", "jdbc_drivers_dir", "/default"));
        Assertions.assertNull(ConnectorConf.get(ctx, "drivers_dir", "jdbc_drivers_dir", null));
    }

    @Test
    public void nullLegacyKeyNeverReadsTheEnvironment() {
        // A setting introduced after this channel has no fe.conf half. It must not accidentally pick up
        // an unrelated engine environment entry that happens to share its (unprefixed) name -- 'doris_home'
        // and 'doris_version' live in that same map.
        ConnectorContext ctx = context(Collections.emptyMap(),
                Collections.singletonMap("drivers_dir", "/from/env"));
        Assertions.assertEquals("/default",
                ConnectorConf.get(ctx, "drivers_dir", null, "/default"));
    }

    @Test
    public void feConfValueIsReturnedVerbatim() {
        // Byte-identical to what the connector read before this channel existed: the fallback path must
        // not start trimming values that fe.conf delivered untrimmed, or a migrated connector's behavior
        // would differ from the one it replaced in a way no test of the new channel would catch.
        ConnectorContext ctx = context(Collections.emptyMap(),
                Collections.singletonMap("hive_default_file_format", " orc "));
        Assertions.assertEquals(" orc ",
                ConnectorConf.get(ctx, "default_file_format", "hive_default_file_format", "parquet"));
    }

    @Test
    public void worksOnAContextThatImplementsNeitherGetter() {
        // Direct-construction unit tests and classpath built-ins get the interface defaults (two empty
        // maps). Reading a setting must degrade to the default, not NPE.
        ConnectorContext bare = new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test_catalog";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }
        };
        Assertions.assertEquals("/default",
                ConnectorConf.get(bare, "drivers_dir", "jdbc_drivers_dir", "/default"));
    }
}
