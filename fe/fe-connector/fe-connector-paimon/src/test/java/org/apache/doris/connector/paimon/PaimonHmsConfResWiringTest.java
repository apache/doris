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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * FIX-HMS-CONFRES connector-wiring test: proves the HMS create path actually CALLS
 * {@code ConnectorContext.loadHiveConfResources} and feeds the result into the HiveConf builder
 * (intent: no silent drop of an external hive-site.xml), not merely that the pure builder works.
 *
 * <p>The HMS catalog cannot be fully created offline (no live metastore). We drive the connector's
 * lazy catalog creation with {@code RecordingConnectorContext.failAuth=true}, so creation fails fast
 * at {@code executeAuthenticated} — AFTER the HMS branch has already resolved the file via the hook,
 * and BEFORE any metastore connection is attempted.
 */
public class PaimonHmsConfResWiringTest {

    @Test
    public void hmsBranchRoutesHiveConfResourcesThroughContext() {
        RecordingConnectorContext ctx = new RecordingConnectorContext();
        ctx.failAuth = true;
        ctx.hiveConfResources = Collections.singletonMap("hive.metastore.sasl.qop", "auth-conf");

        Map<String, String> props = new HashMap<>();
        props.put("paimon.catalog.type", "hms");
        props.put("warehouse", "/wh");
        props.put("hive.metastore.uris", "thrift://nn:9083");
        props.put("hive.conf.resources", "hive-site.xml");

        PaimonConnector connector = new PaimonConnector(props, ctx);

        // getMetadata -> ensureCatalog -> createCatalog: the HMS branch calls loadHiveConfResources
        // first, then createCatalogFromContext fails fast (failAuth) before connecting to a metastore.
        Assertions.assertThrows(RuntimeException.class, () -> connector.getMetadata(null));

        // WHY: a future refactor that builds the HiveConf without consulting the hook would silently
        // drop the external hive-site.xml again (the very defect). MUTATION: HMS branch not calling
        // loadHiveConfResources -> hiveConfResourcesCalled false / wrong arg -> red.
        Assertions.assertTrue(ctx.hiveConfResourcesCalled,
                "the HMS branch must call ConnectorContext.loadHiveConfResources (no silent drop)");
        Assertions.assertEquals("hive-site.xml", ctx.lastHiveConfResourcesArg,
                "the connector must pass the raw hive.conf.resources value to the hook");
    }
}
