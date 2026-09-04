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

package org.apache.doris.connector.es;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

/** What this connector declares to the engine before any catalog of its type is initialized. */
public class EsConnectorProviderTest {

    @Test
    public void switchingToAnEsCatalogLandsInItsSingleDatabase() {
        // Elasticsearch has no database layer; Doris presents one synthetic database for it. SWITCH used to
        // reach it through a hardcoded "es" type check in the engine plus a second copy of the "default_db"
        // literal. The connector now names it, and it must be the very database this connector lists and
        // resolves — otherwise SWITCH lands the session in a database that does not exist.
        Assertions.assertEquals(Optional.of(EsConnectorMetadata.DEFAULT_DB),
                new EsConnectorProvider().defaultDatabaseOnUse());
    }

    @Test
    public void anEsCatalogIsNotForceInitializedForEventSync() {
        // ES exposes no metastore-event source, so the engine's event driver must never force-initialize an
        // idle es catalog just to look for one.
        Assertions.assertFalse(new EsConnectorProvider().providesEventSource());
    }
}
