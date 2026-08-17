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

public class ConnectorContextTest {

    /** A minimal ConnectorContext implementing only the two abstract methods; everything else default. */
    private static ConnectorContext minimalContext() {
        return new ConnectorContext() {
            @Override
            public String getCatalogName() {
                return "test_catalog";
            }

            @Override
            public long getCatalogId() {
                return 1L;
            }
        };
    }

    @Test
    public void getStorageContext_defaultsToNoop() {
        // Storage lives on its own context now; a connector never has to null-check the getter. A catalog
        // whose engine manages no storage (and every test double that does not override this) answers NOOP,
        // whose methods keep the same benign defaults these assertions used to make on ConnectorContext.
        ConnectorStorageContext storage = minimalContext().getStorageContext();
        Assertions.assertSame(ConnectorStorageContext.NOOP, storage,
                "default getStorageContext() must be NOOP, never null");
    }

    @Test
    public void createSiblingConnector_defaultsToNull() {
        // The cross-plugin sibling seam: only a gateway connector's context (fe-core's DefaultConnectorContext)
        // overrides this to build a real sibling; every other connector keeps the default null, so introducing
        // the seam must not change their behavior -- a non-gateway connector that never calls it is unaffected.
        Connector sibling = minimalContext().createSiblingConnector("iceberg", Collections.emptyMap());
        Assertions.assertNull(sibling,
                "default createSiblingConnector() must return null so non-gateway connectors are unaffected");
    }
}
