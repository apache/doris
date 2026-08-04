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

package org.apache.doris.connector.hudi;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** What this provider declares about itself to the engine. */
public class HudiConnectorProviderTest {

    @Test
    public void testHudiIsSiblingOnlyAndNeverAStandaloneCatalogType() {
        HudiConnectorProvider provider = new HudiConnectorProvider();

        Assertions.assertEquals("hudi", provider.getType(),
                "the type string is the key the hive gateway passes to createSiblingConnector");
        Assertions.assertFalse(provider.isStandaloneCatalogType(),
                "There is no type=hudi catalog and no fe-core catalog class for one: a hudi table is always "
                        + "parasitic on an HMS catalog and is served as an embedded sibling of the hms gateway. "
                        + "The engine builds a catalog for any registered type that declares itself standalone, "
                        + "so flipping this to true would let CREATE CATALOG build a hudi catalog with no engine-"
                        + "side semantics behind it. Sibling lookup does not consult this, so declaring false "
                        + "costs hudi nothing.");
    }
}
