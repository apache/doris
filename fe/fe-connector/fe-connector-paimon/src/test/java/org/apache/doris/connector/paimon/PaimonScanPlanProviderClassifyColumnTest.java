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

import org.apache.doris.connector.spi.scan.ConnectorColumnCategory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

class PaimonScanPlanProviderClassifyColumnTest {

    private static final PaimonScanPlanProvider PROVIDER =
            new PaimonScanPlanProvider(PaimonCatalogProperties.of(Collections.emptyMap()), null);

    @Test
    void metadataColumnsAreSynthesized() {
        Assertions.assertEquals(ConnectorColumnCategory.SYNTHESIZED,
                PROVIDER.classifyColumn("__paimon_file_path"));
        Assertions.assertEquals(ConnectorColumnCategory.SYNTHESIZED,
                PROVIDER.classifyColumn("__PAIMON_ROW_INDEX"));
        Assertions.assertEquals(ConnectorColumnCategory.DEFAULT, PROVIDER.classifyColumn("id"));
    }
}
