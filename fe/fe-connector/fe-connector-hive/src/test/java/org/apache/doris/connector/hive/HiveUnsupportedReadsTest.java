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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.spi.DorisConnectorException;
import org.apache.doris.connector.spi.scan.ConnectorScanRequest;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

public class HiveUnsupportedReadsTest {
    @Test
    public void rejectTransactionalTablesBeforePlanningOrCountPushdown() {
        HiveScanPlanProvider provider = new HiveScanPlanProvider(null, HiveTestProperties.minimal(),
                null, null);
        for (String mode : new String[] {"default", "insert_only"}) {
            HiveTableHandle handle = new HiveTableHandle.Builder("db", "tbl", HiveTableType.HIVE)
                    .inputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
                    .tableParameters(Map.of("transactional", "true", "transactional_properties", mode))
                    .build();
            DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                    () -> provider.getScanNodeProperties(new ScopeSession(1L, "q",
                            new TestStatementScope()), handle, Collections.emptyList(), Optional.empty()));
            ConnectorScanRequest request = ConnectorScanRequest.builder(handle, Collections.emptyList()).build();
            Assertions.assertThrows(DorisConnectorException.class, () -> provider.planScan(null, request));
            Assertions.assertThrows(DorisConnectorException.class, () -> provider.streamSplits(null, request));
            Assertions.assertThrows(DorisConnectorException.class,
                    () -> provider.planScanForPartitionBatch(null, request, Collections.singletonList("p=1")));
            Assertions.assertTrue(error.getMessage().contains(
                    "Hive transactional table reads are no longer supported"));
        }
    }
}
