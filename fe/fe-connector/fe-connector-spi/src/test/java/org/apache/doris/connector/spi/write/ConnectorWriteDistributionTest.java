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

package org.apache.doris.connector.spi.write;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class ConnectorWriteDistributionTest {

    @Test
    void externalHashCopiesConnectorOwnedMetadata() {
        List<String> columns = new ArrayList<>(Arrays.asList("part", "id"));
        Map<String, String> options = new LinkedHashMap<>(
                Collections.singletonMap("bucket-count", "8"));
        ConnectorWriteDistribution distribution = ConnectorWriteDistribution.externalHash(
                columns, "connector_bucket", options,
                ConnectorWriteDistribution.WriterAssignment.IDENTITY);

        columns.clear();
        options.clear();

        Assertions.assertEquals(ConnectorWriteDistribution.Mode.EXTERNAL_HASH,
                distribution.getMode());
        Assertions.assertEquals(Arrays.asList("part", "id"), distribution.getRouteColumns());
        Assertions.assertEquals("connector_bucket", distribution.getPartitionFunction());
        Assertions.assertEquals(Collections.singletonMap("bucket-count", "8"),
                distribution.getPartitionFunctionOptions());
        Assertions.assertEquals(ConnectorWriteDistribution.WriterAssignment.IDENTITY,
                distribution.getWriterAssignment());
    }

    @Test
    void hashModesRequireRoutingMetadata() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorWriteDistribution.simple(ConnectorWriteDistribution.Mode.EXTERNAL_HASH));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorWriteDistribution.hash(Collections.emptyList()));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> ConnectorWriteDistribution.externalHash(Collections.singletonList("id"), "",
                        Collections.emptyMap(), ConnectorWriteDistribution.WriterAssignment.SKEWED));
    }
}
