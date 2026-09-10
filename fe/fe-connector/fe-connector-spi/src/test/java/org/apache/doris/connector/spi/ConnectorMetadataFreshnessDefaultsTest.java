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

import org.apache.doris.connector.spi.handle.ConnectorTableHandle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

public class ConnectorMetadataFreshnessDefaultsTest {

    @Test
    public void bulkFreshnessFallsBackToScalarLookups() {
        RecordingMetadata metadata = new RecordingMetadata();

        Map<String, Long> result = metadata.getPartitionsFreshnessMillis(
                null, null, Arrays.asList("p2", "missing", "p1"));

        Map<String, Long> expected = new LinkedHashMap<>();
        expected.put("p2", 2L);
        expected.put("p1", 1L);
        Assertions.assertEquals(expected, result);
        Assertions.assertEquals(Arrays.asList("p2", "missing", "p1"), metadata.lookups);

        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                () -> metadata.getPartitionsFreshnessMillis(null, null, Arrays.asList("p1", "failure", "p2")));
        Assertions.assertEquals("failure", failure.getMessage());
        Assertions.assertEquals(Arrays.asList("p2", "missing", "p1", "p1", "failure"), metadata.lookups);
    }

    private static final class RecordingMetadata implements ConnectorMetadata {
        private final List<String> lookups = new ArrayList<>();

        @Override
        public OptionalLong getPartitionFreshnessMillis(
                ConnectorSession session, ConnectorTableHandle handle, String partitionName) {
            lookups.add(partitionName);
            if ("failure".equals(partitionName)) {
                throw new IllegalStateException("failure");
            }
            if ("missing".equals(partitionName)) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(Long.parseLong(partitionName.substring(1)));
        }
    }
}
