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

package org.apache.doris.connector.lance;

import org.apache.doris.connector.lance.LanceMetadataMetrics.Operation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/** How reading a version's field ids and indexes handles what the SDK cannot describe. */
class LanceIndexMetadataTest {

    /**
     * Lance's JNI converter cannot represent some types, notably Dictionary. Field ids are then unavailable, so no
     * index segment is planned, but the indexes are still discovered and validated.
     */
    @Test
    void knownSchemaConversionFailureStillDiscoversTheIndexes() {
        AtomicInteger discoveries = new AtomicInteger();
        LanceIndexMetadata metadata = LanceIndexMetadata.read(() -> {
            throw new IllegalArgumentException("ArrowSchema conversion error");
        }, () -> {
            discoveries.incrementAndGet();
            return Collections.emptyList();
        }, 7, new LanceMetadataMetrics().start(Operation.METADATA_READ));
        Assertions.assertFalse(metadata.canPlanIndexSegments());
        Assertions.assertFalse(metadata.getLanceFieldId("id").isPresent());
        Assertions.assertEquals(1, discoveries.get());

        // Only that known failure falls back; any other failure of the schema read is an error.
        Assertions.assertEquals("another failure", Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexMetadata.read(() -> {
                    throw new IllegalArgumentException("another failure");
                }, Collections::emptyList, 7, new LanceMetadataMetrics().start(Operation.METADATA_READ)))
                .getMessage());
    }

    @Test
    void schemaFallbackDoesNotHideInvalidIndexMetadata() {
        IllegalArgumentException invalid = new IllegalArgumentException("Lance index names must not be null");
        Assertions.assertSame(invalid, Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexMetadata.read(() -> {
                    throw new IllegalArgumentException("ArrowSchema conversion error");
                }, () -> {
                    throw invalid;
                }, 7, new LanceMetadataMetrics().start(Operation.METADATA_READ))));
    }

    @Test
    void fieldIdAndIndexReadsAreTimedAsTheirStages() {
        LanceMetadataMetrics metrics = new LanceMetadataMetrics();
        try (LanceMetadataMetrics.Timer timer = metrics.start(Operation.METADATA_READ)) {
            LanceIndexMetadata.read(() -> {
                throw new IllegalArgumentException("ArrowSchema conversion error");
            }, Collections::emptyList, 7, timer);
            timer.succeeded();
        }
        Map<String, String> values = metrics.drain();
        Assertions.assertEquals("1", values.get("FieldIdsReadCalls"));
        Assertions.assertEquals("1", values.get("IndexMetadataReadCalls"));
        Assertions.assertFalse(values.containsKey("DatasetOpenCalls"));
    }
}
