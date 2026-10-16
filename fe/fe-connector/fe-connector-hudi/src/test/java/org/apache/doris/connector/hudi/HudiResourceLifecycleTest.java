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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class HudiResourceLifecycleTest {

    @Test
    public void partitionMetadataReaderIsClosedAfterMaterializingPaths() throws Exception {
        RecordingCloseable metadata = new RecordingCloseable(false);
        List<String> paths = Arrays.asList("p=1", "p=2");

        Assertions.assertSame(paths, HudiScanPlanProvider.listAllPartitionPaths(() -> paths, metadata));
        Assertions.assertTrue(metadata.closed);
    }

    @Test
    public void closeFailureIsSuppressedByPartitionListingFailure() {
        RecordingCloseable metadata = new RecordingCloseable(true);
        IllegalStateException listingFailure = new IllegalStateException("listing failed");

        IllegalStateException thrown = Assertions.assertThrows(IllegalStateException.class,
                () -> HudiScanPlanProvider.listAllPartitionPaths(() -> {
                    throw listingFailure;
                }, metadata));

        Assertions.assertSame(listingFailure, thrown);
        Assertions.assertTrue(metadata.closed);
        Assertions.assertEquals(1, thrown.getSuppressed().length);
        Assertions.assertEquals("close failed", thrown.getSuppressed()[0].getMessage());
    }

    private static final class RecordingCloseable implements AutoCloseable {
        private final boolean failOnClose;
        private boolean closed;

        private RecordingCloseable(boolean failOnClose) {
            this.failOnClose = failOnClose;
        }

        @Override
        public void close() throws IOException {
            closed = true;
            if (failOnClose) {
                throw new IOException("close failed");
            }
        }
    }
}
