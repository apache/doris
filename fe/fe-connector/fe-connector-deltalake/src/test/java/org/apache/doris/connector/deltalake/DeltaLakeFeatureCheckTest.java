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


package org.apache.doris.connector.deltalake;

import org.apache.doris.connector.spi.DorisConnectorException;

import io.delta.kernel.internal.actions.Protocol;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Set;

class DeltaLakeFeatureCheckTest {
    @Test
    void legacyReaderOneDoesNotRequireWriterOnlyFeatures() {
        Set<String> readerFeatures = DeltaKernelCompat.readerFeatures(new Protocol(1, 7));
        Assertions.assertTrue(readerFeatures.isEmpty());
        DeltaLakeFeatureCheck.check("ordinary", readerFeatures);
    }

    @Test
    void legacyReaderTwoIncludesImplicitColumnMapping() {
        Set<String> features = DeltaKernelCompat.readerFeatures(new Protocol(2, 5));
        Assertions.assertEquals(Set.of("columnMapping"), features);
        Assertions.assertThrows(DorisConnectorException.class,
                () -> DeltaLakeFeatureCheck.check("mapping", features));
    }

    @Test
    void explicitReaderFeaturesDoNotIncludeWriterOnlyFeatures() {
        Protocol protocol = new Protocol(3, 7, Set.of("deletionVectors"),
                Set.of("deletionVectors", "appendOnly", "invariants"));
        Assertions.assertEquals(Set.of("deletionVectors"), DeltaKernelCompat.readerFeatures(protocol));
    }

    @Test
    void unsupportedFeatureErrorsIdentifyTableAndEveryFeature() {
        DorisConnectorException error = Assertions.assertThrows(DorisConnectorException.class,
                () -> DeltaLakeFeatureCheck.check("warehouse/db/table", Set.of("variantType", "catalogManaged")));
        Assertions.assertTrue(error.getMessage().contains("warehouse/db/table"));
        Assertions.assertTrue(error.getMessage().contains("variantType"));
        Assertions.assertTrue(error.getMessage().contains("catalogManaged"));
        DeltaLakeFeatureCheck.check("empty", Collections.emptySet());
    }
}
