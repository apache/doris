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

package org.apache.doris.datasource.lance.metadata;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.property.storage.StorageProperties;

import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.lance.BasePath;
import org.lance.Dataset;
import org.lance.OpenDatasetBuilder;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

class LanceTvfStoragePolicyTest {
    @Test
    void remoteTvfRejectsLocalManifestBasesBeforeReadingMetadata() throws Exception {
        for (String base : Arrays.asList("file:///private/source.lance", "/private/source.lance",
                "file:/private/source.lance", "file-object-store:///private/source.lance",
                "s3://bucket:-0/source.lance")) {
            Dataset dataset = Mockito.mock(Dataset.class);
            Mockito.when(dataset.getBasePaths()).thenReturn(Collections.singletonList(
                    new BasePath(1, Optional.empty(), base, true)));
            OpenDatasetBuilder builder = Mockito.mock(OpenDatasetBuilder.class, Mockito.RETURNS_SELF);
            Mockito.when(builder.build()).thenReturn(dataset);
            try (MockedStatic<Dataset> datasets = Mockito.mockStatic(Dataset.class)) {
                datasets.when(Dataset::open).thenReturn(builder);
                AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                        () -> LanceMetadataLoader.loadLatestForTvf(
                                "s3://bucket/clone.lance", Collections.emptyList()));
                Assertions.assertFalse(error.getMessage().contains("private"));
                Mockito.verify(dataset, Mockito.never()).getFragments();
                Mockito.verify(dataset, Mockito.never()).getSchema();
                Mockito.verify(dataset).close();
            }
        }
    }

    @Test
    void remoteTvfPreservesValidRemoteManifestBases() throws Exception {
        Dataset dataset = Mockito.mock(Dataset.class);
        Mockito.when(dataset.getBasePaths()).thenReturn(Arrays.asList(
                new BasePath(1, Optional.empty(), "s3://bucket/source data.lance", true),
                new BasePath(2, Optional.empty(), "s3://bucket/other%20data", false)));
        Mockito.when(dataset.version()).thenReturn(7L);
        Mockito.when(dataset.getFragments()).thenReturn(Collections.emptyList());
        Mockito.when(dataset.getSchema()).thenReturn(new Schema(Collections.emptyList()));
        OpenDatasetBuilder builder = Mockito.mock(OpenDatasetBuilder.class, Mockito.RETURNS_SELF);
        Mockito.when(builder.build()).thenReturn(dataset);
        try (MockedStatic<Dataset> datasets = Mockito.mockStatic(Dataset.class)) {
            datasets.when(Dataset::open).thenReturn(builder);
            LanceTableMetadata metadata = LanceMetadataLoader.loadLatestForTvf(
                    "s3://bucket/clone.lance", Collections.emptyList());
            Assertions.assertEquals(7L, metadata.getVersion());
            Mockito.verify(dataset).getFragments();
            Mockito.verify(dataset).getSchema();
            Mockito.verify(dataset).close();
        }
    }

    @Test
    void isolatedRemoteManifestRejectsLocalBaseAndKeepsRemoteReads() throws Exception {
        String clone = System.getenv("DORIS_LANCE_TEST_REMOTE_CLONE_URI");
        String source = System.getenv("DORIS_LANCE_TEST_REMOTE_SOURCE_URI");
        Assumptions.assumeTrue(clone != null && source != null,
                "Requires an explicitly selected isolated S3 storage-policy fixture");
        Map<String, String> properties = new HashMap<>();
        properties.put("s3.endpoint", System.getenv("DORIS_LANCE_TEST_S3_ENDPOINT"));
        properties.put("s3.access_key", System.getenv("DORIS_LANCE_TEST_S3_ACCESS_KEY"));
        properties.put("s3.secret_key", System.getenv("DORIS_LANCE_TEST_S3_SECRET_KEY"));
        properties.put("s3.region", "us-east-1");
        properties.put("use_path_style", "true");
        StorageProperties storage = StorageProperties.createPrimary(properties);
        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                () -> LanceMetadataLoader.loadLatestForTvf(clone, Collections.singletonList(storage)));
        Assertions.assertTrue(error.getMessage().contains("remote manifest storage bases"));
        LanceTableMetadata metadata = LanceMetadataLoader.loadLatestForTvf(
                source, Collections.singletonList(storage));
        Assertions.assertTrue(metadata.getVersion() > 0);
        Assertions.assertTrue(metadata.getSchema().getFields().stream()
                .anyMatch(field -> "row_id".equals(field.getName())));
    }

    @Test
    void catalogSnapshotReadKeepsItsConfiguredManifestBases() {
        Dataset dataset = Mockito.mock(Dataset.class);
        Mockito.when(dataset.version()).thenReturn(7L);
        Mockito.when(dataset.getFragments()).thenReturn(Collections.emptyList());
        Mockito.when(dataset.getSchema()).thenReturn(new Schema(Collections.emptyList()));
        LanceMetadataLoader.read(dataset, new LanceTableAccess("file:///warehouse/table.lance",
                Collections.emptyMap()), LanceMetadataLoader.MetadataScope.BASIC);
        Mockito.verify(dataset, Mockito.never()).getBasePaths();
    }
}
