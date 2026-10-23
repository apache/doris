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

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class LanceMultiSnapshotLoaderTest {

    private static final StorageProperties PROPERTIES = Mockito.mock(StorageProperties.class);
    private static final LanceFragmentInfo FRAGMENT = Mockito.mock(LanceFragmentInfo.class);

    @Test
    public void testUriWithoutSchemeIsRejected() {
        assertRejected("must name a storage location", "not-a-uri");
    }

    @Test
    public void testUriWithoutAuthorityAndPathIsRejected() {
        assertRejected("must name an authority and a path", "s3://bucket");
    }

    @Test
    public void testEmptyPathSegmentIsRejected() {
        assertRejected("must not contain an empty path segment", "s3://bucket/a//b");
    }

    @Test
    public void testTrailingSlashIsRejected() {
        assertRejected("must not end with '/'", "s3://bucket/a/");
    }

    @Test
    public void testDatasetsOnDifferentProvidersAreRejected() {
        assertRejected("one storage provider", "s3://bucket/a", "oss://bucket/b");
    }

    @Test
    public void testMoreThanMaxDatasetsIsRejected() {
        List<String> uris = new ArrayList<>();
        for (int index = 0; index <= LanceMultiSnapshotLoader.MAX_DATASETS; index++) {
            uris.add("s3://bucket/ds" + index);
        }
        assertRejected("must not exceed " + LanceMultiSnapshotLoader.MAX_DATASETS, uris);
    }

    @Test
    public void testDatasetThatCannotBeOpenedFailsTheQuery() {
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> LanceMultiSnapshotLoader.load(
                        Collections.singletonList("file://localhost/tmp/does-not-exist-lance-xyz"),
                        PROPERTIES));
        Assertions.assertTrue(e.getMessage().contains("Failed to open Lance dataset"), e.getMessage());
    }

    @Test
    public void testInvalidVersionIsRejected() {
        assertRequireReadableRejected("returned an invalid version",
                dataset("s3://bucket/a", 0L, schema(1), 1), Collections.emptyList());
    }

    @Test
    public void testMissingSchemaIsRejected() {
        assertRequireReadableRejected("returned no schema",
                dataset("s3://bucket/a", 7L, null, 1), Collections.emptyList());
    }

    @Test
    public void testTooManyFieldsIsRejected() {
        assertRequireReadableRejected("fields, which exceeds",
                dataset("s3://bucket/a", 7L,
                        schema(LanceMultiSnapshotLoader.MAX_FIELDS_PER_SCHEMA + 1), 1),
                Collections.emptyList());
    }

    @Test
    public void testFragmentBudgetIsExceeded() {
        assertRequireReadableRejected("fragments in total",
                dataset("s3://bucket/a", 7L, schema(1),
                        (int) LanceMultiSnapshotLoader.MAX_TOTAL_FRAGMENTS + 1),
                Collections.emptyList());
    }

    private static void assertRejected(String messagePart, String... uris) {
        assertRejected(messagePart, Arrays.asList(uris));
    }

    private static void assertRejected(String messagePart, List<String> uris) {
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> LanceMultiSnapshotLoader.load(uris, PROPERTIES));
        Assertions.assertTrue(e.getMessage().contains(messagePart),
                "expected '" + messagePart + "' in: " + e.getMessage());
    }

    private static void assertRequireReadableRejected(String messagePart, LanceTableMetadata metadata,
            List<LanceTableMetadata> opened) {
        AnalysisException e = Assertions.assertThrows(AnalysisException.class,
                () -> LanceMultiSnapshotLoader.requireReadable(metadata, opened));
        Assertions.assertTrue(e.getMessage().contains(messagePart),
                "expected '" + messagePart + "' in: " + e.getMessage());
    }

    private static LanceTableMetadata dataset(String uri, long version, Schema schema, int fragments) {
        LanceTableMetadata metadata = Mockito.mock(LanceTableMetadata.class);
        Mockito.when(metadata.getDatasetUri()).thenReturn(uri);
        Mockito.when(metadata.getVersion()).thenReturn(version);
        Mockito.when(metadata.getSchema()).thenReturn(schema);
        Mockito.when(metadata.getFragments()).thenReturn(Collections.nCopies(fragments, FRAGMENT));
        return metadata;
    }

    private static Schema schema(int fieldCount) {
        List<Field> fields = new ArrayList<>(fieldCount);
        for (int index = 0; index < fieldCount; index++) {
            fields.add(new Field("f" + index, FieldType.nullable(ArrowType.Utf8.INSTANCE),
                    Collections.emptyList()));
        }
        return new Schema(fields);
    }

    private static Schema int64Schema() {
        return new Schema(Collections.singletonList(
                new Field("f0", FieldType.nullable(new ArrowType.Int(64, true)), Collections.emptyList())));
    }
}
