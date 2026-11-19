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

package org.apache.doris.connector.iceberg;

import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

class ManifestCacheValueTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void dataRetentionDoesNotEvaluateLazyPartitionViews(boolean encrypted) {
        PartitionSpec source = spec(false);
        PartitionData partition = new PartitionData(source.rawPartitionType());
        partition.set(0, 7);
        DataFile file = DataFiles.builder(source).withPath("memory:///data.parquet").withFormat("parquet")
                .withFileSizeInBytes(100).withRecordCount(1).withPartition(partition)
                .withEncryptionKeyMetadata(encrypted ? ByteBuffer.wrap(new byte[] {1}) : null).build();
        AtomicInteger reads = new AtomicInteger();
        ManifestCacheValue cached = ManifestCacheValue.forDataFiles(
                Collections.nCopies(4, observe(file, DataFile.class, reads)), true);
        reads.set(0);
        ManifestCacheValue projected = cached.forScan(spec(true));
        Assertions.assertEquals(1, reads.get(), "Only the first partition type may be inspected before consumption");
        Assertions.assertEquals(!encrypted, projected.isCacheable());
        DataFile first = projected.getDataFiles().get(0);
        Assertions.assertEquals(2, reads.get(), "Consuming one file must not adapt the other files");
        Assertions.assertEquals(7L, first.partition().get(0, Long.class));
        Assertions.assertEquals(file.keyMetadata(), first.keyMetadata());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void deleteRetentionDoesNotEvaluateLazyPartitionViews(boolean encrypted) {
        PartitionSpec source = spec(false);
        PartitionData partition = new PartitionData(source.rawPartitionType());
        partition.set(0, 7);
        DeleteFile file = FileMetadata.deleteFileBuilder(source).ofPositionDeletes()
                .withPath("memory:///delete.parquet").withFormat("parquet").withFileSizeInBytes(100)
                .withRecordCount(1).withPartition(partition)
                .withEncryptionKeyMetadata(encrypted ? ByteBuffer.wrap(new byte[] {1}) : null).build();
        AtomicInteger reads = new AtomicInteger();
        ManifestCacheValue cached = ManifestCacheValue.forDeleteFiles(
                Collections.nCopies(4, observe(file, DeleteFile.class, reads)), true);
        reads.set(0);
        ManifestCacheValue projected = cached.forScan(spec(true));
        Assertions.assertEquals(1, reads.get(), "Only the first partition type may be inspected before consumption");
        Assertions.assertEquals(!encrypted, projected.isCacheable());
        DeleteFile first = projected.getDeleteFiles().get(0);
        Assertions.assertEquals(2, reads.get(), "Consuming one file must not adapt the other files");
        Assertions.assertEquals(7L, first.partition().get(0, Long.class));
        Assertions.assertEquals(file.keyMetadata(), first.keyMetadata());
    }

    private static PartitionSpec spec(boolean promoted) {
        return PartitionSpec.builderFor(new Schema(Types.NestedField.optional(1, "p",
                promoted ? Types.LongType.get() : Types.IntegerType.get()))).identity("p").build();
    }

    private static <F extends ContentFile<F>> F observe(F file, Class<F> type, AtomicInteger reads) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type}, (proxy, method, args) -> {
            if (method.getName().equals("partition")) {
                reads.incrementAndGet();
            }
            try {
                return method.invoke(file, args);
            } catch (InvocationTargetException failure) {
                throw failure.getCause();
            }
        }));
    }
}
