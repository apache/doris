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

package org.apache.doris.persist.meta;

import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.common.io.CountingDataOutputStream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import java.io.DataInputStream;
import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MetaWriterTest {
    @TempDir
    Path directory;

    @Test
    public void testConcurrentImagesKeepTheirOwnIndices() throws Exception {
        File checkpointImage = directory.resolve("image.ckpt").toFile();
        File dumpImage = directory.resolve("image.dump").toFile();
        Env checkpointEnv = mockImageEnv(11L);
        Env dumpEnv = mockImageEnv(29L);
        CountDownLatch checkpointStarted = new CountDownLatch(1);
        CountDownLatch dumpFinished = new CountDownLatch(1);
        Mockito.doAnswer(invocation -> {
            checkpointStarted.countDown();
            Assertions.assertTrue(dumpFinished.await(30, TimeUnit.SECONDS));
            CountingDataOutputStream output = invocation.getArgument(0);
            output.writeLong(11L);
            return 11L;
        }).when(checkpointEnv).saveHeader(Mockito.any(), Mockito.anyLong(), Mockito.anyLong());

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> checkpoint = executor.submit(() -> {
                MetaWriter.write(checkpointImage, checkpointEnv);
                return null;
            });
            Assertions.assertTrue(checkpointStarted.await(30, TimeUnit.SECONDS));
            // Replace the old shared delegate while the checkpoint is inside saveHeader.
            MetaWriter.write(dumpImage, dumpEnv);
            dumpFinished.countDown();
            checkpoint.get(30, TimeUnit.SECONDS);

            assertImage(checkpointImage, 11L);
            assertImage(dumpImage, 29L);
        } finally {
            dumpFinished.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    private Env mockImageEnv(long value) {
        // Keep real image framing and module dispatch, with one long of payload per module.
        return Mockito.mock(CloudEnv.class, invocation -> {
            Object[] arguments = invocation.getArguments();
            if (invocation.getMethod().getName().startsWith("save")
                    && arguments.length > 0 && arguments[0] instanceof CountingDataOutputStream) {
                ((CountingDataOutputStream) arguments[0]).writeLong(value);
                return (long) arguments[arguments.length - 1] ^ value;
            }
            if (invocation.getMethod().getName().startsWith("load")
                    && arguments.length > 0 && arguments[0] instanceof DataInputStream) {
                long actual = ((DataInputStream) arguments[0]).readLong();
                Assertions.assertEquals(value, actual);
                return (long) arguments[arguments.length - 1] ^ actual;
            }
            return Mockito.RETURNS_DEFAULTS.answer(invocation);
        });
    }

    private void assertImage(File image, long value) throws Exception {
        MetaFooter footer = MetaFooter.read(image);
        Assertions.assertEquals(PersistMetaModules.MODULES_IN_ORDER.size() + 1, footer.metaIndices.size());
        Assertions.assertEquals("header", footer.metaIndices.get(0).name);
        long offset = MetaHeader.read(image).getEnd();
        Assertions.assertEquals(offset, footer.metaIndices.get(0).offset);
        for (int i = 0; i < PersistMetaModules.MODULES_IN_ORDER.size(); i++) {
            offset += Long.BYTES;
            MetaIndex index = footer.metaIndices.get(i + 1);
            Assertions.assertEquals(PersistMetaModules.MODULES_IN_ORDER.get(i).name, index.name);
            Assertions.assertEquals(offset, index.offset);
        }
        MetaReader.read(image, mockImageEnv(value));
    }
}
