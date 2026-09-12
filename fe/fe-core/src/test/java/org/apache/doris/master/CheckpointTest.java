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

package org.apache.doris.master;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.CheckpointException;
import org.apache.doris.common.Config;
import org.apache.doris.metric.MetricRepo;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.Storage;
import org.apache.doris.qe.VariableMgr;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;

public class CheckpointTest {
    @TempDir
    Path directory;

    @Test
    public void testPublishOnlyAfterValidation() throws Exception {
        checkPublication(false);
    }

    @Test
    public void testInvalidCheckpointKeepsPreviousImage() throws Exception {
        checkPublication(true);
    }

    private void checkPublication(boolean invalid) throws Exception {
        Path imageDirectory = Files.createDirectory(directory.resolve("image"));
        Path previousImage = Files.write(imageDirectory.resolve("image.1"), new byte[] {1});
        File checkpointImage = imageDirectory.resolve(Storage.IMAGE_NEW).toFile();
        Path publishedImage = imageDirectory.resolve("image.2");
        Env servingEnv = Mockito.mock(Env.class);
        Env writerEnv = Mockito.mock(Env.class);
        Env readerEnv = Mockito.mock(Env.class);
        EditLog editLog = Mockito.mock(EditLog.class);
        Mockito.when(servingEnv.getImageDir()).thenReturn(imageDirectory.toString());
        Mockito.when(servingEnv.isHttpReady()).thenReturn(true);
        Mockito.when(servingEnv.getFrontends(null)).thenReturn(Collections.emptyList());
        Mockito.when(editLog.getFinalizedJournalId()).thenReturn(2L);
        Mockito.when(writerEnv.getReplayedJournalId()).thenReturn(2L);
        Mockito.when(writerEnv.saveCheckpointImage()).thenAnswer(invocation -> {
            Files.write(checkpointImage.toPath(), new byte[] {2});
            return checkpointImage.getAbsolutePath();
        });
        Mockito.doAnswer(invocation -> {
            Assertions.assertTrue(checkpointImage.exists());
            Assertions.assertFalse(Files.exists(publishedImage));
            Assertions.assertEquals(previousImage.toFile(),
                    new Storage(imageDirectory.toString()).getCurrentImageFile());
            if (invalid) {
                throw new IOException("invalid checkpoint checksum");
            }
            return null;
        }).when(readerEnv).loadImage(checkpointImage, 2L);

        boolean enableCheckpoint = Config.enable_checkpoint;
        boolean forceCheckpoint = Config.force_do_metadata_checkpoint;
        String metaDir = Config.meta_dir;
        boolean metricsInitialized = MetricRepo.isInit;
        try (MockedStatic<Env> envMock = Mockito.mockStatic(Env.class);
                MockedStatic<VariableMgr> variableMock = Mockito.mockStatic(VariableMgr.class);
                MockedStatic<Config> configMock = Mockito.mockStatic(Config.class, Mockito.CALLS_REAL_METHODS)) {
            Config.enable_checkpoint = true;
            Config.force_do_metadata_checkpoint = true;
            Config.meta_dir = directory.toString();
            MetricRepo.isInit = false;
            configMock.when(Config::isNotCloudMode).thenReturn(true);
            envMock.when(Env::getServingEnv).thenReturn(servingEnv);
            envMock.when(Env::getCurrentEnv).thenReturn(writerEnv, readerEnv);
            Checkpoint checkpoint = new Checkpoint(editLog);
            if (invalid) {
                CheckpointException exception = Assertions.assertThrows(CheckpointException.class,
                        checkpoint::doCheckpoint);
                Assertions.assertEquals("invalid checkpoint checksum", exception.getMessage());
                Assertions.assertFalse(Files.exists(publishedImage));
                Mockito.verify(editLog, Mockito.never()).deleteJournals(Mockito.anyLong());
            } else {
                checkpoint.doCheckpoint();
                Assertions.assertArrayEquals(new byte[] {2}, Files.readAllBytes(publishedImage));
                Assertions.assertEquals(publishedImage.toFile(),
                        new Storage(imageDirectory.toString()).getCurrentImageFile());
            }
            Mockito.verify(readerEnv).loadImage(checkpointImage, 2L);
            envMock.verify(Env::destroyCheckpoint, Mockito.times(2));
            Assertions.assertFalse(checkpointImage.exists());
            Assertions.assertArrayEquals(new byte[] {1}, Files.readAllBytes(previousImage));
        } finally {
            Config.enable_checkpoint = enableCheckpoint;
            Config.force_do_metadata_checkpoint = forceCheckpoint;
            Config.meta_dir = metaDir;
            MetricRepo.isInit = metricsInitialized;
        }
    }
}
