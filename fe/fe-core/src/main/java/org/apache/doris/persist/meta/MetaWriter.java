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
import org.apache.doris.common.Reference;
import org.apache.doris.common.io.CountingDataOutputStream;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.List;

/**
 * Image Format:
 * |- Image --------------------------------------|
 * | - Magic String (4 bytes)                     |
 * | - Header Length (4 bytes)                    |
 * | |- Header -----------------------------|     |
 * | | |- Json Header ---------------|      |     |
 * | | | - version                   |      |     |
 * | | | - other key/value(undecided)|      |     |
 * | | |-----------------------------|      |     |
 * | |--------------------------------------|     |
 * |                                              |
 * | |- Image Body -------------------------|     |
 * | | Object a                             |     |
 * | | Object b                             |     |
 * | | ...                                  |     |
 * | |--------------------------------------|     |
 * |                                              |
 * | |- Footer -----------------------------|     |
 * | | | - Checksum (8 bytes)               |     |
 * | | |- object index --------------|      |     |
 * | | | - index a                   |      |     |
 * | | | - index b                   |      |     |
 * | | | ...                         |      |     |
 * | | |-----------------------------|      |     |
 * | | - other value(undecided)             |     |
 * | |--------------------------------------|     |
 * | - Footer Length (8 bytes)                    |
 * | - Magic String (4 bytes)                     |
 * |----------------------------------------------|
 */

public class MetaWriter {
    private static final Logger LOG = LogManager.getLogger(MetaWriter.class);

    public static MetaWriter writer = new MetaWriter();

    private interface Delegate {
        long doWork(String name, WriteMethod method) throws IOException;
    }

    private interface WriteMethod {
        long write() throws IOException;
    }

    private Delegate delegate;

    public void setDelegate(CountingDataOutputStream dos, List<MetaIndex> indices) {
        this.delegate = (name, method) -> {
            indices.add(new MetaIndex(name, dos.getCount()));
            return method.write();
        };
    }

    public long doWork(String name, WriteMethod method) throws IOException {
        if (delegate == null) {
            return method.write();
        }
        return delegate.doWork(name, method);
    }

    public static void write(File imageFile, Env env) throws IOException {
        // save image does not need any lock. because only checkpoint thread will call this method.
        LOG.info("start to save image to {}. is ckpt: {}",
                imageFile.getAbsolutePath(), Env.isCheckpointThread());
        final Reference<Long> checksum = new Reference<>(0L);
        long saveImageStartTime = System.currentTimeMillis();
        // MetaHeader should use output stream in the future.
        long startPosition = MetaHeader.write(imageFile);
        List<MetaIndex> metaIndices = Lists.newArrayList();
        FileOutputStream imageFileOut = new FileOutputStream(imageFile, true);
        try (CountingDataOutputStream dos = new CountingDataOutputStream(new BufferedOutputStream(imageFileOut),
                startPosition)) {
            writer.setDelegate(dos, metaIndices);
            long replayedJournalId = env.getReplayedJournalId();
            // 1. write header first
            checksum.setRef(
                    writer.doWork("header", () -> env.saveHeader(dos, replayedJournalId, checksum.getRef())));
            // 2. write other modules
            for (MetaPersistMethod m : PersistMetaModules.MODULES_IN_ORDER) {
                checksum.setRef(writer.doWork(m.name, () -> {
                    try {
                        return (long) m.writeMethod.invoke(env, dos, checksum.getRef());
                    } catch (IllegalAccessException | InvocationTargetException e) {
                        LOG.warn("failed to write meta module: {}", m.name, e);
                        throw new RuntimeException(e);
                    }
                }));
            }
            // 3. force sync to disk
            imageFileOut.getChannel().force(true);
        }
        MetaFooter.write(imageFile, metaIndices, checksum.getRef());

        long saveImageEndTime = System.currentTimeMillis();
        LOG.info("finished save image {} in {} ms. checksum is {}, size is {}", imageFile.getAbsolutePath(),
                (saveImageEndTime - saveImageStartTime), checksum.getRef(), imageFile.length());
    }

}
