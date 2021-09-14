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

package org.apache.doris.common;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.io.CountingDataOutputStream;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

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
            long checksum = method.write();
            indices.add(new MetaIndex(name, dos.getCount()));
            return checksum;
        };
    }

    public long doWork(String name, WriteMethod method) throws IOException {
        if(delegate == null){
            return method.write();
        }
        return delegate.doWork(name, method);
    }

    public static void write(File imageFile, Catalog catalog) throws IOException {
        // save image does not need any lock. because only checkpoint thread will call this method.
        LOG.info("start save image to {}. is ckpt: {}", imageFile.getAbsolutePath(), Catalog.isCheckpointThread());

        // long checksum = 0;
        final AtomicReference<Long> checksum = new AtomicReference<>(0L);
        long saveImageStartTime = System.currentTimeMillis();
        long startPosition = MetaHeader.write(imageFile);
        List<MetaIndex> metaIndices = Lists.newArrayList();
        try (CountingDataOutputStream dos = new CountingDataOutputStream(new BufferedOutputStream(
                new FileOutputStream(imageFile, true)), startPosition)) {
            writer.setDelegate(dos, metaIndices);
            long replayedJournalId = catalog.getReplayedJournalId();
            checksum.set(writer.doWork("header", () -> catalog.saveHeader(dos, replayedJournalId, checksum.get())));
            checksum.set(writer.doWork("masterInfo", () -> catalog.saveMasterInfo(dos, checksum.get())));
            checksum.set(writer.doWork("frontends", () -> catalog.saveFrontends(dos, checksum.get())));
            checksum.set(writer.doWork("backends", () -> Catalog.getCurrentSystemInfo().saveBackends(dos, checksum.get())));
            checksum.set(writer.doWork("db", () -> catalog.saveDb(dos, checksum.get())));
            checksum.set(writer.doWork("loadJob", () -> catalog.saveLoadJob(dos, checksum.get())));
            checksum.set(writer.doWork("alterJob", () -> catalog.saveAlterJob(dos, checksum.get())));
            checksum.set(writer.doWork("recycleBin", () -> catalog.saveRecycleBin(dos, checksum.get())));
            checksum.set(writer.doWork("globalVariable", () -> catalog.saveGlobalVariable(dos, checksum.get())));
            checksum.set(writer.doWork("cluster", () -> catalog.saveCluster(dos, checksum.get())));
            checksum.set(writer.doWork("broker", () -> catalog.saveBrokers(dos, checksum.get())));
            checksum.set(writer.doWork("resources", () -> catalog.saveResources(dos, checksum.get())));
            checksum.set(writer.doWork("exportJob", () -> catalog.saveExportJob(dos, checksum.get())));
            checksum.set(writer.doWork("syncJob", () -> catalog.saveSyncJobs(dos, checksum.get())));
            checksum.set(writer.doWork("backupHandler", () -> catalog.saveBackupHandler(dos, checksum.get())));
            checksum.set(writer.doWork("paloAuth", () -> catalog.savePaloAuth(dos, checksum.get())));
            checksum.set(writer.doWork("transactionState", () -> catalog.saveTransactionState(dos, checksum.get())));
            checksum.set(writer.doWork("colocateTableIndex", () -> catalog.saveColocateTableIndex(dos, checksum.get())));
            checksum.set(writer.doWork("routineLoadJobs", () -> catalog.saveRoutineLoadJobs(dos, checksum.get())));
            checksum.set(writer.doWork("loadJobV2", () -> catalog.saveLoadJobsV2(dos, checksum.get())));
            checksum.set(writer.doWork("smallFiles", () -> catalog.saveSmallFiles(dos, checksum.get())));
            checksum.set(writer.doWork("plugins", () -> catalog.savePlugins(dos, checksum.get())));
            checksum.set(writer.doWork("deleteHandler", () -> catalog.saveDeleteHandler(dos, checksum.get())));
            checksum.set(writer.doWork("sqlBlockRule", () -> catalog.saveSqlBlockRule(dos, checksum.get())));
        }
        MetaFooter.write(imageFile, metaIndices, checksum.get());

        long saveImageEndTime = System.currentTimeMillis();
        LOG.info("finished save image {} in {} ms. checksum is {}",
                imageFile.getAbsolutePath(), (saveImageEndTime - saveImageStartTime), checksum);
    }

}
