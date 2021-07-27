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

    public static void write(File imageFile, Catalog catalog) throws IOException {
        // save image does not need any lock. because only checkpoint thread will call this method.
        LOG.info("start save image to {}. is ckpt: {}", imageFile.getAbsolutePath(), Catalog.isCheckpointThread());

        long checksum = 0;
        long saveImageStartTime = System.currentTimeMillis();
        long startPosition = MetaHeader.write(imageFile);
        List<MetaIndex> metaIndices = Lists.newArrayList();
        try (CountingDataOutputStream dos = new CountingDataOutputStream(new BufferedOutputStream(
                new FileOutputStream(imageFile, true)), startPosition)) {
            long replayedJournalId = catalog.getReplayedJournalId();
            metaIndices.add(new MetaIndex("header", dos.getCount()));
            checksum = catalog.saveHeader(dos, replayedJournalId, checksum);
            metaIndices.add(new MetaIndex("masterInfo", dos.getCount()));
            checksum = catalog.saveMasterInfo(dos, checksum);
            metaIndices.add(new MetaIndex("frontends", dos.getCount()));
            checksum = catalog.saveFrontends(dos, checksum);
            metaIndices.add(new MetaIndex("backends", dos.getCount()));
            checksum = Catalog.getCurrentSystemInfo().saveBackends(dos, checksum);
            metaIndices.add(new MetaIndex("db", dos.getCount()));
            checksum = catalog.saveDb(dos, checksum);
            metaIndices.add(new MetaIndex("loadJob", dos.getCount()));
            checksum = catalog.saveLoadJob(dos, checksum);
            metaIndices.add(new MetaIndex("alterJob", dos.getCount()));
            checksum = catalog.saveAlterJob(dos, checksum);
            metaIndices.add(new MetaIndex("recycleBin", dos.getCount()));
            checksum = catalog.saveRecycleBin(dos, checksum);
            metaIndices.add(new MetaIndex("globalVariable", dos.getCount()));
            checksum = catalog.saveGlobalVariable(dos, checksum);
            metaIndices.add(new MetaIndex("cluster", dos.getCount()));
            checksum = catalog.saveCluster(dos, checksum);
            metaIndices.add(new MetaIndex("broker", dos.getCount()));
            checksum = catalog.saveBrokers(dos, checksum);
            metaIndices.add(new MetaIndex("resources", dos.getCount()));
            checksum = catalog.saveResources(dos, checksum);
            metaIndices.add(new MetaIndex("exportJob", dos.getCount()));
            checksum = catalog.saveExportJob(dos, checksum);
            metaIndices.add(new MetaIndex("backupHandler", dos.getCount()));
            checksum = catalog.saveBackupHandler(dos, checksum);
            metaIndices.add(new MetaIndex("paloAuth", dos.getCount()));
            checksum = catalog.savePaloAuth(dos, checksum);
            metaIndices.add(new MetaIndex("transactionState", dos.getCount()));
            checksum = catalog.saveTransactionState(dos, checksum);
            metaIndices.add(new MetaIndex("colocateTableIndex", dos.getCount()));
            checksum = catalog.saveColocateTableIndex(dos, checksum);
            metaIndices.add(new MetaIndex("routineLoadJobs", dos.getCount()));
            checksum = catalog.saveRoutineLoadJobs(dos, checksum);
            metaIndices.add(new MetaIndex("loadJobV2", dos.getCount()));
            checksum = catalog.saveLoadJobsV2(dos, checksum);
            metaIndices.add(new MetaIndex("smallFiles", dos.getCount()));
            checksum = catalog.saveSmallFiles(dos, checksum);
            metaIndices.add(new MetaIndex("plugins", dos.getCount()));
            checksum = catalog.savePlugins(dos, checksum);
            metaIndices.add(new MetaIndex("deleteHandler", dos.getCount()));
            checksum = catalog.saveDeleteHandler(dos, checksum);
        }
        MetaFooter.write(imageFile, metaIndices, checksum);

        long saveImageEndTime = System.currentTimeMillis();
        LOG.info("finished save image {} in {} ms. checksum is {}",
                imageFile.getAbsolutePath(), (saveImageEndTime - saveImageStartTime), checksum);
    }

}
