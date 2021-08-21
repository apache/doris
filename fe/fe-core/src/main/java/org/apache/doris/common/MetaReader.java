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

import com.google.common.base.Preconditions;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

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
 * | | - Checksum (8 bytes)                 |     |
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

public class MetaReader {
    private static final Logger LOG = LogManager.getLogger(MetaReader.class);

    public static void read(File imageFile, Catalog catalog) throws IOException, DdlException {
        LOG.info("start load image from {}. is ckpt: {}", imageFile.getAbsolutePath(), Catalog.isCheckpointThread());
        long loadImageStartTime = System.currentTimeMillis();
        MetaHeader metaHeader = MetaHeader.read(imageFile);

        long checksum = 0;
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(imageFile)))) {
            IOUtils.skipFully(dis, metaHeader.getEnd());
            checksum = catalog.loadHeader(dis, metaHeader, checksum);
            checksum = catalog.loadMasterInfo(dis, checksum);
            checksum = catalog.loadFrontends(dis, checksum);
            checksum = Catalog.getCurrentSystemInfo().loadBackends(dis, checksum);
            checksum = catalog.loadDb(dis, checksum);
            // ATTN: this should be done after load Db, and before loadAlterJob
            catalog.recreateTabletInvertIndex();
            // rebuild es state state
            catalog.getEsRepository().loadTableFromCatalog();
            checksum = catalog.loadLoadJob(dis, checksum);
            checksum = catalog.loadAlterJob(dis, checksum);
            checksum = catalog.loadRecycleBin(dis, checksum);
            checksum = catalog.loadGlobalVariable(dis, checksum);
            checksum = catalog.loadCluster(dis, checksum);
            checksum = catalog.loadBrokers(dis, checksum);
            checksum = catalog.loadResources(dis, checksum);
            checksum = catalog.loadExportJob(dis, checksum);
            checksum = catalog.loadSyncJobs(dis,checksum);
            checksum = catalog.loadBackupHandler(dis, checksum);
            checksum = catalog.loadPaloAuth(dis, checksum);
            // global transaction must be replayed before load jobs v2
            checksum = catalog.loadTransactionState(dis, checksum);
            checksum = catalog.loadColocateTableIndex(dis, checksum);
            checksum = catalog.loadRoutineLoadJobs(dis, checksum);
            checksum = catalog.loadLoadJobsV2(dis, checksum);
            checksum = catalog.loadSmallFiles(dis, checksum);
            checksum = catalog.loadPlugins(dis, checksum);
            checksum = catalog.loadDeleteHandler(dis, checksum);
            checksum = catalog.loadSqlBlockRule(dis, checksum);
        }

        MetaFooter metaFooter = MetaFooter.read(imageFile);
        long remoteChecksum = metaFooter.checksum;
        Preconditions.checkState(remoteChecksum == checksum, remoteChecksum + " vs. " + checksum);

        long loadImageEndTime = System.currentTimeMillis();
        LOG.info("finished to load image in " + (loadImageEndTime - loadImageStartTime) + " ms");
    }

}
