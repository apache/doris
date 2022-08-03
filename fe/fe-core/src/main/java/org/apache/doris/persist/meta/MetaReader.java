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
import org.apache.doris.common.DdlException;

import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

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

    public static void read(File imageFile, Env env) throws IOException, DdlException {
        LOG.info("start load image from {}. is ckpt: {}", imageFile.getAbsolutePath(), Env.isCheckpointThread());
        long loadImageStartTime = System.currentTimeMillis();
        MetaHeader metaHeader = MetaHeader.read(imageFile);
        MetaFooter metaFooter = MetaFooter.read(imageFile);

        long checksum = 0;
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(imageFile)))) {
            // 1. Skip image file header
            IOUtils.skipFully(dis, metaHeader.getEnd());
            // 2. Read meta header first
            checksum = env.loadHeader(dis, metaHeader, checksum);
            // 3. Read other meta modules
            // Modules must be read in the order in which the metadata was written
            for (MetaIndex metaIndex : metaFooter.metaIndices) {
                if (metaIndex.name.equals("header")) {
                    // skip meta header, which has been read before.
                    continue;
                }
                MetaPersistMethod persistMethod = PersistMetaModules.MODULES_MAP.get(metaIndex.name);
                if (persistMethod == null) {
                    throw new IOException("Unknown meta module: " + metaIndex.name + ". Known moduels: "
                            + PersistMetaModules.MODULE_NAMES);
                }
                checksum = (long) persistMethod.readMethod.invoke(env, dis, checksum);
            }
        } catch (InvocationTargetException | IllegalAccessException e) {
            throw new IOException(e);
        }

        long remoteChecksum = metaFooter.checksum;
        Preconditions.checkState(remoteChecksum == checksum, remoteChecksum + " vs. " + checksum);

        long loadImageEndTime = System.currentTimeMillis();
        LOG.info("finished to load image in " + (loadImageEndTime - loadImageStartTime) + " ms");
    }
}
