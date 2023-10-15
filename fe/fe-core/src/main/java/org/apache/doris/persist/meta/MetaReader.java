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
import org.apache.doris.common.Config;
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
        long footerIndex = imageFile.length()
                - metaFooter.length - MetaFooter.FOOTER_LENGTH_SIZE - MetaMagicNumber.MAGIC_STR.length();
        try (DataInputStream dis = new DataInputStream(new BufferedInputStream(new FileInputStream(imageFile)))) {
            // 1. Skip image file header
            IOUtils.skipFully(dis, metaHeader.getEnd());
            // 2. Read meta header first
            checksum = env.loadHeader(dis, metaHeader, checksum);
            // 3. Read other meta modules
            // Modules must be read in the order in which the metadata was written
            for (int i = 0; i < metaFooter.metaIndices.size(); ++i) {
                MetaIndex metaIndex = metaFooter.metaIndices.get(i);
                if (metaIndex.name.equals("header")) {
                    // skip meta header, which has been read before.
                    continue;
                }
                if (i < metaFooter.metaIndices.size() - 1
                        && metaIndex.offset == metaFooter.metaIndices.get(i + 1).offset) {
                    // skip empty meta
                    LOG.info("Skip {} module since empty meta length.", metaIndex.name);
                    continue;
                } else if (metaIndex.offset == footerIndex) {
                    // skip last empty meta
                    LOG.info("Skip {} module since empty meta length in the end.", metaIndex.name);
                    continue;
                }
                // skip deprecated modules
                if (PersistMetaModules.DEPRECATED_MODULE_NAMES.contains(metaIndex.name)) {
                    LOG.warn("meta modules {} is deprecated, ignore and skip it");
                    // If this is the last module, nothing need to do.
                    if (i < metaFooter.metaIndices.size() - 1) {
                        IOUtils.skipFully(dis, metaFooter.metaIndices.get(i + 1).offset - metaIndex.offset);
                    }
                    continue;
                }
                MetaPersistMethod persistMethod = PersistMetaModules.MODULES_MAP.get(metaIndex.name);
                if (persistMethod == null) {
                    if (Config.ignore_unknown_metadata_module) {
                        LOG.warn("meta modules {} is unknown, ignore and skip it");
                        // If this is the last module, nothing need to do.
                        if (i < metaFooter.metaIndices.size() - 1) {
                            IOUtils.skipFully(dis, metaFooter.metaIndices.get(i + 1).offset - metaIndex.offset);
                        }
                        continue;
                    } else {
                        throw new IOException("Unknown meta module: " + metaIndex.name + ". Known modules: "
                                + PersistMetaModules.MODULE_NAMES);
                    }
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
