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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;

/**
 * Header Format:
 * - Magic String (4 bytes)
 * - Header Length (4 bytes)
 * |- Header -----------------------------|
 * | |- Json Header ---------------|      |
 * | | - version                   |      |
 * | | - other key/value(undecided)|      |
 * | |-----------------------------|      |
 * |--------------------------------------|
 */

public class MetaHeader {
    private static final Logger LOG = LogManager.getLogger(MetaHeader.class);

    public static final MetaHeader EMPTY_HEADER = new MetaHeader(null, 0);
    private static final long HEADER_LENGTH_SIZE = 4L;

    // length of Header
    private long length;
    // format of image
    private FeMetaFormat metaFormat;
    // json header
    public MetaJsonHeader metaJsonHeader;

    public static MetaHeader read(File imageFile) throws IOException {
        try(RandomAccessFile raf = new RandomAccessFile(imageFile, "r")) {
            raf.seek(0);
            MetaMagicNumber magicNumber = MetaMagicNumber.read(raf);
            if (!Arrays.equals(MetaMagicNumber.MAGIC, magicNumber.getBytes())) {
                LOG.warn("Image file {} format mismatch. Expected magic number is {}, actual is {}",
                        imageFile.getPath(), Arrays.toString(MetaMagicNumber.MAGIC), Arrays.toString(magicNumber.getBytes()));
                return EMPTY_HEADER;
            }
            MetaJsonHeader metaJsonHeader = MetaJsonHeader.read(raf);
            if (!MetaJsonHeader.IMAGE_VERSION.equalsIgnoreCase(metaJsonHeader.imageVersion)) {
                String errMsg = "Image file " + imageFile.getPath() + " format version mismatch. " +
                        "Expected version is "+ MetaJsonHeader.IMAGE_VERSION +", actual is" + metaJsonHeader.imageVersion;
                // different versions are incompatible
                throw new IOException(errMsg);
            }

            long length = raf.getFilePointer() - MetaMagicNumber.MAGIC_STR.length() - HEADER_LENGTH_SIZE;
            FeMetaFormat metaFormat = FeMetaFormat.valueOf(new String(magicNumber.getBytes()));
            LOG.info("Image header length: {}, format: {}.", length, metaFormat);
            return new MetaHeader(metaJsonHeader, length, metaFormat);
        }
    }

    public static long write(File imageFile) throws IOException {
        if (imageFile.length() != 0) {
            throw new IOException("Meta header has to be written to an empty file.");
        }

        try (RandomAccessFile raf = new RandomAccessFile(imageFile, "rw")) {
            raf.seek(0);
            MetaMagicNumber.write(raf);
            MetaJsonHeader.write(raf);
            raf.getChannel().force(true);
            return raf.getFilePointer();
        }
    }

    public MetaHeader(MetaJsonHeader metaJsonHeader, long length) {
        this(metaJsonHeader, length, FeMetaFormat.COR1);
    }

    public MetaHeader(MetaJsonHeader metaJsonHeader, long length, FeMetaFormat metaFormat) {
        this.metaJsonHeader = metaJsonHeader;
        this.length = length;
        this.metaFormat = metaFormat;
    }

    public long getEnd() {
        if (length > 0) {
            return MetaMagicNumber.MAGIC_STR.length() + HEADER_LENGTH_SIZE + length;
        }
        return 0;
    }

    public long getLength() {
        return length;
    }

    public FeMetaFormat getMetaFormat() {
        return metaFormat;
    }

    public MetaJsonHeader getMetaJsonHeader() {
        return metaJsonHeader;
    }


}
