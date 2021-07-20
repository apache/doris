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

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;

/**
 * Footer Format:
 * |- Footer -----------------------------|
 * | |- object index --------------|      |
 * | | - index a                   |      |
 * | | - index b                   |      |
 * | | ...                         |      |
 * | |-----------------------------|      |
 * | - other value(undecided)             |
 * |--------------------------------------|
 * - Footer Length (8 bytes)
 * - Magic String (4 bytes)
 */

public class MetaFooter {
    private static final Logger LOG = LogManager.getLogger(MetaFooter.class);

    private static final long FOOTER_LENGTH_SIZE = 8L;
    public static final MetaFooter EMPTY_FOOTER = new MetaFooter(null, 0L);

    // length of footer
    private long length;
    // meta indices
    public List<MetaIndex> metaIndices;

    public static MetaFooter read(File imageFile) throws IOException {
        try(RandomAccessFile raf = new RandomAccessFile(imageFile, "r")) {
            long fileLength = raf.length();
            long footerLengthIndex = fileLength - FOOTER_LENGTH_SIZE - MetaMagicNumber.MAGIC_STR.length();
            raf.seek(footerLengthIndex);
            long footerLength = raf.readLong();
            MetaMagicNumber magicNumber = MetaMagicNumber.read(raf);
            if (!Arrays.equals(MetaMagicNumber.MAGIC, magicNumber.getBytes())) {
                LOG.warn("Image file {} format mismatch. Expected magic number is {}, actual is {}",
                        imageFile.getPath(), Arrays.toString(MetaMagicNumber.MAGIC), Arrays.toString(magicNumber.getBytes()));
                return EMPTY_FOOTER;
            }
            long footerIndex = footerLengthIndex - footerLength;
            raf.seek(footerIndex);
            int indexNum = raf.readInt();
            List<MetaIndex> metaIndices = Lists.newArrayList();
            for (int i = 0; i < indexNum; i++) {
                MetaIndex index = MetaIndex.read(raf);
                metaIndices.add(index);
            }
            LOG.info("Image footer length: {}, indices: {}", footerLength, metaIndices.toArray());
            return new MetaFooter(metaIndices, footerLength);
        }
    }

    public static void write(File imageFile, List<MetaIndex> metaIndices) throws IOException {
        try(RandomAccessFile raf = new RandomAccessFile(imageFile, "rw")) {
            long startIndex = raf.length();
            raf.seek(startIndex);
            raf.writeInt(metaIndices.size());
            for (MetaIndex metaIndex : metaIndices) {
                MetaIndex.write(raf, metaIndex);
            }
            long endIndex = raf.length();
            raf.writeLong(endIndex - startIndex);
            MetaMagicNumber.write(raf);
        }
    }

    public MetaFooter(List<MetaIndex> metaIndices, long length) {
        this.metaIndices = metaIndices;
        this.length = length;
    }

}
