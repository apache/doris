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
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.Charset;
import java.util.Arrays;

public class MetaHeader {
    private static final Logger LOG = LogManager.getLogger(MetaHeader.class);

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final MetaHeader EMPTY_HEADER = new MetaHeader(null, null, 0);

    public long length;
    public FeMetaFormat metaFormat;
    public MetaMagicNumber metaMagicNumber;
    public MetaFileHeader metaFileHeader;

    public static MetaHeader readHeader(File imageFile) throws IOException {
        LOG.info("start load image header from {}.", imageFile.getAbsolutePath());
        try(RandomAccessFile raf = new RandomAccessFile(imageFile, "r")) {
            raf.seek(0);
            MetaMagicNumber magicMagicNumber = MetaMagicNumber.read(raf);
            if (!Arrays.equals(MetaMagicNumber.MAGIC, magicMagicNumber.bytes)) {
                LOG.warn("Image file {} format mismatch. Expected magic number is {}, actual is {}",
                        imageFile.getPath(), Arrays.toString(MetaMagicNumber.MAGIC), Arrays.toString(magicMagicNumber.bytes));
                return EMPTY_HEADER;
            }
            MetaFileHeader metaFileHeader = MetaFileHeader.read(raf);
            if (!MetaFileHeader.IMAGE_VERSION.equalsIgnoreCase(metaFileHeader.imageVersion)) {
                LOG.warn("Image file {} format mismatch. Expected version is {}, actual is {}",
                        imageFile.getPath(), MetaFileHeader.IMAGE_VERSION, metaFileHeader.imageVersion);
                return EMPTY_HEADER;
            }

            return new MetaHeader(magicMagicNumber, metaFileHeader, raf.getFilePointer(),
                    FeMetaFormat.valueOf(new String(magicMagicNumber.bytes)));
        }
    }

    public static long writeHeader(File imageFile) throws IOException {
        LOG.info("start save image header to {}.", imageFile.getAbsolutePath());
        try (RandomAccessFile raf = new RandomAccessFile(imageFile, "rw")) {
            raf.seek(0);
            MetaMagicNumber.write(raf);
            MetaFileHeader.write(raf);
            return raf.getFilePointer();
        }
    }

    public static class MetaMagicNumber {
        public static final String MAGIC_STR = FeConstants.meta_format.getMagicString();
        public static final byte[] MAGIC = MAGIC_STR.getBytes(Charset.forName("ASCII"));
        public byte[] bytes;

        public static MetaMagicNumber read(RandomAccessFile raf) throws IOException {
            MetaMagicNumber metaMagicNumber = new MetaMagicNumber();
            byte[] magicBytes = new byte[MAGIC_STR.length()];
            raf.readFully(magicBytes);
            metaMagicNumber.bytes = magicBytes;
            return metaMagicNumber;
        }

        public static void write(RandomAccessFile raf) throws IOException {
            raf.write(MAGIC);
        }
    }

    public static class MetaFileHeader {
        public static final String IMAGE_VERSION = FeConstants.meta_format.getVersion();
        // the version of image format
        public String imageVersion;

        public static MetaFileHeader read(RandomAccessFile raf) throws IOException {
            int headerLength = raf.readInt();
            LOG.info("read json length {}", headerLength);
            byte[] headerBytes = new byte[headerLength];
            LOG.info("read json {}", headerBytes);
            raf.readFully(headerBytes);
            return MetaFileHeader.fromJson(new String(headerBytes));
        }

        public static void write(RandomAccessFile raf) throws IOException {
            MetaFileHeader metaFileHeader = new MetaFileHeader();
            metaFileHeader.imageVersion = IMAGE_VERSION;
            byte[] headerBytes =  MetaFileHeader.toJson(metaFileHeader).getBytes();
            raf.writeInt(headerBytes.length);
            raf.write(headerBytes);
        }

        private static MetaFileHeader fromJson(String json) throws IOException {
            return (MetaFileHeader) OBJECT_MAPPER.readValue(json, MetaFileHeader.class);
        }

        private static String toJson(MetaFileHeader fileHeader) throws IOException {
            return OBJECT_MAPPER.writeValueAsString(fileHeader);
        }
    }

    public MetaHeader(MetaMagicNumber metaMagicNumber, MetaFileHeader metaFileHeader, long length) {
        this(metaMagicNumber, metaFileHeader, length, FeMetaFormat.COR1);
    }

    public MetaHeader(MetaMagicNumber metaMagicNumber, MetaFileHeader metaFileHeader, long length, FeMetaFormat metaFormat) {
        this.metaMagicNumber = metaMagicNumber;
        this.metaFileHeader = metaFileHeader;
        this.length = length;
        this.metaFormat = metaFormat;
    }

    public long getLength() {
        return length;
    }

    public FeMetaFormat getMetaFormat() {
        return metaFormat;
    }

    public MetaMagicNumber getMetaMagicNumber() {
        return metaMagicNumber;
    }

    public MetaFileHeader getMetaFileHeader() {
        return metaFileHeader;
    }
}
