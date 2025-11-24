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

package org.apache.doris.datasource.property.fileformat;

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileTextScanRangeParams;
import org.apache.doris.thrift.TParquetCompressionType;
import org.apache.doris.thrift.TParquetVersion;
import org.apache.doris.thrift.TResultFileSinkOptions;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class ParquetFileFormatProperties extends FileFormatProperties {
    public static final String PARQUET_DISABLE_DICTIONARY = "disable_dictionary";
    public static final String PARQUET_VERSION = "version";
    public static final String ENABLE_INT96_TIMESTAMPS = "enable_int96_timestamps";
    public static final String PARQUET_PROP_PREFIX = "parquet.";

    public static final Logger LOG = LogManager.getLogger(ParquetFileFormatProperties.class);
    public static final Map<String, TParquetCompressionType> PARQUET_COMPRESSION_TYPE_MAP = Maps.newHashMap();
    public static final Map<String, TParquetVersion> PARQUET_VERSION_MAP = Maps.newHashMap();

    static {
        PARQUET_COMPRESSION_TYPE_MAP.put("snappy", TParquetCompressionType.SNAPPY);
        PARQUET_COMPRESSION_TYPE_MAP.put("gzip", TParquetCompressionType.GZIP);
        PARQUET_COMPRESSION_TYPE_MAP.put("brotli", TParquetCompressionType.BROTLI);
        PARQUET_COMPRESSION_TYPE_MAP.put("zstd", TParquetCompressionType.ZSTD);
        PARQUET_COMPRESSION_TYPE_MAP.put("lz4", TParquetCompressionType.LZ4);
        // arrow do not support lzo and bz2 compression type.
        // PARQUET_COMPRESSION_TYPE_MAP.put("lzo", TParquetCompressionType.LZO);
        // PARQUET_COMPRESSION_TYPE_MAP.put("bz2", TParquetCompressionType.BZ2);
        PARQUET_COMPRESSION_TYPE_MAP.put("plain", TParquetCompressionType.UNCOMPRESSED);

        PARQUET_VERSION_MAP.put("v1", TParquetVersion.PARQUET_1_0);
        PARQUET_VERSION_MAP.put("latest", TParquetVersion.PARQUET_2_LATEST);
    }

    private TParquetCompressionType parquetCompressionType = TParquetCompressionType.SNAPPY;
    private boolean parquetDisableDictionary = false;
    private TParquetVersion parquetVersion = TParquetVersion.PARQUET_1_0;
    private boolean enableInt96Timestamps = true;

    public ParquetFileFormatProperties() {
        super(TFileFormatType.FORMAT_PARQUET, FileFormatProperties.FORMAT_PARQUET);
    }

    @Override
    public void analyzeFileFormatProperties(Map<String, String> formatProperties, boolean isRemoveOriginProperty)
            throws AnalysisException {
        // save compress type
        if (formatProperties.containsKey(PROP_COMPRESS_TYPE)) {
            if (PARQUET_COMPRESSION_TYPE_MAP.containsKey(formatProperties.get(PROP_COMPRESS_TYPE)
                    .toLowerCase())) {
                this.parquetCompressionType = PARQUET_COMPRESSION_TYPE_MAP.get(
                        formatProperties.get(PROP_COMPRESS_TYPE).toLowerCase());
                formatProperties.remove(PROP_COMPRESS_TYPE);
            } else {
                throw new AnalysisException("parquet compression type ["
                        + formatProperties.get(PROP_COMPRESS_TYPE)
                        + "] is invalid, please choose one among SNAPPY, GZIP, BROTLI, ZSTD, LZ4, LZO, BZ2 or PLAIN");
            }
        }

        //save the enable int96 timestamp property
        if (formatProperties.containsKey(ENABLE_INT96_TIMESTAMPS)) {
            this.enableInt96Timestamps = Boolean.valueOf(formatProperties.get(ENABLE_INT96_TIMESTAMPS)).booleanValue();
        }

        // save all parquet prefix property
        Iterator<Entry<String, String>> iter = formatProperties.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> entry = iter.next();
            if (entry.getKey().startsWith(PARQUET_PROP_PREFIX)) {
                iter.remove();
                if (entry.getKey().substring(PARQUET_PROP_PREFIX.length())
                        .equals(PARQUET_DISABLE_DICTIONARY)) {
                    this.parquetDisableDictionary = Boolean.valueOf(entry.getValue());
                } else if (entry.getKey().substring(PARQUET_PROP_PREFIX.length())
                        .equals(PARQUET_VERSION)) {
                    if (PARQUET_VERSION_MAP.containsKey(entry.getValue())) {
                        this.parquetVersion = PARQUET_VERSION_MAP.get(entry.getValue());
                    } else {
                        LOG.debug("not set parquet version type or is invalid, set default to PARQUET_1.0 version.");
                    }
                }
            }
        }
    }


    @Override
    public void fullTResultFileSinkOptions(TResultFileSinkOptions sinkOptions) {
        sinkOptions.setParquetCompressionType(parquetCompressionType);
        sinkOptions.setParquetDisableDictionary(parquetDisableDictionary);
        sinkOptions.setParquetVersion(parquetVersion);
        sinkOptions.setEnableInt96Timestamps(enableInt96Timestamps);
    }

    @Override
    public TFileAttributes toTFileAttributes() {
        TFileAttributes fileAttributes = new TFileAttributes();
        TFileTextScanRangeParams fileTextScanRangeParams = new TFileTextScanRangeParams();
        fileAttributes.setTextParams(fileTextScanRangeParams);
        return fileAttributes;
    }

    public TParquetCompressionType getParquetCompressionType() {
        return parquetCompressionType;
    }

    public boolean isParquetDisableDictionary() {
        return parquetDisableDictionary;
    }

    public boolean isEnableInt96Timestamps() {
        return enableInt96Timestamps;
    }
}
