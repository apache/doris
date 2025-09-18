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
import org.apache.doris.thrift.TParquetCompressionType;
import org.apache.doris.thrift.TParquetVersion;
import org.apache.doris.thrift.TResultFileSinkOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class ParquetFileFormatPropertiesTest {

    private ParquetFileFormatProperties parquetFileFormatProperties;

    @Before
    public void setUp() {
        parquetFileFormatProperties = new ParquetFileFormatProperties();
    }

    @Test
    public void testAnalyzeFileFormatProperties() {
        Map<String, String> properties = new HashMap<>();
        // Add properties if needed
        parquetFileFormatProperties.analyzeFileFormatProperties(properties, true);

        Assert.assertEquals(TParquetCompressionType.SNAPPY, parquetFileFormatProperties.getParquetCompressionType());
        Assert.assertEquals(false, parquetFileFormatProperties.isParquetDisableDictionary());
    }

    @Test
    public void testSupportedCompressionTypes() {
        Map<String, String> properties = new HashMap<>();
        String[] types = {"snappy", "gzip", "brotli", "zstd", "lz4", "plain"};
        TParquetCompressionType[] expected = {
            TParquetCompressionType.SNAPPY,
            TParquetCompressionType.GZIP,
            TParquetCompressionType.BROTLI,
            TParquetCompressionType.ZSTD,
            TParquetCompressionType.LZ4,
            TParquetCompressionType.UNCOMPRESSED
        };
        for (int i = 0; i < types.length; i++) {
            properties.put("compress_type", types[i]);
            parquetFileFormatProperties.analyzeFileFormatProperties(properties, true);
            Assert.assertEquals(expected[i], parquetFileFormatProperties.getParquetCompressionType());
        }
    }

    @Test
    public void testCompressionTypeCaseInsensitive() {
        Map<String, String> properties = new HashMap<>();
        properties.put("compress_type", "SNAPPY");
        parquetFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(TParquetCompressionType.SNAPPY, parquetFileFormatProperties.getParquetCompressionType());
    }

    @Test(expected = AnalysisException.class)
    public void testInvalidCompressionType() {
        Map<String, String> properties = new HashMap<>();
        properties.put("compress_type", "invalid_type");
        parquetFileFormatProperties.analyzeFileFormatProperties(properties, true);
    }

    @Test
    public void testParquetDisableDictionary() {
        Map<String, String> properties = new HashMap<>();
        properties.put("parquet.disable_dictionary", "true");
        parquetFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertTrue(parquetFileFormatProperties.isParquetDisableDictionary());
        properties.put("parquet.disable_dictionary", "false");
        parquetFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertFalse(parquetFileFormatProperties.isParquetDisableDictionary());
    }

    @Test
    public void testEnableInt96Timestamps() {
        Map<String, String> properties = new HashMap<>();
        properties.put("enable_int96_timestamps", "true");
        parquetFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertTrue(parquetFileFormatProperties.isEnableInt96Timestamps());
        properties.put("enable_int96_timestamps", "false");
        parquetFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertFalse(parquetFileFormatProperties.isEnableInt96Timestamps());
    }

    @Test
    public void testParquetVersion() {
        Map<String, String> properties = new HashMap<>();
        properties.put("parquet.version", "v1");
        parquetFileFormatProperties.analyzeFileFormatProperties(properties, true);

        TResultFileSinkOptions sinkOptions = new TResultFileSinkOptions();
        parquetFileFormatProperties.fullTResultFileSinkOptions(sinkOptions);
        Assert.assertEquals(TParquetVersion.PARQUET_1_0, sinkOptions.getParquetVersion());

        properties.put("parquet.version", "latest");
        parquetFileFormatProperties.analyzeFileFormatProperties(properties, true);
        sinkOptions = new TResultFileSinkOptions();
        parquetFileFormatProperties.fullTResultFileSinkOptions(sinkOptions);
        Assert.assertEquals(TParquetVersion.PARQUET_2_LATEST, sinkOptions.getParquetVersion());
    }

    @Test
    public void testParquetVersionInvalid() {
        Map<String, String> properties = new HashMap<>();
        properties.put("parquet.version", "invalid");
        parquetFileFormatProperties.analyzeFileFormatProperties(properties, true);

        TResultFileSinkOptions sinkOptions = new TResultFileSinkOptions();
        parquetFileFormatProperties.fullTResultFileSinkOptions(sinkOptions);
        Assert.assertEquals(TParquetVersion.PARQUET_1_0, sinkOptions.getParquetVersion());
    }

    @Test
    public void testFullTResultFileSinkOptions() {
        TResultFileSinkOptions sinkOptions = new TResultFileSinkOptions();
        parquetFileFormatProperties.fullTResultFileSinkOptions(sinkOptions);
        Assert.assertEquals(parquetFileFormatProperties.getParquetCompressionType(), sinkOptions.getParquetCompressionType());
        Assert.assertEquals(parquetFileFormatProperties.isParquetDisableDictionary(), sinkOptions.isParquetDisableDictionary());
    }

    @Test
    public void testToTFileAttributes() {
        TFileAttributes attrs = parquetFileFormatProperties.toTFileAttributes();
        Assert.assertNotNull(attrs);
        Assert.assertNotNull(attrs.getTextParams());
    }
}
