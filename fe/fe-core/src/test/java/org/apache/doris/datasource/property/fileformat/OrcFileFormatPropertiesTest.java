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

import org.apache.doris.thrift.TFileAttributes;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TResultFileSinkOptions;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class OrcFileFormatPropertiesTest {

    private OrcFileFormatProperties orcFileFormatProperties;

    @Before
    public void setUp() {
        orcFileFormatProperties = new OrcFileFormatProperties();
    }

    @Test
    public void testAnalyzeFileFormatProperties() {
        Map<String, String> properties = new HashMap<>();
        // Add properties if needed
        orcFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(TFileCompressType.ZLIB, orcFileFormatProperties.getOrcCompressionType());
    }

    @Test
    public void testSupportedCompressionTypes() {
        Map<String, String> properties = new HashMap<>();
        properties.put("compress_type", "plain");
        orcFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(TFileCompressType.PLAIN, orcFileFormatProperties.getOrcCompressionType());

        properties.put("compress_type", "snappy");
        orcFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(TFileCompressType.SNAPPYBLOCK, orcFileFormatProperties.getOrcCompressionType());

        properties.put("compress_type", "zlib");
        orcFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(TFileCompressType.ZLIB, orcFileFormatProperties.getOrcCompressionType());

        properties.put("compress_type", "zstd");
        orcFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(TFileCompressType.ZSTD, orcFileFormatProperties.getOrcCompressionType());
    }

    @Test
    public void testCompressionTypeCaseInsensitive() {
        Map<String, String> properties = new HashMap<>();
        properties.put("compress_type", "SNAPPY");
        orcFileFormatProperties.analyzeFileFormatProperties(properties, true);
        Assert.assertEquals(TFileCompressType.SNAPPYBLOCK, orcFileFormatProperties.getOrcCompressionType());
    }

    @Test(expected = org.apache.doris.nereids.exceptions.AnalysisException.class)
    public void testInvalidCompressionType() {
        Map<String, String> properties = new HashMap<>();
        properties.put("compress_type", "invalid_type");
        orcFileFormatProperties.analyzeFileFormatProperties(properties, true);
    }

    @Test
    public void testFullTResultFileSinkOptions() {
        TResultFileSinkOptions sinkOptions = new TResultFileSinkOptions();
        orcFileFormatProperties.fullTResultFileSinkOptions(sinkOptions);
        Assert.assertEquals(orcFileFormatProperties.getOrcCompressionType(), sinkOptions.getOrcCompressionType());
        Assert.assertEquals(1, sinkOptions.getOrcWriterVersion());
    }

    @Test
    public void testToTFileAttributes() {
        TFileAttributes attrs = orcFileFormatProperties.toTFileAttributes();
        Assert.assertNotNull(attrs);
        Assert.assertNotNull(attrs.getTextParams());
    }
}
