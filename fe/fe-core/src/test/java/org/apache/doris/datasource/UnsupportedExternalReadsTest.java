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

package org.apache.doris.datasource;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.log.CatalogLog;
import org.apache.doris.datasource.property.fileformat.DeferredFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.tablefunction.HdfsTableValuedFunction;
import org.apache.doris.tablefunction.S3TableValuedFunction;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TResultFileSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class UnsupportedExternalReadsTest {
    @Test
    public void testAvroFileFormatIsAccepted() {
        for (String format : new String[] {"avro", "AVRO"}) {
            FileFormatProperties properties = FileFormatProperties.createFileFormatProperties(format);
            Assertions.assertEquals("avro", properties.getFormatName());
            Assertions.assertEquals(TFileFormatType.FORMAT_AVRO,
                    properties.getFileFormatType());
        }
    }

    @Test
    public void testAvroTableFunctionsAcceptContainerFiles() throws Exception {
        boolean previous = FeConstants.runningUnitTest;
        FeConstants.runningUnitTest = true;
        try {
            Map<String, String> s3 = new HashMap<>();
            s3.put("uri", "s3://bucket/avro/data.avro");
            s3.put("s3.access_key", "ak");
            s3.put("s3.secret_key", "sk");
            s3.put("s3.region", "us-east-1");
            s3.put("s3.endpoint", "s3.us-east-1.amazonaws.com");
            s3.put("format", "avro");
            Assertions.assertEquals(TFileFormatType.FORMAT_AVRO,
                    new S3TableValuedFunction(s3).getTFileFormatType());
            Map<String, String> hdfs = new HashMap<>();
            hdfs.put("uri", "hdfs://localhost:8020/avro/data.avro");
            hdfs.put("fs.defaultFS", "hdfs://localhost:8020");
            hdfs.put("format", "AVRO");
            Assertions.assertEquals(TFileFormatType.FORMAT_AVRO,
                    new HdfsTableValuedFunction(hdfs).getTFileFormatType());
        } finally {
            FeConstants.runningUnitTest = previous;
        }
    }

    @Test
    public void testAvroDeferredInferenceAndReadOnlyOutput() {
        DeferredFileFormatProperties properties = new DeferredFileFormatProperties();
        properties.analyzeFileFormatProperties(new HashMap<>(), false);
        properties.deferInit(TFileFormatType.FORMAT_AVRO);
        Assertions.assertEquals(TFileFormatType.FORMAT_AVRO, properties.getDelegate().getFileFormatType());
        Assertions.assertThrows(AnalysisException.class,
                () -> properties.fullTResultFileSinkOptions(new TResultFileSinkOptions()));
    }

    @Test
    public void testLakeSoulCatalogIsRejected() {
        CatalogLog log = new CatalogLog();
        log.setCatalogId(1L);
        log.setCatalogName("removed_catalog");
        log.setProps(new HashMap<>(Collections.singletonMap("type", "lakesoul")));
        DdlException error = Assertions.assertThrows(DdlException.class, () -> CatalogFactory.createFromLog(log));
        Assertions.assertTrue(error.getMessage().contains("Lakesoul catalog is no longer supported"));
    }
}
