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

package org.apache.doris.tablefunction;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.datasource.property.fileformat.AvroFileFormatProperties;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class HttpStreamTableValuedFunctionTest {
    @Test
    public void rejectsAvroBeforePlanningAnUnsupportedStreamReader() {
        AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                () -> new HttpStreamTableValuedFunction(Map.of("format", "AvRo")));
        Assertions.assertTrue(error.getMessage().contains("avro"));
    }

    @Test
    public void preservesStreamFormatsAndExistingColumnarRejection() {
        for (String format : new String[] {"csv", "json"}) {
            Assertions.assertDoesNotThrow(() -> Assertions.assertEquals(TFileType.FILE_STREAM,
                    new HttpStreamTableValuedFunction(Map.of("format", format)).getTFileType()));
        }
        for (String format : new String[] {"parquet", "orc"}) {
            Assertions.assertThrows(AnalysisException.class,
                    () -> new HttpStreamTableValuedFunction(Map.of("format", format)));
        }
    }

    @Test
    public void keepsAvroAvailableForSeekableFileInputs() {
        FileFormatProperties properties = FileFormatProperties.createFileFormatProperties("avro");
        Assertions.assertInstanceOf(AvroFileFormatProperties.class, properties);
        Assertions.assertEquals(TFileFormatType.FORMAT_AVRO, properties.getFileFormatType());
    }
}
