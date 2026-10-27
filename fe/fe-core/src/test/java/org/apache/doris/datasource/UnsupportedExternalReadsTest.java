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
import org.apache.doris.datasource.log.CatalogLog;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.nereids.exceptions.AnalysisException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;

public class UnsupportedExternalReadsTest {
    @Test
    public void testAvroFileFormatIsRejected() {
        for (String format : new String[] {"avro", "AVRO"}) {
            AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                    () -> FileFormatProperties.createFileFormatProperties(format));
            Assertions.assertTrue(error.getMessage().contains("Avro file reads are no longer supported"));
        }
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
