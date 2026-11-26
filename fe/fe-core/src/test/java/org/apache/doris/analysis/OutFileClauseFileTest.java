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

package org.apache.doris.analysis;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

class OutFileClauseFileTest {
    @Test
    void outfileRejectsTopLevelAndNestedFile() {
        boolean original = Config.enable_outfile_to_local;
        Config.enable_outfile_to_local = true;
        try {
            for (String format : Arrays.asList("csv", "csv_with_names", "csv_with_names_and_types", "parquet", "orc")) {
                for (Type type : Arrays.asList(Type.FILE, new ArrayType(Type.FILE),
                        new MapType(Type.STRING, Type.FILE), new StructType(new ArrayList<>(
                                Collections.singletonList(new StructField("asset", new ArrayType(Type.FILE))))))) {
                    OutFileClause clause = new OutFileClause("file:///export/file_", format, Collections.emptyMap());
                    AnalysisException error = Assertions.assertThrows(AnalysisException.class,
                            () -> clause.analyze(Collections.singletonList(new SlotRef(type, true)),
                                    Collections.singletonList("payload"), true));
                    Assertions.assertEquals("OUTFILE does not support FILE", error.getDetailMessage(), format);
                }
            }
        } finally {
            Config.enable_outfile_to_local = original;
        }
    }

    @Test
    void parquetRetainsOrdinaryNestedTypes() throws Exception {
        boolean original = Config.enable_outfile_to_local;
        Config.enable_outfile_to_local = true;
        try {
            OutFileClause clause = new OutFileClause("file:///tmp/ordinary_", "parquet", Collections.emptyMap());
            clause.analyze(Arrays.asList(new SlotRef(Type.INT, true),
                            new SlotRef(new ArrayType(Type.STRING), true)),
                    Arrays.asList("id", "items"), true);
            Assertions.assertEquals(2, clause.toSinkOptions().getParquetSchemasSize());
        } finally {
            Config.enable_outfile_to_local = original;
        }
    }
}
