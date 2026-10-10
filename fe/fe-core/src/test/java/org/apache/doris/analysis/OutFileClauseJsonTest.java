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
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.thrift.TFileCompressType;
import org.apache.doris.thrift.TResultFileSinkOptions;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

class OutFileClauseJsonTest {
    private boolean enableOutfileToLocal;

    @BeforeEach
    void allowLocalOutput() {
        enableOutfileToLocal = Config.enable_outfile_to_local;
        Config.enable_outfile_to_local = true;
    }

    @AfterEach
    void restoreLocalOutput() {
        Config.enable_outfile_to_local = enableOutfileToLocal;
    }

    @Test
    void defaultOptionsWithoutProperties() throws Exception {
        for (boolean nullProperties : Arrays.asList(true, false)) {
            OutFileClause clause = new OutFileClause("file:///tmp/json_", "json",
                    nullProperties ? null : Collections.emptyMap());
            clause.analyze(Collections.singletonList(new SlotRef(Type.INT, true)),
                    Collections.singletonList("ID"), true);
            TResultFileSinkOptions options = clause.toSinkOptions();
            Assertions.assertEquals(Collections.singletonList("id"), options.getJsonColumnNames());
            Assertions.assertEquals(TFileCompressType.PLAIN, options.getCompressionType());
            Assertions.assertEquals("\n", options.getLineDelimiter());
        }
    }

    @Test
    void canonicalNamesSurviveWireAndClone() throws Exception {
        Locale originalLocale = Locale.getDefault();
        OutFileClause clause = new OutFileClause("file:///tmp/json_", "json", Collections.emptyMap());
        try {
            Locale.setDefault(Locale.forLanguageTag("tr-TR"));
            clause.analyze(Arrays.asList(new SlotRef(Type.INT, true), new SlotRef(Type.STRING, true),
                    new SlotRef(Type.STRING, true)), Arrays.asList("I", "space A", "quote\"And\nNewline"), true);
        } finally {
            Locale.setDefault(originalLocale);
        }
        List<String> names = Arrays.asList("i", "space a", "quote\"and\nnewline");
        TResultFileSinkOptions decoded = new TResultFileSinkOptions();
        new TDeserializer().deserialize(decoded, new TSerializer().serialize(clause.toSinkOptions()));
        Assertions.assertEquals(names, decoded.getJsonColumnNames());
        Assertions.assertEquals(names, clause.clone().toSinkOptions().getJsonColumnNames());
    }

    @Test
    void rejectVarbinaryInArrayAndMapChildren() {
        for (Type type : Arrays.asList(new ArrayType(Type.VARBINARY), new MapType(Type.STRING, Type.VARBINARY),
                new MapType(Type.VARBINARY, Type.FILE))) {
            OutFileClause clause = new OutFileClause("file:///tmp/json_", "json", Collections.emptyMap());
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> clause.analyze(Collections.singletonList(new SlotRef(type, true)),
                            Collections.singletonList("payload"), true));
            Assertions.assertTrue(exception.getMessage().contains("JSON OUTFILE does not support type varbinary"),
                    exception.getMessage());
        }
    }

    @Test
    void otherFormatsKeepDuplicateLabelsAndNoJsonNames() throws Exception {
        OutFileClause clause = new OutFileClause("file:///tmp/csv_", "csv", Collections.emptyMap());
        clause.analyze(Arrays.asList(new SlotRef(Type.INT, true), new SlotRef(Type.STRING, true)),
                Arrays.asList("ID", "id"), true);
        Assertions.assertFalse(clause.toSinkOptions().isSetJsonColumnNames());
    }
}
