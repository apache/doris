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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.FileFormatConstants;
import org.apache.doris.common.util.FileFormatUtils;
import org.apache.doris.datasource.lance.LanceTableMetadata;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.datasource.property.fileformat.LanceFileFormatProperties;
import org.apache.doris.thrift.TFileFormatType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ExternalFileTableValuedFunctionTest {
    @Test
    public void testLanceIsAcceptedByFileFormatFactory() {
        FileFormatProperties properties = FileFormatProperties.createFileFormatProperties("LaNcE");
        Assert.assertTrue(properties instanceof LanceFileFormatProperties);
        Assert.assertEquals(TFileFormatType.FORMAT_LANCE, properties.getFileFormatType());
    }

    @Test
    public void testCsvSchemaParse() {
        Config.enable_date_conversion = true;
        Map<String, String> properties = Maps.newHashMap();
        properties.put(FileFormatConstants.PROP_CSV_SCHEMA,
                "k1:int;k2:bigint;k3:float;k4:double;k5:smallint;k6:tinyint;k7:bool;"
                        + "k8:char(10);k9:varchar(20);k10:date;k11:datetime;k12:decimal(10,2)");
        List<Column> csvSchema = Lists.newArrayList();
        try {
            FileFormatUtils.parseCsvSchema(csvSchema, properties.get(FileFormatConstants.PROP_CSV_SCHEMA));
            Assert.fail();
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.assertTrue(e.getMessage().contains("unsupported column type: bool"));
        }

        csvSchema.clear();
        properties.put(FileFormatConstants.PROP_CSV_SCHEMA,
                "k1:int;k2:bigint;k3:float;k4:double;k5:smallint;k6:tinyint;k7:boolean;"
                        + "k8:string;k9:date;k10:datetime;k11:decimal(10, 2);k12:decimal( 38,10); k13:datetime(5)");
        try {
            FileFormatUtils.parseCsvSchema(csvSchema, properties.get(FileFormatConstants.PROP_CSV_SCHEMA));
            Assert.assertEquals(13, csvSchema.size());
            Column decimalCol = csvSchema.get(10);
            Assert.assertEquals(10, decimalCol.getPrecision());
            Assert.assertEquals(2, decimalCol.getScale());
            decimalCol = csvSchema.get(11);
            Assert.assertEquals(38, decimalCol.getPrecision());
            Assert.assertEquals(10, decimalCol.getScale());
            Column datetimeCol = csvSchema.get(12);
            Assert.assertEquals(5, datetimeCol.getScale());

            for (int i = 0; i < csvSchema.size(); i++) {
                Column col = csvSchema.get(i);
                switch (col.getName()) {
                    case "k1":
                        Assert.assertEquals(PrimitiveType.INT, col.getType().getPrimitiveType());
                        break;
                    case "k2":
                        Assert.assertEquals(PrimitiveType.BIGINT, col.getType().getPrimitiveType());
                        break;
                    case "k3":
                        Assert.assertEquals(PrimitiveType.FLOAT, col.getType().getPrimitiveType());
                        break;
                    case "k4":
                        Assert.assertEquals(PrimitiveType.DOUBLE, col.getType().getPrimitiveType());
                        break;
                    case "k5":
                        Assert.assertEquals(PrimitiveType.SMALLINT, col.getType().getPrimitiveType());
                        break;
                    case "k6":
                        Assert.assertEquals(PrimitiveType.TINYINT, col.getType().getPrimitiveType());
                        break;
                    case "k7":
                        Assert.assertEquals(PrimitiveType.BOOLEAN, col.getType().getPrimitiveType());
                        break;
                    case "k8":
                        Assert.assertEquals(PrimitiveType.STRING, col.getType().getPrimitiveType());
                        break;
                    case "k9":
                        Assert.assertEquals(PrimitiveType.DATEV2, col.getType().getPrimitiveType());
                        break;
                    case "k10":
                        Assert.assertEquals(PrimitiveType.DATETIMEV2, col.getType().getPrimitiveType());
                        break;
                    case "k11":
                        Assert.assertEquals(PrimitiveType.DECIMAL64, col.getType().getPrimitiveType());
                        break;
                    case "k12":
                        Assert.assertEquals(PrimitiveType.DECIMAL128, col.getType().getPrimitiveType());
                        break;
                    case "k13":
                        Assert.assertEquals(PrimitiveType.DATETIMEV2, col.getType().getPrimitiveType());
                        break;
                    default:
                        Assert.fail("unknown column name: " + col.getName());
                }
            }
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    // Verifies a shared-storage Lance TVF executes on the backend that provided its schema.
    @Test
    public void testLocalLanceExecutionUsesSchemaBackend() throws Exception {
        LocalTableValuedFunction tvf =
                Mockito.mock(LocalTableValuedFunction.class, Mockito.CALLS_REAL_METHODS);
        setLongField(tvf, "backendId", -1L);
        setLongField(tvf, "backendIdForRequest", 23L);

        Mockito.doReturn(true).when(tvf).isLanceFormat();
        Assert.assertEquals(23L, tvf.getBackendIdForExecution());

        Mockito.doReturn(false).when(tvf).isLanceFormat();
        Assert.assertEquals(-1L, tvf.getBackendIdForExecution());
    }

    // Verifies S3 Lance metadata records which columns require the current BE reader.
    @Test
    public void testLanceMetadataTracksCurrentReaderColumns() throws Exception {
        ExternalFileTableValuedFunction tvf =
                Mockito.mock(ExternalFileTableValuedFunction.class, Mockito.CALLS_REAL_METHODS);
        Field jsonField = new Field(
                "json_value",
                new FieldType(true, ArrowType.Utf8.INSTANCE, null,
                        Collections.singletonMap("ARROW:extension:name", "arrow.json")),
                Collections.emptyList());
        LanceTableMetadata metadata = LanceTableMetadata.withoutIndexSegments(
                "s3://bucket/table.lance", 1L,
                new Schema(Arrays.asList(
                        jsonField, Field.nullable("ordinary", ArrowType.Utf8.INSTANCE))),
                Collections.emptyList(), Collections.emptyMap());

        tvf.setLanceTableMetadata(metadata);

        Assert.assertTrue(tvf.requiresCurrentLanceReader("JSON_VALUE"));
        Assert.assertFalse(tvf.requiresCurrentLanceReader("ordinary"));
    }

    // Sets a private long field without invoking the table function's environment-dependent constructor.
    private static void setLongField(Object target, String fieldName, long value) throws Exception {
        java.lang.reflect.Field field =
                LocalTableValuedFunction.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setLong(target, value);
    }
}
