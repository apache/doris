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

package org.apache.doris.nereids.load;

import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.utframe.TestWithFeService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for NereidsLoadScanProvider.inferComplexTypeRenameMapping()
 */
public class NereidsLoadScanProviderTest extends TestWithFeService {

    @Override
    protected void runBeforeAll() throws Exception {
        createDatabase("test_load_scan");
        useDatabase("test_load_scan");

        // Create a table with complex types for testing
        String createTableSql = "CREATE TABLE test_complex_table (\n"
                + "  c_int INT,\n"
                + "  c_array ARRAY<INT>,\n"
                + "  c_map MAP<STRING, INT>,\n"
                + "  c_struct STRUCT<f1:INT, f2:STRING>\n"
                + ") DUPLICATE KEY(c_int)\n"
                + "DISTRIBUTED BY HASH(c_int) BUCKETS 1\n"
                + "PROPERTIES(\"replication_num\" = \"1\");";
        createTable(createTableSql);
    }

    /**
     * Test inferComplexTypeRenameMapping with Parquet format and direct column mapping
     */
    @Test
    public void testInferComplexTypeRenameMapping_Parquet_DirectMapping() throws Exception {
        OlapTable table = (OlapTable) connectContext.getCurrentCatalog()
                .getDbOrAnalysisException("test_load_scan")
                .getTableOrAnalysisException("test_complex_table");

        // Create column descriptors: direct mapping (c_int, c_array, c_map, c_struct)
        List<NereidsImportColumnDesc> columnExprs = new ArrayList<>();
        columnExprs.add(new NereidsImportColumnDesc("c_int", null));
        columnExprs.add(new NereidsImportColumnDesc("c_array", null));
        columnExprs.add(new NereidsImportColumnDesc("c_map", null));
        columnExprs.add(new NereidsImportColumnDesc("c_struct", null));

        Map<String, Type> result = invokeInferComplexTypeRenameMapping(
                TFileFormatType.FORMAT_PARQUET, table, columnExprs);

        // Direct mapping should return empty map (no rename inference needed)
        Assertions.assertTrue(result.isEmpty());
    }

    /**
     * Test inferComplexTypeRenameMapping with Parquet format and simple rename mapping
     */
    @Test
    public void testInferComplexTypeRenameMapping_Parquet_RenameMapping() throws Exception {
        OlapTable table = (OlapTable) connectContext.getCurrentCatalog()
                .getDbOrAnalysisException("test_load_scan")
                .getTableOrAnalysisException("test_complex_table");

        // Create column descriptors with rename: c_array = tmp_array
        List<NereidsImportColumnDesc> columnExprs = new ArrayList<>();
        columnExprs.add(new NereidsImportColumnDesc("c_int", null));
        columnExprs.add(new NereidsImportColumnDesc("tmp_array", null));  // file column
        columnExprs.add(new NereidsImportColumnDesc("c_array", new UnboundSlot("tmp_array")));  // target = source

        Map<String, Type> result = invokeInferComplexTypeRenameMapping(
                TFileFormatType.FORMAT_PARQUET, table, columnExprs);

        // Should infer tmp_array -> ARRAY<INT>
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.containsKey("tmp_array"));
        Assertions.assertTrue(result.get("tmp_array").isArrayType());
    }

    /**
     * Test inferComplexTypeRenameMapping with ORC format
     */
    @Test
    public void testInferComplexTypeRenameMapping_ORC_RenameMapping() throws Exception {
        OlapTable table = (OlapTable) connectContext.getCurrentCatalog()
                .getDbOrAnalysisException("test_load_scan")
                .getTableOrAnalysisException("test_complex_table");

        // Create column descriptors with rename for multiple complex columns
        List<NereidsImportColumnDesc> columnExprs = new ArrayList<>();
        columnExprs.add(new NereidsImportColumnDesc("c_int", null));
        columnExprs.add(new NereidsImportColumnDesc("src_array", null));
        columnExprs.add(new NereidsImportColumnDesc("src_map", null));
        columnExprs.add(new NereidsImportColumnDesc("c_array", new UnboundSlot("src_array")));
        columnExprs.add(new NereidsImportColumnDesc("c_map", new UnboundSlot("src_map")));

        Map<String, Type> result = invokeInferComplexTypeRenameMapping(
                TFileFormatType.FORMAT_ORC, table, columnExprs);

        // Should infer both source columns
        Assertions.assertEquals(2, result.size());
        Assertions.assertTrue(result.containsKey("src_array"));
        Assertions.assertTrue(result.get("src_array").isArrayType());
        Assertions.assertTrue(result.containsKey("src_map"));
        Assertions.assertTrue(result.get("src_map").isMapType());
    }

    /**
     * Test inferComplexTypeRenameMapping with CSV format (should return empty)
     */
    @Test
    public void testInferComplexTypeRenameMapping_CSV_NotSupported() throws Exception {
        OlapTable table = (OlapTable) connectContext.getCurrentCatalog()
                .getDbOrAnalysisException("test_load_scan")
                .getTableOrAnalysisException("test_complex_table");

        List<NereidsImportColumnDesc> columnExprs = new ArrayList<>();
        columnExprs.add(new NereidsImportColumnDesc("c_int", null));
        columnExprs.add(new NereidsImportColumnDesc("tmp_array", null));
        columnExprs.add(new NereidsImportColumnDesc("c_array", new UnboundSlot("tmp_array")));

        Map<String, Type> result = invokeInferComplexTypeRenameMapping(
                TFileFormatType.FORMAT_CSV_PLAIN, table, columnExprs);

        // CSV doesn't support complex types natively, should return empty
        Assertions.assertTrue(result.isEmpty());
    }

    /**
     * Test inferComplexTypeRenameMapping with JSON format (should return empty)
     */
    @Test
    public void testInferComplexTypeRenameMapping_JSON_NotSupported() throws Exception {
        OlapTable table = (OlapTable) connectContext.getCurrentCatalog()
                .getDbOrAnalysisException("test_load_scan")
                .getTableOrAnalysisException("test_complex_table");

        List<NereidsImportColumnDesc> columnExprs = new ArrayList<>();
        columnExprs.add(new NereidsImportColumnDesc("c_int", null));
        columnExprs.add(new NereidsImportColumnDesc("tmp_array", null));
        columnExprs.add(new NereidsImportColumnDesc("c_array", new UnboundSlot("tmp_array")));

        Map<String, Type> result = invokeInferComplexTypeRenameMapping(
                TFileFormatType.FORMAT_JSON, table, columnExprs);

        // JSON doesn't support complex types natively, should return empty
        Assertions.assertTrue(result.isEmpty());
    }

    /**
     * Test error case: complex type with non-direct expression mapping
     */
    @Test
    public void testInferComplexTypeRenameMapping_NonDirectMapping_Error() throws Exception {
        OlapTable table = (OlapTable) connectContext.getCurrentCatalog()
                .getDbOrAnalysisException("test_load_scan")
                .getTableOrAnalysisException("test_complex_table");

        // Use expression instead of simple rename: c_array = tmp_array + 1
        List<NereidsImportColumnDesc> columnExprs = new ArrayList<>();
        columnExprs.add(new NereidsImportColumnDesc("c_int", null));
        columnExprs.add(new NereidsImportColumnDesc("tmp_array", null));
        columnExprs.add(new NereidsImportColumnDesc("c_array",
                new Add(new UnboundSlot("tmp_array"), new IntegerLiteral(1))));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            invokeInferComplexTypeRenameMapping(TFileFormatType.FORMAT_PARQUET, table, columnExprs);
        });

        Assertions.assertTrue(exception.getMessage().contains("Complex type load only supports direct column mapping"));
    }

    /**
     * Test error case: complex type mapping to non-existent source column
     */
    @Test
    public void testInferComplexTypeRenameMapping_NonExistentSource_Error() throws Exception {
        OlapTable table = (OlapTable) connectContext.getCurrentCatalog()
                .getDbOrAnalysisException("test_load_scan")
                .getTableOrAnalysisException("test_complex_table");

        // Map to source column that doesn't exist in file columns
        List<NereidsImportColumnDesc> columnExprs = new ArrayList<>();
        columnExprs.add(new NereidsImportColumnDesc("c_int", null));
        columnExprs.add(new NereidsImportColumnDesc("c_array", new UnboundSlot("non_existent_column")));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            invokeInferComplexTypeRenameMapping(TFileFormatType.FORMAT_PARQUET, table, columnExprs);
        });

        Assertions.assertTrue(exception.getMessage().contains("Complex type load only supports direct column mapping"));
    }

    /**
     * Test error case: multiple complex columns with different types map to same source
     */
    @Test
    public void testInferComplexTypeRenameMapping_TypeConflict_Error() throws Exception {
        OlapTable table = (OlapTable) connectContext.getCurrentCatalog()
                .getDbOrAnalysisException("test_load_scan")
                .getTableOrAnalysisException("test_complex_table");

        // Both c_array and c_map try to map from the same source "src_col"
        // This would create a type conflict
        List<NereidsImportColumnDesc> columnExprs = new ArrayList<>();
        columnExprs.add(new NereidsImportColumnDesc("c_int", null));
        columnExprs.add(new NereidsImportColumnDesc("src_col", null));
        columnExprs.add(new NereidsImportColumnDesc("c_array", new UnboundSlot("src_col")));
        columnExprs.add(new NereidsImportColumnDesc("c_map", new UnboundSlot("src_col")));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () -> {
            invokeInferComplexTypeRenameMapping(TFileFormatType.FORMAT_PARQUET, table, columnExprs);
        });

        Assertions.assertTrue(exception.getMessage().contains("Multiple complex columns with different types"));
    }

    /**
     * Test with scalar column rename (should be ignored, only complex types are processed)
     */
    @Test
    public void testInferComplexTypeRenameMapping_ScalarRename_Ignored() throws Exception {
        OlapTable table = (OlapTable) connectContext.getCurrentCatalog()
                .getDbOrAnalysisException("test_load_scan")
                .getTableOrAnalysisException("test_complex_table");

        // Rename scalar column (should not appear in result)
        List<NereidsImportColumnDesc> columnExprs = new ArrayList<>();
        columnExprs.add(new NereidsImportColumnDesc("tmp_int", null));
        columnExprs.add(new NereidsImportColumnDesc("c_int", new UnboundSlot("tmp_int")));  // scalar rename
        columnExprs.add(new NereidsImportColumnDesc("tmp_array", null));
        columnExprs.add(new NereidsImportColumnDesc("c_array", new UnboundSlot("tmp_array")));  // complex rename

        Map<String, Type> result = invokeInferComplexTypeRenameMapping(
                TFileFormatType.FORMAT_PARQUET, table, columnExprs);

        // Only complex type rename should be in the result
        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.containsKey("tmp_array"));
        Assertions.assertFalse(result.containsKey("tmp_int"));  // scalar should be ignored
    }

    /**
     * Test with empty column list
     */
    @Test
    public void testInferComplexTypeRenameMapping_EmptyColumns() throws Exception {
        OlapTable table = (OlapTable) connectContext.getCurrentCatalog()
                .getDbOrAnalysisException("test_load_scan")
                .getTableOrAnalysisException("test_complex_table");

        List<NereidsImportColumnDesc> columnExprs = new ArrayList<>();

        Map<String, Type> result = invokeInferComplexTypeRenameMapping(
                TFileFormatType.FORMAT_PARQUET, table, columnExprs);

        Assertions.assertTrue(result.isEmpty());
    }

    /**
     * Helper method to invoke private inferComplexTypeRenameMapping method via reflection
     */
    private Map<String, Type> invokeInferComplexTypeRenameMapping(
            TFileFormatType fileFormatType,
            OlapTable table,
            List<NereidsImportColumnDesc> columnExprs) throws Exception {

        NereidsLoadScanProvider provider = new NereidsLoadScanProvider(null, null);

        Method method = NereidsLoadScanProvider.class.getDeclaredMethod(
                "inferComplexTypeRenameMapping",
                TFileFormatType.class,
                org.apache.doris.catalog.Table.class,
                List.class);
        method.setAccessible(true);

        try {
            return (Map<String, Type>) method.invoke(provider, fileFormatType, table, columnExprs);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof AnalysisException) {
                throw (AnalysisException) e.getCause();
            }
            throw e;
        }
    }
}
