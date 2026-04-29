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

package org.apache.doris.hive;

import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.VectorColumn;
import org.apache.doris.common.jni.vec.VectorTable;
import org.apache.doris.thrift.TFileFormatType;

import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Unit tests for HiveJNIScanner.
 * Tests include:
 * - RCFile column projection fix (column ID parsing, full schema handling)
 * - count(*) query handling (empty columns, getNext optimization)
 * - Edge cases and error handling
 *
 */
public class HiveJNIScannerTest {

    /**
     * Test that column_ids parameter is correctly parsed from BE.
     */
    @Test
    public void testColumnIdsParsingFromBE() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(HiveProperties.FILE_TYPE, "0");
        params.put(HiveProperties.FILE_FORMAT, "17");
        params.put(HiveProperties.REQUIRED_FIELDS, "add_price,delivery_id,add_status");
        params.put(HiveProperties.COLUMNS_TYPES, "float#string#boolean");
        params.put(HiveProperties.COLUMN_IDS, "2,1,3");
        params.put(HiveProperties.URI, "hdfs://test/path");
        params.put(HiveProperties.SPLIT_START_OFFSET, "0");
        params.put(HiveProperties.SPLIT_SIZE, "1024");

        HiveJNIScanner scanner = new HiveJNIScanner(100, params);

        int[] columnIds = getRequiredColumnIds(scanner);
        Assertions.assertNotNull(columnIds);
        Assertions.assertEquals(3, columnIds.length);
        Assertions.assertEquals(2, columnIds[0]);
        Assertions.assertEquals(1, columnIds[1]);
        Assertions.assertEquals(3, columnIds[2]);
    }

    /**
     * Test fallback behavior when column_ids parameter is not provided.
     */
    @Test
    public void testColumnIdsFallbackWhenNotProvided() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(HiveProperties.FILE_TYPE, "0");
        params.put(HiveProperties.FILE_FORMAT, "17");
        params.put(HiveProperties.REQUIRED_FIELDS, "col0,col1,col2");
        params.put(HiveProperties.COLUMNS_TYPES, "int#string#float");
        params.put(HiveProperties.URI, "hdfs://test/path");
        params.put(HiveProperties.SPLIT_START_OFFSET, "0");
        params.put(HiveProperties.SPLIT_SIZE, "1024");

        HiveJNIScanner scanner = new HiveJNIScanner(100, params);

        int[] columnIds = getRequiredColumnIds(scanner);
        Assertions.assertNotNull(columnIds);
        Assertions.assertEquals(3, columnIds.length);
        Assertions.assertEquals(0, columnIds[0]);
        Assertions.assertEquals(1, columnIds[1]);
        Assertions.assertEquals(2, columnIds[2]);
    }

    /**
     * Test createProperties() with full schema.
     */
    @Test
    public void testCreatePropertiesWithFullSchema() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(HiveProperties.FILE_TYPE, "0");
        params.put(HiveProperties.FILE_FORMAT, "17");
        params.put(HiveProperties.REQUIRED_FIELDS, "add_price,delivery_id");
        params.put(HiveProperties.COLUMNS_TYPES, "float#string");
        params.put(HiveProperties.COLUMN_IDS, "2,1");
        params.put(HiveProperties.FULL_SCHEMA_NAMES, "logging_time,delivery_id,add_price,add_status,dt");
        params.put(HiveProperties.FULL_SCHEMA_TYPES, "string,string,float,string,string");
        params.put(HiveProperties.URI, "hdfs://test/path");
        params.put(HiveProperties.SPLIT_START_OFFSET, "0");
        params.put(HiveProperties.SPLIT_SIZE, "1024");

        HiveJNIScanner scanner = new HiveJNIScanner(100, params);
        Properties properties = invokeCreateProperties(scanner);

        Assertions.assertEquals("logging_time,delivery_id,add_price,add_status,dt",
                properties.getProperty(HiveProperties.COLUMNS));
        Assertions.assertEquals("string,string,float,string,string",
                properties.getProperty(HiveProperties.COLUMNS2TYPES));
        Assertions.assertEquals("2,1",
                properties.getProperty(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
    }

    /**
     * Test createProperties() without full schema.
     */
    @Test
    public void testCreatePropertiesWithoutFullSchema() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(HiveProperties.FILE_TYPE, "0");
        params.put(HiveProperties.FILE_FORMAT, "17");
        params.put(HiveProperties.REQUIRED_FIELDS, "add_price,delivery_id");
        params.put(HiveProperties.COLUMNS_TYPES, "float#string");
        params.put(HiveProperties.COLUMN_IDS, "2,1");
        params.put(HiveProperties.URI, "hdfs://test/path");
        params.put(HiveProperties.SPLIT_START_OFFSET, "0");
        params.put(HiveProperties.SPLIT_SIZE, "1024");

        HiveJNIScanner scanner = new HiveJNIScanner(100, params);
        Properties properties = invokeCreateProperties(scanner);

        Assertions.assertEquals("add_price,delivery_id", properties.getProperty(HiveProperties.COLUMNS));
        Assertions.assertEquals("float,string", properties.getProperty(HiveProperties.COLUMNS2TYPES));
    }

    /**
     * Test single column projection.
     */
    @Test
    public void testSingleColumnProjection() throws Exception {
        Map<String, String> params = new HashMap<>();
        params.put(HiveProperties.FILE_TYPE, "0");
        params.put(HiveProperties.FILE_FORMAT, "17");
        params.put(HiveProperties.REQUIRED_FIELDS, "add_status");
        params.put(HiveProperties.COLUMNS_TYPES, "boolean");
        params.put(HiveProperties.COLUMN_IDS, "3");
        params.put(HiveProperties.FULL_SCHEMA_NAMES, "col0,col1,col2,add_status,col4");
        params.put(HiveProperties.FULL_SCHEMA_TYPES, "int,string,float,boolean,date");
        params.put(HiveProperties.URI, "hdfs://test/path");
        params.put(HiveProperties.SPLIT_START_OFFSET, "0");
        params.put(HiveProperties.SPLIT_SIZE, "1024");

        HiveJNIScanner scanner = new HiveJNIScanner(100, params);
        int[] columnIds = getRequiredColumnIds(scanner);
        Properties properties = invokeCreateProperties(scanner);

        Assertions.assertEquals(1, columnIds.length);
        Assertions.assertEquals(3, columnIds[0]);
        Assertions.assertEquals("3", properties.getProperty(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
    }

    /**
     * Test that empty columnTypes string results in empty array (line 98).
     */
    @Test
    public void testEmptyColumnTypesString() throws Exception {
        Map<String, String> params = createBaseParams();
        params.put(HiveProperties.COLUMNS_TYPES, "");
        params.put(HiveProperties.REQUIRED_FIELDS, "");

        HiveJNIScanner scanner = new HiveJNIScanner(100, params);

        String[] columnTypes = getFieldValue(scanner, "columnTypes", String[].class);
        Assertions.assertNotNull(columnTypes);
        Assertions.assertEquals(0, columnTypes.length, "Empty columnTypes string should result in empty array");
    }

    /**
     * Test that null columnTypes string results in empty array.
     */
    @Test
    public void testNullColumnTypesString() throws Exception {
        Map<String, String> params = createBaseParams();
        params.remove(HiveProperties.COLUMNS_TYPES);
        params.put(HiveProperties.REQUIRED_FIELDS, "");

        HiveJNIScanner scanner = new HiveJNIScanner(100, params);

        String[] columnTypes = getFieldValue(scanner, "columnTypes", String[].class);
        Assertions.assertNotNull(columnTypes);
        Assertions.assertEquals(0, columnTypes.length);
    }

    /**
     * Test that empty requiredFields string results in empty array.
     */
    @Test
    public void testEmptyRequiredFieldsString() throws Exception {
        Map<String, String> params = createBaseParams();
        params.put(HiveProperties.COLUMNS_TYPES, "");
        params.put(HiveProperties.REQUIRED_FIELDS, "");

        HiveJNIScanner scanner = new HiveJNIScanner(100, params);

        String[] requiredFields = getFieldValue(scanner, "requiredFields", String[].class);
        Assertions.assertNotNull(requiredFields);
        Assertions.assertEquals(0, requiredFields.length);
    }

    /**
     * Test that null requiredFields string results in empty array.
     */
    @Test
    public void testNullRequiredFieldsString() throws Exception {
        Map<String, String> params = createBaseParams();
        params.put(HiveProperties.COLUMNS_TYPES, "");
        params.remove(HiveProperties.REQUIRED_FIELDS);

        HiveJNIScanner scanner = new HiveJNIScanner(100, params);

        String[] requiredFields = getFieldValue(scanner, "requiredFields", String[].class);
        Assertions.assertNotNull(requiredFields);
        Assertions.assertEquals(0, requiredFields.length);
    }

    /**
     * Test that empty column_id throws IllegalArgumentException.
     */
    @Test
    public void testEmptyColumnIdThrowsException() {
        Map<String, String> params = createBaseParams();
        params.put(HiveProperties.REQUIRED_FIELDS, "col1,col2,col3");
        params.put(HiveProperties.COLUMNS_TYPES, "int#string#float");
        params.put(HiveProperties.COLUMN_IDS, "1,,3");

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new HiveJNIScanner(100, params)
        );

        Assertions.assertTrue(exception.getMessage().contains("Empty column_id at index"));
    }

    /**
     * Test that empty column_id at beginning throws exception.
     */
    @Test
    public void testEmptyColumnIdAtBeginningThrowsException() {
        Map<String, String> params = createBaseParams();
        params.put(HiveProperties.REQUIRED_FIELDS, "col1,col2");
        params.put(HiveProperties.COLUMNS_TYPES, "int#string");
        params.put(HiveProperties.COLUMN_IDS, ",2");

        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new HiveJNIScanner(100, params)
        );

        Assertions.assertTrue(exception.getMessage().contains("Empty column_id at index 0"));
    }

    /**
     * Test count(*) query setup.
     */
    @Test
    public void testCountStarQuerySetup() throws Exception {
        Map<String, String> params = createBaseParams();
        params.put(HiveProperties.COLUMNS_TYPES, "");
        params.put(HiveProperties.REQUIRED_FIELDS, "");

        HiveJNIScanner scanner = new HiveJNIScanner(100, params);

        String[] columnTypes = getFieldValue(scanner, "columnTypes", String[].class);
        String[] requiredFields = getFieldValue(scanner, "requiredFields", String[].class);
        int[] requiredColumnIds = getFieldValue(scanner, "requiredColumnIds", int[].class);
        ColumnType[] requiredTypes = getFieldValue(scanner, "requiredTypes", ColumnType[].class);

        Assertions.assertEquals(0, columnTypes.length);
        Assertions.assertEquals(0, requiredFields.length);
        Assertions.assertEquals(0, requiredColumnIds.length);
        Assertions.assertEquals(0, requiredTypes.length);
    }

    /**
     * Test getNext() for count(*) counts rows without deserializing.
     * Directly tests the actual HiveJNIScanner.getNext() method.
     */
    @Test
    public void testGetNextCountStarCountsRows() throws Exception {
        HiveJNIScanner scanner = createCountStarScanner();
        setupMockReader(scanner, 10);

        int result = scanner.getNext();

        Assertions.assertEquals(10, result, "Should count all available rows");
    }

    /**
     * Test getNext() for count(*) respects batch size limit.
     */
    @Test
    public void testGetNextCountStarRespectsBatchSize() throws Exception {
        HiveJNIScanner scanner = createCountStarScannerWithBatchSize(5);
        setupMockReader(scanner, 100);

        int result = scanner.getNext();

        Assertions.assertEquals(5, result, "Should respect batch size limit");
    }

    /**
     * Test getNext() for count(*) with zero rows.
     */
    @Test
    public void testGetNextCountStarNoRows() throws Exception {
        HiveJNIScanner scanner = createCountStarScanner();
        setupMockReader(scanner, 0);

        int result = scanner.getNext();

        Assertions.assertEquals(0, result, "Should return 0 when no rows available");
    }

    /**
     * Test getNext() for count(*) breaks when reader returns false.
     */
    @Test
    public void testGetNextCountStarBreaksOnReaderEnd() throws Exception {
        HiveJNIScanner scanner = createCountStarScanner();
        setupMockReader(scanner, 7);

        int result = scanner.getNext();

        Assertions.assertEquals(7, result, "Should stop when reader has no more rows");
    }

    /**
     * Test single column type parsing.
     */
    @Test
    public void testSingleColumnTypeParsing() throws Exception {
        Map<String, String> params = createBaseParams();
        params.put(HiveProperties.COLUMNS_TYPES, "int");
        params.put(HiveProperties.REQUIRED_FIELDS, "col1");
        params.put(HiveProperties.COLUMN_IDS, "0");

        HiveJNIScanner scanner = new HiveJNIScanner(100, params);

        String[] columnTypes = getFieldValue(scanner, "columnTypes", String[].class);
        String[] requiredFields = getFieldValue(scanner, "requiredFields", String[].class);

        Assertions.assertEquals(1, columnTypes.length);
        Assertions.assertEquals("int", columnTypes[0]);
        Assertions.assertEquals(1, requiredFields.length);
        Assertions.assertEquals("col1", requiredFields[0]);
    }

    /**
     * Test getNextBatchMeta() for count(*) with rows returned.
     * Mocks VectorTable and VectorColumn static factory methods.
     */
    @Test
    public void testGetNextBatchMetaCountStarWithRows() throws Exception {
        try (MockedStatic<VectorTable> vectorTableMock = Mockito.mockStatic(VectorTable.class);
                MockedStatic<VectorColumn> vectorColumnMock = Mockito.mockStatic(VectorColumn.class)) {

            // Create mock objects
            VectorTable mockTable = Mockito.mock(VectorTable.class);
            VectorColumn mockColumn = Mockito.mock(VectorColumn.class);

            // Configure static method mocks
            vectorTableMock.when(() -> VectorTable.createWritableTable(
                    Mockito.any(), Mockito.any(), Mockito.anyInt()))
                    .thenReturn(mockTable);
            vectorColumnMock.when(() -> VectorColumn.createWritableColumn(
                    Mockito.any(ColumnType.class), Mockito.anyInt()))
                    .thenReturn(mockColumn);

            Mockito.when(mockColumn.dataAddress()).thenReturn(12345L);

            HiveJNIScanner scanner = createCountStarScanner();
            setupMockReader(scanner, 10);

            setFieldValue(scanner, "types", new ColumnType[0]);
            setFieldValue(scanner, "fields", new String[0]);

            long result = scanner.getNextBatchMeta();

            Assertions.assertEquals(12345L, result);

            // Verify mock interactions
            Mockito.verify(mockColumn).reset();
            Mockito.verify(mockColumn).appendLong(10);
            Mockito.verify(mockColumn).dataAddress();
        }
    }

    /**
     * Test getNextBatchMeta() for count(*) with zero rows.
     */
    @Test
    public void testGetNextBatchMetaCountStarZeroRows() throws Exception {
        try (MockedStatic<VectorTable> vectorTableMock = Mockito.mockStatic(VectorTable.class);
                MockedStatic<VectorColumn> vectorColumnMock = Mockito.mockStatic(VectorColumn.class)) {

            // Create mock objects
            VectorTable mockTable = Mockito.mock(VectorTable.class);
            VectorColumn mockColumn = Mockito.mock(VectorColumn.class);

            // Configure static method mocks
            vectorTableMock.when(() -> VectorTable.createWritableTable(
                    Mockito.any(), Mockito.any(), Mockito.anyInt()))
                    .thenReturn(mockTable);
            vectorColumnMock.when(() -> VectorColumn.createWritableColumn(
                    Mockito.any(ColumnType.class), Mockito.anyInt()))
                    .thenReturn(mockColumn);

            // Create count(*) scanner with zero rows
            HiveJNIScanner scanner = createCountStarScanner();
            setupMockReader(scanner, 0);

            // Set types field to empty array to trigger count(*) path
            setFieldValue(scanner, "types", new ColumnType[0]);
            setFieldValue(scanner, "fields", new String[0]);

            // Call getNextBatchMeta
            long result = scanner.getNextBatchMeta();

            // Verify returns 0 when no rows
            Assertions.assertEquals(0L, result);
        }
    }

    /**
     * Test getNextBatchMeta() for count(*) with IOException from getNext.
     */
    @Test
    public void testGetNextBatchMetaCountStarWithIOException() throws Exception {
        try (MockedStatic<VectorTable> vectorTableMock = Mockito.mockStatic(VectorTable.class);
                MockedStatic<VectorColumn> vectorColumnMock = Mockito.mockStatic(VectorColumn.class)) {

            // Create mock objects
            VectorTable mockTable = Mockito.mock(VectorTable.class);
            VectorColumn mockColumn = Mockito.mock(VectorColumn.class);

            // Configure static method mocks
            vectorTableMock.when(() -> VectorTable.createWritableTable(
                    Mockito.any(), Mockito.any(), Mockito.anyInt()))
                    .thenReturn(mockTable);
            vectorColumnMock.when(() -> VectorColumn.createWritableColumn(
                    Mockito.any(ColumnType.class), Mockito.anyInt()))
                    .thenReturn(mockColumn);

            // Create count(*) scanner with error-throwing reader
            HiveJNIScanner scanner = createCountStarScanner();
            setupMockReaderWithException(scanner);

            // Set types field to empty array to trigger count(*) path
            setFieldValue(scanner, "types", new ColumnType[0]);
            setFieldValue(scanner, "fields", new String[0]);

            // Call getNextBatchMeta and expect IOException
            Assertions.assertThrows(IOException.class, scanner::getNextBatchMeta);
        }
    }

    /**
     * Test getNextBatchMeta() reuses existing vectorTable on subsequent calls.
     */
    @Test
    public void testGetNextBatchMetaReusesVectorTable() throws Exception {
        try (MockedStatic<VectorTable> vectorTableMock = Mockito.mockStatic(VectorTable.class);
                MockedStatic<VectorColumn> vectorColumnMock = Mockito.mockStatic(VectorColumn.class)) {

            // Create mock objects
            VectorTable mockTable = Mockito.mock(VectorTable.class);
            VectorColumn mockColumn = Mockito.mock(VectorColumn.class);

            // Configure static method mocks
            vectorTableMock.when(() -> VectorTable.createWritableTable(
                    Mockito.any(), Mockito.any(), Mockito.anyInt()))
                    .thenReturn(mockTable);
            vectorColumnMock.when(() -> VectorColumn.createWritableColumn(
                    Mockito.any(ColumnType.class), Mockito.anyInt()))
                    .thenReturn(mockColumn);

            // Configure mock column behavior
            Mockito.when(mockColumn.dataAddress()).thenReturn(12345L);

            // Create count(*) scanner
            HiveJNIScanner scanner = createCountStarScanner();

            // Set types field to empty array to trigger count(*) path
            setFieldValue(scanner, "types", new ColumnType[0]);
            setFieldValue(scanner, "fields", new String[0]);

            // Pre-set the vectorTable to simulate it already exists
            setFieldValue(scanner, "vectorTable", mockTable);
            setFieldValue(scanner, "countMeta", mockColumn);

            // Setup mock reader
            setupMockReader(scanner, 5);

            // Call getNextBatchMeta
            long result = scanner.getNextBatchMeta();

            // Verify VectorTable.createWritableTable was NOT called (reused existing)
            vectorTableMock.verify(
                    () -> VectorTable.createWritableTable(Mockito.any(), Mockito.any(), Mockito.anyInt()),
                    Mockito.never());

            Assertions.assertEquals(12345L, result);
        }
    }


    /**
     * Test close() cleans up countMeta when it exists.
     */
    @Test
    public void testCloseWithCountMeta() throws Exception {
        // Create count(*) scanner
        HiveJNIScanner scanner = createCountStarScanner();

        // Create mock countMeta
        VectorColumn mockCountMeta = Mockito.mock(VectorColumn.class);
        setFieldValue(scanner, "countMeta", mockCountMeta);

        // Call close
        scanner.close();

        // Verify countMeta.close() was called
        Mockito.verify(mockCountMeta).close();

        // Verify countMeta was set to null
        VectorColumn countMetaAfterClose = getFieldValue(scanner, "countMeta", VectorColumn.class);
        Assertions.assertNull(countMetaAfterClose);
    }

    /**
     * Test close() with reader.
     */
    @Test
    public void testCloseWithReader() throws Exception {
        // Create scanner
        HiveJNIScanner scanner = createCountStarScanner();

        // Create mock reader
        MockRecordReader mockReader = new MockRecordReader(0);
        setFieldValue(scanner, "reader", mockReader);

        Assertions.assertDoesNotThrow(scanner::close);
    }

    private Map<String, String> createBaseParams() {
        Map<String, String> params = new HashMap<>();
        params.put(HiveProperties.FILE_TYPE, "0");
        params.put(HiveProperties.FILE_FORMAT, "17");
        params.put(HiveProperties.URI, "hdfs://test/path");
        params.put(HiveProperties.SPLIT_START_OFFSET, "0");
        params.put(HiveProperties.SPLIT_SIZE, "1024");
        return params;
    }

    private HiveJNIScanner createCountStarScanner() throws Exception {
        return createCountStarScannerWithBatchSize(100);
    }

    private HiveJNIScanner createCountStarScannerWithBatchSize(int batchSize) throws Exception {
        Map<String, String> params = createBaseParams();
        params.put(HiveProperties.COLUMNS_TYPES, "");
        params.put(HiveProperties.REQUIRED_FIELDS, "");

        HiveJNIScanner scanner = new HiveJNIScanner(batchSize, params);

        // Set batchSize field
        setFieldValue(scanner, "batchSize", batchSize);

        // Set classLoader to current thread's context class loader
        setFieldValue(scanner, "classLoader", Thread.currentThread().getContextClassLoader());

        return scanner;
    }

    /**
     * Set up a mock RecordReader for count(*) testing.
     * This directly manipulates the scanner's internal reader, key, and value fields.
     */
    private void setupMockReader(HiveJNIScanner scanner, int rowCount) throws Exception {
        // Create mock key and value
        Writable mockKey = NullWritable.get();
        Writable mockValue = NullWritable.get();

        // Create mock reader that returns the specified number of rows
        MockRecordReader mockReader = new MockRecordReader(rowCount);

        // Set fields via reflection
        setFieldValue(scanner, "reader", mockReader);
        setFieldValue(scanner, "key", mockKey);
        setFieldValue(scanner, "value", mockValue);
    }

    /**
     * Set up a mock RecordReader that throws IOException.
     */
    private void setupMockReaderWithException(HiveJNIScanner scanner) throws Exception {
        // Create mock key and value
        Writable mockKey = NullWritable.get();
        Writable mockValue = NullWritable.get();

        // Create mock reader that throws exception
        ExceptionThrowingRecordReader mockReader = new ExceptionThrowingRecordReader();

        // Set fields via reflection
        setFieldValue(scanner, "reader", mockReader);
        setFieldValue(scanner, "key", mockKey);
        setFieldValue(scanner, "value", mockValue);
    }

    private int[] getRequiredColumnIds(HiveJNIScanner scanner) throws Exception {
        Field field = scanner.getClass().getDeclaredField("requiredColumnIds");
        field.setAccessible(true);
        return (int[]) field.get(scanner);
    }

    private Properties invokeCreateProperties(HiveJNIScanner scanner) throws Exception {
        Field hiveFileContextField = scanner.getClass().getDeclaredField("hiveFileContext");
        hiveFileContextField.setAccessible(true);
        Field fileFormatField = scanner.getClass().getDeclaredField("fileFormat");
        fileFormatField.setAccessible(true);
        TFileFormatType fileFormat = (TFileFormatType) fileFormatField.get(scanner);
        HiveFileContext hiveFileContext = new HiveFileContext(fileFormat);
        hiveFileContextField.set(scanner, hiveFileContext);

        Method method = scanner.getClass().getDeclaredMethod("createProperties");
        method.setAccessible(true);
        return (Properties) method.invoke(scanner);
    }

    @SuppressWarnings("unchecked")
    private <T> T getFieldValue(Object obj, String fieldName, Class<T> type) throws Exception {
        Class<?> clazz = obj.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                return (T) field.get(obj);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException("Field " + fieldName + " not found");
    }

    private void setFieldValue(Object obj, String fieldName, Object value) throws Exception {
        Class<?> clazz = obj.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(obj, value);
                return;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException("Field " + fieldName + " not found");
    }

    /**
     * Simple mock RecordReader for testing count(*) without native dependencies.
     */
    private static class MockRecordReader implements RecordReader<Writable, Writable> {
        private final int totalRows;
        private int currentRow = 0;

        public MockRecordReader(int totalRows) {
            this.totalRows = totalRows;
        }

        @Override
        public boolean next(Writable key, Writable value) throws IOException {
            if (currentRow < totalRows) {
                currentRow++;
                return true;
            }
            return false;
        }

        @Override
        public Writable createKey() {
            return NullWritable.get();
        }

        @Override
        public Writable createValue() {
            return NullWritable.get();
        }

        @Override
        public long getPos() throws IOException {
            return currentRow;
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public float getProgress() throws IOException {
            return totalRows > 0 ? (float) currentRow / totalRows : 0;
        }
    }

    /**
     * Mock RecordReader that throws IOException for testing error handling.
     */
    private static class ExceptionThrowingRecordReader implements RecordReader<Writable, Writable> {

        @Override
        public boolean next(Writable key, Writable value) throws IOException {
            throw new IOException("Simulated read error");
        }

        @Override
        public Writable createKey() {
            return NullWritable.get();
        }

        @Override
        public Writable createValue() {
            return NullWritable.get();
        }

        @Override
        public long getPos() throws IOException {
            return 0;
        }

        @Override
        public void close() throws IOException {
        }

        @Override
        public float getProgress() throws IOException {
            return 0;
        }
    }
}
