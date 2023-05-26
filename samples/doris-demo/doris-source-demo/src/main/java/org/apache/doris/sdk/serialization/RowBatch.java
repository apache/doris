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

package org.apache.doris.sdk.serialization;

import org.apache.doris.sdk.model.Schema;
import org.apache.doris.sdk.thrift.TScanBatchResult;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.Preconditions;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * row batch data container.
 */
public class RowBatch {
    private static Logger logger = LoggerFactory.getLogger(RowBatch.class);
    private final List<Row> rowBatch = new ArrayList<>();
    private final ArrowStreamReader arrowStreamReader;
    private final RootAllocator rootAllocator;
    private final Schema schema;
    private final String DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";
    private final String DATETIMEV2_PATTERN = "yyyy-MM-dd HH:mm:ss.SSSSSS";
    private final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(DATETIME_PATTERN);
    private final DateTimeFormatter dateTimeV2Formatter = DateTimeFormatter.ofPattern(DATETIMEV2_PATTERN);
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    // offset for iterate the rowBatch
    private int offsetInRowBatch = 0;
    private int rowCountInOneBatch = 0;
    private int readRowCount = 0;
    private VectorSchemaRoot root;
    private List<FieldVector> fieldVectors;
    public RowBatch(TScanBatchResult nextResult, Schema schema) {
        this.schema = schema;
        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        this.arrowStreamReader = new ArrowStreamReader(
                new ByteArrayInputStream(nextResult.getRows()), rootAllocator);
        this.offsetInRowBatch = 0;
    }

    public RowBatch readArrow() {
        try {
            this.root = arrowStreamReader.getVectorSchemaRoot();
            while (arrowStreamReader.loadNextBatch()) {
                fieldVectors = root.getFieldVectors();
                if (fieldVectors.size() != schema.size()) {
                    logger.error("Schema size '{}' is not equal to arrow field size '{}'.",
                            fieldVectors.size(), schema.size());
                    throw new RuntimeException("Load Doris data failed, schema size of fetch data is wrong.");
                }
                if (fieldVectors.size() == 0 || root.getRowCount() == 0) {
                    logger.debug("One batch in arrow has no data.");
                    continue;
                }
                rowCountInOneBatch = root.getRowCount();
                // init the rowBatch
                for (int i = 0; i < rowCountInOneBatch; ++i) {
                    rowBatch.add(new Row(fieldVectors.size()));
                }
                convertArrowToRowBatch();
                readRowCount += root.getRowCount();
            }
            return this;
        } catch (Exception e) {
            logger.error("Read Doris Data failed because: ", e);
            throw new RuntimeException(e.getMessage());
        } finally {
            close();
        }
    }

    public boolean hasNext() {
        if (offsetInRowBatch < readRowCount) {
            return true;
        }
        return false;
    }

    private void addValueToRow(int rowIndex, Object obj) {
        if (rowIndex > rowCountInOneBatch) {
            String errMsg = "Get row offset: " + rowIndex + " larger than row size: " +
                    rowCountInOneBatch;
            logger.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        rowBatch.get(readRowCount + rowIndex).put(obj);
    }

    public void convertArrowToRowBatch() {
        try {
            for (int col = 0; col < fieldVectors.size(); col++) {
                FieldVector fieldVector = fieldVectors.get(col);
                Types.MinorType minorType = fieldVector.getMinorType();
                final String currentType = schema.get(col).getType();
                for (int rowIndex = 0; rowIndex < rowCountInOneBatch; rowIndex++) {
                    boolean passed = doConvert(col, rowIndex, minorType, currentType, fieldVector);
                    Preconditions.checkArgument(passed, typeMismatchMessage(currentType, minorType));
                }
            }
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    private boolean doConvert(int col,
            int rowIndex,
            Types.MinorType minorType,
            String currentType,
            FieldVector fieldVector) {
        switch (currentType) {
            case "NULL_TYPE":
                break;
            case "BOOLEAN":
                if (!minorType.equals(Types.MinorType.BIT)) {
                    return false;
                }
                BitVector bitVector = (BitVector) fieldVector;
                Object fieldValue = bitVector.isNull(rowIndex) ? null : bitVector.get(rowIndex) != 0;
                addValueToRow(rowIndex, fieldValue);
                break;
            case "TINYINT":
                if (!minorType.equals(Types.MinorType.TINYINT)) {
                    return false;
                }
                TinyIntVector tinyIntVector = (TinyIntVector) fieldVector;
                fieldValue = tinyIntVector.isNull(rowIndex) ? null : tinyIntVector.get(rowIndex);
                addValueToRow(rowIndex, fieldValue);
                break;
            case "SMALLINT":
                if (!minorType.equals(Types.MinorType.SMALLINT)) {
                    return false;
                }
                SmallIntVector smallIntVector = (SmallIntVector) fieldVector;
                fieldValue = smallIntVector.isNull(rowIndex) ? null : smallIntVector.get(rowIndex);
                addValueToRow(rowIndex, fieldValue);
                break;
            case "INT":
                if (!minorType.equals(Types.MinorType.INT)) {
                    return false;
                }
                IntVector intVector = (IntVector) fieldVector;
                fieldValue = intVector.isNull(rowIndex) ? null : intVector.get(rowIndex);
                addValueToRow(rowIndex, fieldValue);
                break;
            case "BIGINT":
                if (!minorType.equals(Types.MinorType.BIGINT)) {
                    return false;
                }
                BigIntVector bigIntVector = (BigIntVector) fieldVector;
                fieldValue = bigIntVector.isNull(rowIndex) ? null : bigIntVector.get(rowIndex);
                addValueToRow(rowIndex, fieldValue);
                break;
            case "FLOAT":
                if (!minorType.equals(Types.MinorType.FLOAT4)) {
                    return false;
                }
                Float4Vector float4Vector = (Float4Vector) fieldVector;
                fieldValue = float4Vector.isNull(rowIndex) ? null : float4Vector.get(rowIndex);
                addValueToRow(rowIndex, fieldValue);
                break;
            case "TIME":
            case "DOUBLE":
                if (!minorType.equals(Types.MinorType.FLOAT8)) {
                    return false;
                }
                Float8Vector float8Vector = (Float8Vector) fieldVector;
                fieldValue = float8Vector.isNull(rowIndex) ? null : float8Vector.get(rowIndex);
                addValueToRow(rowIndex, fieldValue);
                break;
            case "BINARY":
                if (!minorType.equals(Types.MinorType.VARBINARY)) {
                    return false;
                }

                VarBinaryVector varBinaryVector = (VarBinaryVector) fieldVector;
                fieldValue = varBinaryVector.isNull(rowIndex) ? null : varBinaryVector.get(rowIndex);
                addValueToRow(rowIndex, fieldValue);
                break;
            case "DECIMAL":
            case "DECIMALV2":
            case "DECIMAL32":
            case "DECIMAL64":
            case "DECIMAL128I":
                if (!minorType.equals(Types.MinorType.DECIMAL)) {
                    return false;
                }
                DecimalVector decimalVector = (DecimalVector) fieldVector;
                if (decimalVector.isNull(rowIndex)) {
                    addValueToRow(rowIndex, null);
                    break;
                }
                BigDecimal value = decimalVector.getObject(rowIndex).stripTrailingZeros();
                addValueToRow(rowIndex, value);
                break;
            case "DATE":
            case "DATEV2":
                if (!minorType.equals(Types.MinorType.VARCHAR)) {
                    return false;
                }
                VarCharVector date = (VarCharVector) fieldVector;
                if (date.isNull(rowIndex)) {
                    addValueToRow(rowIndex, null);
                    break;
                }
                String stringValue = new String(date.get(rowIndex));
                LocalDate localDate = LocalDate.parse(stringValue, dateFormatter);
                addValueToRow(rowIndex, localDate);
                break;
            case "DATETIME":
                if (!minorType.equals(Types.MinorType.VARCHAR)) {
                    return false;
                }
                VarCharVector timeStampSecVector = (VarCharVector) fieldVector;
                if (timeStampSecVector.isNull(rowIndex)) {
                    addValueToRow(rowIndex, null);
                    break;
                }
                stringValue = new String(timeStampSecVector.get(rowIndex));
                LocalDateTime parse = LocalDateTime.parse(stringValue, dateTimeFormatter);
                addValueToRow(rowIndex, parse);
                break;
            case "DATETIMEV2":
                if (!minorType.equals(Types.MinorType.VARCHAR)) {
                    return false;
                }
                VarCharVector timeStampV2SecVector = (VarCharVector) fieldVector;
                if (timeStampV2SecVector.isNull(rowIndex)) {
                    addValueToRow(rowIndex, null);
                    break;
                }
                stringValue = new String(timeStampV2SecVector.get(rowIndex));
                stringValue = completeMilliseconds(stringValue);
                parse = LocalDateTime.parse(stringValue, dateTimeV2Formatter);
                addValueToRow(rowIndex, parse);
                break;
            case "LARGEINT":
            case "CHAR":
            case "VARCHAR":
            case "STRING":
            case "JSONB":
                if (!minorType.equals(Types.MinorType.VARCHAR)) {
                    return false;
                }
                VarCharVector varCharVector = (VarCharVector) fieldVector;
                if (varCharVector.isNull(rowIndex)) {
                    addValueToRow(rowIndex, null);
                    break;
                }
                stringValue = new String(varCharVector.get(rowIndex));
                addValueToRow(rowIndex, stringValue);
                break;
            case "ARRAY":
                if (!minorType.equals(Types.MinorType.LIST)) {
                    return false;
                }
                ListVector listVector = (ListVector) fieldVector;
                Object listValue = listVector.isNull(rowIndex) ? null : listVector.getObject(rowIndex);
                // todo: when the subtype of array is date, conversion is required
                addValueToRow(rowIndex, listValue);
                break;
            default:
                String errMsg = "Unsupported type " + schema.get(col).getType();
                logger.error(errMsg);
                throw new RuntimeException(errMsg);
        }
        return true;
    }

    private String completeMilliseconds(String stringValue) {
        if (stringValue.length() == DATETIMEV2_PATTERN.length()) {
            return stringValue;
        }
        StringBuilder sb = new StringBuilder(stringValue);
        if (stringValue.length() == DATETIME_PATTERN.length()) {
            sb.append(".");
        }
        while (sb.toString().length() < DATETIMEV2_PATTERN.length()) {
            sb.append(0);
        }
        return sb.toString();
    }

    public List<Object> next() {
        if (!hasNext()) {
            String errMsg = "Get row offset:" + offsetInRowBatch + " larger than row size: " + readRowCount;
            logger.error(errMsg);
            throw new NoSuchElementException(errMsg);
        }
        return rowBatch.get(offsetInRowBatch++).getCols();
    }

    private String typeMismatchMessage(final String flinkType, final Types.MinorType arrowType) {
        final String messageTemplate = "FLINK type is %1$s, but arrow type is %2$s.";
        return String.format(messageTemplate, flinkType, arrowType.name());
    }

    public int getReadRowCount() {
        return readRowCount;
    }

    public void close() {
        try {
            if (arrowStreamReader != null) {
                arrowStreamReader.close();
            }
            if (rootAllocator != null) {
                rootAllocator.close();
            }
        } catch (IOException ioe) {
            // do nothing
        }
    }

    public static class Row {
        private final List<Object> cols;

        Row(int colCount) {
            this.cols = new ArrayList<>(colCount);
        }

        public List<Object> getCols() {
            return cols;
        }

        public void put(Object o) {
            cols.add(o);
        }
    }
}
