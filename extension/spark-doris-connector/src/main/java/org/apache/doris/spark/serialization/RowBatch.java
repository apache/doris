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

package org.apache.doris.spark.serialization;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.arrow.memory.RootAllocator;
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
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.Types;
import org.apache.doris.spark.exception.DorisException;
import org.apache.doris.spark.rest.models.Schema;
import org.apache.doris.thrift.TScanBatchResult;
import org.apache.spark.sql.types.Decimal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * row batch data container.
 */
public class RowBatch {
    private static Logger logger = LoggerFactory.getLogger(RowBatch.class);

    public static class Row {
        private List<Object> cols;

        Row(int colCount) {
            this.cols = new ArrayList<>(colCount);
        }

        List<Object> getCols() {
            return cols;
        }

        public void put(Object o) {
            cols.add(o);
        }
    }

    private int offsetInOneBatch = 0;
    private int rowCountInOneBatch = 0;
    private int readRowCount = 0;
    private final ArrowStreamReader arrowStreamReader;
    private final VectorSchemaRoot root;
    private List<FieldVector> fieldVectors;
    private RootAllocator rootAllocator;
    private final Schema schema;

    public RowBatch(TScanBatchResult nextResult, Schema schema) throws DorisException {
        this.schema = schema;
        this.rootAllocator = new RootAllocator(Integer.MAX_VALUE);
        this.arrowStreamReader = new ArrowStreamReader(
                new ByteArrayInputStream(nextResult.getRows()),
                rootAllocator
                );
        try {
            this.root = arrowStreamReader.getVectorSchemaRoot();
        } catch (Exception e) {
            logger.error("Read Doris Data failed because: ", e);
            close();
            throw new DorisException(e.getMessage());
        }
    }

    public boolean hasNext() throws DorisException {
        if (offsetInOneBatch < rowCountInOneBatch) {
            return true;
        }
        try {
            try {
                while (arrowStreamReader.loadNextBatch()) {
                    fieldVectors = root.getFieldVectors();
                    readRowCount += root.getRowCount();
                    if (fieldVectors.size() != schema.size()) {
                        logger.error("Schema size '{}' is not equal to arrow field size '{}'.",
                                fieldVectors.size(), schema.size());
                        throw new DorisException("Load Doris data failed, schema size of fetch data is wrong.");
                    }
                    if (fieldVectors.size() == 0 || root.getRowCount() == 0) {
                        logger.debug("One batch in arrow has no data.");
                        continue;
                    }
                    offsetInOneBatch = 0;
                    rowCountInOneBatch = root.getRowCount();
                    return true;
                }
            } catch (IOException e) {
                logger.error("Load arrow next batch failed.", e);
                throw new DorisException("Cannot load arrow next batch fetching from Doris.");
            }
        } catch (Exception e) {
            close();
            throw e;
        }
        return false;
    }

    public List<Object> next() throws DorisException {
        try {
            if (!hasNext()) {
                String errMsg = "Get row offset:" + offsetInOneBatch + " larger than row size: " + rowCountInOneBatch;
                logger.error(errMsg);
                throw new NoSuchElementException(errMsg);
            }
            Row row = new Row(fieldVectors.size());
            for (int j = 0; j < fieldVectors.size(); j++) {
                FieldVector curFieldVector = fieldVectors.get(j);
                Types.MinorType mt = curFieldVector.getMinorType();
                if (curFieldVector.isNull(offsetInOneBatch)) {
                    row.put(null);
                    continue;
                }

                final String currentType = schema.get(j).getType();
                switch (currentType) {
                    case "NULL_TYPE":
                        row.put(null);
                        break;
                    case "BOOLEAN":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.BIT),
                                typeMismatchMessage(currentType, mt));
                        BitVector bitVector = (BitVector) curFieldVector;
                        int bit = bitVector.get(offsetInOneBatch);
                        row.put(bit != 0);
                        break;
                    case "TINYINT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.TINYINT),
                                typeMismatchMessage(currentType, mt));
                        TinyIntVector tinyIntVector = (TinyIntVector) curFieldVector;
                        row.put(tinyIntVector.get(offsetInOneBatch));
                        break;
                    case "SMALLINT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.SMALLINT),
                                typeMismatchMessage(currentType, mt));
                        SmallIntVector smallIntVector = (SmallIntVector) curFieldVector;
                        row.put(smallIntVector.get(offsetInOneBatch));
                        break;
                    case "INT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.INT),
                                typeMismatchMessage(currentType, mt));
                        IntVector intVector = (IntVector) curFieldVector;
                        row.put(intVector.get(offsetInOneBatch));
                        break;
                    case "BIGINT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.BIGINT),
                                typeMismatchMessage(currentType, mt));
                        BigIntVector bigIntVector = (BigIntVector) curFieldVector;
                        row.put(bigIntVector.get(offsetInOneBatch));
                        break;
                    case "FLOAT":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.FLOAT4),
                                typeMismatchMessage(currentType, mt));
                        Float4Vector float4Vector = (Float4Vector) curFieldVector;
                        row.put(float4Vector.get(offsetInOneBatch));
                        break;
                    case "TIME":
                    case "DOUBLE":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.FLOAT8),
                                typeMismatchMessage(currentType, mt));
                        Float8Vector float8Vector = (Float8Vector) curFieldVector;
                        row.put(float8Vector.get(offsetInOneBatch));
                        break;
                    case "BINARY":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARBINARY),
                                typeMismatchMessage(currentType, mt));
                        VarBinaryVector varBinaryVector = (VarBinaryVector) curFieldVector;
                        row.put(varBinaryVector.get(offsetInOneBatch));
                        break;
                    case "DECIMAL":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARCHAR),
                                typeMismatchMessage(currentType, mt));
                        VarCharVector varCharVectorForDecimal = (VarCharVector) curFieldVector;
                        String decimalValue = new String(varCharVectorForDecimal.get(offsetInOneBatch));
                        Decimal decimal = new Decimal();
                        try {
                            decimal.set(new scala.math.BigDecimal(new BigDecimal(decimalValue)));
                        } catch (NumberFormatException e) {
                            String errMsg = "Decimal response result '" + decimalValue + "' is illegal.";
                            logger.error(errMsg, e);
                            throw new DorisException(errMsg);
                        }
                        row.put(decimal);
                        break;
                    case "DECIMALV2":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.DECIMAL),
                                typeMismatchMessage(currentType, mt));
                        DecimalVector decimalVector = (DecimalVector) curFieldVector;
                        Decimal decimalV2 = Decimal.apply(decimalVector.getObject(offsetInOneBatch));
                        row.put(decimalV2);
                        break;
                    case "DATE":
                    case "DATETIME":
                    case "LARGEINT":
                    case "CHAR":
                    case "VARCHAR":
                        Preconditions.checkArgument(mt.equals(Types.MinorType.VARCHAR),
                                typeMismatchMessage(currentType, mt));
                        VarCharVector varCharVector = (VarCharVector) curFieldVector;
                        String value = new String(varCharVector.get(offsetInOneBatch));
                        row.put(value);
                        break;
                    default:
                        String errMsg = "Unsupported type " + schema.get(j).getType();
                        logger.error(errMsg);
                        throw new DorisException(errMsg);
                }
            }
            offsetInOneBatch++;
            return row.getCols();
        } catch (Exception e) {
            close();
            throw e;
        }
    }

    private String typeMismatchMessage(final String sparkType, final Types.MinorType arrowType) {
        final String messageTemplate = "Spark type is %1$s, but arrow type is %2$s.";
        return String.format(messageTemplate, sparkType, arrowType.name());
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
}
