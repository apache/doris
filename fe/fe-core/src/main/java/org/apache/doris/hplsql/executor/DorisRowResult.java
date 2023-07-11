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

package org.apache.doris.hplsql.executor;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Type;
import org.apache.doris.hplsql.exception.QueryException;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.statistics.util.InternalQueryBuffer;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.util.List;

public class DorisRowResult implements RowResult {

    private Coordinator coord;

    private List<String> columnNames;

    private List<Type> dorisTypes;

    private RowBatch batch;

    private int index;

    private boolean isLazyLoading;

    private boolean eof;

    private Object[] current;

    public DorisRowResult(Coordinator coord, List<String> columnNames, List<Type> dorisTypes) {
        this.coord = coord;
        this.columnNames = columnNames;
        this.dorisTypes = dorisTypes;
        this.current = new Object[columnNames.size()];
        this.isLazyLoading = false;
        this.eof = false;
    }

    @Override
    public boolean next() {
        if (eof) {
            return false;
        }
        try {
            if (batch == null || batch.getBatch() == null
                    || index == batch.getBatch().getRowsSize() - 1) {
                batch = coord.getNext();
                index = 0;
                if (batch.isEos()) {
                    eof = true;
                    return false;
                }
            } else {
                ++index;
            }
            isLazyLoading = true;
        } catch (Exception e) {
            throw new QueryException(e);
        }
        return true;
    }

    @Override
    public void close() {

    }

    @Override
    public <T> T get(int columnIndex, Class<T> type) {
        if (isLazyLoading) {
            convertToJavaType(batch.getBatch().getRows().get(index));
            isLazyLoading = false;
        }
        if (current[columnIndex] == null) {
            return null;
        }
        if (type.isInstance(current[columnIndex])) {
            return (T) current[columnIndex];
        } else {
            if (current[columnIndex] instanceof Number) {
                if (type.equals(Long.class)) {
                    return type.cast(((Number) current[columnIndex]).longValue());
                } else if (type.equals(Integer.class)) {
                    return type.cast(((Number) current[columnIndex]).intValue());
                } else if (type.equals(Short.class)) {
                    return type.cast(((Number) current[columnIndex]).shortValue());
                } else if (type.equals(Byte.class)) {
                    return type.cast(((Number) current[columnIndex]).byteValue());
                }
            }
            throw new ClassCastException(current[columnIndex].getClass() + " cannot be casted to " + type);
        }
    }

    @Override
    public ByteBuffer getMysqlRow() {
        return batch.getBatch().getRows().get(index);
    }

    private void convertToJavaType(ByteBuffer buffer) {
        InternalQueryBuffer queryBuffer = new InternalQueryBuffer(buffer.slice());
        for (int i = 0; i < columnNames.size(); i++) {
            String value = queryBuffer.readStringWithLength();
            current[i] = toJavaType(dorisTypes.get(i).getPrimitiveType(), value);
        }
    }

    // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-type-conversions.html
    private Object toJavaType(PrimitiveType type, String value) {
        if (value == null) {
            return null;
        }
        switch (type) {
            case BOOLEAN:
                return Boolean.valueOf(value);
            case TINYINT:
            case SMALLINT:
            case INT:
                return Integer.valueOf(value);
            case BIGINT:
                return Long.valueOf(value);
            case FLOAT:
                return Float.valueOf(value);
            case DOUBLE:
                return Double.valueOf(value);
            case TIME:
            case TIMEV2:
                return Time.valueOf(value);
            case DATE:
            case DATEV2:
                return Date.valueOf(value);
            case DATETIME:
                if (type.isTimeType()) {
                    return Time.valueOf(value);
                }
                return new DateTimeLiteral(value).toJavaDateType();
            case DATETIMEV2:
                if (type.isTimeType()) {
                    return Time.valueOf(value);
                }
                return new DateTimeV2Literal(value).toJavaDateType();
            case DECIMALV2:
            case DECIMAL32:
            case DECIMAL64:
            case DECIMAL128:
                return new BigDecimal(value);
            default:
                return value;
        }
    }
}
