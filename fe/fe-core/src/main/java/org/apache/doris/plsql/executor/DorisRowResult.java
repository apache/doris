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

package org.apache.doris.plsql.executor;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.plsql.exception.QueryException;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.statistics.util.InternalQueryBuffer;

import java.nio.ByteBuffer;
import java.util.List;

// only running form mysql client
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
        this.current = columnNames != null ? new Object[columnNames.size()] : null;
        this.isLazyLoading = false;
        this.eof = false;
    }

    @Override
    public boolean next() {
        if (eof || coord == null) {
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
        // TODO
    }

    @Override
    public <T> T get(int columnIndex, Class<T> type) throws AnalysisException {
        if (isLazyLoading) {
            readFromDorisType(batch.getBatch().getRows().get(index));
            isLazyLoading = false;
        }
        if (current[columnIndex] == null) {
            return null;
        }
        current[columnIndex] = ((Literal) current[columnIndex]).getValue();
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
    public Literal get(int columnIndex) throws AnalysisException {
        if (isLazyLoading) {
            readFromDorisType(batch.getBatch().getRows().get(index));
            isLazyLoading = false;
        }
        if (current[columnIndex] == null) {
            return null;
        }
        return (Literal) current[columnIndex];
    }

    @Override
    public ByteBuffer getMysqlRow() {
        return batch.getBatch().getRows().get(index);
    }

    private void readFromDorisType(ByteBuffer buffer) throws AnalysisException {
        InternalQueryBuffer queryBuffer = new InternalQueryBuffer(buffer.slice());
        for (int i = 0; i < columnNames.size(); i++) {
            String value = queryBuffer.readStringWithLength();
            if (value == null) {
                current[i] = Literal.of(null);
            } else {
                current[i] = Literal.fromLegacyLiteral(LiteralExpr.create(value, dorisTypes.get(i)), dorisTypes.get(i));
            }
        }
    }
}
