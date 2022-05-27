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

package org.apache.doris.qe;

import java.util.List;

public abstract class AbstractResultSet implements ResultSet {
    protected ResultSetMetaData metaData;
    protected List<List<String>> resultRows;
    int rowIdx;

    public AbstractResultSet() {
    }

    public AbstractResultSet(ResultSetMetaData metaData, List<List<String>> resultRows) {
        this.metaData = metaData;
        this.resultRows = resultRows;
        rowIdx = -1;
    }

    @Override
    public boolean next() {
        if (rowIdx + 1 >= resultRows.size()) {
            return false;
        }
        rowIdx++;
        return true;
    }

    @Override
    public List<List<String>> getResultRows() {
        return resultRows;
    }

    @Override
    public ResultSetMetaData getMetaData() {
        return metaData;
    }

    @Override
    public String getString(int col) {
        return resultRows.get(rowIdx).get(col);
    }

    @Override
    public byte getByte(int col) {
        return Byte.valueOf(getString(col));
    }

    @Override
    public int getInt(int col) {
        return Integer.valueOf(getString(col));
    }

    @Override
    public long getLong(int col) {
        return Long.valueOf(getString(col));
    }

    @Override
    public short getShort(int col) {
        return Short.valueOf(getString(col));
    }

}
