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

/**
 * Combination of limit and offset expressions.
 */
public class LimitElement {
    public static LimitElement NO_LIMIT = new LimitElement();
    
    /////////////////////////////////////////
    // BEGIN: Members that need to be reset()
    
    private long limit;
    private long offset;

    // END: Members that need to be reset()
    /////////////////////////////////////////
    
    public LimitElement() {
        limit = -1;
        offset = 0;
    }

    public LimitElement(long limit) {
        this.limit = limit;
        offset = 0;
    }

    public LimitElement(long offset, long limit) {
        this.offset = offset;
        this.limit = limit;
    }

    protected LimitElement(LimitElement other) {
        limit = other.limit;
        offset = other.offset;
    }

    @Override
    public LimitElement clone() {
        return new LimitElement(this);
    }

    /**
     * Returns the integer limit, evaluated from the limit expression. Must call analyze()
     * first. If no limit was set, then -1 is returned.
     */
    public long getLimit() {
        return limit;
    }

    public boolean hasLimit() {
        return limit != -1;
    }

    /**
     * Returns the integer offset, evaluated from the offset expression. Must call
     * analyze() first. If no offsetExpr exists, then 0 (the default offset) is returned.
     */
    public long getOffset() {
        return offset;
    }

    public boolean hasOffset() {
        return offset != 0;
    }

    public String toSql() {
        if (limit == -1) {
            return "";
        }
        StringBuilder sb = new StringBuilder(" LIMIT ");
        if (offset != 0) {
            sb.append(offset + ", ");
        }
        sb.append("" + limit);
        return sb.toString();
    }

    public void analyze(Analyzer analyzer) {
        if (limit == 0) analyzer.setHasEmptyResultSet();
    }
    
    public void reset() {
    }
}
