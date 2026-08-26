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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/LimitElement.java
// and modified by Doris

package org.apache.doris.analysis;

import java.util.List;

/**
 * Combination of limit and offset expressions.
 */
public class LimitElement {
    private final long limit;
    private final long offset;

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

    /**
     * Returns the window of {@code rows} selected by this offset and limit.
     *
     * <p>Both values reach here as user supplied 64-bit integers, so the range is computed in
     * long and saturated at {@code rows.size()} before it is narrowed to int. Narrowing first
     * wraps: an offset or limit above {@link Integer#MAX_VALUE} can truncate to zero and
     * silently return an empty window, or truncate to a negative index and make
     * {@link List#subList} throw {@link IndexOutOfBoundsException}.
     *
     * <p>When no limit is set, the window runs from the offset to the end of {@code rows}.
     */
    public <T> List<T> applyTo(List<T> rows) {
        int size = rows.size();
        long begin = Math.min(Math.max(offset, 0L), size);
        long end = size;
        if (hasLimit()) {
            end = begin + limit;
            // A negative sum means the long addition itself overflowed.
            if (end < 0 || end > size) {
                end = size;
            }
        }
        return rows.subList((int) begin, (int) end);
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

    public String toDigest() {
        if (limit == -1) {
            return "";
        }
        StringBuilder sb = new StringBuilder(" LIMIT ");
        if (offset != 0) {
            sb.append(offset + "?, ");
        }
        sb.append("" + " ? ");
        return sb.toString();
    }
}
