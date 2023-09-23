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

import org.apache.doris.common.AnalysisException;

/*
 * To represent following stmt:
 *      TABLESAMPLE (10 PERCENT)
 *      TABLESAMPLE (100 ROWS)
 *      TABLESAMPLE (10 PERCENT) REPEATABLE 2
 *      TABLESAMPLE (100 ROWS) REPEATABLE 2
 *
 * references:
 *      https://simplebiinsights.com/sql-server-tablesample-retrieving-random-data-from-sql-server/
 *      https://sqlrambling.net/2018/01/24/tablesample-basic-examples/
 */
public class TableSample implements ParseNode {

    private final Long sampleValue;
    private final boolean isPercent;
    private Long seek = -1L;

    public TableSample(boolean isPercent, Long sampleValue) {
        this.sampleValue = sampleValue;
        this.isPercent = isPercent;
    }

    public TableSample(boolean isPercent, Long sampleValue, Long seek) {
        this.sampleValue = sampleValue;
        this.isPercent = isPercent;
        this.seek = seek;
    }

    public TableSample(TableSample other) {
        this.sampleValue = other.sampleValue;
        this.isPercent = other.isPercent;
        this.seek = other.seek;
    }

    public Long getSampleValue() {
        return sampleValue;
    }

    public boolean isPercent() {
        return isPercent;
    }

    public Long getSeek() {
        return seek;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (sampleValue <= 0 || (isPercent && sampleValue > 100)) {
            throw new AnalysisException("table sample value must be greater than 0, percent need less than 100.");
        }
    }

    @Override
    public String toSql() {
        if (sampleValue == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("TABLESAMPLE ( ");
        sb.append(sampleValue);
        if (isPercent) {
            sb.append(" PERCENT ");
        } else {
            sb.append(" ROWS ");
        }
        sb.append(")");
        if (seek != 0) {
            sb.append(" REPEATABLE ");
            sb.append(seek);
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
