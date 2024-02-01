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

package org.apache.doris.nereids.trees;

import java.util.Objects;

/**
 * To represent following stmt:
 *      TABLESAMPLE (10 PERCENT)
 *      TABLESAMPLE (100 ROWS)
 *      TABLESAMPLE (10 PERCENT) REPEATABLE 2
 *      TABLESAMPLE (100 ROWS) REPEATABLE 2
 */
public class TableSample {

    public final long sampleValue;

    public final boolean isPercent;

    public final long seek;

    public TableSample(long sampleValue, boolean isPercent, long seek) {
        this.sampleValue = sampleValue;
        this.isPercent = isPercent;
        this.seek = seek;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof TableSample)) {
            return false;
        }
        TableSample tableSample = (TableSample) obj;
        return this.sampleValue == tableSample.sampleValue
                && this.isPercent == tableSample.isPercent
                && this.seek == tableSample.seek;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sampleValue, isPercent, seek);
    }
}
