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

package org.apache.doris.catalog;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;

import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

/**
 * Statistics for a single column.
 */
public class ColumnStats {
    private static final Logger LOG = LogManager.getLogger(ColumnStats.class);

    @SerializedName(value = "avgSerializedSize")
    private float avgSerializedSize;  // in bytes; includes serialization overhead
    @SerializedName(value = "maxSize")
    private long  maxSize;  // in bytes
    @SerializedName(value = "numDistinctValues")
    private long  numDistinctValues;
    @SerializedName(value = "numNulls")
    private long  numNulls;

    /**
     * For fixed-length type (those which don't need additional storage besides
     * the slot they occupy), sets avgSerializedSize and maxSize to their slot size.
     */
    public ColumnStats() {
        avgSerializedSize = -1;
        maxSize = -1;
        numDistinctValues = -1;
        numNulls = -1;
    }

    public ColumnStats(ColumnStats other) {
        avgSerializedSize = other.avgSerializedSize;
        maxSize = other.maxSize;
        numDistinctValues = other.numDistinctValues;
        numNulls = other.numNulls;
    }

    public long getNumDistinctValues() {
        return numDistinctValues;
    }

    public void setNumDistinctValues(long numDistinctValues) {
        this.numDistinctValues = numDistinctValues;
    }

    public float getAvgSerializedSize() {
        return avgSerializedSize;
    }

    public void setAvgSerializedSize(float avgSize) {
        this.avgSerializedSize = avgSize;
    }

    public long getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(long maxSize) {
        this.maxSize = maxSize;
    }

    public boolean hasNulls() {
        return numNulls > 0;
    }

    public long getNumNulls() {
        return numNulls;
    }

    public void setNumNulls(long numNulls) {
        this.numNulls = numNulls;
    }

    public boolean hasAvgSerializedSize() {
        return avgSerializedSize >= 0;
    }

    public boolean hasMaxSize() {
        return maxSize >= 0;
    }

    public boolean hasNumDistinctValues() {
        return numDistinctValues >= 0;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this.getClass()).add("avgSerializedSize",
          avgSerializedSize).add("maxSize", maxSize).add("numDistinct", numDistinctValues).add(
          "numNulls", numNulls).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hash(avgSerializedSize, maxSize, numDistinctValues, numNulls);
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ColumnStats)) {
            return false;
        }

        ColumnStats stats = (ColumnStats) obj;
        return (numDistinctValues == stats.numDistinctValues)
                && (avgSerializedSize == stats.avgSerializedSize)
                && (maxSize == stats.maxSize)
                && (numNulls == stats.numNulls);
    }

    /**
     * For fixed-length type (those which don't need additional storage besides
     * the slot they occupy), sets avgSerializedSize and maxSize to their slot size.
     */
    public ColumnStats(PrimitiveType colType) {
        avgSerializedSize = -1;
        maxSize = -1;
        numDistinctValues = -1;
        numNulls = -1;
        if (colType.isNumericType() || colType.isDateType()) {
            avgSerializedSize = colType.getSlotSize();
            maxSize = colType.getSlotSize();
        }
    }

    /**
     * Creates ColumnStats from the given expr. Sets numDistinctValues and if the expr
     * is a SlotRef also numNulls.
     */
    public static ColumnStats fromExpr(Expr expr) {
        Preconditions.checkNotNull(expr);
        Preconditions.checkState(expr.getType().isValid());
        ColumnStats stats = new ColumnStats(expr.getType().getPrimitiveType());
        stats.setNumDistinctValues(expr.getNumDistinctValues());
        SlotRef slotRef = expr.unwrapSlotRef();
        if (slotRef == null) {
            return stats;
        }
        ColumnStats slotStats = slotRef.getDesc().getStats();
        if (slotStats == null) {
            return stats;
        }
        stats.numNulls = slotStats.getNumNulls();
        stats.avgSerializedSize = slotStats.getAvgSerializedSize();
        stats.maxSize = slotStats.getMaxSize();
        return stats;
    }
}
