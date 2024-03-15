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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

import java.util.List;

// Describe the partition key values in create table or add partition clause
public class PartitionKeyDesc {
    public static final PartitionKeyDesc MAX_VALUE = new PartitionKeyDesc();
    // UNPARTITIONED table not have real keyDesc,therefore, provide this DUMMY_KEY_DESC
    public static final PartitionKeyDesc DUMMY_KEY_DESC = new PartitionKeyDesc();

    public enum PartitionKeyValueType {
        INVALID,
        LESS_THAN,
        FIXED,
        IN
    }

    private List<PartitionValue> lowerValues;
    private List<PartitionValue> upperValues;
    private List<List<PartitionValue>> inValues;
    private PartitionKeyValueType partitionKeyValueType;

    private Long timeInterval;
    private String timeType;

    public static PartitionKeyDesc createMaxKeyDesc() {
        return MAX_VALUE;
    }

    private PartitionKeyDesc() {
        partitionKeyValueType = PartitionKeyValueType.LESS_THAN; // LESS_THAN is default type.
    }

    public static PartitionKeyDesc createLessThan(List<PartitionValue> upperValues) {
        PartitionKeyDesc desc = new PartitionKeyDesc();
        desc.upperValues = upperValues;
        desc.partitionKeyValueType = PartitionKeyValueType.LESS_THAN;
        return desc;
    }

    public static PartitionKeyDesc createIn(List<List<PartitionValue>> inValues) {
        PartitionKeyDesc desc = new PartitionKeyDesc();
        desc.inValues = inValues;
        desc.partitionKeyValueType = PartitionKeyValueType.IN;
        return desc;
    }

    public static PartitionKeyDesc createFixed(List<PartitionValue> lowerValues, List<PartitionValue> upperValues) {
        PartitionKeyDesc desc = new PartitionKeyDesc();
        desc.lowerValues = lowerValues;
        desc.upperValues = upperValues;
        desc.partitionKeyValueType = PartitionKeyValueType.FIXED;
        return desc;
    }

    public static PartitionKeyDesc createMultiFixed(
            List<PartitionValue> lowerValues,
            List<PartitionValue> upperValues,
            Long timeInterval,
            String timeType) {
        PartitionKeyDesc desc = new PartitionKeyDesc();
        desc.lowerValues = lowerValues;
        desc.upperValues = upperValues;
        desc.timeInterval = timeInterval;
        desc.timeType = timeType;
        desc.partitionKeyValueType = PartitionKeyValueType.FIXED;
        return desc;
    }

    public static PartitionKeyDesc createMultiFixed(
            List<PartitionValue> lowerValues,
            List<PartitionValue> upperValues,
            Long interval) {
        PartitionKeyDesc desc = new PartitionKeyDesc();
        desc.lowerValues = lowerValues;
        desc.upperValues = upperValues;
        desc.timeInterval = interval;
        desc.timeType = "";
        desc.partitionKeyValueType = PartitionKeyValueType.FIXED;
        return desc;
    }

    public Long getTimeInterval() {
        return timeInterval;
    }

    public String getTimeType() {
        return timeType;
    }

    public List<PartitionValue> getLowerValues() {
        return lowerValues;
    }

    public List<PartitionValue> getUpperValues() {
        return upperValues;
    }

    public List<List<PartitionValue>> getInValues() {
        return inValues;
    }

    public boolean isMax() {
        return this == MAX_VALUE;
    }

    public boolean isDummy() {
        return this == DUMMY_KEY_DESC;
    }

    public boolean hasLowerValues() {
        return lowerValues != null;
    }

    public boolean hasUpperValues() {
        return upperValues != null;
    }

    public PartitionKeyValueType getPartitionType() {
        return partitionKeyValueType;
    }

    public void analyze(int partColNum) throws AnalysisException {
        if (isDummy()) {
            return;
        }
        if (!isMax()) {
            if ((upperValues != null && (upperValues.isEmpty() || upperValues.size() > partColNum))) {
                throw new AnalysisException("Partition values number is more than partition column number: " + toSql());
            }
        }

        // Currently, we do not support MAXVALUE in multi partition range values. eg: ("100", "200", MAXVALUE);
        // Because we still don’t support expressing such values on the BE side.
        // Maybe support later.
        // But we can support MAXVALUE in single partition values.
        if (lowerValues != null && lowerValues.size() > 1) {
            for (PartitionValue lowerVal : lowerValues) {
                if (lowerVal.isMax()) {
                    throw new AnalysisException("Not support MAXVALUE in multi partition range values.");
                }
            }
        }

        if (upperValues != null && upperValues.size() > 1) {
            for (PartitionValue upperVal : upperValues) {
                if (upperVal.isMax()) {
                    throw new AnalysisException("Not support MAXVALUE in multi partition range values.");
                }
            }
        }
    }

    // returns:
    // 1: MAXVALUE
    // 2: ("100", "200", MAXVALUE)
    // 3: [("100", "200"), ("300", "200"))
    public String toSql() {
        if (isMax()) {
            return "MAXVALUE";
        }

        if (partitionKeyValueType == PartitionKeyValueType.LESS_THAN) {
            return getPartitionValuesStr(upperValues);
        } else if (partitionKeyValueType == PartitionKeyValueType.FIXED) {
            StringBuilder sb = new StringBuilder("[");
            sb.append(getPartitionValuesStr(lowerValues)).append(", ").append(getPartitionValuesStr(upperValues));
            sb.append(")");
            return sb.toString();
        } else if (partitionKeyValueType == PartitionKeyValueType.IN) {
            StringBuilder sb = new StringBuilder("(");
            for (int i = 0; i < inValues.size(); i++) {
                String valueStr = getPartitionValuesStr(inValues.get(i));
                if (inValues.get(i).size() == 1) {
                    valueStr = valueStr.substring(1, valueStr.length() - 1);
                }
                sb.append(valueStr);
                if (i < inValues.size() - 1) {
                    sb.append(",");
                }
            }
            sb.append(")");
            return sb.toString();
        } else {
            return "INVALID";
        }
    }

    private String getPartitionValuesStr(List<PartitionValue> values) {
        StringBuilder sb = new StringBuilder("(");
        Joiner.on(", ").appendTo(sb, Lists.transform(values, new Function<PartitionValue, String>() {
            @Override
            public String apply(PartitionValue v) {
                if (v.isMax()) {
                    return v.getStringValue();
                } else {
                    return "'" + v.getStringValue() + "'";
                }
            }
        })).append(")");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PartitionKeyDesc that = (PartitionKeyDesc) o;
        return Objects.equal(lowerValues, that.lowerValues)
                && Objects.equal(upperValues, that.upperValues)
                && Objects.equal(inValues, that.inValues)
                && partitionKeyValueType == that.partitionKeyValueType
                && Objects.equal(timeInterval, that.timeInterval)
                && Objects.equal(timeType, that.timeType);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(lowerValues, upperValues, inValues, partitionKeyValueType, timeInterval, timeType);
    }
}
