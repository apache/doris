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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

// 用于表达在创建表、创建rollup中key range partition中所使用的key信息
// 在知道具体列信息后，可以转成PartitionKey，仅仅在语法解析中短暂的有意义
public class PartitionKeyDesc implements Writable {
    public enum PartitionRangeType {
        INVALID,
        LESS_THAN,
        FIXED
    }

    private List<PartitionValue> lowerValues;
    private List<PartitionValue> upperValues;
    private PartitionRangeType partitionType;
    public static PartitionKeyDesc createMaxKeyDesc() {
        return new PartitionKeyDesc();
    }

    public PartitionKeyDesc() {
        partitionType = PartitionRangeType.LESS_THAN; //LESS_THAN is default type.
    }

    // used by SQL parser
    public PartitionKeyDesc(List<PartitionValue> upperValues) {
        this.upperValues = upperValues;
        partitionType = PartitionRangeType.LESS_THAN;
    }

    public PartitionKeyDesc(List<PartitionValue> lowerValues, List<PartitionValue> upperValues) {
        this.lowerValues = lowerValues;
        this.upperValues = upperValues;
        partitionType = PartitionRangeType.FIXED;
    }

    public void setLowerValues(List<PartitionValue> lowerValues) {
        this.lowerValues = lowerValues;
    }

    public List<PartitionValue> getLowerValues() {
        return lowerValues;
    }

    public List<PartitionValue> getUpperValues() {
        return upperValues;
    }

    public boolean isMax() {
        return lowerValues == null && upperValues == null;
    }

    public boolean hasLowerValues() {
        return lowerValues != null;
    }

    public boolean hasUpperValues() {
        return upperValues != null;
    }

    public PartitionRangeType getPartitionType () {
        return partitionType;
    }

    public String toSql() {
        if (this.isMax()) {
            return "MAXVALUE";
        }

        if (upperValues != null) {
            StringBuilder sb = new StringBuilder("(");
            Joiner.on(", ").appendTo(sb, Lists.transform(upperValues, new Function<PartitionValue, String>() {
                @Override
                public String apply(PartitionValue v) {
                    return "'" + v.getStringValue() + "'";
                }
            })).append(")");
            return sb.toString();
        } else {
            return "()";
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int count = lowerValues == null ? 0 : lowerValues.size();
        out.writeInt(count);
        if (count > 0) {
            for (PartitionValue value : lowerValues) {
                Text.writeString(out, value.getStringValue());
            }
        }

        count = upperValues == null ? 0: upperValues.size();
        out.writeInt(count);
        if (count > 0) {
            for (PartitionValue value : upperValues) {
                Text.writeString(out, value.getStringValue());
            }
        }

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            String v = Text.readString(in);
            if (v.equals("MAXVALUE")) {
                lowerValues.add(new PartitionValue());
            } else {
                lowerValues.add(new PartitionValue(v));
            }
        }

        count = in.readInt();
        for (int i = 0; i < count; i++) {
            String v = Text.readString(in);
            if (v.equals("MAXVALUE")) {
                upperValues.add(new PartitionValue());
            } else {
                upperValues.add(new PartitionValue(v));
            }
        }
    }
}
