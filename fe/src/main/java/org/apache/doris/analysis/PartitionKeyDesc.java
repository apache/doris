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
    // public static final PartitionKeyDesc MAX_VALUE = new PartitionKeyDesc();
    // lower values only be used for restore
    private List<String> lowerValues;
    private List<String> upperValues;

    public static PartitionKeyDesc createMaxKeyDesc() {
        return new PartitionKeyDesc();
    }

    public PartitionKeyDesc() {
        lowerValues = Lists.newArrayList();
        upperValues = Lists.newArrayList();
    }

    // used by SQL parser
    public PartitionKeyDesc(List<String> upperValues) {
        this.lowerValues = Lists.newArrayList();
        this.upperValues = upperValues;
    }

    public PartitionKeyDesc(List<String> lowerValues, List<String> upperValues) {
        this.lowerValues = lowerValues;
        this.upperValues = upperValues;
    }

    public void setLowerValues(List<String> lowerValues) {
        this.lowerValues = lowerValues;
    }

    public List<String> getLowerValues() {
        return lowerValues;
    }

    public List<String> getUpperValues() {
        return upperValues;
    }

    public boolean isMax() {
        return upperValues.isEmpty();
    }

    public String toSql() {
        if (this.isMax()) {
            return "MAXVALUE";
        }
        StringBuilder sb = new StringBuilder("(");
        Joiner.on(", ").appendTo(sb, Lists.transform(upperValues, new Function<String, String>() {
            @Override
            public String apply(String s) {
                return "'" + s + "'";
            }
        })).append(")");
        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int count = lowerValues.size();
        out.writeInt(count);
        for (String value : lowerValues) {
            Text.writeString(out, value);
        }

        count = upperValues.size();
        out.writeInt(count);
        for (String value : upperValues) {
            Text.writeString(out, value);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int count = in.readInt();
        for (int i = 0; i < count; i++) {
            lowerValues.add(Text.readString(in));
        }

        count = in.readInt();
        for (int i = 0; i < count; i++) {
            upperValues.add(Text.readString(in));
        }
    }
}
