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

package org.apache.doris.common.util;

import org.apache.doris.thrift.TUnit;

// Counter means indicators field. The counter's name is key, the counter itself is value.
public class Counter {
    private volatile long value;
    private volatile int type;
    private volatile long level;

    public long getValue() {
        return value;
    }

    public void setValue(TUnit type, long value) {
        this.type = type.getValue();
        this.value = value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public TUnit getType() {
        return TUnit.findByValue(type);
    }

    public void setLevel(long level) {
        this.level = level;
    }

    public void setType(TUnit type) {
        this.type = type.getValue();
    }

    public Counter(TUnit type, long value) {
        this.value = value;
        this.type = type.getValue();
        this.level = 2;
    }

    public Counter(TUnit type, long value, long level) {
        this.value = value;
        this.type = type.getValue();
        this.level = level;
    }

    public void addValue(Counter other) {
        if (other == null) {
            return;
        }
        this.value += other.value;
    }

    public void minValue(Counter other) {
        if (other == null) {
            return;
        }
        this.value = Math.min(this.value, other.value);
    }

    public void maxValue(Counter other) {
        if (other == null) {
            return;
        }
        this.value = Math.max(this.value, other.value);
    }

    public void divValue(long div) {
        if (div <= 0) {
            return;
        }
        value /= div;
    }

    public boolean isTimeType() {
        TUnit ttype = TUnit.findByValue(type);
        return ttype == TUnit.TIME_MS || ttype == TUnit.TIME_NS || ttype == TUnit.TIME_S;
    }

    public long getLevel() {
        return this.level;
    }

    public String print() {
        return RuntimeProfile.printCounter(value, getType());
    }

}
